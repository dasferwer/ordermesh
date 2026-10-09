"""Проверить отказ брокера в собственном одноразовом Compose-проекте."""

import argparse
import ipaddress
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from urllib.request import Request, urlopen
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
SERVICES = (
    "orders-db",
    "inventory-db",
    "rabbitmq",
    "migrate",
    "api",
    "outbox-publisher",
    "fulfillment-worker",
    "status-worker",
)


def validate_options(project, subnet, outage_seconds, orders):
    if not re.fullmatch(r"proof-ordermesh-[a-z0-9][a-z0-9-]{0,40}", project):
        raise ValueError("Нужен уникальный проект proof-ordermesh-<имя>")
    network = ipaddress.ip_network(subnet)
    private = [
        ipaddress.ip_network(value) for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
    ]
    if (
        network.version != 4
        or network.prefixlen != 24
        or not any(network.subnet_of(block) for block in private)
    ):
        raise ValueError("Нужна частная IPv4-подсеть /24")
    if not 15 <= outage_seconds <= 300 or not 1 <= orders <= 20:
        raise ValueError("Отказ: 15–300 секунд; число заказов: 1–20")
    return network


def run(command, **kwargs):
    return subprocess.run(command, check=True, timeout=300, **kwargs)


def cleanup(compose, image):
    errors = []
    actions = [
        lambda: compose("logs", "--tail", "40", stdout=sys.stderr, stderr=sys.stderr),
        lambda: compose("down", "-v", "--remove-orphans", stdout=sys.stderr, stderr=sys.stderr),
        lambda: run(["docker", "image", "rm", image], stdout=sys.stderr, stderr=sys.stderr),
    ]
    for action in actions:
        try:
            action()
        except Exception as error:
            errors.append(str(error))
    return errors


def preflight(project, network):
    # Проверки только читают Docker; до их завершения проект не создаётся.
    for resource in ("container", "network", "volume"):
        existing = run(
            [
                "docker",
                resource,
                "ls",
                "-q",
                "--filter",
                f"label=com.docker.compose.project={project}",
            ],
            capture_output=True,
            text=True,
        ).stdout.strip()
        if existing:
            raise ValueError("Имя проекта уже занято; существующие ресурсы не изменяются")
    if run(
        ["docker", "image", "ls", "-q", project + ":runtime"], capture_output=True, text=True
    ).stdout.strip():
        raise ValueError("Тег образа уже занят; выберите новое имя proof-проекта")
    ids = run(["docker", "network", "ls", "-q"], capture_output=True, text=True).stdout.split()
    if ids:
        networks = json.loads(
            run(
                ["docker", "network", "inspect", *ids],
                capture_output=True,
                text=True,
            ).stdout
        )
        for item in networks:
            for config in item.get("IPAM", {}).get("Config", []) or []:
                if config.get("Subnet"):
                    other = ipaddress.ip_network(config["Subnet"])
                    if other.version == network.version and network.overlaps(other):
                        raise ValueError("Подсеть занята; выберите другую частную /24")


def isolated_config(network, key):
    # URL и имя исходного стенда не наследуются из пользовательского окружения.
    env = {k: v for k, v in os.environ.items() if not k.startswith(("ORDERMESH_", "COMPOSE_"))}
    config = json.loads(
        run(
            [
                "docker",
                "compose",
                "--env-file",
                "/dev/null",
                "-f",
                str(ROOT / "docker-compose.yml"),
                "config",
                "--format",
                "json",
            ],
            env=env,
            capture_output=True,
            text=True,
        ).stdout
    )
    result = {
        "services": {},
        "volumes": {},
        "networks": {"default": {"ipam": {"config": [{"subnet": str(network)}]}}},
    }
    for name in SERVICES:
        service = config["services"][name]
        service.pop("container_name", None)
        service.pop("profiles", None)
        for volume in service.get("volumes", []):
            if volume["type"] != "volume":
                raise ValueError("Proof не допускает bind mounts")
            result["volumes"][volume["source"]] = {}
        if "build" in service:
            service["build"]["context"] = str(ROOT)
            service["image"] = "ordermesh-proof:runtime"
        if name == "rabbitmq":
            service.pop("ports", None)
        if name == "api":
            service["ports"] = [{"target": 8000, "published": "0", "host_ip": "127.0.0.1"}]
        if name not in {"orders-db", "inventory-db", "rabbitmq"}:
            service["environment"].update(
                {
                    "ORDERMESH_API_KEYS": json.dumps({"proof": key}),
                    "ORDERMESH_ALLOW_FAILURE_SIMULATION": "true",
                    "ORDERMESH_RETRY_DELAY_MS": "100",
                }
            )
        result["services"][name] = service
    return result


def wait_for(check, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.25)
    raise TimeoutError("Проверяемое состояние не достигнуто")


def verify(compose, base, key, count, outage_seconds):
    def sql(service, database, statement):
        return compose(
            "exec",
            "-T",
            service,
            "psql",
            "-U",
            "ordermesh",
            "-d",
            database,
            "-XAt",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            statement,
            capture_output=True,
            text=True,
        ).stdout.strip()

    def request(method, path, data=None, token=None):
        headers = {"Authorization": "Bearer " + key, "Content-Type": "application/json"}
        if token:
            headers["Idempotency-Key"] = token
        with urlopen(
            Request(
                base + path,
                method=method,
                headers=headers,
                data=json.dumps(data).encode() if data is not None else None,
            ),
            timeout=10,
        ) as response:
            return response.status, json.load(response)

    sku = "PROOF-" + uuid4().hex.upper()
    stock = count + 5
    sql(
        "inventory-db",
        "inventory",
        "INSERT INTO inventory_items "
        "(id,sku,name,available_quantity) VALUES "
        f"('{uuid4()}','{sku}','Proof',{stock})",
    )
    ids, tokens, payloads = [], [], []
    compose("stop", "rabbitmq", stdout=sys.stderr, stderr=sys.stderr)
    stopped = time.monotonic()
    try:
        for number in range(count):
            token = "proof-" + str(uuid4())
            payload = {
                "customer_email": "proof@example.com",
                "items": [{"sku": sku, "quantity": 1}],
                "simulate_transient_failures": 2 if number == 0 else 0,
            }
            status, order = request("POST", "/api/v1/orders", payload, token)
            assert status == 201 and order["status"] == "pending", (status, order)
            ids.append(order["id"])
            tokens.append(token)
            payloads.append(payload)
            status, replay = request("POST", "/api/v1/orders", payload, token)
            assert status == 200 and replay["id"] == order["id"]
        id_list = ",".join("'" + value + "'" for value in ids)
        pending_sql = f"SELECT count(*) FROM orders WHERE id IN ({id_list}) AND status='pending'"
        remaining = max(0, outage_seconds - (time.monotonic() - stopped))
        time.sleep(remaining)
        assert int(sql("orders-db", "orders", pending_sql)) == count
        assert (
            int(
                sql(
                    "orders-db",
                    "orders",
                    "SELECT count(*) FROM outbox_events "
                    f"WHERE payload->>'order_id' IN ({id_list}) AND published_at IS NULL",
                )
            )
            == count
        )
        assert (
            int(
                sql(
                    "inventory-db",
                    "inventory",
                    f"SELECT count(*) FROM reservations WHERE order_id IN ({id_list})",
                )
            )
            == 0
        )
        assert (
            int(
                sql(
                    "inventory-db",
                    "inventory",
                    f"SELECT available_quantity FROM inventory_items WHERE sku='{sku}'",
                )
            )
            == stock
        )
        actual_outage = time.monotonic() - stopped
    finally:
        compose("start", "rabbitmq", stdout=sys.stderr, stderr=sys.stderr)
    resumed = time.monotonic()
    wait_for(
        lambda: (
            int(
                sql(
                    "orders-db",
                    "orders",
                    f"SELECT count(*) FROM orders WHERE id IN ({id_list}) AND status='fulfilled'",
                )
            )
            == count
        )
    )
    drain_seconds = time.monotonic() - resumed

    def assert_invariants():
        assert (
            int(
                sql(
                    "inventory-db",
                    "inventory",
                    f"SELECT available_quantity FROM inventory_items WHERE sku='{sku}'",
                )
            )
            == stock - count
        )
        assert (
            int(
                sql(
                    "inventory-db",
                    "inventory",
                    f"SELECT count(*) FROM reservations WHERE order_id IN ({id_list}) "
                    "AND status='reserved'",
                )
            )
            == count
        )
        assert (
            int(
                sql(
                    "inventory-db",
                    "inventory",
                    f"SELECT count(*) FROM reservation_items WHERE sku='{sku}' AND quantity=1",
                )
            )
            == count
        )
        for service, database in (("orders-db", "orders"), ("inventory-db", "inventory")):
            assert (
                int(
                    sql(
                        service,
                        database,
                        "SELECT count(*) FROM outbox_events "
                        f"WHERE payload->>'order_id' IN ({id_list}) AND published_at IS NULL",
                    )
                )
                == 0
            )

    assert_invariants()
    event = sql(
        "orders-db", "orders", f"SELECT id FROM outbox_events WHERE payload->>'order_id'='{ids[0]}'"
    )
    consumer_logs = compose(
        "logs", "--no-color", "fulfillment-worker", capture_output=True, text=True
    ).stdout
    observed_attempts = [
        int(value)
        for value in re.findall(r"event_id=" + re.escape(event) + r" attempt=(\d+)", consumer_logs)
    ]
    assert observed_attempts == [0, 1, 2], observed_attempts
    compose(
        "exec",
        "-T",
        "api",
        "python",
        "-m",
        "ordermesh.replay",
        "orders",
        event,
        "--actor",
        "proof",
        "--reason",
        "Проверка дубликата после отказа брокера",
        stdout=sys.stderr,
        stderr=sys.stderr,
    )
    wait_for(
        lambda: (
            int(
                sql("orders-db", "orders", f"SELECT attempts FROM outbox_events WHERE id='{event}'")
            )
            >= 2
        )
    )
    # Барьер consumer: новое событие с тем же SKU проходит после дубликата в очереди.
    barrier_token = "proof-" + str(uuid4())
    status, barrier = request("POST", "/api/v1/orders", payloads[-1], barrier_token)
    assert status == 201
    wait_for(lambda: request("GET", "/api/v1/orders/" + barrier["id"])[1]["status"] == "fulfilled")
    assert (
        int(
            sql(
                "inventory-db",
                "inventory",
                f"SELECT available_quantity FROM inventory_items WHERE sku='{sku}'",
            )
        )
        == stock - count - 1
    )
    assert (
        int(
            sql(
                "inventory-db",
                "inventory",
                f"SELECT count(*) FROM reservations WHERE order_id IN ({id_list})",
            )
        )
        == count
    )
    return {
        "orders": count,
        "outage_seconds": round(actual_outage, 3),
        "drain_seconds": round(drain_seconds, 3),
        "pending_during_outage": count,
        "reservations": count,
        "transient_retries": len(observed_attempts) - 1,
        "observed_consumer_attempts": observed_attempts,
        "replayed_event": event,
        "stock_after_barrier": stock - count - 1,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--subnet", default="10.242.4.0/24")
    parser.add_argument("--outage-seconds", type=int, default=30)
    parser.add_argument("--orders", type=int, default=10)
    args = parser.parse_args()
    network = validate_options(args.project, args.subnet, args.outage_seconds, args.orders)
    preflight(args.project, network)
    key = "proof-" + uuid4().hex
    with tempfile.TemporaryDirectory(prefix="ordermesh-proof-") as directory:
        path = Path(directory) / "compose.json"
        config = isolated_config(network, key)
        # Уникальный image tag позволяет убрать только собственный результат сборки.
        image = args.project + ":runtime"
        for service in config["services"].values():
            if "build" in service:
                service["image"] = image
        path.write_text(json.dumps(config))

        def compose(*commands, **kwargs):
            return run(
                ["docker", "compose", "-f", str(path), "-p", args.project, *commands], **kwargs
            )

        try:
            compose(
                "up",
                "--build",
                "-d",
                "--wait",
                "--wait-timeout",
                "180",
                "api",
                "outbox-publisher",
                "fulfillment-worker",
                "status-worker",
                stdout=sys.stderr,
                stderr=sys.stderr,
            )
            address = compose("port", "api", "8000", capture_output=True, text=True).stdout.strip()
            receipt = verify(compose, "http://" + address, key, args.orders, args.outage_seconds)
            print(
                json.dumps(
                    {"project": args.project, "subnet": str(network), **receipt}, ensure_ascii=False
                )
            )
        finally:
            original_error = sys.exc_info()[0] is not None
            errors = cleanup(compose, image)
            if errors:
                print("Ошибки уборки proof: " + "; ".join(errors), file=sys.stderr)
                if not original_error:
                    raise RuntimeError("Уборка proof не завершена")


if __name__ == "__main__":
    main()
