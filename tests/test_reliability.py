from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace
from uuid import UUID, uuid4

import pika
import pytest
from sqlalchemy import func, select

from ordermesh import services
from ordermesh.broker import DLQ, RESULT_DLQ, connect, declare_topology
from ordermesh.db import InventorySessionLocal, OrdersSessionLocal
from ordermesh.models import (
    InventoryItem,
    InventoryOutboxEvent,
    Order,
    OrderEventReplay,
    OrderOutboxEvent,
    Reservation,
)
from ordermesh.replay import replay_event


def parallel(*operations):
    barrier = Barrier(len(operations))

    def run(operation):
        barrier.wait(timeout=10)
        return operation()

    with ThreadPoolExecutor(max_workers=len(operations)) as pool:
        return list(pool.map(run, operations))


def test_concurrent_checkout(client, order_payload):
    payload, key = order_payload(), str(uuid4())
    results = parallel(
        *[
            lambda: client.post("/api/v1/orders", headers={"Idempotency-Key": key}, json=payload)
            for _ in range(8)
        ]
    )
    assert sorted(r.status_code for r in results) == [200] * 7 + [201]
    order_id = results[0].json()["id"]
    assert {r.json()["id"] for r in results} == {order_id}
    with OrdersSessionLocal() as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(OrderOutboxEvent)
                .where(OrderOutboxEvent.payload["order_id"].astext == order_id)
            )
            == 1
        )


def test_client_isolation(client, order_payload):
    key, payload = str(uuid4()), order_payload()
    a = client.post("/api/v1/orders", headers={"Idempotency-Key": key}, json=payload)
    other = {"Authorization": "Bearer other-client-key-long", "Idempotency-Key": key}
    b = client.post("/api/v1/orders", headers=other, json=payload)
    assert a.status_code == b.status_code == 201
    assert a.json()["id"] != b.json()["id"]
    assert client.get("/api/v1/orders/" + a.json()["id"], headers=other).status_code == 404
    listing = client.get("/api/v1/orders", headers=other).json()
    assert all(row["id"] != a.json()["id"] for row in listing["items"])
    assert client.get("/api/v1/orders", headers={"Authorization": ""}).status_code == 401
    assert (
        client.post(
            "/api/v1/orders",
            headers={"Idempotency-Key": str(uuid4())},
            json=order_payload(simulate_transient_failures=1),
        ).status_code
        == 422
    )


def inventory_payload():
    sku = "TEST-" + uuid4().hex.upper()
    with InventorySessionLocal.begin() as db:
        db.add(InventoryItem(sku=sku, name="Тестовый товар", available_quantity=10))
    return dict(
        event_id=uuid4(),
        order_id=uuid4(),
        correlation_id="test-flow",
        items=[{"sku": sku, "quantity": 3}],
        attempt=0,
    )


def reserve(payload):
    with InventorySessionLocal() as db:
        return services.process_fulfillment(db, **payload)


def test_duplicate_events_reserve_once():
    payload = inventory_payload()
    results = parallel(*[lambda: reserve(payload) for _ in range(6)])
    assert sum(not r.duplicate for r in results) == 1
    assert reserve({**payload, "event_id": uuid4()}).duplicate
    with InventorySessionLocal() as db:
        assert (
            db.scalar(
                select(InventoryItem.available_quantity).where(
                    InventoryItem.sku == payload["items"][0]["sku"]
                )
            )
            == 7
        )
        assert (
            db.scalar(
                select(func.count())
                .select_from(Reservation)
                .where(Reservation.order_id == payload["order_id"])
            )
            == 1
        )
    with pytest.raises(services.EventConflict):
        reserve({**payload, "items": [{"sku": payload["items"][0]["sku"], "quantity": 1}]})


def test_stock_lock_order_and_no_overselling():
    first, second = inventory_payload(), inventory_payload()
    skus = [first["items"][0]["sku"], second["items"][0]["sku"]]
    a = {**first, "items": [{"sku": sku, "quantity": 6} for sku in skus]}
    b = {**second, "items": list(reversed(a["items"]))}
    results = parallel(lambda: reserve(a), lambda: reserve(b))
    assert sorted(r.outcome for r in results) == ["failed", "fulfilled"]
    with InventorySessionLocal() as db:
        assert list(
            db.scalars(select(InventoryItem.available_quantity).where(InventoryItem.sku.in_(skus)))
        ) == [4, 4]


def test_terminal_results_conflict(client, order_payload):
    order = client.post(
        "/api/v1/orders", headers={"Idempotency-Key": str(uuid4())}, json=order_payload()
    ).json()

    def apply(event_type):
        try:
            with OrdersSessionLocal() as db:
                return services.apply_order_result(
                    db,
                    event_id=uuid4(),
                    event_type=event_type,
                    order_id=UUID(order["id"]),
                    reason=None,
                )
        except services.EventConflict:
            return "conflict"

    results = parallel(lambda: apply("order.fulfilled"), lambda: apply("order.failed"))
    assert True in results and "conflict" in results
    with OrdersSessionLocal() as db:
        status = db.get(Order, UUID(order["id"])).status
    assert apply("order." + status) is False
    assert apply("order.unknown") == "conflict"


def test_result_event_id_cannot_change_order(client, order_payload):
    orders = [
        client.post(
            "/api/v1/orders", headers={"Idempotency-Key": str(uuid4())}, json=order_payload()
        ).json()
        for _ in range(2)
    ]
    event_id = uuid4()
    with OrdersSessionLocal() as db:
        assert services.apply_order_result(
            db,
            event_id=event_id,
            event_type="order.fulfilled",
            order_id=UUID(orders[0]["id"]),
            reason=None,
        )
    with OrdersSessionLocal() as db, pytest.raises(services.EventConflict):
        services.apply_order_result(
            db,
            event_id=event_id,
            event_type="order.fulfilled",
            order_id=UUID(orders[1]["id"]),
            reason=None,
        )


def test_inventory_transaction_rollback(monkeypatch):
    payload = inventory_payload()

    def fail(*args):
        raise RuntimeError("Сбой записи outbox")

    monkeypatch.setattr(services, "_record_inventory_result", fail)
    with pytest.raises(RuntimeError):
        reserve(payload)
    with InventorySessionLocal() as db:
        assert (
            db.scalar(
                select(InventoryItem.available_quantity).where(
                    InventoryItem.sku == payload["items"][0]["sku"]
                )
            )
            == 10
        )
        assert (
            db.scalar(select(Reservation).where(Reservation.order_id == payload["order_id"]))
            is None
        )


def test_event_replay_keeps_identity_and_audit(client, order_payload):
    order = client.post(
        "/api/v1/orders", headers={"Idempotency-Key": str(uuid4())}, json=order_payload()
    ).json()
    with OrdersSessionLocal() as db:
        event = db.scalar(
            select(OrderOutboxEvent).where(
                OrderOutboxEvent.payload["order_id"].astext == order["id"]
            )
        )
        event_id, payload = event.id, event.payload
    replay_event("orders", event_id, "operator", "Восстановление после сбоя брокера")
    with OrdersSessionLocal() as db:
        assert db.get(OrderOutboxEvent, event_id).payload == payload
        assert (
            db.scalar(select(OrderEventReplay).where(OrderEventReplay.event_id == event_id)).actor
            == "operator"
        )


def test_malformed_broker_messages_are_quarantined():
    from ordermesh.config import get_settings
    from ordermesh.workers.fulfillment import handle_message as fulfillment
    from ordermesh.workers.status import handle_message as status

    assert "rabbitmq-test" in get_settings().rabbitmq_url
    connection = connect()
    try:
        channel = connection.channel()
        declare_topology(channel)
        channel.confirm_delivery()
        for handler, queue in [(fulfillment, DLQ), (status, RESULT_DLQ)]:
            channel.queue_purge(queue)
            source = channel.queue_declare(queue="", exclusive=True).method.queue
            for body in [b"{broken", b"null", b'{"event_id": 1}']:
                channel.basic_publish(exchange="", routing_key=source, body=body, mandatory=True)
                method, properties, actual = channel.basic_get(source)
                handler(channel, method, properties, actual)
                returned, _, quarantined = channel.basic_get(queue, auto_ack=True)
                assert returned is not None and quarantined == body
            assert channel.queue_declare(queue=source, passive=True).method.message_count == 0
    finally:
        connection.close()


def test_quarantine_failure_does_not_acknowledge():
    from ordermesh.workers.status import handle_message

    class Channel:
        acked = False

        def basic_publish(self, **kwargs):
            raise RuntimeError("Брокер не подтвердил запись")

        def basic_ack(self, **kwargs):
            self.acked = True

    channel = Channel()
    with pytest.raises(RuntimeError):
        handle_message(channel, SimpleNamespace(delivery_tag=1), pika.BasicProperties(), b"bad")
    assert not channel.acked


def test_retry_exhaustion_is_atomic_and_repeatable():
    payload = inventory_payload()
    args = {key: value for key, value in payload.items() if key != "attempt"}

    def exhausted():
        with InventorySessionLocal() as db:
            return services.record_retry_exhausted(db, **args, reason="Исчерпаны повторы")

    results = parallel(exhausted, exhausted)
    assert sum(not r.duplicate for r in results) == 1
    with InventorySessionLocal() as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(InventoryOutboxEvent)
                .where(InventoryOutboxEvent.payload["order_id"].astext == str(payload["order_id"]))
            )
            == 1
        )


def test_broker_pipeline_and_retry(client, order_payload):
    import time

    from ordermesh.broker import FULFILLMENT_QUEUE, RESULT_QUEUE, RETRY_QUEUE, publish_event
    from ordermesh.workers.fulfillment import handle_message as fulfill
    from ordermesh.workers.status import handle_message as apply

    order = client.post(
        "/api/v1/orders", headers={"Idempotency-Key": str(uuid4())}, json=order_payload()
    ).json()
    with OrdersSessionLocal() as db:
        event = db.scalar(
            select(OrderOutboxEvent).where(
                OrderOutboxEvent.payload["order_id"].astext == order["id"]
            )
        )
        payload = {"event_id": str(event.id), **event.payload, "simulate_transient_failures": 1}
    connection = connect()
    try:
        channel = connection.channel()
        declare_topology(channel)
        channel.confirm_delivery()
        for queue in (FULFILLMENT_QUEUE, RESULT_QUEUE, RETRY_QUEUE):
            channel.queue_purge(queue)
        publish_event(channel, "order.created", payload)
        method, properties, body = channel.basic_get(FULFILLMENT_QUEUE)
        assert method is not None
        fulfill(channel, method, properties, body)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            method, properties, body = channel.basic_get(FULFILLMENT_QUEUE)
            if method is not None:
                break
            connection.process_data_events(time_limit=0.05)
        assert method is not None and properties.headers["x-retry-count"] == 1
        fulfill(channel, method, properties, body)
        with InventorySessionLocal() as db:
            result = db.scalar(
                select(InventoryOutboxEvent).where(
                    InventoryOutboxEvent.payload["order_id"].astext == order["id"]
                )
            )
            assert result is not None
            publish_event(
                channel, result.event_type, {"event_id": str(result.id), **result.payload}
            )
        method, properties, body = channel.basic_get(RESULT_QUEUE)
        assert method is not None
        apply(channel, method, properties, body)
        assert client.get("/api/v1/orders/" + order["id"]).json()["status"] == "fulfilled"
    finally:
        connection.close()


def test_unconfirmed_publication_stays_pending(monkeypatch):
    from ordermesh.workers import publisher

    with OrdersSessionLocal.begin() as db:
        event = OrderOutboxEvent(event_type="order.created", payload={"order_id": str(uuid4())})
        db.add(event)
        db.flush()
        event_id = event.id

    def fail(*args, **kwargs):
        raise RuntimeError("Нет подтверждения")

    monkeypatch.setattr(publisher, "publish_event", fail)
    publisher.publish_order_events(object())
    with OrdersSessionLocal() as db:
        event = db.get(OrderOutboxEvent, event_id)
        assert event.published_at is None and event.attempts > 0
        assert event.last_error == "RuntimeError"
