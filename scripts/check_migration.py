"""Проверить обновление исторических данных в отдельных временных базах."""

# ruff: noqa: E402
from ordermesh.test_safety import ensure_test_environment

# До engine, снимка данных и создания временной БД проверяем исходный профиль.
ensure_test_environment()

import hashlib
import json
import os
import subprocess
from uuid import uuid4

from sqlalchemy import create_engine, inspect, text

from ordermesh.db import inventory_engine, orders_engine


def fingerprint(rows: dict[str, list[dict[str, object]]]) -> str:
    return hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode()).hexdigest()


def check(source, area: str, tables: tuple[str, ...]) -> None:
    if source.url.database != area + "_test":
        raise RuntimeError("Проверка разрешена только для тестовой базы")
    temporary = "migration_" + uuid4().hex
    admin = create_engine(source.url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    target = create_engine(source.url.set(database=temporary))
    variable = "ORDERMESH_" + ("ORDERS" if area == "orders" else "INVENTORY") + "_DATABASE_URL"
    environment = {**os.environ, variable: target.url.render_as_string(hide_password=False)}
    command = ["alembic", "-c", "alembic-" + area + ".ini", "upgrade"]
    try:
        with admin.connect() as connection:
            connection.execute(text("CREATE DATABASE " + temporary))
        subprocess.run([*command, "20260831_0001_" + area], env=environment, check=True)
        baseline = {}
        inspector = inspect(target)
        with source.connect() as original, target.begin() as restored:
            for table in tables:
                columns = {column["name"] for column in inspector.get_columns(table)}
                rows = list(
                    original.execute(
                        text("SELECT to_jsonb(t) FROM " + table + " t ORDER BY id")
                    ).scalars()
                )
                baseline[table] = [{k: v for k, v in row.items() if k in columns} for row in rows]
                if table == "orders":
                    # Старая схема требовала глобальной уникальности, а новые клиенты делят ключи.
                    for row in baseline[table]:
                        row["idempotency_key"] = "legacy-" + str(row["id"])
                restored.execute(
                    text(
                        "INSERT INTO "
                        + table
                        + " SELECT * FROM json_populate_recordset(NULL::"
                        + table
                        + ", CAST(:rows AS json))"
                    ),
                    {"rows": json.dumps(baseline[table])},
                )
        subprocess.run([*command, "head"], env=environment, check=True)
        after = {}
        with target.connect() as connection:
            for table in tables:
                rows = list(
                    connection.execute(
                        text("SELECT to_jsonb(t) FROM " + table + " t ORDER BY id")
                    ).scalars()
                )
                after[table] = [
                    {
                        k: v
                        for k, v in row.items()
                        if k not in {"client_id", "payload_hash", "request_hash"}
                        or k == "request_hash"
                        and table == "orders"
                    }
                    for row in rows
                ]
        if fingerprint(baseline) != fingerprint(after):
            raise RuntimeError("Исторические данные изменились: " + area)
        print(area + ": исторические данные сохранены")
    finally:
        target.dispose()
        with admin.connect() as connection:
            connection.execute(text("DROP DATABASE IF EXISTS " + temporary))
        admin.dispose()


def main() -> None:
    check(orders_engine, "orders", ("orders", "order_items", "outbox_events", "inbox_events"))
    check(
        inventory_engine,
        "inventory",
        ("inventory_items", "reservations", "reservation_items", "outbox_events", "inbox_events"),
    )


if __name__ == "__main__":
    main()
