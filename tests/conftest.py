from collections.abc import Callable, Generator
from typing import Any
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from ordermesh.db import inventory_engine, orders_engine
from ordermesh.main import app
from ordermesh.seed import seed_database


@pytest.fixture(scope="session", autouse=True)
def reset_databases() -> Generator[None, None, None]:
    if (
        orders_engine.url.database != "orders_test"
        or inventory_engine.url.database != "inventory_test"
    ):
        raise RuntimeError("Тесты разрешены только в orders_test и inventory_test")
    with orders_engine.begin() as connection:
        connection.execute(
            text(
                "TRUNCATE TABLE event_replays, inbox_events, outbox_events, "
                "order_items, orders CASCADE"
            )
        )
    with inventory_engine.begin() as connection:
        connection.execute(
            text(
                "TRUNCATE TABLE event_replays, outbox_events, inbox_events, "
                "reservation_items, reservations, "
                "inventory_items CASCADE"
            )
        )
    seed_database()
    yield


@pytest.fixture(scope="session")
def client() -> Generator[TestClient, None, None]:
    with TestClient(app, headers={"Authorization": "Bearer test-client-key-long"}) as test_client:
        yield test_client


@pytest.fixture
def order_payload() -> Callable[..., dict[str, Any]]:
    def factory(**overrides: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "customer_email": f"buyer-{uuid4()}@example.com",
            "items": [{"sku": "BOOK-FASTAPI", "quantity": 1}],
            "simulate_transient_failures": 0,
        }
        payload.update(overrides)
        return payload

    return factory
