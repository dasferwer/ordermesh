from collections.abc import Callable
from typing import Any
from uuid import uuid4

from fastapi.testclient import TestClient


def test_health(client: TestClient) -> None:
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "orders_database": "ok",
        "rabbitmq": "ok",
    }
    assert response.headers["X-Correlation-ID"]


def test_idempotency_replays_same_order(
    client: TestClient, order_payload: Callable[..., dict[str, Any]]
) -> None:
    key = f"order-{uuid4()}"
    headers = {"Idempotency-Key": key, "X-Correlation-ID": "checkout-123"}
    payload = order_payload()

    first = client.post("/api/v1/orders", headers=headers, json=payload)
    replay = client.post("/api/v1/orders", headers=headers, json=payload)

    assert first.status_code == 201
    assert replay.status_code == 200
    assert replay.headers["Idempotent-Replayed"] == "true"
    assert first.json()["id"] == replay.json()["id"]
    assert first.json()["correlation_id"] == "checkout-123"


def test_idempotency_rejects_changed_payload(
    client: TestClient, order_payload: Callable[..., dict[str, Any]]
) -> None:
    headers = {"Idempotency-Key": f"order-{uuid4()}"}
    first = client.post("/api/v1/orders", headers=headers, json=order_payload())
    changed = client.post(
        "/api/v1/orders",
        headers=headers,
        json=order_payload(items=[{"sku": "BOOK-FASTAPI", "quantity": 2}]),
    )

    assert first.status_code == 201
    assert changed.status_code == 409
