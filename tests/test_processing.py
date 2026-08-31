from uuid import uuid4

import pytest
from sqlalchemy import select

from ordermesh.db import InventorySessionLocal, OrdersSessionLocal
from ordermesh.models import InventoryItem, InventoryOutboxEvent, Order, OrderStatus
from ordermesh.schemas import OrderCreate
from ordermesh.services import (
    TransientFulfillmentError,
    apply_order_result,
    create_order,
    load_order,
    process_fulfillment,
)


def create_service_order(quantity: int = 2) -> tuple[Order, str]:
    correlation_id = str(uuid4())
    data = OrderCreate(
        customer_email="service@example.com",
        items=[{"sku": "COURSE-ASYNC", "quantity": quantity}],
    )
    with OrdersSessionLocal() as db:
        order, _ = create_order(db, data, f"service-{uuid4()}", correlation_id)
    return order, correlation_id


def test_fulfillment_and_result_consumers_are_idempotent() -> None:
    order, correlation_id = create_service_order()
    event_id = uuid4()

    with InventorySessionLocal() as db:
        before = db.scalar(
            select(InventoryItem.available_quantity).where(InventoryItem.sku == "COURSE-ASYNC")
        )
        result = process_fulfillment(
            db,
            event_id=event_id,
            order_id=order.id,
            correlation_id=correlation_id,
            items=[{"sku": "COURSE-ASYNC", "quantity": 2}],
            attempt=0,
        )
        event = db.scalar(
            select(InventoryOutboxEvent).where(
                InventoryOutboxEvent.event_type == "order.fulfilled",
                InventoryOutboxEvent.payload["order_id"].as_string() == str(order.id),
            )
        )
    assert result.outcome == "fulfilled"
    assert event is not None

    with InventorySessionLocal() as db:
        duplicate = process_fulfillment(
            db,
            event_id=event_id,
            order_id=order.id,
            correlation_id=correlation_id,
            items=[{"sku": "COURSE-ASYNC", "quantity": 2}],
            attempt=0,
        )
        after = db.scalar(
            select(InventoryItem.available_quantity).where(InventoryItem.sku == "COURSE-ASYNC")
        )
    assert duplicate.duplicate is True
    assert before is not None
    assert after == before - 2

    with OrdersSessionLocal() as db:
        changed = apply_order_result(
            db,
            event_id=event.id,
            event_type="order.fulfilled",
            order_id=order.id,
            reason=None,
        )
        repeated = apply_order_result(
            db,
            event_id=event.id,
            event_type="order.fulfilled",
            order_id=order.id,
            reason=None,
        )
        loaded = load_order(db, order.id)
    assert changed is True
    assert repeated is False
    assert loaded.status == OrderStatus.FULFILLED


def test_transient_failure_does_not_write_inventory() -> None:
    order, correlation_id = create_service_order()

    with InventorySessionLocal() as db, pytest.raises(TransientFulfillmentError):
        process_fulfillment(
            db,
            event_id=uuid4(),
            order_id=order.id,
            correlation_id=correlation_id,
            items=[{"sku": "COURSE-ASYNC", "quantity": 1}],
            attempt=0,
            simulate_transient_failures=1,
        )


def test_insufficient_inventory_produces_failure_event() -> None:
    order, correlation_id = create_service_order(quantity=1000)

    with InventorySessionLocal() as db:
        result = process_fulfillment(
            db,
            event_id=uuid4(),
            order_id=order.id,
            correlation_id=correlation_id,
            items=[{"sku": "COURSE-ASYNC", "quantity": 1000}],
            attempt=0,
        )

    assert result.outcome == "failed"
    assert result.reason == "Insufficient inventory for COURSE-ASYNC"
