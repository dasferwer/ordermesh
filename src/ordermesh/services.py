import hashlib
import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from ordermesh.models import (
    InventoryInboxEvent,
    InventoryItem,
    InventoryOutboxEvent,
    Order,
    OrderInboxEvent,
    OrderItem,
    OrderOutboxEvent,
    OrderStatus,
    Reservation,
    ReservationItem,
    ReservationStatus,
)
from ordermesh.schemas import OrderCreate


class TransientFulfillmentError(RuntimeError):
    pass


@dataclass(frozen=True)
class FulfillmentResult:
    outcome: str
    duplicate: bool = False
    reason: str | None = None


def request_hash(data: OrderCreate) -> str:
    canonical = json.dumps(data.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def load_order(db: Session, order_id: UUID) -> Order:
    order = db.scalar(select(Order).where(Order.id == order_id).options(selectinload(Order.items)))
    if order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return order


def create_order(
    db: Session,
    data: OrderCreate,
    idempotency_key: str,
    correlation_id: str,
) -> tuple[Order, bool]:
    payload_hash = request_hash(data)
    existing = db.scalar(
        select(Order)
        .where(Order.idempotency_key == idempotency_key)
        .options(selectinload(Order.items))
    )
    if existing is not None:
        if existing.request_hash != payload_hash:
            raise HTTPException(status_code=409, detail="Idempotency key reused with other data")
        return existing, True

    order = Order(
        idempotency_key=idempotency_key,
        request_hash=payload_hash,
        customer_email=str(data.customer_email),
        correlation_id=correlation_id,
    )
    db.add(order)
    db.flush()
    for item in data.items:
        db.add(OrderItem(order_id=order.id, sku=item.sku, quantity=item.quantity))
    db.add(
        OrderOutboxEvent(
            event_type="order.created",
            payload={
                "order_id": str(order.id),
                "correlation_id": correlation_id,
                "simulate_transient_failures": data.simulate_transient_failures,
                "items": [item.model_dump() for item in data.items],
            },
        )
    )
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        concurrent = db.scalar(
            select(Order)
            .where(Order.idempotency_key == idempotency_key)
            .options(selectinload(Order.items))
        )
        if concurrent is None or concurrent.request_hash != payload_hash:
            raise HTTPException(status_code=409, detail="Idempotency key conflict") from None
        return concurrent, True
    return load_order(db, order.id), False


def _record_inventory_result(
    db: Session,
    event_id: UUID,
    order_id: UUID,
    correlation_id: str,
    outcome: str,
    reason: str | None,
) -> None:
    event_type = "order.fulfilled" if outcome == "fulfilled" else "order.failed"
    db.add(InventoryInboxEvent(event_id=event_id, event_type="order.created"))
    db.add(
        InventoryOutboxEvent(
            event_type=event_type,
            payload={
                "order_id": str(order_id),
                "correlation_id": correlation_id,
                "reason": reason,
            },
        )
    )


def process_fulfillment(
    db: Session,
    *,
    event_id: UUID,
    order_id: UUID,
    correlation_id: str,
    items: list[dict[str, Any]],
    attempt: int,
    simulate_transient_failures: int = 0,
) -> FulfillmentResult:
    if db.scalar(select(InventoryInboxEvent).where(InventoryInboxEvent.event_id == event_id)):
        return FulfillmentResult(outcome="duplicate", duplicate=True)
    if attempt < simulate_transient_failures:
        raise TransientFulfillmentError(f"Simulated transient failure #{attempt + 1}")

    skus = [str(item["sku"]) for item in items]
    inventory = {
        row.sku: row
        for row in db.scalars(
            select(InventoryItem).where(InventoryItem.sku.in_(skus)).with_for_update()
        )
    }
    reason = next(
        (
            f"Insufficient inventory for {item['sku']}"
            for item in items
            if str(item["sku"]) not in inventory
            or inventory[str(item["sku"])].available_quantity < int(item["quantity"])
        ),
        None,
    )
    if reason:
        db.add(
            Reservation(
                order_id=order_id,
                status=ReservationStatus.REJECTED,
                reason=reason,
            )
        )
        _record_inventory_result(db, event_id, order_id, correlation_id, "failed", reason)
        db.commit()
        return FulfillmentResult(outcome="failed", reason=reason)

    reservation = Reservation(order_id=order_id, status=ReservationStatus.RESERVED)
    db.add(reservation)
    db.flush()
    for item in items:
        sku = str(item["sku"])
        quantity = int(item["quantity"])
        inventory[sku].available_quantity -= quantity
        db.add(ReservationItem(reservation_id=reservation.id, sku=sku, quantity=quantity))
    _record_inventory_result(db, event_id, order_id, correlation_id, "fulfilled", None)
    db.commit()
    return FulfillmentResult(outcome="fulfilled")


def record_retry_exhausted(
    db: Session,
    *,
    event_id: UUID,
    order_id: UUID,
    correlation_id: str,
    reason: str,
) -> FulfillmentResult:
    if db.scalar(select(InventoryInboxEvent).where(InventoryInboxEvent.event_id == event_id)):
        return FulfillmentResult(outcome="duplicate", duplicate=True)
    db.add(
        Reservation(
            order_id=order_id,
            status=ReservationStatus.REJECTED,
            reason=reason,
        )
    )
    _record_inventory_result(db, event_id, order_id, correlation_id, "failed", reason)
    db.commit()
    return FulfillmentResult(outcome="failed", reason=reason)


def apply_order_result(
    db: Session,
    *,
    event_id: UUID,
    event_type: str,
    order_id: UUID,
    reason: str | None,
) -> bool:
    if db.scalar(select(OrderInboxEvent).where(OrderInboxEvent.event_id == event_id)):
        return False
    order = db.get(Order, order_id)
    if order is None:
        raise LookupError("Order not found")
    order.status = OrderStatus.FULFILLED if event_type == "order.fulfilled" else OrderStatus.FAILED
    order.failure_reason = reason if event_type == "order.failed" else None
    db.add(OrderInboxEvent(event_id=event_id, event_type=event_type))
    db.commit()
    return True
