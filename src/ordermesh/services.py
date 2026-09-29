import hashlib
import json
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select, text
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


class EventConflict(ValueError):
    pass


def transaction_lock(db: Session, namespace: str, key: str) -> None:
    db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": namespace + ":" + key},
    )


def digest(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


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
    *,
    client_id: str = "internal",
) -> tuple[Order, bool]:
    payload_hash = request_hash(data)
    transaction_lock(db, "checkout", client_id + ":" + idempotency_key)
    existing = db.scalar(
        select(Order)
        .where(Order.client_id == client_id, Order.idempotency_key == idempotency_key)
        .options(selectinload(Order.items))
    )
    if existing is not None:
        if existing.request_hash != payload_hash:
            raise HTTPException(status_code=409, detail="Idempotency key reused with other data")
        return existing, True

    order = Order(
        client_id=client_id,
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
    db.commit()
    return load_order(db, order.id), False


def _record_inventory_result(
    db: Session,
    event_id: UUID,
    order_id: UUID,
    correlation_id: str,
    outcome: str,
    reason: str | None,
    payload_hash: str,
) -> None:
    event_type = "order.fulfilled" if outcome == "fulfilled" else "order.failed"
    db.add(
        InventoryInboxEvent(
            event_id=event_id, event_type="order.created", payload_hash=payload_hash
        )
    )
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


def inventory_duplicate(db: Session, event_id: UUID, order_id: UUID, payload_hash: str) -> bool:
    transaction_lock(db, "inventory-event", str(event_id))
    transaction_lock(db, "inventory-order", str(order_id))
    inbox = db.scalar(select(InventoryInboxEvent).where(InventoryInboxEvent.event_id == event_id))
    reservation = db.scalar(select(Reservation).where(Reservation.order_id == order_id))
    if inbox is not None:
        if inbox.payload_hash is None or inbox.payload_hash != payload_hash:
            raise EventConflict("ID события повторно использован с другими данными")
        return True
    if reservation is not None:
        if reservation.request_hash is None or reservation.request_hash != payload_hash:
            raise EventConflict("Заказ уже обработан с другими или неизвестными данными")
        db.add(
            InventoryInboxEvent(
                event_id=event_id, event_type="order.created", payload_hash=payload_hash
            )
        )
        db.commit()
        return True
    return False


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
    payload_hash = digest(
        {
            "order_id": str(order_id),
            "items": sorted(items, key=lambda item: item["sku"]),
            "correlation_id": correlation_id,
            "simulate_transient_failures": simulate_transient_failures,
        }
    )
    if inventory_duplicate(db, event_id, order_id, payload_hash):
        return FulfillmentResult(outcome="duplicate", duplicate=True)
    if attempt < simulate_transient_failures:
        raise TransientFulfillmentError(f"Simulated transient failure #{attempt + 1}")

    skus = [str(item["sku"]) for item in items]
    inventory = {
        row.sku: row
        for row in db.scalars(
            select(InventoryItem)
            .where(InventoryItem.sku.in_(skus))
            .order_by(InventoryItem.sku)
            .with_for_update()
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
                request_hash=payload_hash,
                status=ReservationStatus.REJECTED,
                reason=reason,
            )
        )
        _record_inventory_result(
            db, event_id, order_id, correlation_id, "failed", reason, payload_hash
        )
        db.commit()
        return FulfillmentResult(outcome="failed", reason=reason)

    reservation = Reservation(
        order_id=order_id, status=ReservationStatus.RESERVED, request_hash=payload_hash
    )
    db.add(reservation)
    db.flush()
    for item in items:
        sku = str(item["sku"])
        quantity = int(item["quantity"])
        inventory[sku].available_quantity -= quantity
        db.add(ReservationItem(reservation_id=reservation.id, sku=sku, quantity=quantity))
    _record_inventory_result(
        db, event_id, order_id, correlation_id, "fulfilled", None, payload_hash
    )
    db.commit()
    return FulfillmentResult(outcome="fulfilled")


def record_retry_exhausted(
    db: Session,
    *,
    event_id: UUID,
    order_id: UUID,
    correlation_id: str,
    reason: str,
    items: list[dict[str, Any]],
    simulate_transient_failures: int = 0,
) -> FulfillmentResult:
    payload_hash = digest(
        {
            "order_id": str(order_id),
            "items": sorted(items, key=lambda item: item["sku"]),
            "correlation_id": correlation_id,
            "simulate_transient_failures": simulate_transient_failures,
        }
    )
    if inventory_duplicate(db, event_id, order_id, payload_hash):
        return FulfillmentResult(outcome="duplicate", duplicate=True)
    db.add(
        Reservation(
            order_id=order_id,
            request_hash=payload_hash,
            status=ReservationStatus.REJECTED,
            reason=reason,
        )
    )
    _record_inventory_result(db, event_id, order_id, correlation_id, "failed", reason, payload_hash)
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
    if event_type not in {"order.fulfilled", "order.failed"}:
        raise EventConflict("Неизвестный тип результата")
    payload_hash = digest({"order_id": str(order_id), "event_type": event_type, "reason": reason})
    transaction_lock(db, "result-event", str(event_id))
    inbox = db.scalar(select(OrderInboxEvent).where(OrderInboxEvent.event_id == event_id))
    if inbox is not None:
        if inbox.payload_hash is None or inbox.payload_hash != payload_hash:
            raise EventConflict("ID результата повторно использован с другими данными")
        return False
    order = db.scalar(
        select(Order)
        .where(Order.id == order_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if order is None:
        raise EventConflict("Заказ не найден")
    target = OrderStatus.FULFILLED if event_type == "order.fulfilled" else OrderStatus.FAILED
    if order.status != OrderStatus.PENDING and (
        order.status != target or order.failure_reason != reason
    ):
        raise EventConflict("Результат противоречит завершённому заказу")
    changed = order.status == OrderStatus.PENDING
    order.status = target
    order.failure_reason = reason if event_type == "order.failed" else None
    db.add(OrderInboxEvent(event_id=event_id, event_type=event_type, payload_hash=payload_hash))
    db.commit()
    return changed
