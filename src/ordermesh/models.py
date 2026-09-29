from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from ordermesh.db import InventoryBase, OrdersBase


class OrderStatus(StrEnum):
    PENDING = "pending"
    FULFILLED = "fulfilled"
    FAILED = "failed"


class ReservationStatus(StrEnum):
    RESERVED = "reserved"
    REJECTED = "rejected"


class UUIDMixin:
    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class Order(UUIDMixin, TimestampMixin, OrdersBase):
    __tablename__ = "orders"
    __table_args__ = (
        CheckConstraint("status IN ('pending', 'fulfilled', 'failed')", name="ck_orders_status"),
        UniqueConstraint("client_id", "idempotency_key", name="orders_client_key"),
        Index("ix_orders_status_created", "status", "created_at"),
    )

    client_id: Mapped[str] = mapped_column(String(100), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    customer_email: Mapped[str] = mapped_column(String(320), nullable=False)
    status: Mapped[OrderStatus] = mapped_column(
        String(20), default=OrderStatus.PENDING, nullable=False
    )
    correlation_id: Mapped[str] = mapped_column(String(128), nullable=False)
    failure_reason: Mapped[str | None] = mapped_column(Text)

    items: Mapped[list["OrderItem"]] = relationship(
        back_populates="order", cascade="all, delete-orphan", order_by="OrderItem.sku"
    )


class OrderItem(UUIDMixin, OrdersBase):
    __tablename__ = "order_items"
    __table_args__ = (
        UniqueConstraint("order_id", "sku", name="order_items_order_sku_key"),
        CheckConstraint("quantity > 0", name="ck_order_items_quantity"),
    )

    order_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("orders.id", ondelete="CASCADE"), nullable=False
    )
    sku: Mapped[str] = mapped_column(String(80), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    order: Mapped[Order] = relationship(back_populates="items")


class OrderOutboxEvent(UUIDMixin, OrdersBase):
    __tablename__ = "outbox_events"
    __table_args__ = (Index("ix_orders_outbox_pending", "published_at", "created_at"),)

    event_type: Mapped[str] = mapped_column(String(120), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OrderInboxEvent(UUIDMixin, OrdersBase):
    __tablename__ = "inbox_events"
    __table_args__ = (UniqueConstraint("event_id", name="orders_inbox_event_id_key"),)

    payload_hash: Mapped[str | None] = mapped_column(String(64))
    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(120), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class InventoryItem(UUIDMixin, TimestampMixin, InventoryBase):
    __tablename__ = "inventory_items"
    __table_args__ = (
        CheckConstraint("available_quantity >= 0", name="ck_inventory_non_negative"),
        UniqueConstraint("sku", name="inventory_items_sku_key"),
    )

    sku: Mapped[str] = mapped_column(String(80), nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    available_quantity: Mapped[int] = mapped_column(Integer, nullable=False)


class Reservation(UUIDMixin, InventoryBase):
    __tablename__ = "reservations"
    __table_args__ = (
        CheckConstraint("status IN ('reserved', 'rejected')", name="ck_reservations_status"),
        UniqueConstraint("order_id", name="reservations_order_id_key"),
    )

    request_hash: Mapped[str | None] = mapped_column(String(64))
    order_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    status: Mapped[ReservationStatus] = mapped_column(String(20), nullable=False)
    reason: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    items: Mapped[list["ReservationItem"]] = relationship(
        back_populates="reservation", cascade="all, delete-orphan"
    )


class ReservationItem(UUIDMixin, InventoryBase):
    __tablename__ = "reservation_items"
    __table_args__ = (
        CheckConstraint("quantity > 0", name="ck_reservation_items_quantity"),
        UniqueConstraint("reservation_id", "sku", name="reservation_items_sku_key"),
    )

    reservation_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("reservations.id", ondelete="CASCADE"), nullable=False
    )
    sku: Mapped[str] = mapped_column(String(80), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    reservation: Mapped[Reservation] = relationship(back_populates="items")


class InventoryInboxEvent(UUIDMixin, InventoryBase):
    __tablename__ = "inbox_events"
    __table_args__ = (UniqueConstraint("event_id", name="inventory_inbox_event_id_key"),)

    payload_hash: Mapped[str | None] = mapped_column(String(64))
    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(120), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class InventoryOutboxEvent(UUIDMixin, InventoryBase):
    __tablename__ = "outbox_events"
    __table_args__ = (Index("ix_inventory_outbox_pending", "published_at", "created_at"),)

    event_type: Mapped[str] = mapped_column(String(120), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ReplayMixin(UUIDMixin):
    event_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("outbox_events.id"), nullable=False
    )
    actor: Mapped[str] = mapped_column(String(100), nullable=False)
    reason: Mapped[str] = mapped_column(String(500), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class OrderEventReplay(ReplayMixin, OrdersBase):
    __tablename__ = "event_replays"


class InventoryEventReplay(ReplayMixin, InventoryBase):
    __tablename__ = "event_replays"
