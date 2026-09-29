"""Повторная отправка исходного события с сохранением причины и оператора."""

import argparse
from typing import cast
from uuid import UUID

from sqlalchemy import select

from ordermesh.db import InventorySessionLocal, OrdersSessionLocal
from ordermesh.models import (
    InventoryEventReplay,
    InventoryOutboxEvent,
    OrderEventReplay,
    OrderOutboxEvent,
)


def replay_event(source: str, event_id: UUID, actor: str, reason: str) -> None:
    actor, reason = actor.strip(), reason.strip()
    if not 1 <= len(actor) <= 100 or not 5 <= len(reason) <= 500:
        raise ValueError("Укажите оператора и причину повтора длиной от 5 до 500 символов")
    if source not in {"orders", "inventory"}:
        raise ValueError("Неизвестное хранилище событий")
    factory = OrdersSessionLocal if source == "orders" else InventorySessionLocal
    event_model: type[OrderOutboxEvent] | type[InventoryOutboxEvent] = (
        OrderOutboxEvent if source == "orders" else InventoryOutboxEvent
    )
    replay_model: type[OrderEventReplay] | type[InventoryEventReplay] = (
        OrderEventReplay if source == "orders" else InventoryEventReplay
    )
    with factory.begin() as db:
        event = cast(
            OrderOutboxEvent | InventoryOutboxEvent | None,
            db.scalar(select(event_model).where(event_model.id == event_id).with_for_update()),
        )
        if event is None:
            raise LookupError("Событие не найдено")
        event.published_at = None
        db.add(replay_model(event_id=event.id, actor=actor, reason=reason))


def main() -> None:
    parser = argparse.ArgumentParser(description="Повторить отправку исходного события outbox")
    parser.add_argument("source", choices=["orders", "inventory"])
    parser.add_argument("event_id", type=UUID)
    parser.add_argument("--actor", required=True)
    parser.add_argument("--reason", required=True)
    args = parser.parse_args()
    replay_event(args.source, args.event_id, args.actor, args.reason)
    print("Повторная отправка запланирована; аудит сохранён")


if __name__ == "__main__":
    main()
