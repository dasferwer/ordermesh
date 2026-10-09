"""Валидация retry-header связана с наблюдаемой попыткой consumer."""

import json
import logging
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

from ordermesh.workers import fulfillment


def test_validated_attempt_is_observed_without_payload(caplog, monkeypatch):
    event = uuid4()
    body = json.dumps(
        {
            "event_id": str(event),
            "order_id": str(uuid4()),
            "correlation_id": "proof",
            "items": [{"sku": "BOOK-FASTAPI", "quantity": 1}],
            "simulate_transient_failures": 2,
        }
    ).encode()
    monkeypatch.setattr(fulfillment, "InventorySessionLocal", lambda: nullcontext(None))
    monkeypatch.setattr(fulfillment, "process_fulfillment", lambda *args, **kwargs: None)
    caplog.set_level(logging.INFO, logger=fulfillment.__name__)
    fulfillment.handle_message(
        Mock(),
        SimpleNamespace(delivery_tag=1),
        SimpleNamespace(type="order.created", headers={"x-retry-count": 2}),
        body,
    )
    assert f"event_id={event} attempt=2" in caplog.text
    assert "BOOK-FASTAPI" not in caplog.text
