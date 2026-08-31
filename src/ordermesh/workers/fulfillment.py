import json
import logging
import time
from uuid import UUID

import pika

from ordermesh.broker import DLQ, FULFILLMENT_QUEUE, RETRY_QUEUE, connect, declare_topology
from ordermesh.config import get_settings
from ordermesh.db import InventorySessionLocal
from ordermesh.services import (
    TransientFulfillmentError,
    process_fulfillment,
    record_retry_exhausted,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("pika").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def retry_or_dead_letter(
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
    payload: dict[str, object],
    reason: str,
) -> None:
    settings = get_settings()
    headers = dict(properties.headers or {})
    attempt = int(headers.get("x-retry-count", 0))
    if attempt < settings.max_retries:
        headers["x-retry-count"] = attempt + 1
        channel.basic_publish(
            exchange="",
            routing_key=RETRY_QUEUE,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent,
                message_id=properties.message_id,
                correlation_id=properties.correlation_id,
                type=properties.type,
                headers=headers,
            ),
        )
        logger.warning("Scheduled retry %s/%s: %s", attempt + 1, settings.max_retries, reason)
    else:
        headers["x-final-error"] = reason[:500]
        channel.basic_publish(
            exchange="",
            routing_key=DLQ,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=pika.DeliveryMode.Persistent,
                message_id=properties.message_id,
                correlation_id=properties.correlation_id,
                type=properties.type,
                headers=headers,
            ),
        )
        with InventorySessionLocal() as db:
            record_retry_exhausted(
                db,
                event_id=UUID(str(payload["event_id"])),
                order_id=UUID(str(payload["order_id"])),
                correlation_id=str(payload["correlation_id"]),
                reason=f"Retries exhausted: {reason}",
            )
        logger.error("Message moved to DLQ after %s retries", settings.max_retries)
    channel.basic_ack(delivery_tag=method.delivery_tag)


def handle_message(
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
) -> None:
    payload = json.loads(body)
    attempt = int((properties.headers or {}).get("x-retry-count", 0))
    try:
        with InventorySessionLocal() as db:
            result = process_fulfillment(
                db,
                event_id=UUID(payload["event_id"]),
                order_id=UUID(payload["order_id"]),
                correlation_id=payload["correlation_id"],
                items=payload["items"],
                attempt=attempt,
                simulate_transient_failures=int(payload.get("simulate_transient_failures", 0)),
            )
        logger.info("Processed order %s: %s", payload["order_id"], result.outcome)
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except TransientFulfillmentError as exc:
        retry_or_dead_letter(channel, method, properties, body, payload, str(exc))
    except Exception as exc:
        logger.exception("Unexpected fulfillment error")
        retry_or_dead_letter(channel, method, properties, body, payload, str(exc))


def run() -> None:
    while True:
        connection: pika.BlockingConnection | None = None
        try:
            connection = connect()
            channel = connection.channel()
            declare_topology(channel)
            channel.basic_qos(prefetch_count=10)
            channel.basic_consume(queue=FULFILLMENT_QUEUE, on_message_callback=handle_message)
            logger.info("Fulfillment worker is ready")
            channel.start_consuming()
        except Exception:
            logger.exception("Fulfillment worker lost its connection; retrying")
            time.sleep(3)
        finally:
            if connection is not None and connection.is_open:
                connection.close()


if __name__ == "__main__":
    run()
