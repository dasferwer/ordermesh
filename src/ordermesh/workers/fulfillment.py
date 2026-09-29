import logging
import time

import pika

from ordermesh.broker import (
    DLQ,
    FULFILLMENT_QUEUE,
    RETRY_QUEUE,
    connect,
    declare_topology,
    quarantine,
)
from ordermesh.config import get_settings
from ordermesh.db import InventorySessionLocal
from ordermesh.schemas import FulfillmentMessage
from ordermesh.services import (
    EventConflict,
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
    message: FulfillmentMessage,
    attempt: int,
) -> None:
    settings = get_settings()
    if attempt < settings.max_retries:
        channel.basic_publish(
            exchange="",
            routing_key=RETRY_QUEUE,
            body=body,
            mandatory=True,
            properties=pika.BasicProperties(
                delivery_mode=2, type="order.created", headers={"x-retry-count": attempt + 1}
            ),
        )
        channel.basic_ack(delivery_tag=method.delivery_tag)
    else:
        with InventorySessionLocal() as db:
            record_retry_exhausted(
                db,
                event_id=message.event_id,
                order_id=message.order_id,
                correlation_id=message.correlation_id,
                reason="Исчерпан бюджет повторов обработки",
                items=[item.model_dump() for item in message.items],
                simulate_transient_failures=message.simulate_transient_failures,
            )
        quarantine(channel, method.delivery_tag, body, DLQ, "Исчерпан бюджет повторов")


def handle_message(
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
) -> None:
    try:
        message = FulfillmentMessage.model_validate_json(body)
        if properties.type != "order.created":
            raise ValueError("Неверный тип события")
        attempt = (properties.headers or {}).get("x-retry-count", 0)
        if type(attempt) is not int or not 0 <= attempt <= get_settings().max_retries:
            raise ValueError("Неверный номер попытки")
    except ValueError:
        quarantine(channel, method.delivery_tag, body, DLQ, "Некорректное сообщение")
        return
    try:
        with InventorySessionLocal() as db:
            process_fulfillment(
                db,
                event_id=message.event_id,
                order_id=message.order_id,
                correlation_id=message.correlation_id,
                items=[item.model_dump() for item in message.items],
                attempt=attempt,
                simulate_transient_failures=message.simulate_transient_failures,
            )
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except TransientFulfillmentError:
        retry_or_dead_letter(channel, method, properties, body, message, attempt)
    except EventConflict:
        quarantine(channel, method.delivery_tag, body, DLQ, "Конфликт события")
    # Сбой БД прерывает consumer: reconnect с задержкой вернёт неподтверждённое сообщение.


def run() -> None:
    while True:
        connection: pika.BlockingConnection | None = None
        try:
            connection = connect()
            channel = connection.channel()
            declare_topology(channel)
            channel.confirm_delivery()
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
