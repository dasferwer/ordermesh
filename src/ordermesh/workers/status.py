import logging
import time

import pika

from ordermesh.broker import RESULT_DLQ, RESULT_QUEUE, connect, declare_topology, quarantine
from ordermesh.db import OrdersSessionLocal
from ordermesh.schemas import ResultMessage
from ordermesh.services import EventConflict, apply_order_result

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("pika").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def handle_message(
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.BasicProperties,
    body: bytes,
) -> None:
    try:
        message = ResultMessage.model_validate_json(body)
        event_type = str(properties.type)
        if event_type not in {"order.fulfilled", "order.failed"}:
            raise ValueError("Неизвестный тип результата")
        if event_type == "order.fulfilled" and message.reason is not None:
            raise ValueError("Успешный результат не должен содержать причину отказа")
    except ValueError:
        quarantine(channel, method.delivery_tag, body, RESULT_DLQ, "Некорректный результат")
        return
    try:
        with OrdersSessionLocal() as db:
            apply_order_result(
                db,
                event_id=message.event_id,
                event_type=event_type,
                order_id=message.order_id,
                reason=message.reason,
            )
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except EventConflict:
        quarantine(channel, method.delivery_tag, body, RESULT_DLQ, "Конфликт результата")


def run() -> None:
    while True:
        connection: pika.BlockingConnection | None = None
        try:
            connection = connect()
            channel = connection.channel()
            declare_topology(channel)
            channel.confirm_delivery()
            channel.basic_qos(prefetch_count=20)
            channel.basic_consume(queue=RESULT_QUEUE, on_message_callback=handle_message)
            logger.info("Status worker is ready")
            channel.start_consuming()
        except Exception:
            logger.exception("Status worker lost its connection; retrying")
            time.sleep(3)
        finally:
            if connection is not None and connection.is_open:
                connection.close()


if __name__ == "__main__":
    run()
