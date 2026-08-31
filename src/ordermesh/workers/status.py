import json
import logging
import time
from uuid import UUID

import pika

from ordermesh.broker import RESULT_QUEUE, connect, declare_topology
from ordermesh.db import OrdersSessionLocal
from ordermesh.services import apply_order_result

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
        payload = json.loads(body)
        with OrdersSessionLocal() as db:
            changed = apply_order_result(
                db,
                event_id=UUID(payload["event_id"]),
                event_type=str(properties.type),
                order_id=UUID(payload["order_id"]),
                reason=payload.get("reason"),
            )
        logger.info("Order result applied=%s order_id=%s", changed, payload["order_id"])
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception:
        logger.exception("Order result processing failed")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def run() -> None:
    while True:
        connection: pika.BlockingConnection | None = None
        try:
            connection = connect()
            channel = connection.channel()
            declare_topology(channel)
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
