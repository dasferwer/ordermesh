import json
from collections.abc import Mapping
from typing import Any

import pika

from ordermesh.config import get_settings

EXCHANGE = "ordermesh.events"
FULFILLMENT_QUEUE = "ordermesh.fulfillment"
RETRY_QUEUE = "ordermesh.fulfillment.retry"
DLQ = "ordermesh.fulfillment.dlq"
RESULT_QUEUE = "ordermesh.results"


def connect() -> pika.BlockingConnection:
    parameters = pika.URLParameters(get_settings().rabbitmq_url)
    parameters.connection_attempts = 3
    parameters.retry_delay = 1
    parameters.socket_timeout = 5
    parameters.blocked_connection_timeout = 5
    return pika.BlockingConnection(parameters)


def declare_topology(channel: pika.channel.Channel) -> None:
    settings = get_settings()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=FULFILLMENT_QUEUE, durable=True)
    channel.queue_bind(queue=FULFILLMENT_QUEUE, exchange=EXCHANGE, routing_key="order.created")
    channel.queue_declare(
        queue=RETRY_QUEUE,
        durable=True,
        arguments={
            "x-message-ttl": settings.retry_delay_ms,
            "x-dead-letter-exchange": EXCHANGE,
            "x-dead-letter-routing-key": "order.created",
        },
    )
    channel.queue_declare(queue=DLQ, durable=True)
    channel.queue_declare(queue=RESULT_QUEUE, durable=True)
    channel.queue_bind(queue=RESULT_QUEUE, exchange=EXCHANGE, routing_key="order.fulfilled")
    channel.queue_bind(queue=RESULT_QUEUE, exchange=EXCHANGE, routing_key="order.failed")


def publish_event(
    channel: pika.channel.Channel,
    event_type: str,
    body: Mapping[str, Any],
    *,
    headers: dict[str, object] | None = None,
) -> None:
    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=event_type,
        body=json.dumps(body).encode(),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
            message_id=str(body["event_id"]),
            correlation_id=str(body.get("correlation_id", "")),
            type=event_type,
            headers=headers or {},
        ),
    )


def check_connection() -> None:
    connection = connect()
    connection.close()
