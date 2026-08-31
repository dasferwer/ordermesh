# OrderMesh architecture decisions

## Service-owned data

Orders and Inventory use different PostgreSQL instances and different Alembic
histories. The API reads only Orders data; the fulfillment worker owns inventory
and reservations; the status worker consumes result events to update Orders.
No cross-database transaction is required.

## Idempotency at every boundary

- HTTP requests have a unique `Idempotency-Key` and a SHA-256 hash of the
  canonical body. Replays return the original resource; key reuse with other
  data returns `409 Conflict`.
- Consumers store the source event UUID in an inbox table with a unique
  constraint before committing a side effect.
- Each service commits its state change and outgoing outbox event atomically.

## Retry and DLQ

The retry queue applies a TTL and dead-letters the message back to
`order.created`. `x-retry-count` is bounded by configuration. Exhausted messages
are retained in `ordermesh.fulfillment.dlq`, and a terminal failure event keeps
the user-visible order from remaining pending forever.

## Concurrency

Inventory rows are selected with `FOR UPDATE`. Validation and decrement happen
in one transaction, preventing two consumers from reserving the same final
unit. Outbox publishers use `FOR UPDATE SKIP LOCKED` to permit horizontal
scaling.

## Failure scenarios

| Failure | Behaviour |
|---|---|
| Duplicate HTTP request | Same resource is returned without a second event |
| Duplicate RabbitMQ delivery | Inbox event ID suppresses the second side effect |
| RabbitMQ unavailable | Local commits survive in outbox until retry |
| Temporary fulfillment error | Delayed retry, then DLQ after bounded attempts |
| Inventory is insufficient | Reservation is rejected and order becomes failed |
| Publisher crashes after delivery | At-least-once redelivery is safe via inbox |
| Concurrent reservations | Row lock prevents negative stock |
| One database unavailable | Only the owning service fails; the message remains recoverable |
