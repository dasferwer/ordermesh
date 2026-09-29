"""Защитить принадлежность заказов и обработку повторных событий."""

import sqlalchemy as sa
from alembic import op

revision = "20260929_0002_orders"
down_revision = "20260831_0001_orders"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("inbox_events", sa.Column("payload_hash", sa.String(64)))
    op.add_column(
        "orders", sa.Column("client_id", sa.String(100), nullable=False, server_default="legacy")
    )
    op.alter_column("orders", "client_id", server_default=None)
    op.drop_constraint("orders_idempotency_key_key", "orders", type_="unique")
    op.create_unique_constraint("orders_client_key", "orders", ["client_id", "idempotency_key"])
    op.create_check_constraint("ck_order_items_quantity", "order_items", "quantity > 0")


def downgrade() -> None:
    raise RuntimeError("Откат уберёт защиту данных; используйте исправляющую миграцию")
