"""Защитить принадлежность заказов и обработку повторных событий."""

import sqlalchemy as sa
from alembic import op

revision = "20260929_0002_inventory"
down_revision = "20260831_0001_inventory"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("inbox_events", sa.Column("payload_hash", sa.String(64)))
    op.add_column("reservations", sa.Column("request_hash", sa.String(64)))
    op.create_check_constraint("ck_reservation_items_quantity", "reservation_items", "quantity > 0")
    op.create_unique_constraint(
        "reservation_items_sku_key", "reservation_items", ["reservation_id", "sku"]
    )


def downgrade() -> None:
    raise RuntimeError("Откат уберёт защиту данных; используйте исправляющую миграцию")
