"""Сохранить аудит повторной отправки событий."""

import sqlalchemy as sa
from alembic import op

revision = "20260929_0003_orders"
down_revision = "20260929_0002_orders"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "event_replays",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("event_id", sa.Uuid(), sa.ForeignKey("outbox_events.id"), nullable=False),
        sa.Column("actor", sa.String(100), nullable=False),
        sa.Column("reason", sa.String(500), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )


def downgrade() -> None:
    raise RuntimeError("Аудит повторов сохраняется; используйте исправляющую миграцию")
