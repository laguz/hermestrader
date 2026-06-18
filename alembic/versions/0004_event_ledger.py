"""event_ledger — append-only event store (event sourcing)

Revision ID: 0004
Revises: 0003
Create Date: 2026-06-17

Promotes ``event_ledger`` from an ORM-only / create_all-bootstrapped table to a
first-class part of the canonical Postgres schema. It is the source-of-truth log
for the event-sourcing layer: read models (trades, pending_orders,
system_settings, …) are projections of this log, ordered globally by ``id``.

Idempotent (``CREATE TABLE IF NOT EXISTS`` / ``CREATE INDEX IF NOT EXISTS``), so
it self-heals existing DBs that already have the table from create_all.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS event_ledger (
            id           BIGSERIAL,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
            event_type   TEXT NOT NULL,
            payload      JSONB NOT NULL,
            PRIMARY KEY (id, created_at)
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_event_ledger_type "
        "ON event_ledger(event_type, id)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS event_ledger")
