"""trade entry_features — Phase-0 outcome instrumentation

Revision ID: 0002
Revises: 0001
Create Date: 2026-06-16

Adds the ``trades.entry_features`` JSONB column. It snapshots the resolved
tunables ("knobs") plus the market context at entry (POP, short delta, width,
credit, DTE) so realized P&L can later be attributed back to the settings that
produced each trade — the labelled dataset the outcome-driven tuner trains on.

Idempotent (``ADD COLUMN IF NOT EXISTS``), so it is safe to run against a DB
that already has the column from ``schema.sql``.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry_features JSONB"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE trades DROP COLUMN IF EXISTS entry_features")
