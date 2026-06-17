"""exit_ticks — Phase-3 exit-state trajectory capture

Revision ID: 0003
Revises: 0002
Create Date: 2026-06-17

Adds the ``exit_ticks`` table: one row per open trade per management tick while
exit-policy capture is enabled. With each trade's realized P&L these rows form
the (state, action, return) trajectories the offline exit policy learns from.

Idempotent (``CREATE TABLE IF NOT EXISTS`` / ``CREATE INDEX IF NOT EXISTS``).
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS exit_ticks (
            id                 BIGSERIAL PRIMARY KEY,
            ts                 TIMESTAMPTZ NOT NULL DEFAULT now(),
            trade_id           BIGINT NOT NULL,
            strategy_id        TEXT NOT NULL,
            symbol             TEXT NOT NULL,
            dte                INT,
            unrealized_pnl_pct DOUBLE PRECISION,
            debit              DOUBLE PRECISION,
            entry_credit       DOUBLE PRECISION,
            action             TEXT NOT NULL DEFAULT 'hold',
            close_reason       TEXT
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS idx_exit_ticks_trade ON exit_ticks(trade_id, ts)"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS exit_ticks")
