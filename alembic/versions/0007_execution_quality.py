"""execution quality — add mid_at_submit + entry_slippage to trades

Revision ID: 0007
Revises: 0006
Create Date: 2026-07-12

``mid_at_submit`` is the net quote midpoint of the order's legs captured at
submission (same credit/debit convention as entry_credit/entry_debit);
``entry_slippage`` is the fill-vs-mid cost per contract (positive = filled
worse than mid). Both nullable — NULL means "unknown", never coerce to 0.

NOT applied automatically: data is live in Timescale; run by hand with
operator sign-off.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0007"
down_revision: Union[str, None] = "0006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS mid_at_submit NUMERIC(10, 4)")
    op.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry_slippage NUMERIC(10, 4)")


def downgrade() -> None:
    op.execute("ALTER TABLE trades DROP COLUMN IF EXISTS entry_slippage")
    op.execute("ALTER TABLE trades DROP COLUMN IF EXISTS mid_at_submit")
