"""drop unrealized_pnl_pct — remove unused unrealized_pnl_pct column from exit_ticks table

Revision ID: 0006
Revises: 0005
Create Date: 2026-07-04
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0006"
down_revision: Union[str, None] = "0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE exit_ticks DROP COLUMN IF EXISTS unrealized_pnl_pct")


def downgrade() -> None:
    op.execute("ALTER TABLE exit_ticks ADD COLUMN unrealized_pnl_pct DOUBLE PRECISION")
