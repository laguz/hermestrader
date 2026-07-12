"""implied volatility — add implied_volatility table

Revision ID: 0008
Revises: 0007
Create Date: 2026-07-12

NOT applied automatically: data is live in Timescale; run by hand with
operator sign-off.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0008"
down_revision: Union[str, None] = "0007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "CREATE TABLE IF NOT EXISTS implied_volatility ("
        "  ts TIMESTAMPTZ NOT NULL,"
        "  symbol TEXT NOT NULL,"
        "  iv NUMERIC(12, 6) NOT NULL,"
        "  PRIMARY KEY (symbol, ts)"
        ")"
    )
    # Convert to TimescaleDB hypertable
    op.execute("SELECT create_hypertable('implied_volatility', 'ts', if_not_exists => TRUE)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS implied_volatility")
