"""portfolio greeks snapshot — add portfolio_greeks_snapshots table

Revision ID: 0009
Revises: 0008
Create Date: 2026-07-12

NOT applied automatically: data is live in Timescale; run by hand with
operator sign-off.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "0009"
down_revision: Union[str, None] = "0008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        "CREATE TABLE IF NOT EXISTS portfolio_greeks_snapshots ("
        "  ts TIMESTAMPTZ NOT NULL PRIMARY KEY,"
        "  net_delta NUMERIC(12, 4) NOT NULL,"
        "  net_vega NUMERIC(12, 4) NOT NULL,"
        "  net_theta NUMERIC(12, 4) NOT NULL"
        ")"
    )
    # Convert to TimescaleDB hypertable
    op.execute("SELECT create_hypertable('portfolio_greeks_snapshots', 'ts', if_not_exists => TRUE)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS portfolio_greeks_snapshots")
