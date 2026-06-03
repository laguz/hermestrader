"""baseline — current HermesTrader TimescaleDB schema

Revision ID: 0001
Revises:
Create Date: 2026-06-02

Captures the existing schema as the migration baseline. ``upgrade()`` applies
``hermes/db/schema.sql`` verbatim — the single canonical DDL artifact (tables,
TimescaleDB hypertables, compression/retention policies, indexes, and the
``pnl_daily`` view). All statements are idempotent (``IF NOT EXISTS`` /
``if_not_exists => TRUE``), so this is safe to run against a fresh database.

For an existing populated database, do **not** run upgrade — stamp it as
already-migrated instead::

    alembic stamp 0001

``downgrade()`` is intentionally unimplemented: this is the baseline, and
dropping the entire schema is never the intended migration direction.
"""
from __future__ import annotations

from pathlib import Path
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Repo root is three parents up: versions/ -> alembic/ -> repo root.
SCHEMA_SQL = Path(__file__).resolve().parents[2] / "hermes" / "db" / "schema.sql"


def upgrade() -> None:
    sql = SCHEMA_SQL.read_text(encoding="utf-8")
    # Same split as HermesDB.init_schema: schema.sql is plain DDL with no
    # dollar-quoted bodies, so splitting on ';' yields whole statements.
    for stmt in (s.strip() for s in sql.split(";")):
        if stmt:
            op.execute(stmt)


def downgrade() -> None:
    raise NotImplementedError(
        "0001 is the schema baseline; there is no downgrade. To rebuild from "
        "scratch, drop the database and re-run `alembic upgrade head`."
    )
