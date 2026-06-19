"""baseline — current HermesTrader schema (ORM tables + TimescaleDB addendum)

Revision ID: 0001
Revises:
Create Date: 2026-06-02

Provisions a fresh database in two steps from the single source of truth:

1. **Tables from the ORM.** ``Base.metadata`` (``hermes/db/orm.py``) is the
   authoritative catalog of every table, column, and index. ``upgrade()``
   emits their DDL via ``metadata.create_all`` so the migration can never drift
   from the models the application actually uses.
2. **TimescaleDB addendum.** ``hermes/db/schema.sql`` adds only what the ORM
   cannot express — the raw ``bars_*`` tables, hypertable conversions,
   compression/retention policies, and the ``pnl_daily`` view — applied *after*
   the ORM tables exist.

All statements are idempotent (``create_all(checkfirst=True)`` online,
``IF NOT EXISTS`` / ``if_not_exists => TRUE`` in the addendum), so this is safe
to re-run against a fresh database. In Alembic *offline* (``--sql``) mode the
tables are emitted unconditionally (``checkfirst`` reflection has no live DB).

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
    from hermes.db.orm import Base

    bind = op.get_bind()
    offline = op.get_context().as_sql

    # 1) ORM tables — the authoritative catalog. Offline (--sql) has no live DB
    #    to reflect, so emit unconditionally; online, checkfirst keeps it
    #    idempotent on a partially-provisioned database.
    Base.metadata.create_all(bind, checkfirst=not offline)

    # 2) TimescaleDB addendum — bars_* tables, hypertables, compression, view.
    #    schema.sql is plain DDL with no dollar-quoted bodies, so splitting on
    #    ';' yields whole statements.
    addendum = SCHEMA_SQL.read_text(encoding="utf-8")
    for stmt in (s.strip() for s in addendum.split(";")):
        if stmt:
            op.execute(stmt)


def downgrade() -> None:
    raise NotImplementedError(
        "0001 is the schema baseline; there is no downgrade. To rebuild from "
        "scratch, drop the database and re-run `alembic upgrade head`."
    )
