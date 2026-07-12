"""Schema-seam guardrail.

The persistence layer has a single source of truth for table/column structure —
the **ORM** (`hermes.db.orm.Base.metadata`), which `create_all` provisions on
every backend and which the Alembic baseline generates its tables from. The only
schema that lives outside the ORM is the irreducible TimescaleDB layer in
**schema.sql**: the raw `bars_*` tables, hypertable conversions, compression
policies, and the `pnl_daily` view, applied *after* the ORM tables exist.

Because columns are no longer duplicated between the two artifacts, there is no
column drift to police. What remains is a narrow *seam*, and this test guards it:

1. **schema.sql never re-declares an ORM table.** Any `CREATE TABLE` in the
   addendum must be one of the SQL-only `bars_*` tables — otherwise the column
   duplication this refactor removed has crept back in.
2. **Every hypertable-backed ORM table has its `create_hypertable` line.** A
   time-partitioned table that the ORM creates but the addendum forgets to
   convert would silently run as a plain Postgres table (the "added a table,
   forgot the Timescale bits" failure mode).
3. **No orphan hypertable targets.** Every `create_hypertable('x', …)` names a
   table that actually exists in the ORM or as a SQL-only table — catches typos
   and tables deleted on one side only.
"""
from __future__ import annotations

import re
from pathlib import Path

from hermes.db.orm import Base

# Base.metadata is populated by importing hermes.db.orm above. The Phase-0
# teardown removed the ML modules (ledger / regime_weights) that used to
# register their own ORM tables here, so there is nothing further to import.

SCHEMA_SQL = Path(__file__).resolve().parents[1] / "hermes" / "db" / "schema.sql"

# Tables that exist ONLY as raw Postgres/TimescaleDB tables in schema.sql and
# are never modelled as ORM classes (written/read by the time-series paths).
# These are the only `CREATE TABLE`s allowed to appear in the addendum.
SQL_ONLY_TABLES = {"bars_daily", "bars_intraday"}

# ORM tables that are time-partitioned and MUST be converted to hypertables by
# the addendum. Keep this in lock-step with the time-series tables in orm.py.
HYPERTABLE_ORM_TABLES = {
    "trades", "pending_orders", "bot_logs", "ai_decisions", "predictions", "implied_volatility",
    "portfolio_greeks_snapshots",
}


def _orm_tables() -> set[str]:
    return {t.name for t in Base.metadata.sorted_tables}


def _schema_sql_created_tables() -> set[str]:
    sql = SCHEMA_SQL.read_text()
    return set(re.findall(r"CREATE TABLE(?:\s+IF NOT EXISTS)?\s+(\w+)", sql))


def _schema_sql_hypertable_targets() -> set[str]:
    sql = SCHEMA_SQL.read_text()
    return set(re.findall(r"create_hypertable\(\s*'(\w+)'", sql))


def test_schema_sql_declares_no_orm_tables():
    """The addendum must not re-declare any ORM table's columns."""
    created = _schema_sql_created_tables()
    orm = _orm_tables()
    re_declared = created & orm
    assert not re_declared, (
        "schema.sql re-declares ORM-owned tables: "
        f"{sorted(re_declared)}. Columns belong to hermes/db/orm.py only; the "
        "addendum should hold hypertable/compression/view DDL, not CREATE TABLE "
        "for tables the ORM already owns."
    )
    # And every CREATE TABLE it *does* have is an expected SQL-only table.
    unexpected = created - SQL_ONLY_TABLES
    assert not unexpected, (
        "schema.sql creates tables that are neither ORM-owned nor allow-listed "
        f"SQL-only: {sorted(unexpected)}. Add them to the ORM, or to "
        "SQL_ONLY_TABLES here with a reason."
    )


def test_hypertable_orm_tables_are_converted():
    """Every time-partitioned ORM table has a create_hypertable line."""
    targets = _schema_sql_hypertable_targets()
    orm = _orm_tables()

    # The named ORM tables must actually exist (guard a stale list)...
    missing_from_orm = HYPERTABLE_ORM_TABLES - orm
    assert not missing_from_orm, (
        "HYPERTABLE_ORM_TABLES names tables not in the ORM: "
        f"{sorted(missing_from_orm)}."
    )
    # ...and each must be converted by the addendum.
    not_converted = HYPERTABLE_ORM_TABLES - targets
    assert not not_converted, (
        "ORM tables that should be hypertables but have no create_hypertable in "
        f"schema.sql: {sorted(not_converted)}. They would run as plain tables — "
        "add the create_hypertable line (or drop them from HYPERTABLE_ORM_TABLES)."
    )


def test_no_orphan_hypertable_targets():
    """Every create_hypertable target is a real ORM table or SQL-only table."""
    targets = _schema_sql_hypertable_targets()
    known = _orm_tables() | SQL_ONLY_TABLES
    orphans = targets - known
    assert not orphans, (
        "schema.sql converts tables that exist in neither the ORM nor "
        f"SQL_ONLY_TABLES: {sorted(orphans)} — likely a typo or a table removed "
        "on only one side."
    )


def test_sql_only_tables_are_created():
    """Guard against a stale allow-list: every SQL-only table is really created."""
    created = _schema_sql_created_tables()
    missing = SQL_ONLY_TABLES - created
    assert not missing, (
        f"SQL_ONLY_TABLES names tables not created in schema.sql: {sorted(missing)}"
    )
