"""Schema-parity guardrail.

The persistence layer has two legitimate schema artifacts that must agree:

* the **ORM** (`hermes.db.orm.Base.metadata`) — what `create_all` provisions on
  *every* backend (SQLite and Postgres), and the source the runtime reconciler
  (`HermesDB.run_migrations`) derives column self-heal from;
* **schema.sql** — the canonical Postgres/TimescaleDB DDL (hypertables,
  compression, the `pnl_daily` view) that the Alembic baseline applies.

Historically these drifted silently — e.g. `trades.entry_features` was added to
one but not the other and a deployed instance broke on upgrade. This test makes
that drift a loud CI failure instead.

It compares **column names** of every table the two sides share. Types and
defaults are intentionally *not* compared: a column may legitimately be `JSONB`
on Postgres and `JSON` on SQLite, or carry a Python-side default rather than a
server default. Names are where the painful drift happens.

Two categories of table live in only one artifact *by design*; they are
allow-listed here with the reason. Adding a new table to one side without the
other — or extending a shared table on only one side — fails this test until the
divergence is either reconciled or consciously allow-listed.
"""
from __future__ import annotations

import re
from pathlib import Path

from hermes.db.orm import Base

# Populate Base.metadata deterministically, independent of test order: import
# every module that registers ORM tables. The core tables come in via
# hermes.db.orm above; the ML tables live in modules that no-op when their
# optional deps are missing, so import them best-effort.
for _mod in ("hermes.ml.ledger", "hermes.ml.regime_weights"):
    try:
        __import__(_mod)
    except Exception:                                          # pragma: no cover
        pass

SCHEMA_SQL = Path(__file__).resolve().parents[1] / "hermes" / "db" / "schema.sql"

# Tables the ORM owns on BOTH backends via create_all, deliberately kept out of
# schema.sql (which is the Postgres/Timescale baseline). event_ledger and
# doctrine_embeddings are newer; doctrine_embeddings in particular needs the
# pgvector extension on Postgres, so its DDL lives in the dialect-aware ORM
# (SafeVector → vector/Text) rather than as static SQL.
ORM_ONLY_TABLES = {"event_ledger", "doctrine_embeddings"}

# Same idea, but defined in optional ML modules that only register their table
# when their deps import. Allow-listed for the divergence check but not asserted
# to exist (a CI run without the ML extras won't have loaded them).
ORM_ONLY_OPTIONAL = {"prediction_ledger", "regime_weights"}

# Tables that exist only as raw Postgres/TimescaleDB hypertables and are never
# provisioned through the ORM (they are written/read by the time-series paths,
# not the declarative models).
SQL_ONLY_TABLES = {"bars_daily", "bars_intraday"}


def _schema_sql_columns() -> dict[str, set[str]]:
    """Parse schema.sql into {table: {column, ...}}.

    Captures both the columns declared in each ``CREATE TABLE`` body and any
    added later via ``ALTER TABLE … ADD COLUMN`` (schema.sql self-heals older
    columns this way, so the effective column set is the union of both).
    """
    sql = SCHEMA_SQL.read_text()
    tables: dict[str, set[str]] = {}

    for m in re.finditer(
        r"CREATE TABLE IF NOT EXISTS (\w+)\s*\((.*?)\n\);", sql, re.S
    ):
        name, body = m.group(1), m.group(2)
        cols: set[str] = set()
        for raw in body.splitlines():
            line = raw.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue
            # Skip table-level constraint clauses.
            if re.match(
                r"(PRIMARY KEY|FOREIGN KEY|UNIQUE|CONSTRAINT|CHECK)\b", line, re.I
            ):
                continue
            token = line.split()[0].strip('"')
            if token.isidentifier():
                cols.add(token)
        tables[name] = cols

    for m in re.finditer(
        r"ALTER TABLE (\w+)\s+ADD COLUMN(?:\s+IF NOT EXISTS)?\s+(\w+)",
        sql, re.I,
    ):
        table, col = m.group(1), m.group(2)
        tables.setdefault(table, set()).add(col)

    return tables


def _orm_columns() -> dict[str, set[str]]:
    return {
        t.name: {c.name for c in t.columns}
        for t in Base.metadata.sorted_tables
    }


def test_no_unclassified_table_divergence():
    """Every table is in both artifacts, or in a documented allow-list."""
    orm = set(_orm_columns())
    sql = set(_schema_sql_columns())

    orm_only = orm - sql - ORM_ONLY_TABLES - ORM_ONLY_OPTIONAL
    sql_only = sql - orm - SQL_ONLY_TABLES

    assert not orm_only, (
        "Tables in the ORM but missing from schema.sql (and not allow-listed "
        f"as ORM-only): {sorted(orm_only)}. Either add them to schema.sql or, "
        "if create_all should own them on both backends, add them to "
        "ORM_ONLY_TABLES here with a reason."
    )
    assert not sql_only, (
        "Tables in schema.sql but missing from the ORM (and not allow-listed "
        f"as Postgres-only): {sorted(sql_only)}."
    )


def test_shared_tables_have_matching_columns():
    """For every table both artifacts define, their column sets must agree."""
    orm = _orm_columns()
    sql = _schema_sql_columns()

    mismatches: dict[str, dict[str, list[str]]] = {}
    for table in sorted(set(orm) & set(sql)):
        only_orm = orm[table] - sql[table]
        only_sql = sql[table] - orm[table]
        if only_orm or only_sql:
            mismatches[table] = {
                "missing_from_schema_sql": sorted(only_orm),
                "missing_from_orm": sorted(only_sql),
            }

    assert not mismatches, (
        "Column drift between the ORM and schema.sql for shared tables — "
        "reconcile both sides:\n" + "\n".join(
            f"  {t}: {d}" for t, d in mismatches.items()
        )
    )


def test_allowlisted_tables_still_exist():
    """Guard against stale allow-lists: every allow-listed table must be real."""
    orm = set(_orm_columns())
    sql = set(_schema_sql_columns())
    assert ORM_ONLY_TABLES <= orm, (
        f"ORM_ONLY_TABLES names a table not in the ORM: {sorted(ORM_ONLY_TABLES - orm)}"
    )
    assert SQL_ONLY_TABLES <= sql, (
        f"SQL_ONLY_TABLES names a table not in schema.sql: {sorted(SQL_ONLY_TABLES - sql)}"
    )
