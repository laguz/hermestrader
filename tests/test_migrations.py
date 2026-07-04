"""Alembic baseline + boot-time self-heal.

The baseline checks are offline: the module is imported by path and the
migration is rendered in Alembic's *offline* (``--sql``) mode, which emits SQL
without opening a connection — so they need no server. The ``run_migrations``
self-heal tests further down exercise the real reconciler against a live
Timescale database and skip when no server is reachable.
"""
from __future__ import annotations

import importlib.util
import io
from pathlib import Path

import pytest

from alembic import command
from alembic.config import Config

REPO_ROOT = Path(__file__).resolve().parents[1]
BASELINE = REPO_ROOT / "alembic" / "versions" / "0001_baseline.py"


def _load_baseline():
    spec = importlib.util.spec_from_file_location("baseline_0001", BASELINE)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _config() -> Config:
    cfg = Config(str(REPO_ROOT / "alembic.ini"))
    # Make the script location absolute so the test passes regardless of cwd.
    cfg.set_main_option("script_location", str(REPO_ROOT / "alembic"))
    return cfg


def test_baseline_revision_metadata():
    mod = _load_baseline()
    assert mod.revision == "0001"
    assert mod.down_revision is None          # it is the base
    assert callable(mod.upgrade)
    assert callable(mod.downgrade)


def test_baseline_downgrade_refuses():
    mod = _load_baseline()
    with pytest.raises(NotImplementedError):
        mod.downgrade()


def test_schema_sql_is_the_baseline_source():
    mod = _load_baseline()
    assert mod.SCHEMA_SQL == REPO_ROOT / "hermes" / "db" / "schema.sql"
    assert mod.SCHEMA_SQL.exists()


def test_offline_upgrade_emits_full_schema():
    """Render base→0001 as SQL (no DB) and confirm the schema lands.

    This exercises env.py end-to-end: it must read the DSN from settings,
    configure the dialect, and run the baseline's ``op.execute`` statements.
    """
    buf = io.StringIO()
    cfg = _config()
    cfg.output_buffer = buf
    command.upgrade(cfg, "0001", sql=True)
    sql = buf.getvalue()

    upper = sql.upper()
    assert "CREATE TABLE" in upper
    # Tables come from metadata.create_all; the Timescale-specific bits
    # (create_hypertable / compression / pnl_daily) come from the schema.sql
    # addendum. Both must land in a single base→0001 render.
    for needle in (
        "strategies", "trades", "predictions", "system_settings",
        "create_hypertable", "add_compression_policy", "pnl_daily",
    ):
        assert needle in sql, f"baseline SQL missing {needle!r}"


# ---------------------------------------------------------------------------
# Boot-time self-heal (HermesDB.run_migrations)
#
# run_migrations runs at agent/watcher boot and must bring an older DB up to the
# current schema — including create_all-bootstrapped DBs, where create_all never
# alters existing tables. It is now *derived* from the ORM (no hand-maintained
# statement list), so instead of asserting on a static list we exercise the real
# behavior: stand up a DB that predates several columns + a whole table, run the
# reconciler, and confirm the gaps are healed. This guards the exact regression
# that took the paper bot down (trades.entry_features missing on upgrade).
#
# Unlike the offline checks above, these need a live Timescale server (they
# skip via the ``pg_available``/``ephemeral_dsn`` fixtures when none is set).
# ---------------------------------------------------------------------------
import psycopg                                                    # noqa: E402

from sqlalchemy import create_engine, inspect                    # noqa: E402

from hermes.db.models import HermesDB                             # noqa: E402


def _make_stale_db(dsn: str) -> None:
    """Create a stale schema that predates entry_features/tag/.../expires_at + exit_ticks."""
    with psycopg.connect(dsn.replace("+psycopg", ""), autocommit=True) as con:
        con.execute(
            "CREATE TABLE strategies ("
            "strategy_id TEXT PRIMARY KEY, priority INTEGER, "
            "status TEXT, created_at TIMESTAMPTZ)"
        )
        con.execute(
            "CREATE TABLE trades ("
            "id BIGINT PRIMARY KEY, opened_at TIMESTAMPTZ, strategy_id TEXT, "
            "symbol TEXT, side_type TEXT, lots INTEGER, status TEXT)"
        )
        con.execute(
            "CREATE TABLE pending_approvals ("
            "id BIGINT PRIMARY KEY, created_at TIMESTAMPTZ, strategy_id TEXT, "
            "symbol TEXT, action_type TEXT, action_json TEXT, status TEXT)"
        )


async def test_run_migrations_self_heals_columns_and_tables(ephemeral_dsn):
    dsn = ephemeral_dsn()
    _make_stale_db(dsn)

    # __init__'s create_all skips the pre-existing (stale) tables via
    # checkfirst; run_migrations is what must add the missing columns.
    db = HermesDB(dsn)
    await db.run_migrations()

    eng = create_engine(dsn)
    try:
        insp = inspect(eng)
        trade_cols = {c["name"] for c in insp.get_columns("trades")}
        for col in ("broker_order_id", "tag", "close_tag", "exit_price",
                    "entry_features"):
            assert col in trade_cols, f"trades.{col} not self-healed on upgrade"

        appr_cols = {c["name"] for c in insp.get_columns("pending_approvals")}
        assert "expires_at" in appr_cols, "pending_approvals.expires_at not self-healed"

        # A table absent from the stale DB is created (Phase-3 exit capture).
        assert insp.has_table("exit_ticks"), "exit_ticks table not created on upgrade"
    finally:
        eng.dispose()
        db.engine.dispose()
        from .conftest import _safe_dispose_async_engine
        _safe_dispose_async_engine(db.async_engine)


async def test_run_migrations_is_idempotent(db):
    """Second pass over an already-current DB is a clean no-op."""
    from sqlalchemy import inspect

    await db.run_migrations()

    def get_db_schema_details(conn):
        insp = inspect(conn)
        tables = insp.get_table_names()
        columns = {}
        for table in tables:
            columns[table] = {col["name"] for col in insp.get_columns(table)}
        return tables, columns

    async with db.async_engine.connect() as conn:
        tables_before, columns_before = await conn.run_sync(get_db_schema_details)

    await db.run_migrations()    # must not raise

    async with db.async_engine.connect() as conn:
        tables_after, columns_after = await conn.run_sync(get_db_schema_details)

    assert set(tables_before) == set(tables_after)
    assert columns_before == columns_after
