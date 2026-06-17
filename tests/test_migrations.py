"""Alembic baseline — offline checks that need no live database.

CI runs these without Postgres: the baseline module is imported by path, and
the migration is rendered in Alembic's *offline* (``--sql``) mode, which uses
the dialect to emit SQL without ever opening a connection.
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
    # Core tables + the Timescale-specific bits that only live in schema.sql.
    for needle in (
        "strategies", "trades", "predictions", "system_settings",
        "create_hypertable", "add_compression_policy", "pnl_daily",
    ):
        assert needle in sql, f"baseline SQL missing {needle!r}"


# ---------------------------------------------------------------------------
# Boot-time self-heal migrations (HermesDB.run_migrations)
#
# These run at agent/watcher boot and must bring an older DB up to the current
# schema — including create_all-bootstrapped DBs, where create_all never alters
# existing tables. Guards the exact regression that took the paper bot down: a
# new column/table shipped in code but missing from MIGRATIONS, so the running
# instance crashed on image upgrade (trades.entry_features).
# ---------------------------------------------------------------------------
from hermes.db.models import HermesDB                              # noqa: E402

_MIGRATIONS = HermesDB.MIGRATIONS


def test_every_self_heal_migration_is_idempotent():
    for stmt in _MIGRATIONS:
        assert "IF NOT EXISTS" in stmt, f"non-idempotent migration: {stmt}"


def test_phase0_entry_features_is_self_healed():
    assert any("ADD COLUMN IF NOT EXISTS entry_features" in s for s in _MIGRATIONS), \
        "trades.entry_features missing from run_migrations — Phase-0 capture breaks on upgrade"


def test_phase3_exit_ticks_is_self_healed():
    assert any("CREATE TABLE IF NOT EXISTS exit_ticks" in s for s in _MIGRATIONS), \
        "exit_ticks missing from run_migrations — Phase-3 capture breaks on upgrade"
    assert any("idx_exit_ticks_trade" in s for s in _MIGRATIONS), \
        "exit_ticks index missing from run_migrations"


def test_late_added_trade_columns_are_all_covered():
    joined = " ".join(_MIGRATIONS)
    for col in ("broker_order_id", "tag", "close_tag", "exit_price",
                "entry_features"):
        assert f"ADD COLUMN IF NOT EXISTS {col}" in joined, f"{col} not self-healed"
