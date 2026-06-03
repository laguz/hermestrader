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
