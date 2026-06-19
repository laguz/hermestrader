"""Shared pytest fixtures.

HermesTrader is Postgres/Timescale-only, so the DB-backed tests run against a
real Timescale server. Point ``HERMES_TEST_DSN`` at a server's maintenance
database (``.../postgres``); each test that asks for a database gets a fresh,
isolated throwaway database that is dropped at teardown. The default targets the
docker-compose ``db`` service on host port 5433.

The server is contacted *lazily* — only when a test actually requests the ``db``
or ``make_db`` fixture — so the majority of tests, which use the stub
broker/DB pattern and never touch a real database, still run with no server
present. When no server is reachable, the DB-backed tests skip rather than fail.
"""
from __future__ import annotations

import os

import psycopg
import pytest

from hermes.db.provisioning import (
    apply_schema_addendum,
    create_ephemeral_db,
    drop_ephemeral_db,
)

TEST_SERVER_DSN = os.environ.get(
    "HERMES_TEST_DSN", "postgresql+psycopg://hermes:hermes@localhost:5433/postgres"
)


def _server_reachable() -> bool:
    try:
        with psycopg.connect(
            TEST_SERVER_DSN.replace("+psycopg", ""), connect_timeout=3
        ) as conn:
            conn.execute("SELECT 1")
        return True
    except Exception:  # noqa: BLE001 — any connection failure means "no server"
        return False


@pytest.fixture(scope="session")
def pg_available():
    """Skip the dependent test unless a Timescale server is reachable."""
    if not _server_reachable():
        pytest.skip(
            "No Postgres/Timescale server reachable — set HERMES_TEST_DSN "
            "(default postgresql+psycopg://hermes:hermes@localhost:5433/postgres)"
        )


@pytest.fixture
def make_db(pg_available):
    """Factory yielding fresh ``HermesDB`` instances on isolated throwaway DBs.

    Call ``make_db()`` for the common case (ORM tables only), or
    ``make_db(schema=True)`` when the test needs the TimescaleDB addendum
    (raw ``bars_*`` tables / hypertables / the ``pnl_daily`` view). Every
    database created during the test is dropped at teardown.
    """
    import asyncio

    from hermes.db.models import HermesDB

    created: list[tuple[str, "HermesDB"]] = []

    def _make(schema: bool = False):
        dsn = create_ephemeral_db(TEST_SERVER_DSN, prefix="hermes_test")
        db = HermesDB(dsn)  # __init__ runs create_all for the ORM tables
        created.append((dsn, db))
        if schema:
            apply_schema_addendum(dsn)
        return db

    yield _make

    for dsn, db in created:
        # Dispose pooled connections before dropping so the drop isn't blocked
        # and async connections don't leak ResourceWarnings.
        try:
            db.engine.dispose()
        except Exception:  # noqa: BLE001 — teardown is best-effort
            pass
        try:
            asyncio.run(db.async_engine.dispose())
        except Exception:  # noqa: BLE001
            pass
        try:
            drop_ephemeral_db(dsn)
        except Exception:  # noqa: BLE001
            pass


@pytest.fixture
def db(make_db):
    """A single fresh ``HermesDB`` (ORM tables only) — the common case."""
    return make_db()


@pytest.fixture
def ephemeral_dsn(pg_available):
    """Factory yielding raw, *empty* throwaway-database DSNs.

    Unlike ``make_db``, this does not build a ``HermesDB`` or run ``create_all``
    — the database has only the extensions installed. Used by the migration
    self-heal tests, which must stand up an intentionally stale schema before
    the reconciler runs. Every database created is dropped at teardown.
    """
    created: list[str] = []

    def _make() -> str:
        dsn = create_ephemeral_db(TEST_SERVER_DSN, prefix="hermes_raw")
        created.append(dsn)
        return dsn

    yield _make

    for dsn in created:
        try:
            drop_ephemeral_db(dsn)
        except Exception:  # noqa: BLE001 — teardown is best-effort
            pass
