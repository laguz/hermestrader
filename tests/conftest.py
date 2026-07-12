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
    clone_ephemeral_db,
    create_ephemeral_db,
    drop_ephemeral_db,
    lock_as_template,
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


def _safe_dispose_async_engine(engine) -> None:
    try:
        import asyncio
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None
        if loop and loop.is_running():
            loop.create_task(engine.dispose())
        else:
            asyncio.run(engine.dispose())
    except Exception as e:
        import logging
        logging.getLogger("tests.conftest").warning("Failed to dispose async engine: %s", e)
@pytest.fixture(autouse=True)
async def cleanup_tasks():
    yield
    try:
        import asyncio
        loop = asyncio.get_running_loop()
        try:
            await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            pass
        
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task(loop)]
        if tasks:
            for task in tasks:
                task.cancel()
            try:
                await asyncio.gather(*tasks, return_exceptions=True)
            except asyncio.CancelledError:
                pass
    except RuntimeError:
        pass


@pytest.fixture(scope="session")
def pg_available():
    """Skip the dependent test unless a Timescale server is reachable."""
    if not _server_reachable():
        pytest.skip(
            "No Postgres/Timescale server reachable — set HERMES_TEST_DSN "
            "(default postgresql+psycopg://hermes:hermes@localhost:5433/postgres)"
        )


@pytest.fixture(scope="session")
def _db_template(pg_available):
    """Session-scoped template-database factory; the per-test DBs clone these.

    Building the schema from scratch (extension load + ``create_all`` + the
    TimescaleDB addendum) costs ~0.5s and was paid once *per* DB-backed test.
    Instead we provision each schema variant once, pin it with
    :func:`lock_as_template`, and let every test clone it via ``CREATE DATABASE
    ... TEMPLATE`` — a file copy that's ~70% cheaper. Per-test isolation is
    unchanged: every test still gets its own fresh, throwaway database.

    Two variants are built lazily so the ``make_db(schema=...)`` contract holds
    exactly: ``schema=False`` clones carry only the ORM tables, ``schema=True``
    clones also carry the ``bars_*`` hypertables / ``pnl_daily`` view.
    """
    from hermes.db.models import HermesDB

    built: dict[bool, str] = {}

    def _template_for(schema: bool) -> str:
        if schema in built:
            return built[schema]
        dsn = create_ephemeral_db(TEST_SERVER_DSN, prefix="hermes_tmpl")
        db = HermesDB(dsn)  # __init__ runs create_all for the ORM tables
        if schema:
            apply_schema_addendum(dsn)
        db.engine.dispose()
        _safe_dispose_async_engine(db.async_engine)
        lock_as_template(dsn)
        built[schema] = dsn
        return dsn

    yield _template_for

    for dsn in built.values():
        try:
            drop_ephemeral_db(dsn)
        except Exception:  # noqa: BLE001 — teardown is best-effort
            pass


@pytest.fixture
def make_db(_db_template):
    """Factory yielding fresh ``HermesDB`` instances on isolated throwaway DBs.

    Call ``make_db()`` for the common case (ORM tables only), or
    ``make_db(schema=True)`` when the test needs the TimescaleDB addendum
    (raw ``bars_*`` tables / hypertables / the ``pnl_daily`` view). Each test
    gets its own fresh database — cloned from the matching session template
    (see ``_db_template``) — and every database created is dropped at teardown.
    """
    from hermes.db.models import HermesDB

    created: list[tuple[str, "HermesDB"]] = []

    def _make(schema: bool = False):
        dsn = clone_ephemeral_db(_db_template(schema), prefix="hermes_test")
        db = HermesDB(dsn)  # create_all is a no-op against the cloned schema
        created.append((dsn, db))
        return db

    yield _make

    for dsn, db in created:
        # Dispose pooled connections before dropping so the drop isn't blocked
        # and async connections don't leak ResourceWarnings.
        try:
            db.engine.dispose()
        except Exception:  # noqa: BLE001 — teardown is best-effort
            pass
        _safe_dispose_async_engine(db.async_engine)
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
