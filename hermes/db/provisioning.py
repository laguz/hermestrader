"""Ephemeral Postgres/Timescale database provisioning.

HermesTrader is Postgres/Timescale-only — there is no SQLite fallback. Both the
test suite and the simulation backtester need a *throwaway* database that is
isolated from the live ``hermes`` database, created fresh and dropped when done.
This module owns that lifecycle and the one-time extension setup the schema
needs (``timescaledb`` for the hypertables, ``vector`` for the doctrine
embeddings).

A "server DSN" here is a normal SQLAlchemy DSN whose database component points
at the maintenance database (``postgres``); the helpers create a uniquely-named
database on that same server and hand back a ready-to-use ``HermesDB`` DSN.
"""
from __future__ import annotations

import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg

# Default server DSN for local dev: the docker-compose ``db`` service published
# on host port 5433. Override with ``HERMES_TEST_DSN``.
DEFAULT_SERVER_DSN = "postgresql+psycopg://hermes:hermes@localhost:5433/postgres"

_SCHEMA_SQL = Path(__file__).with_name("schema.sql")


def _pq(dsn: str) -> str:
    """Strip the SQLAlchemy ``+driver`` suffix so psycopg can connect directly."""
    return dsn.replace("+psycopg", "").replace("+asyncpg", "")


def _with_dbname(dsn: str, name: str) -> str:
    parts = urlsplit(dsn)
    return urlunsplit((parts.scheme, parts.netloc, f"/{name}", parts.query, parts.fragment))


def _dbname(dsn: str) -> str:
    return urlsplit(dsn).path.lstrip("/")


def _admin_dsn(dsn: str) -> str:
    """A psycopg-connectable DSN pointed at the server's ``postgres`` database."""
    return _pq(_with_dbname(dsn, "postgres"))


def create_ephemeral_db(server_dsn: str = DEFAULT_SERVER_DSN, *, prefix: str = "hermes_eph",
                        apply_schema: bool = False) -> str:
    """Create a fresh, uniquely-named database and return its SQLAlchemy DSN.

    The ``timescaledb`` and ``vector`` extensions are installed before any
    caller builds the ORM tables, because the doctrine-embedding ``vector``
    column and the hypertable conversions both depend on them. When
    ``apply_schema`` is set, the ``schema.sql`` TimescaleDB addendum (raw
    ``bars_*`` tables, hypertables, compression, the ``pnl_daily`` view) is
    applied too — but note the ORM tables must already exist for the hypertable
    conversions to resolve, so callers that need the addendum build their
    ``HermesDB`` (which runs ``create_all``) first, then call
    :func:`apply_schema_addendum`.
    """
    name = f"{prefix}_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(_admin_dsn(server_dsn), autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{name}"')

    new_dsn = _with_dbname(server_dsn, name)
    with psycopg.connect(_pq(new_dsn), autocommit=True) as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    if apply_schema:
        apply_schema_addendum(new_dsn)
    return new_dsn


def apply_schema_addendum(dsn: str) -> None:
    """Apply the ``schema.sql`` TimescaleDB addendum to an existing database.

    Idempotent. Run *after* the ORM tables exist so the ``create_hypertable``
    statements resolve. ``--`` line comments are stripped first (they may
    contain ``;``), then statements are split on ``;`` — ``schema.sql`` is kept
    free of functions / dollar-quoting so this split is safe.
    """
    raw = _SCHEMA_SQL.read_text(encoding="utf-8")
    sql = "\n".join(line.split("--", 1)[0] for line in raw.splitlines())
    with psycopg.connect(_pq(dsn), autocommit=True) as conn:
        for stmt in (s.strip() for s in sql.split(";")):
            if stmt:
                conn.execute(stmt)


def drop_ephemeral_db(dsn: str) -> None:
    """Drop a database created by :func:`create_ephemeral_db`. Best-effort."""
    name = _dbname(dsn)
    with psycopg.connect(_admin_dsn(dsn), autocommit=True) as conn:
        conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (name,),
        )
        conn.execute(f'DROP DATABASE IF EXISTS "{name}"')
