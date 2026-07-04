"""Ephemeral Postgres/Timescale database provisioning.

HermesTrader is Postgres/Timescale-only — there is no SQLite fallback. Both the
test suite and the simulation backtester need a *throwaway* database that is
isolated from the live ``hermes`` database, created fresh and dropped when done.
This module owns that lifecycle and the one-time extension setup the schema
needs (``timescaledb`` for the hypertables).

A "server DSN" here is a normal SQLAlchemy DSN whose database component points
at the maintenance database (``postgres``); the helpers create a uniquely-named
database on that same server and hand back a ready-to-use ``HermesDB`` DSN.
"""
from __future__ import annotations

import uuid
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg
from psycopg import sql

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

    The ``timescaledb`` extension is installed before any caller builds the ORM
    tables, because the hypertable conversions depend on it. When
    ``apply_schema`` is set, the ``schema.sql`` TimescaleDB addendum (raw
    ``bars_*`` tables, hypertables, compression, the ``pnl_daily`` view) is
    applied too — but note the ORM tables must already exist for the hypertable
    conversions to resolve, so callers that need the addendum build their
    ``HermesDB`` (which runs ``create_all``) first, then call
    :func:`apply_schema_addendum`.
    """
    name = f"{prefix}_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(_admin_dsn(server_dsn), autocommit=True) as conn:
        conn.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name)))

    new_dsn = _with_dbname(server_dsn, name)
    with psycopg.connect(_pq(new_dsn), autocommit=True) as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
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


def lock_as_template(dsn: str) -> None:
    """Pin a database so it can be used as a ``CREATE DATABASE`` template.

    TimescaleDB attaches a per-database background worker that holds an open
    session, which makes the database ineligible as a ``TEMPLATE`` (Postgres
    rejects the clone with "source database is being accessed by other users").
    Disallowing new connections and terminating the worker pins the database so
    it stays clone-able for the rest of the session. Idempotent.
    """
    name = _dbname(dsn)
    with psycopg.connect(_admin_dsn(dsn), autocommit=True) as conn:
        conn.execute(
            sql.SQL('ALTER DATABASE {} WITH ALLOW_CONNECTIONS false').format(sql.Identifier(name))
        )
        conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (name,),
        )


def clone_ephemeral_db(template_dsn: str, *, prefix: str = "hermes_eph") -> str:
    """Create a fresh database by cloning a locked template; return its DSN.

    Much cheaper than :func:`create_ephemeral_db`: it skips the ``timescaledb``
    extension load and the ORM ``create_all`` because the clone is a file-level
    copy of an already-provisioned template (built once, pinned with
    :func:`lock_as_template`). The clone inherits the template's
    ``ALLOW_CONNECTIONS=false``, so it is flipped back on before returning.
    """
    template = _dbname(template_dsn)
    name = f"{prefix}_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(_admin_dsn(template_dsn), autocommit=True) as conn:
        conn.execute(
            sql.SQL('CREATE DATABASE {} TEMPLATE {}').format(
                sql.Identifier(name), sql.Identifier(template)
            )
        )
        conn.execute(
            sql.SQL('ALTER DATABASE {} WITH ALLOW_CONNECTIONS true').format(sql.Identifier(name))
        )
    return _with_dbname(template_dsn, name)


def drop_ephemeral_db(dsn: str) -> None:
    """Drop a database created by :func:`create_ephemeral_db`. Best-effort."""
    name = _dbname(dsn)
    with psycopg.connect(_admin_dsn(dsn), autocommit=True) as conn:
        conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (name,),
        )
        conn.execute(
            sql.SQL('DROP DATABASE IF EXISTS {}').format(sql.Identifier(name))
        )
