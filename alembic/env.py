"""Alembic environment for HermesTrader.

The DB URL is taken from ``hermes.config.settings.hermes_dsn`` (which honours
the ``HERMES_DSN`` env override the app uses) rather than being hardcoded in
``alembic.ini`` — one source of connection truth.

A **synchronous** engine is used deliberately: migrations are a one-shot ops
step with no concurrency to overlap, and ``settings.hermes_dsn`` is already a
sync ``postgresql+psycopg://`` URL. ``target_metadata`` points at the ORM
``Base.metadata`` so future ``alembic revision --autogenerate`` can diff
against the models.

This module is import-safe with no database present (it only *configures*
Alembic); a connection is opened lazily, and only when a migration command
actually runs.
"""
from __future__ import annotations

from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool

from alembic import context

from hermes.config import settings
from hermes.db.models import Base

config = context.config

# Inject the app's DSN so migrations target the same database the app does.
config.set_main_option("sqlalchemy.url", settings.hermes_dsn)

if config.config_file_name is not None:
    # disable_existing_loggers=False is important: the default (True) would
    # silently disable every logger already configured — including the app's
    # ``hermes.*`` loggers — when env.py runs inside the same process (e.g.
    # the test suite or an app-embedded migration).
    fileConfig(config.config_file_name, disable_existing_loggers=False)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Emit SQL to stdout without a DB connection (``alembic upgrade --sql``)."""
    context.configure(
        url=config.get_main_option("sqlalchemy.url"),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live connection."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
