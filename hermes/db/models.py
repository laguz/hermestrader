"""
[TimescaleDB-Schema] — persistence facade for both services.

The ORM table classes and pure helpers now live in :mod:`hermes.db.orm`, and
the SQL surface is split across focused mixins in
:mod:`hermes.db.repositories`. This module re-exports those names and assembles
them into :class:`HermesDB`, so existing call-sites keep importing everything
from ``hermes.db.models`` unchanged::

    from hermes.db.models import HermesDB, Base, Trade, Prediction, ...

``HermesDB`` itself keeps only connection/engine setup and schema lifecycle
(``__init__`` / ``init_schema`` / ``run_migrations``); every query method is
contributed by a repository mixin.
"""
from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import (
    AsyncSession, async_sessionmaker, create_async_engine,
)
from sqlalchemy.orm import sessionmaker

# Re-exported so ``from hermes.db.models import Base, Trade, ...`` keeps working.
from hermes.db.orm import (  # noqa: F401
    AIDecision, Base, BotLog, EventLedger, ExitTick, PendingApproval, PendingOrder,
    Prediction, Strategy, StrategyWatchlist, SystemSetting, Trade,
    VetoSuppression, _close_reason_from_tag, _compute_realized_pnl,
    sync_to_async_dsn,
)
from hermes.db.repositories import (
    AnalyticsRepositoryMixin, ApprovalsRepositoryMixin,
    DecisionsRepositoryMixin, LogsRepositoryMixin, SettingsRepositoryMixin,
    TimeSeriesRepositoryMixin, TradesRepositoryMixin, WatchlistRepositoryMixin,
)

logger = logging.getLogger("hermes.db")

__all__ = [
    "Base", "Strategy", "StrategyWatchlist", "Trade", "PendingOrder",
    "PendingApproval", "VetoSuppression", "BotLog", "EventLedger", "AIDecision", "Prediction",
    "SystemSetting", "ExitTick", "HermesDB", "sync_to_async_dsn",
    "_close_reason_from_tag", "_compute_realized_pnl",
]


class HermesDB(
    LogsRepositoryMixin,
    DecisionsRepositoryMixin,
    TradesRepositoryMixin,
    WatchlistRepositoryMixin,
    ApprovalsRepositoryMixin,
    SettingsRepositoryMixin,
    TimeSeriesRepositoryMixin,
    AnalyticsRepositoryMixin,
):
    """Thin repo layer; matches the surface the engine + UI consume.

    The query methods come from the repository mixins above — each owns one
    concern (logs, trades, approvals, …) and shares the engine/session
    attributes set up here. This class itself only manages connections and
    schema lifecycle.
    """

    def __init__(self, dsn: str):
        # Adapt schema dynamically for SQLite compatibility
        if "sqlite" in dsn:
            from sqlalchemy import JSON
            from sqlalchemy.dialects.postgresql import JSONB
            for table in Base.metadata.tables.values():
                composite_pk = len(table.primary_key.columns) > 1
                if composite_pk:
                    for col in table.primary_key.columns:
                        if col.autoincrement:
                            col.autoincrement = False
                for col in table.columns:
                    if isinstance(col.type, JSONB):
                        col.type = JSON()

        self.engine = create_engine(dsn, pool_pre_ping=True, future=True)
        self.Session = sessionmaker(self.engine, expire_on_commit=False, future=True)

        async_dsn = sync_to_async_dsn(dsn)
        self.async_engine = create_async_engine(async_dsn, pool_pre_ping=True, future=True)
        self.AsyncSession = async_sessionmaker(self.async_engine, expire_on_commit=False, class_=AsyncSession, future=True)

        from hermes.db.timeseries import TimeSeriesEngine
        self.ts_engine = TimeSeriesEngine(self)

        try:
            Base.metadata.create_all(self.engine, checkfirst=True)
        except Exception:                                       # noqa: BLE001
            # Don't crash on import — the next real query surfaces the cause.
            pass
        self.engine.dispose()

    async def init_schema(self, schema_sql_path: str) -> None:
        with open(schema_sql_path, "r", encoding="utf-8") as fh:
            sql = fh.read()
        async with self.async_engine.begin() as conn:
            for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
                await conn.exec_driver_sql(stmt + ";")

    # ------------------------------------------------------------------
    # Schema migrations applied at agent/watcher boot. Every statement is
    # idempotent (IF NOT EXISTS), so a freshly-pulled image self-heals a DB
    # that predates a column/table — including ``create_all``-bootstrapped
    # DBs, where create_all never alters existing tables. **When a new
    # alembic migration adds a column or table, add the matching idempotent
    # statement here too**, or the running instance breaks on image upgrade
    # (this is exactly how trades.entry_features went missing).
    # ------------------------------------------------------------------
    MIGRATIONS: tuple[str, ...] = (
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS broker_order_id TEXT",
        "CREATE INDEX IF NOT EXISTS idx_trades_open_order_id "
        "ON trades(broker_order_id) WHERE status = 'OPEN'",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS tag TEXT",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS close_tag TEXT",
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS exit_price NUMERIC(10,4)",
        "ALTER TABLE pending_approvals ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ",
        # Phase 0 — per-trade entry-feature snapshot for outcome learning.
        "ALTER TABLE trades ADD COLUMN IF NOT EXISTS entry_features JSONB",
        # Phase 3 — per-tick exit-state trajectory capture.
        "CREATE TABLE IF NOT EXISTS exit_ticks ("
        "id BIGSERIAL PRIMARY KEY, "
        "ts TIMESTAMPTZ NOT NULL DEFAULT now(), "
        "trade_id BIGINT NOT NULL, "
        "strategy_id TEXT NOT NULL, "
        "symbol TEXT NOT NULL, "
        "dte INT, "
        "unrealized_pnl_pct DOUBLE PRECISION, "
        "debit DOUBLE PRECISION, "
        "entry_credit DOUBLE PRECISION, "
        "action TEXT NOT NULL DEFAULT 'hold', "
        "close_reason TEXT)",
        "CREATE INDEX IF NOT EXISTS idx_exit_ticks_trade ON exit_ticks(trade_id, ts)",
    )

    async def run_migrations(self) -> None:
        from sqlalchemy import text as sa_text
        async with self.async_engine.begin() as conn:
            for sql in self.MIGRATIONS:
                await conn.execute(sa_text(sql))
