"""
[TimescaleDB-Schema] — persistence facade for both services.

The ORM table classes and pure helpers now live in :mod:`hermes.db.orm`, and
the SQL surface is split across focused mixins in
:mod:`hermes.db.repositories`. This module re-exports those names and assembles
them into :class:`HermesDB`, so existing call-sites keep importing everything
from ``hermes.db.models`` unchanged::

    from hermes.db.models import HermesDB, Base, Trade, Prediction, ...

``HermesDB`` itself keeps only connection/engine setup and schema lifecycle
(``__init__`` / ``run_migrations``); every query method is
contributed by a repository mixin.
"""
from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import (
    AsyncSession, async_sessionmaker, create_async_engine,
)
from sqlalchemy.orm import sessionmaker

# Re-exported so ``from hermes.db.models import Base, Trade, ...`` keeps working.
from hermes.db.orm import (
    AIDecision, Base, BotLog, EventLedger, ExitTick, OperatorCommand,
    PendingApproval, PendingOrder, Prediction, Strategy, StrategyWatchlist,
    SystemSetting, Trade, VetoSuppression, _close_reason_from_tag,
    _compute_realized_pnl, sync_to_async_dsn, PortfolioGreeksSnapshot,
)
from hermes.db.repositories import (
    AnalyticsRepository, ApprovalsRepository, CommandsRepository,
    DecisionsRepository, LogsRepository, SettingsRepository,
    TimeSeriesRepository, TradesRepository, WatchlistRepository,
)

logger = logging.getLogger("hermes.db")

__all__ = [
    "Base", "Strategy", "StrategyWatchlist", "Trade", "PendingOrder",
    "PendingApproval", "VetoSuppression", "BotLog", "EventLedger", "AIDecision", "Prediction",
    "SystemSetting", "OperatorCommand", "ExitTick", "HermesDB", "sync_to_async_dsn",
    "_close_reason_from_tag", "_compute_realized_pnl", "PortfolioGreeksSnapshot",
]


class HermesDB:
    """Thin repo layer; matches the surface the engine + UI consume.

    The query methods live on owned repositories — each owns one concern
    (``self.logs``, ``self.trades``, ``self.approvals``, …) and reads the
    engine/session handles set up here through its back-reference. Call sites
    use the explicit namespaced form (``db.logs.write_log(...)``). This class
    itself only manages connections and schema lifecycle.
    """

    def __init__(self, dsn: str):
        self.engine = create_engine(dsn, pool_pre_ping=True, future=True)
        self.Session = sessionmaker(self.engine, expire_on_commit=False, future=True)

        async_dsn = sync_to_async_dsn(dsn)
        self.async_engine = create_async_engine(async_dsn, pool_pre_ping=True, future=True)
        self.AsyncSession = async_sessionmaker(self.async_engine, expire_on_commit=False, class_=AsyncSession, future=True)

        from hermes.db.timeseries import TimeSeriesEngine
        self.ts_engine = TimeSeriesEngine(self)

        # Owned repositories — each contributes one concern's query methods and
        # reads the handles above via its back-reference to this instance.
        self.logs = LogsRepository(self)
        self.decisions = DecisionsRepository(self)
        self.trades = TradesRepository(self)
        self.watchlist = WatchlistRepository(self)
        self.approvals = ApprovalsRepository(self)
        self.commands = CommandsRepository(self)
        self.settings = SettingsRepository(self)
        self.timeseries = TimeSeriesRepository(self)
        self.analytics = AnalyticsRepository(self)

        try:
            Base.metadata.create_all(self.engine, checkfirst=True)
        except Exception as exc:
            # Don't crash on import — the next real query surfaces the cause.
            logger.warning("Initial schema creation failed (will retry on query): %s", exc)
        self.engine.dispose()

    # ------------------------------------------------------------------
    # Boot-time schema self-heal, applied at agent/watcher startup.
    #
    # The ORM (``Base.metadata``) is the single source of truth for every table
    # ``create_all`` owns, so this is *derived* from the models rather than a
    # hand-maintained list of ALTER statements — a derived diff can never fall
    # behind the models (the failure mode that took ``trades.entry_features``
    # down on an image upgrade). Postgres/Timescale objects the ORM cannot
    # express (hypertables, compression, the ``pnl_daily`` view) stay owned by
    # ``schema.sql`` / Alembic; ``tests/test_schema_parity.py`` keeps the two
    # sides honest.
    # ------------------------------------------------------------------
    async def run_migrations(self) -> None:
        """Bring the live Postgres/Timescale DB up to the current ORM, idempotently."""
        async with self.async_engine.begin() as conn:
            await conn.run_sync(self._reconcile_orm_schema)

        from pathlib import Path
        schema_path = Path(__file__).parent / "schema.sql"
        if schema_path.exists():
            with open(schema_path, "r", encoding="utf-8") as fh:
                raw_sql = fh.read()
            sql_clean = "\n".join(line.split("--", 1)[0] for line in raw_sql.splitlines())
            for stmt in [s.strip() for s in sql_clean.split(";") if s.strip()]:
                try:
                    async with self.async_engine.begin() as conn:
                        await conn.exec_driver_sql(stmt)
                except Exception as e:
                    logger.warning("Failed to run schema statement %r: %s", stmt[:50], e)

    async def get_setting(self, key: str, default: str | None = None) -> str | None:
        return await self.settings.get_setting(key, default)

    async def set_setting(self, key: str, value: str) -> None:
        await self.settings.set_setting(key, value)

    async def daily_bars(self, symbol: str, lookback_days: int = 400) -> Any:
        return await self.timeseries.daily_bars(symbol, lookback_days)

    async def intraday_bars(self, symbol: str, lookback_days: int = 10) -> Any:
        return await self.timeseries.intraday_bars(symbol, lookback_days)

    async def list_all_watchlists(self) -> Any:
        return await self.watchlist.list_all_watchlists()

    async def last_price(self, symbol: str) -> Any:
        return await self.timeseries.last_price(symbol)

    async def write_prediction(self, symbol: str, ret: float, price: float, spot: float = 0.0) -> Any:
        return await self.decisions.write_prediction(symbol, ret, price, spot)

    async def save_daily_bars(self, symbol: str, df: Any) -> Any:
        return await self.timeseries.save_daily_bars(symbol, df)

    async def save_intraday_bars(self, symbol: str, df: Any) -> Any:
        return await self.timeseries.save_intraday_bars(symbol, df)

    @staticmethod
    def _reconcile_orm_schema(sync_conn) -> None:
        """Add any missing ORM table or column to ``sync_conn``'s database.

        Additive only — never drops or retypes a column, and adds every column
        as NULLABLE so it is safe against an already-populated table. Runs on a
        sync connection (via ``run_sync``) so the SQLAlchemy inspector and
        ``create_all`` can be used directly.
        """
        from sqlalchemy import inspect as sa_inspect

        # 1) Missing tables (+ their indexes) — create_all is idempotent.
        Base.metadata.create_all(sync_conn, checkfirst=True)

        # 2) Missing columns on existing tables — create_all never ALTERs, so
        #    diff each ORM table against the live columns and add the gaps.
        insp = sa_inspect(sync_conn)
        dialect = sync_conn.dialect
        for table in Base.metadata.sorted_tables:
            if not insp.has_table(table.name):
                continue
            live_cols = {c["name"] for c in insp.get_columns(table.name)}
            for col in table.columns:
                if col.name in live_cols:
                    continue
                coltype = col.type.compile(dialect=dialect)
                sync_conn.exec_driver_sql(
                    f'ALTER TABLE "{table.name}" '
                    f'ADD COLUMN "{col.name}" {coltype}'
                )
                logger.info("run_migrations: added missing column %s.%s (%s)",
                            table.name, col.name, coltype)
