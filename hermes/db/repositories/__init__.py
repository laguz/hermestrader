"""Focused repository mixins composed onto :class:`hermes.db.models.HermesDB`.

Each mixin owns one slice of the persistence surface (logs, trades, approvals,
…). They share the engine/session attributes that ``HermesDB.__init__`` sets
up (``self.AsyncSession``, ``self.async_engine``, ``self.ts_engine``) and may
call across one another via ``self`` — e.g. the trades mixin calls
``self.write_log`` from the logs mixin. Splitting by concern keeps each file
small without changing the public surface that the engine and watcher consume.
"""
from .analytics import AnalyticsRepositoryMixin
from .approvals import ApprovalsRepositoryMixin
from .decisions import DecisionsRepositoryMixin
from .logs import LogsRepositoryMixin
from .settings import SettingsRepositoryMixin
from .timeseries import TimeSeriesRepositoryMixin
from .trades import TradesRepositoryMixin
from .watchlist import WatchlistRepositoryMixin

__all__ = [
    "AnalyticsRepositoryMixin",
    "ApprovalsRepositoryMixin",
    "DecisionsRepositoryMixin",
    "LogsRepositoryMixin",
    "SettingsRepositoryMixin",
    "TimeSeriesRepositoryMixin",
    "TradesRepositoryMixin",
    "WatchlistRepositoryMixin",
]
