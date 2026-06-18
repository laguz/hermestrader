"""Focused repositories composed onto :class:`hermes.db.models.HermesDB`.

Each repository owns one slice of the persistence surface (logs, trades,
approvals, …). ``HermesDB`` *owns* one instance of each (``db.trades``,
``db.approvals``, …) rather than inheriting them as mixins, so the
collaborators are explicit and inspectable. They read the shared engine /
session handles through their owner (``self._db``; see :class:`Repository`) and
call into siblings explicitly, e.g. ``self._db.logs.write_log(...)``.

For one transition period ``HermesDB`` also forwards each repository's public
methods as flat attributes (``db.write_log(...)`` → ``db.logs.write_log(...)``)
so existing call-sites keep working while they migrate to the namespaced form.
"""
from .analytics import AnalyticsRepository
from .approvals import ApprovalsRepository
from .base import Repository
from .decisions import DecisionsRepository
from .logs import LogsRepository
from .settings import SettingsRepository
from .timeseries import TimeSeriesRepository
from .trades import TradesRepository
from .watchlist import WatchlistRepository

__all__ = [
    "Repository",
    "AnalyticsRepository",
    "ApprovalsRepository",
    "DecisionsRepository",
    "LogsRepository",
    "SettingsRepository",
    "TimeSeriesRepository",
    "TradesRepository",
    "WatchlistRepository",
]
