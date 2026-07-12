"""Historical replay (backtest) harness.

Drives the real ``CascadingEngine`` + strategies through a
:class:`~hermes.clock.SimulatedClock` against historical bars, with all broker
traffic served by :class:`~hermes.replay.broker.ReplayBroker` (a
``MockBroker`` subclass — never a real Tradier client) and all persistence in
:class:`~hermes.replay.memdb.ReplayDB` (in-memory). The engine is untouched:
the tick pipeline order, ``dry_run`` semantics and approval gating all run
exactly as in production; the harness only adapts the world around them.
"""
from .data import ReplayDataSource
from .broker import ReplayBroker
from .memdb import ReplayDB
from .harness import ReplayConfig, ReplayHarness, ReplayResult
from .report import build_report, render_report

__all__ = [
    "ReplayDataSource",
    "ReplayBroker",
    "ReplayDB",
    "ReplayConfig",
    "ReplayHarness",
    "ReplayResult",
    "build_report",
    "render_report",
]
