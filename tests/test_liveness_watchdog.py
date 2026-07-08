"""Regression test for the process-liveness watchdog.

If the durable event consumer wedges on a different event type (the class of
bug fixed in PR #209 for MARKET_DATA), CLOCK_TICK silently stops being
dispatched while the process stays up — `docker ps` shows healthy, but the
agent does nothing until a human notices. `main.py`'s watchdog force-exits
the process once `hermes.service1_agent.liveness.seconds_since_last_tick()`
grows stale, so Docker's `restart: unless-stopped` policy can bring back a
clean process. That only works if `handle_clock_tick_internal` actually
touches the liveness mark on every dispatch — including the early-return
paths (paused, circuit-breaker cooldown) — since reaching the function at
all is the signal that the consumer isn't wedged.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from ._stubs import alias_db_namespaces

from hermes.events.bus import ClockTickEvent
from hermes.service1_agent.core import CascadingEngine
from hermes.service1_agent.control_state import ControlState
from hermes.service1_agent import liveness


def _engine(db, strategies, config):
    broker = MagicMock()
    return CascadingEngine(broker=broker, db=db, strategies=strategies, config=config)


def test_liveness_mark_resets_staleness():
    liveness._last_tick_monotonic = 0.0
    assert liveness.seconds_since_last_tick() > 0
    liveness.mark_tick_alive()
    assert liveness.seconds_since_last_tick() < 1.0


async def test_clock_tick_marks_liveness_even_when_paused():
    """The paused early-return must still prove the consumer dispatched the
    tick — otherwise a paused bot would trip the watchdog for no reason."""
    liveness._last_tick_monotonic = 0.0

    db = AsyncMock()
    alias_db_namespaces(db)
    db.get_settings = AsyncMock(return_value={})
    db.get_setting = AsyncMock(return_value=None)
    db.list_all_watchlists = AsyncMock(return_value={})
    db.fetch_approved_actions = AsyncMock(return_value=[])
    db.write_log = AsyncMock()

    strat = MagicMock()
    strat.PRIORITY = 1
    engine = _engine(db, [strat], {"max_orders_per_tick": 5})

    cs = ControlState()
    cs.paused = True
    engine.control_state = cs

    stale_before = liveness.seconds_since_last_tick()
    await engine._handle_clock_tick_internal(ClockTickEvent())

    assert liveness.seconds_since_last_tick() < stale_before
