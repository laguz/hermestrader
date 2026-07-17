"""Regression tests for the market-open-aligned clock tick.

The hourly clock tick's phase floats with process start time, so before
this task existed the first post-open evaluation could land up to a full
tick interval after 9:30 ET — leaving DS0's day-limit entries unplaced
for most of the morning (observed live 2026-07-16/17).
"""
from __future__ import annotations

import asyncio
from datetime import timedelta

from hermes.events.bus import ClockTickEvent
from hermes.market_hours import ET
from hermes.service1_agent import scheduler as scheduler_mod
from hermes.service1_agent.scheduler import Scheduler
from hermes.utils import now as clock_now


class _RecordingBus:
    def __init__(self) -> None:
        self.events: list[object] = []

    def emit(self, event: object) -> None:
        self.events.append(event)


def _patch_opens(monkeypatch, first_open):
    """next_open returns `first_open` once, then a far-future open."""
    opens = [first_open, clock_now(ET) + timedelta(days=365)]

    def fake_next_open(now=None):
        return opens.pop(0) if len(opens) > 1 else opens[0]

    monkeypatch.setattr(scheduler_mod, "next_open", fake_next_open)


async def test_open_tick_fires_once_shortly_after_open(monkeypatch):
    bus = _RecordingBus()
    _patch_opens(monkeypatch, clock_now(ET) + timedelta(seconds=1.0))
    monkeypatch.setattr(scheduler_mod, "_OPEN_TICK_DELAY_S", 0.0)
    monkeypatch.setattr(scheduler_mod, "_OPEN_TICK_MAX_SLEEP_S", 0.05)

    sched = Scheduler(bus, tick_interval_s=3600)
    task = asyncio.create_task(sched._run_market_open_tick())
    try:
        await asyncio.sleep(0.1)
        assert not bus.events, "must not tick before the open"

        deadline = asyncio.get_event_loop().time() + 5.0
        while not bus.events and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.05)
        assert sum(isinstance(e, ClockTickEvent) for e in bus.events) == 1

        # The loop must re-arm for the *next* open, not re-fire for today's.
        await asyncio.sleep(0.3)
        assert sum(isinstance(e, ClockTickEvent) for e in bus.events) == 1
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_scheduler_start_registers_open_tick_task():
    bus = _RecordingBus()
    sched = Scheduler(bus, tick_interval_s=3600)
    sched.start()
    try:
        names = {t.get_coro().__qualname__ for t in sched._tasks}
        assert "Scheduler._run_market_open_tick" in names
    finally:
        await sched.stop()
