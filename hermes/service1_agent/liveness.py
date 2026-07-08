"""Process-liveness tracking for the agent's slow-heartbeat clock tick.

``handle_clock_tick_internal`` (see ``_engine_pipeline.py``) is dispatched by
the single-threaded durable event consumer on every ``ClockTickEvent``. If
that consumer ever wedges on a different event type (the class of bug fixed
in PR #209 for MARKET_DATA), CLOCK_TICK stops being dispatched too, and the
agent looks alive in ``docker ps`` while doing nothing. ``main.py`` polls
``seconds_since_last_tick()`` and force-exits the process if it grows past a
threshold, so Docker's ``restart: unless-stopped`` policy can bring back a
clean process instead of the wedge sitting there indefinitely.
"""
from __future__ import annotations

import time

_last_tick_monotonic: float = time.monotonic()


def mark_tick_alive() -> None:
    global _last_tick_monotonic
    _last_tick_monotonic = time.monotonic()


def seconds_since_last_tick() -> float:
    return time.monotonic() - _last_tick_monotonic
