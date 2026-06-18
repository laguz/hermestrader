"""Regression tests for two event-driven control-plane gaps.

Both guard correctness of the event-sourced control plane introduced in the
"five core architectural improvements" change:

1. **Backstop re-sync** — control state is normally updated by settings events,
   but a dropped Postgres NOTIFY must not leave the agent trading on stale pause
   / kill-switch state. The clock tick re-hydrates from the DB as a safety net.

2. **Lot-cap wiring** — the operator's per-strategy lot settings live on
   ControlState; the risk engine reads them from the shared engine config, so
   ``process_entries`` must push them across before evaluation (otherwise the
   risk engine falls back to a hard-coded 1-lot cap and the bot under-trades).
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from hermes.events.bus import ClockTickEvent
from hermes.service1_agent.core import CascadingEngine
from hermes.service1_agent.control_state import ControlState


def _engine(db, strategies, config):
    broker = MagicMock()
    return CascadingEngine(broker=broker, db=db, strategies=strategies, config=config)


async def test_clock_tick_backstop_reloads_stale_pause():
    """A dropped pause event self-heals: the clock-tick backstop re-reads the DB
    and halts the tick before any trading pipeline runs."""
    db = AsyncMock()
    # The operator paused in the DB, but the agent missed the PauseChangedEvent.
    db.get_settings = AsyncMock(return_value={"agent_paused": "true"})
    db.get_setting = AsyncMock(return_value=None)
    db.list_all_watchlists = AsyncMock(return_value={})
    db.fetch_approved_actions = AsyncMock(return_value=[])
    db.write_log = AsyncMock()

    strat = MagicMock()
    strat.PRIORITY = 1
    engine = _engine(db, [strat], {"max_orders_per_tick": 5})

    cs = ControlState()
    cs.paused = False        # stale — the event never arrived
    cs.last_sync_ts = None   # force the backstop to re-hydrate
    engine.control_state = cs

    # Spy on pipeline steps that must NOT run while paused.
    engine.sync_positions = AsyncMock()
    engine.process_entries = AsyncMock()

    await engine._handle_clock_tick_internal(ClockTickEvent())

    assert cs.paused is True, "backstop did not re-hydrate the missed pause from the DB"
    engine.process_entries.assert_not_called()
    engine.sync_positions.assert_not_called()


async def test_clock_tick_backstop_throttled_when_recently_synced():
    """A just-synced control state is not re-read on every (e.g. IPC-triggered) tick."""
    from datetime import datetime, timezone

    db = AsyncMock()
    db.get_settings = AsyncMock(return_value={})
    db.get_setting = AsyncMock(return_value=None)
    db.list_all_watchlists = AsyncMock(return_value={})
    db.fetch_approved_actions = AsyncMock(return_value=[])
    db.write_log = AsyncMock()

    strat = MagicMock()
    strat.PRIORITY = 1
    engine = _engine(db, [strat], {"max_orders_per_tick": 5})

    cs = ControlState()
    cs.paused = True                                  # halt the tick early/cheaply
    cs.last_sync_ts = datetime.now(timezone.utc)      # just synced → skip reload
    engine.control_state = cs

    await engine._handle_clock_tick_internal(ClockTickEvent())

    db.get_settings.assert_not_called()  # throttled: no redundant reload


async def test_process_entries_syncs_operator_lot_caps_into_risk_config():
    """Operator lot caps on ControlState reach the risk engine's config."""
    db = AsyncMock()
    strat = MagicMock()
    strat.PRIORITY = 1
    strat.NAME = "CS75"
    strat.strategy_id = "CS75"
    strat.execute_entries = AsyncMock(return_value=[])

    engine = _engine(db, [strat], {"max_orders_per_tick": 5})
    engine.submit = AsyncMock()
    engine.risk_engine.evaluate_and_scale = AsyncMock(return_value=[])

    cs = ControlState()
    cs.lot_settings["cs75_max_lots"] = 7   # operator raised the CS75 cap
    engine.control_state = cs

    await engine.process_entries(["AAPL"])

    # The cap must be visible to the risk engine (which reads the shared config),
    # not dropped so it falls back to its hard-coded 1-lot default.
    assert engine.config["cs75_max_lots"] == 7
    assert engine.risk_engine.config is engine.config
