"""Regression tests: operator lot changes must reach the strategies that size
entries, in the same tick, for every strategy.

Two bugs made lot changes look intermittent to the operator:

1. ``process_entries`` pushed ``ControlState.lot_settings`` into the shared
   config *after* the strategies had already run ``execute_entries`` — the
   risk engine saw the new cap immediately, but strategy-side sizing
   (``{prefix}target_lots`` / ``{prefix}max_lots`` reads from ``self.config``)
   only saw it a full tick later (an hour on the live instance). The reactive
   entry path never synced at all.

2. The dashboard writes HermesAlpha lots under the legacy ``alpha_max_lots``
   key (routes/strategies.py ``_LOT_SPECS``), but the agent reads
   ``hermesalpha_target_lots`` / ``hermesalpha_max_lots`` and ControlState
   tracked neither — changing Alpha lots in the UI did nothing, ever.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from ._stubs import alias_db_namespaces
from hermes.service1_agent.control_state import ControlState
from hermes.service1_agent.core import CascadingEngine


def _engine(db, strategies, config):
    broker = MagicMock()
    return CascadingEngine(broker=broker, db=db, strategies=strategies, config=config)


def _db():
    db = AsyncMock()
    alias_db_namespaces(db)
    return db


async def test_lot_caps_visible_to_strategies_in_same_tick():
    """execute_entries must already see the operator's new cap — not one tick later."""
    db = _db()
    seen: dict = {}

    strat = MagicMock()
    strat.PRIORITY = 1
    strat.NAME = "CS75"
    strat.strategy_id = "CS75"

    engine = _engine(db, [strat], {"max_orders_per_tick": 5, "cs75_max_lots": 1})

    async def capture_entries(wl):
        seen["cs75_max_lots"] = engine.config.get("cs75_max_lots")
        return []
    strat.execute_entries = AsyncMock(side_effect=capture_entries)
    engine.risk_engine.evaluate_and_scale = AsyncMock(return_value=[])

    cs = ControlState()
    cs.lot_settings["cs75_max_lots"] = 7   # operator raised the cap this tick
    engine.control_state = cs

    await engine.process_entries(["AAPL"])

    assert seen["cs75_max_lots"] == 7, (
        "strategy sized entries from the stale cap — lot settings must be "
        "synced into config BEFORE execute_entries runs"
    )


async def test_reactive_entries_sync_lot_caps():
    """The reactive (support/resistance-triggered) entry path must sync too —
    it can fire between clock ticks, long after the operator's change."""
    db = _db()
    seen: dict = {}

    strat = MagicMock()
    strat.PRIORITY = 1
    strat.NAME = "CS75"
    strat.strategy_id = "CS75"

    engine = _engine(db, [strat], {"max_orders_per_tick": 5, "cs75_max_lots": 1})
    engine._watchlist_for = AsyncMock(return_value=["AAPL"])

    async def capture_entries(wl):
        seen["cs75_max_lots"] = engine.config.get("cs75_max_lots")
        return []
    strat.execute_entries = AsyncMock(side_effect=capture_entries)

    cs = ControlState()
    cs.lot_settings["cs75_max_lots"] = 7
    engine.control_state = cs

    await engine.reactive.process_reactive_entries("AAPL")

    assert seen["cs75_max_lots"] == 7, (
        "reactive entries sized from the stale cap — lot settings must be "
        "synced into config before the reactive path runs strategies"
    )


def test_alpha_max_lots_event_reaches_hermesalpha_keys():
    """The dashboard's alpha_max_lots setting must land on the keys the agent
    actually reads (hermesalpha_target_lots / hermesalpha_max_lots)."""
    cs = ControlState()
    cs._update_setting("alpha_max_lots", "4")
    assert cs.lot_settings.get("hermesalpha_max_lots") == 4
    assert cs.lot_settings.get("hermesalpha_target_lots") == 4


async def test_alpha_max_lots_loaded_from_db_backstop():
    """The clock-tick DB backstop must pick up alpha_max_lots as well, so a
    dropped settings event still self-heals for HermesAlpha."""
    db = _db()
    db.get_settings = AsyncMock(return_value={"alpha_max_lots": "3"})
    db.get_setting = AsyncMock(return_value=None)
    db.list_all_watchlists = AsyncMock(return_value={})

    cs = ControlState()
    await cs.load_from_db(db, {})

    assert cs.lot_settings.get("hermesalpha_max_lots") == 3
    assert cs.lot_settings.get("hermesalpha_target_lots") == 3
