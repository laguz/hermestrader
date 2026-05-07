"""Regression tests for MoneyManager.sync_broker_orders.

These cover the critical bug where Tradier sanitises ``HERMES_CS75`` tags into
``HERMES-CS75`` and the matcher was looking for the un-sanitised prefix —
silently producing zero broker-side counts on every tick.
"""
from __future__ import annotations

from typing import Any, Dict, List

import pytest

from hermes.service1_agent.core import CascadingEngine, MoneyManager


class _StubBroker:
    def __init__(self, orders: List[Dict[str, Any]]):
        self._orders = orders

    def get_orders(self) -> List[Dict[str, Any]]:
        return self._orders

    # Methods CascadingEngine.tick may call through; left as no-ops for the
    # subset of tests that need them.
    def get_positions(self):
        return []

    def get_account_balances(self):
        return {"option_buying_power": 0.0, "account_type": "margin"}


class _StubDB:
    def write_log(self, *_args, **_kwargs):
        pass

    def upsert_positions(self, *_args, **_kwargs):
        pass

    def tracked_option_symbols(self):
        return set()

    def flag_orphans(self, *_args, **_kwargs):
        pass


def _mm() -> MoneyManager:
    return MoneyManager(broker=_StubBroker([]), db=_StubDB(), config={})


def test_sync_accepts_sanitised_hyphen_tag():
    """Tradier rewrites HERMES_CS75 → HERMES-CS75 before persisting it."""
    orders = [{
        "status": "open",
        "tag": "HERMES-CS75",
        "symbol": "AAPL",
        "quantity": 2,
        "leg": [
            {"option_symbol": "AAPL250620P00150000", "quantity": 2},
            {"option_symbol": "AAPL250620P00145000", "quantity": 2},
        ],
    }]
    mm = MoneyManager(broker=_StubBroker(orders), db=_StubDB(), config={})
    mm.sync_broker_orders()
    assert mm._broker_order_counts == {("CS75", "AAPL", "put"): 2}


def test_sync_accepts_legacy_underscore_tag():
    """Pre-sanitisation tag form still matches (e.g. mock broker)."""
    orders = [{
        "status": "pending",
        "tag": "HERMES_TT45",
        "symbol": "SPY",
        "quantity": 1,
        "leg": [
            {"option_symbol": "SPY250620C00500000", "quantity": 1},
            {"option_symbol": "SPY250620C00505000", "quantity": 1},
        ],
    }]
    mm = MoneyManager(broker=_StubBroker(orders), db=_StubDB(), config={})
    mm.sync_broker_orders()
    assert mm._broker_order_counts == {("TT45", "SPY", "call"): 1}


def test_sync_handles_single_leg_option_order():
    """Wheel orders have no `leg` array — option_symbol is at the top level."""
    orders = [{
        "status": "open",
        "tag": "HERMES-WHEEL",
        "symbol": "MSFT",
        "quantity": 3,
        "option_symbol": "MSFT250620P00400000",
    }]
    mm = MoneyManager(broker=_StubBroker(orders), db=_StubDB(), config={})
    mm.sync_broker_orders()
    assert mm._broker_order_counts == {("WHEEL", "MSFT", "put"): 3}


def test_sync_skips_non_hermes_orders():
    orders = [{"status": "open", "tag": "MANUAL", "symbol": "AAPL"}]
    mm = MoneyManager(broker=_StubBroker(orders), db=_StubDB(), config={})
    mm.sync_broker_orders()
    assert mm._broker_order_counts == {}


def test_sync_skips_inactive_statuses():
    orders = [{
        "status": "filled",
        "tag": "HERMES-CS75",
        "symbol": "AAPL",
        "quantity": 1,
        "leg": [{"option_symbol": "AAPL250620P00150000", "quantity": 1}],
    }]
    mm = MoneyManager(broker=_StubBroker(orders), db=_StubDB(), config={})
    mm.sync_broker_orders()
    assert mm._broker_order_counts == {}


def test_engine_tick_survives_missing_money_manager():
    """Legacy callers that don't pass mm should still run without AttributeError."""
    engine = CascadingEngine(broker=_StubBroker([]), db=_StubDB(),
                             strategies=[], money_manager=None)
    # No strategies, so tick should complete without raising.
    stats = engine.tick(watchlist=[])
    assert "managed" in stats and "entries" in stats

def test_sync_handles_sanitised_closing_tag():
    """Tradier sanitises HERMES_CS75_CLOSE_TP-50 -> HERMES-CS75-CLOSE-TP-50."""
    orders = [{
        "status": "open",
        "tag": "HERMES-CS75-CLOSE-TP-50",
        "symbol": "AAPL",
        "quantity": 1,
        "leg": [{"option_symbol": "AAPL250620P00150000", "quantity": 1}],
    }]
    mm = MoneyManager(broker=_StubBroker(orders), db=_StubDB(), config={})
    mm.sync_broker_orders()
    assert mm._broker_order_counts == {("CS75", "AAPL", "put"): 1}

def test_sync_extracts_strategy_id_robustly():
    """Verify strategy ID extraction from various tag shapes including multiples."""
    # This matches the current logic: tag.replace("_", "-")[7:].split('-', 1)[0]
    # Tag: HERMES-CS75-CLOSE -> CS75
    # Tag: HERMES_CS75_CLOSE -> CS75
    # Tag: HERMES-CS75_CLOSE -> CS75
    # Tag: HERMES_CS75-CLOSE -> CS75

    mm = MoneyManager(broker=_StubBroker([]), db=_StubDB(), config={})

    tags = {
        "HERMES_CS75": "CS75",
        "HERMES-CS75": "CS75",
        "HERMES_CS75_CLOSE": "CS75",
        "HERMES-CS75-CLOSE": "CS75",
        "HERMES_CS75-CLOSE": "CS75",
        "HERMES-CS75_CLOSE": "CS75",
    }

    for tag, expected in tags.items():
        orders = [{
            "status": "open",
            "tag": tag,
            "symbol": "AAPL",
            "quantity": 1,
            "option_symbol": "AAPL250620P00150000",
        }]
        mm.broker = _StubBroker(orders)
        mm.sync_broker_orders()
        assert mm._broker_order_counts == {(expected, "AAPL", "put"): 1}, f"Failed for tag {tag}"
