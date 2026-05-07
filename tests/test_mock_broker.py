"""Regression tests for the MockBroker surface.

The mock broker stands in for Tradier in unit tests and dev mode. Two gaps
silently broke its parity with the real broker:

- ``get_orders`` was missing entirely, so ``CascadingEngine.tick`` →
  ``MoneyManager.sync_broker_orders`` raised ``AttributeError`` and the
  except-clause swallowed it. Mock-mode broker-side capacity tracking
  ran on an empty cache forever.
- ``get_account_balances`` didn't include ``account_type``, which the
  MoneyManager logs reference. Debug lines rendered ``None``.
"""
from __future__ import annotations

from hermes.service1_agent.core import CascadingEngine, MoneyManager
from hermes.service1_agent.mock_broker import MockBroker


class _StubDB:
    def write_log(self, *_a, **_kw):
        pass

    def upsert_positions(self, *_a, **_kw):
        pass

    def tracked_option_symbols(self):
        return set()

    def flag_orphans(self, *_a, **_kw):
        pass


def test_mock_broker_get_orders_returns_empty_list():
    """Required by MoneyManager.sync_broker_orders — must not raise."""
    broker = MockBroker({})
    assert broker.get_orders() == []


def test_mock_broker_balances_include_account_type():
    """MoneyManager logs reference balances['account_type']."""
    balances = MockBroker({}).get_account_balances()
    assert balances.get("account_type") == "margin"
    assert balances.get("option_buying_power", 0) > 0


def test_engine_tick_against_mock_broker_does_not_raise():
    """End-to-end: with a real MockBroker, tick() should run cleanly."""
    broker = MockBroker({})
    db = _StubDB()
    mm = MoneyManager(broker=broker, db=db, config={})
    engine = CascadingEngine(broker=broker, db=db, strategies=[],
                             money_manager=mm)
    stats = engine.tick(watchlist=[])
    assert "managed" in stats and "entries" in stats
    # sync_broker_orders should have run without raising and produced no
    # broker-side counts (mock returns no orders).
    assert mm._broker_order_counts == {}
