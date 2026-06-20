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
from ._stubs import RepoNamespaceMixin


class _StubDB(RepoNamespaceMixin):
    async def write_log(self, *_a, **_kw):
        pass

    async def upsert_positions(self, *_a, **_kw):
        pass

    async def tracked_option_symbols(self):
        return set()

    async def flag_orphans(self, *_a, **_kw):
        pass

    async def fetch_pending(self, *_a, **_kw):
        return []



async def test_mock_broker_get_orders_returns_empty_list():
    """Required by MoneyManager.sync_broker_orders — must not raise."""
    broker = MockBroker({})
    assert await broker.get_orders() == []


async def test_mock_broker_balances_include_account_type():
    """MoneyManager logs reference balances['account_type']."""
    balances = await MockBroker({}).get_account_balances()
    assert balances.get("account_type") == "margin"
    assert balances.get("option_buying_power", 0) > 0


async def test_engine_tick_against_mock_broker_does_not_raise():
    """End-to-end: with a real MockBroker, tick() should run cleanly."""
    broker = MockBroker({})
    db = _StubDB()
    mm = MoneyManager(broker=broker, db=db, config={})
    engine = CascadingEngine(broker=broker, db=db, strategies=[],
                             money_manager=mm)
    stats = await engine.tick(watchlist=[])
    assert "managed" in stats and "entries" in stats
    # sync_broker_orders should have run without raising and produced no
    # broker-side counts (mock returns no orders).
    assert mm._broker_order_counts == {}


async def test_mock_broker_dynamic_options_chain():
    """Verify that MockBroker generates strikes dynamically based on underlying spot."""
    broker = MockBroker({})
    
    # Test SPY (high spot price)
    spy_spot = broker._get_symbol_price("SPY")
    # Spot should be base = 100 + hash("SPY") % 200
    assert spy_spot > 100.0
    
    chain = await broker.get_option_chains("SPY", "2026-06-20")
    assert len(chain) > 0
    
    # Strikes should be clustered around spy_spot
    strikes = [leg.strike for leg in chain]
    assert min(strikes) < spy_spot < max(strikes)
    
    # Every option symbol should be correctly formatted OCC style:
    # E.g. "SPY   260620P00300000"
    for leg in chain:
        assert leg.symbol.startswith("SPY")
        assert "260620" in leg.symbol
        assert leg.option_type in ("call", "put")
        assert 0.01 <= leg.bid < leg.ask
        assert -1.0 <= leg.delta <= 1.0


async def test_mock_broker_order_slippage_and_fill_rules():
    """Verify MockBroker places filled/unfilled orders based on simulated limits and slippage."""
    broker = MockBroker({})
    from hermes.service1_agent.trade_action import TradeAction
    
    # Build a buy TradeAction
    action_buy = TradeAction(
        strategy_id="CS75",
        symbol="AAPL",
        order_class="multileg",
        legs=[{"option_symbol": "AAPL  260620P00170000", "side": "buy", "quantity": 1}],
        price=10.0,  # Highly generous debit limit price
        side="buy",
        order_type="debit"
    )
    
    # Should fill (ok) because price (10.0) is greater than simulated net debit
    result_ok = await broker.place_order_from_action(action_buy)
    assert result_ok.status == "ok"
    
    # Now set a very aggressive debit limit price (e.g. at most $0.01)
    action_aggressive = TradeAction(
        strategy_id="CS75",
        symbol="AAPL",
        order_class="multileg",
        legs=[{"option_symbol": "AAPL  260620P00170000", "side": "buy", "quantity": 1}],
        price=0.01,
        side="buy",
        order_type="debit"
    )
    
    # Should be rejected because the package cannot be bought for only $0.01
    result_rejected = await broker.place_order_from_action(action_aggressive)
    assert result_rejected.status == "rejected"

