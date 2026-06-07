import pytest
import asyncio
from datetime import date, datetime
from typing import Dict, List, Any, Iterable

from hermes.service1_agent.core import CascadingEngine, MoneyManager, TradeAction
from hermes.service1_agent.strategy_base import AbstractStrategy
from ._stubs import StubBroker, StubDB


class DummyStrategy(AbstractStrategy):
    def __init__(self, strategy_id: str, priority: int, name: str, money_manager: MoneyManager, db: Any, actions: List[TradeAction]):
        super().__init__(
            broker=money_manager.broker.broker,
            db=db,
            money_manager=money_manager,
            ic_builder=None,
            config={}
        )
        self.strategy_id = strategy_id
        self.PRIORITY = priority
        self.NAME = name
        self.actions = actions

    async def execute_entries(self, watchlist: Iterable[str]) -> List[TradeAction]:
        return self.actions

    async def manage_positions(self) -> List[TradeAction]:
        return []


@pytest.mark.asyncio
async def test_scale_quantity_bypasses_bp_cap_when_optimized():
    db = StubDB()
    # Buying power is $500. Margin per lot is $500. So max affordable is 1 lot.
    broker = StubBroker(option_buying_power=500.0)
    
    # 1. Without portfolio_optimization: should clamp to 1 lot
    mm_normal = MoneyManager(broker, db, config={"portfolio_optimization": False})
    scaled_normal = await mm_normal.scale_quantity(
        requested_lots=5,
        requirement_per_lot=500.0,
        symbol="AAPL",
        side="put",
        strategy_id="CS75",
        max_lots=10,
        expiry="2026-06-20"
    )
    assert scaled_normal == 1

    # 2. With portfolio_optimization: should bypass bp_cap and return min(requested, side_cap) = 5
    mm_opt = MoneyManager(broker, db, config={"portfolio_optimization": True})
    scaled_opt = await mm_opt.scale_quantity(
        requested_lots=5,
        requirement_per_lot=500.0,
        symbol="AAPL",
        side="put",
        strategy_id="CS75",
        max_lots=10,
        expiry="2026-06-20"
    )
    assert scaled_opt == 5


@pytest.mark.asyncio
async def test_optimize_allocation_kelly_allocation():
    db = StubDB()
    broker = StubBroker(option_buying_power=10000.0)
    mm = MoneyManager(broker, db, config={"portfolio_optimization": True, "kelly_fraction": 0.5})

    # Prepare some proposed actions
    # Action A: CS75 put spread (pop=0.85, width=5.0, price=1.25 -> Score = 1 - 0.15 * (5/1.25) = 0.40)
    # Margin requirement per lot = 5.0 * 100 = 500.0
    action_a = TradeAction(
        strategy_id="CS75",
        symbol="AAPL",
        order_class="multileg",
        legs=[
            {"option_symbol": "AAPL260620P00150000", "side": "sell_to_open", "quantity": 10},
            {"option_symbol": "AAPL260620P00145000", "side": "buy_to_open", "quantity": 10}
        ],
        price=1.25,
        side="sell",
        quantity=1,
        width=5.0,
        strategy_params={"pop": 0.85}
    )

    # Action B: CS7 put spread (pop=0.75, width=1.0, price=0.10 -> Score = 1 - 0.25 * (1/0.10) = -1.5 -> clamped to 0.01)
    # Margin requirement per lot = 1.0 * 100 = 100.0
    action_b = TradeAction(
        strategy_id="CS7",
        symbol="TSLA",
        order_class="multileg",
        legs=[
            {"option_symbol": "TSLA260620P00200000", "side": "sell_to_open", "quantity": 10},
            {"option_symbol": "TSLA260620P00199000", "side": "buy_to_open", "quantity": 10}
        ],
        price=0.10,
        side="sell",
        quantity=1,
        width=1.0,
        strategy_params={"pop": 0.75}
    )

    # Run optimizer with $10,000 available buying power
    optimized = await mm.optimize_allocation([action_a, action_b], 10000.0)

    print("\n--- DEBUG OPTIMIZE ---")
    print(f"Optimized actions count: {len(optimized)}")
    for a in optimized:
        print(f"Action: {a.symbol}, Qty: {a.quantity}, Legs Qty: {[l['quantity'] for l in a.legs]}")

    # Action A should get 4 lots
    assert len(optimized) == 1
    assert optimized[0].symbol == "AAPL"
    assert optimized[0].legs[0]["quantity"] == 4
    
    # Try with more buying power to see if Action B gets allocated
    action_a.legs[0]["quantity"] = 10
    action_a.legs[1]["quantity"] = 10
    action_b.legs[0]["quantity"] = 10
    action_b.legs[1]["quantity"] = 10
    
    optimized_large = await mm.optimize_allocation([action_a, action_b], 100000.0)
    assert len(optimized_large) == 2
    
    a_opt = next(a for a in optimized_large if a.symbol == "AAPL")
    b_opt = next(a for a in optimized_large if a.symbol == "TSLA")
    assert a_opt.legs[0]["quantity"] == 10
    assert b_opt.legs[0]["quantity"] == 5


@pytest.mark.asyncio
async def test_cascading_engine_process_entries_optimized(monkeypatch):
    monkeypatch.setenv("HERMES_ALLOW_OFFHOURS_TRADES", "true")
    db = StubDB()
    broker = StubBroker(option_buying_power=10000.0)
    config = {"portfolio_optimization": True, "max_orders_per_tick": 5}
    mm = MoneyManager(broker, db, config=config)

    # Action from Strategy 1 (CS75)
    action_a = TradeAction(
        strategy_id="CS75", symbol="AAPL", order_class="multileg",
        legs=[
            {"option_symbol": "AAPL260620P00150000", "side": "sell_to_open", "quantity": 5},
            {"option_symbol": "AAPL260620P00145000", "side": "buy_to_open", "quantity": 5}
        ],
        price=1.25, side="sell", quantity=1, width=5.0,
        strategy_params={"pop": 0.85}
    )

    # Action from Strategy 2 (CS7)
    action_b = TradeAction(
        strategy_id="CS7", symbol="TSLA", order_class="multileg",
        legs=[
            {"option_symbol": "TSLA260620P00200000", "side": "sell_to_open", "quantity": 5},
            {"option_symbol": "TSLA260620P00199000", "side": "buy_to_open", "quantity": 5}
        ],
        price=0.10, side="sell", quantity=1, width=1.0,
        strategy_params={"pop": 0.75}
    )

    strat_1 = DummyStrategy("CS75", 1, "CS75", mm, db, [action_a])
    strat_2 = DummyStrategy("CS7", 2, "CS7", mm, db, [action_b])

    engine = CascadingEngine(broker, db, [strat_1, strat_2], money_manager=mm, config=config)

    count = await engine.process_entries(["AAPL", "TSLA"])

    # Kelly optimization with $10,000 BP:
    # Action A: Score = 0.40 -> target = 4 lots.
    # Action B: Score = 0.01 -> target = 0 lots.
    # Only Action A should be submitted.
    assert count == 1
    assert len(broker.placed) == 1
    assert len(db.pending_orders) == 1
