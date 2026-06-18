import pytest
import asyncio
from hermes.service1_agent.trade_action import TradeAction
from hermes.service1_agent.risk_engine import PortfolioRiskEngine
from ._stubs import StubDB, StubBroker


def _action(strategy_id: str, symbol: str, quantity: int, width: float, price: float, side: str = "sell") -> TradeAction:
    return TradeAction(
        strategy_id=strategy_id,
        symbol=symbol,
        order_class="multileg",
        legs=[
            {"option_symbol": f"{symbol}250620P00090000", "side": "sell_to_open", "quantity": quantity},
            {"option_symbol": f"{symbol}250620P00085000", "side": "buy_to_open",  "quantity": quantity},
        ],
        price=price,
        side=side,
        quantity=quantity,
        order_type="credit",
        tag=f"HERMES_{strategy_id}",
        strategy_params={"side_type": "put", "pop": 0.85},
        expiry="2025-06-20",
        width=width
    )


@pytest.mark.asyncio
async def test_risk_engine_priority_sequential_scaling():
    db = StubDB()
    broker = StubBroker(option_buying_power=1000.0)  # Enough for 2 lots of width 5 (margin req = 5 * 100 = 500 per lot)
    config = {"cs75_max_lots": 5, "cs7_max_lots": 5}

    risk_engine = PortfolioRiskEngine(broker, db, config)

    # Strategy 1: CS75 (Priority 1) -> 2 lots on AAPL, width 5, price 1.0 -> margin req = (5-1)*100 = 400 per contract -> total 800.
    # Strategy 2: CS7 (Priority 2) -> 2 lots on TSLA, width 5, price 1.0 -> margin req = 400 per contract.
    action_a = _action("CS75", "AAPL", 2, 5.0, 1.0)
    action_b = _action("CS7", "TSLA", 2, 5.0, 1.0)

    # CS75 should get its 2 lots allocated. CS7 should get scaled to 0 because OBP is depleted by CS75.
    validated = await risk_engine.evaluate_and_scale([action_b, action_a])

    assert len(validated) == 1
    assert validated[0].strategy_id == "CS75"
    assert validated[0].quantity == 2


@pytest.mark.asyncio
async def test_risk_engine_capacity_enforcement():
    db = StubDB()
    broker = StubBroker(option_buying_power=10000.0)
    config = {"cs75_max_lots": 2}

    risk_engine = PortfolioRiskEngine(broker, db, config)

    # CS75 capacity limit is 2. Let's propose 5 lots.
    action = _action("CS75", "AAPL", 5, 5.0, 1.0)

    validated = await risk_engine.evaluate_and_scale([action])

    assert len(validated) == 1
    assert validated[0].quantity == 2  # scaled down to capacity limit


@pytest.mark.asyncio
async def test_risk_engine_safety_gateway_integration():
    db = StubDB()
    await db.set_setting("safety_gateway_enabled", "true")
    await db.set_setting("safety_max_risk_bp_ratio", "0.05")  # Max 5% risk of OBP ($500 limit on $10k OBP)
    
    broker = StubBroker(option_buying_power=10000.0)
    config = {"cs75_max_lots": 5}

    risk_engine = PortfolioRiskEngine(broker, db, config)

    # 2 lots on AAPL -> risk = (5-1) * 2 * 100 = $800. This exceeds the $500 safety limit!
    action = _action("CS75", "AAPL", 2, 5.0, 1.0)

    validated = await risk_engine.evaluate_and_scale([action])

    # Should be rejected / scaled to 0
    assert len(validated) == 0
    assert any("[SAFETY VIOLATION]" in log for log in db.logs)


@pytest.mark.asyncio
async def test_risk_engine_portfolio_optimization_kelly():
    db = StubDB()
    broker = StubBroker(option_buying_power=10000.0)
    config = {
        "portfolio_optimization": True,
        "max_orders_per_tick": 5,
        "max_symbol_concentration_pct": 0.15
    }

    risk_engine = PortfolioRiskEngine(broker, db, config)

    action_a = _action("CS75", "AAPL", 5, 5.0, 1.25)
    action_b = _action("CS7", "TSLA", 5, 1.0, 0.10)

    # In portfolio optimization mode, evaluate_and_scale uses Kelly Criterion
    validated = await risk_engine.evaluate_and_scale([action_a, action_b])

    assert len(validated) == 1
    assert validated[0].symbol == "AAPL"
    assert validated[0].quantity == 4
