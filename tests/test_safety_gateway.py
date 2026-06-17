from __future__ import annotations

import pytest
from unittest.mock import MagicMock, AsyncMock

from hermes.portfolio.safety_gateway import SafetyGateway, SafetyValidationError
from hermes.service1_agent.trade_action import TradeAction


def test_safety_gateway_approved_trade():
    gateway = SafetyGateway()
    
    # 100k OBP, risk of (5.0 - 1.5) * 1 * 100 = 350.0. Max allowed is 5000.0 (5% of 100k).
    balances = {"option_buying_power": 100000.0}
    action = TradeAction(
        strategy_id="CS75",
        symbol="AAPL",
        order_class="multileg",
        legs=[
            {"option_symbol": "AAPL260620P00150000", "side": "sell_to_open", "quantity": 1},
            {"option_symbol": "AAPL260620P00145000", "side": "buy_to_open", "quantity": 1}
        ],
        price=1.50,
        side="sell",
        quantity=1,
        width=5.0
    )
    
    report = gateway.validate_action(action, balances, [])
    assert report.decision == "APPROVED"
    assert not report.violations
    assert report.metrics["calculated_risk"] == 350.0


def test_safety_gateway_rejected_by_risk_limit():
    # Max risk limit is 1% of OBP (so $1,000 max risk)
    gateway = SafetyGateway(config={"safety_max_risk_bp_ratio": 0.01})
    balances = {"option_buying_power": 100000.0}
    
    # Risk is (10.0 - 2.0) * 2 * 100 = 1600.0 (exceeds $1,000)
    action = TradeAction(
        strategy_id="CS75",
        symbol="AAPL",
        order_class="multileg",
        legs=[
            {"option_symbol": "AAPL260620P00150000", "side": "sell_to_open", "quantity": 2},
            {"option_symbol": "AAPL260620P00140000", "side": "buy_to_open", "quantity": 2}
        ],
        price=2.00,
        side="sell",
        quantity=2,
        width=10.0
    )
    
    report = gateway.validate_action(action, balances, [])
    assert report.decision == "REJECTED"
    assert any("exceeds safety limit" in v for v in report.violations)


def test_safety_gateway_rejected_by_concentration_limit():
    # Max exposure per symbol is 10% of OBP ($10,000)
    gateway = SafetyGateway(config={"safety_max_symbol_exposure_ratio": 0.10})
    balances = {"option_buying_power": 100000.0}
    
    # Existing positions on TSLA have $1,800 of risk
    open_trades = [
        {"symbol": "TSLA", "width": 10.0, "entry_credit": 1.0, "lots": 1, "side_type": "put"},
        {"symbol": "TSLA", "width": 10.0, "entry_credit": 1.0, "lots": 1, "side_type": "put"},
    ]
    
    # Proposed order on TSLA: risk of (10.0 - 1.0) * 10 * 100 = 9000.0.
    # Total risk on TSLA: 1800 + 9000 = 10800.0 (exceeds 10% of 100k = 10000.0)
    action = TradeAction(
        strategy_id="CS75",
        symbol="TSLA",
        order_class="multileg",
        legs=[
            {"option_symbol": "TSLA260620P00150000", "side": "sell_to_open", "quantity": 10},
            {"option_symbol": "TSLA260620P00140000", "side": "buy_to_open", "quantity": 10}
        ],
        price=1.00,
        side="sell",
        quantity=10,
        width=10.0
    )
    
    report = gateway.validate_action(action, balances, open_trades)
    assert report.decision == "REJECTED"
    assert any("exceeds safety limit" in v for v in report.violations)


def test_safety_gateway_rejected_by_max_trades_count():
    gateway = SafetyGateway(config={"safety_max_symbol_trades": 2})
    balances = {"option_buying_power": 100000.0}
    
    # Already 2 open trades on AAPL
    open_trades = [
        {"symbol": "AAPL", "width": 5.0, "entry_credit": 1.5, "lots": 1, "side_type": "put"},
        {"symbol": "AAPL", "width": 5.0, "entry_credit": 1.5, "lots": 1, "side_type": "call"}
    ]
    
    action = TradeAction(
        strategy_id="CS7",
        symbol="AAPL",
        order_class="multileg",
        legs=[
            {"option_symbol": "AAPL260620P00150000", "side": "sell_to_open", "quantity": 1},
            {"option_symbol": "AAPL260620P00145000", "side": "buy_to_open", "quantity": 1}
        ],
        price=1.50,
        side="sell",
        quantity=1,
        width=5.0
    )
    
    report = gateway.validate_action(action, balances, open_trades)
    assert report.decision == "REJECTED"
    assert any("violating concentration count limit" in v for v in report.violations)


def test_safety_gateway_side_lock_violation():
    gateway = SafetyGateway(config={"safety_side_lock_enabled": True})
    balances = {"option_buying_power": 100000.0}
    
    # Already hold a short put spread (side_type="put")
    open_trades = [
        {"symbol": "AAPL", "width": 5.0, "entry_credit": 1.5, "lots": 1, "side_type": "put"}
    ]
    
    # Propose another short put spread (contains 'P' in OCC option symbol)
    action = TradeAction(
        strategy_id="CS75",
        symbol="AAPL",
        order_class="multileg",
        legs=[
            {"option_symbol": "AAPL260620P00150000", "side": "sell_to_open", "quantity": 1},
            {"option_symbol": "AAPL260620P00145000", "side": "buy_to_open", "quantity": 1}
        ],
        price=1.50,
        side="sell",
        quantity=1,
        width=5.0
    )
    
    report = gateway.validate_action(action, balances, open_trades)
    assert report.decision == "REJECTED"
    assert any("Side lock violation" in v for v in report.violations)


def test_safety_gateway_bypasses_closing_trades():
    gateway = SafetyGateway(config={"safety_max_risk_bp_ratio": 0.01})
    balances = {"option_buying_power": 100000.0}
    
    # Bypassed because legs side is to_close
    action = TradeAction(
        strategy_id="CS75",
        symbol="AAPL",
        order_class="multileg",
        legs=[
            {"option_symbol": "AAPL260620P00150000", "side": "buy_to_close", "quantity": 10},
            {"option_symbol": "AAPL260620P00145000", "side": "sell_to_close", "quantity": 10}
        ],
        price=3.50,
        side="buy",
        quantity=10,
        width=5.0
    )
    
    report = gateway.validate_action(action, balances, [])
    assert report.decision == "APPROVED"
    assert not report.violations


@pytest.mark.asyncio
async def test_broker_wrapper_safety_validation_interception():
    from hermes.service1_agent.broker_wrapper import AsyncBrokerWrapper
    
    mock_broker = MagicMock()
    mock_broker.get_account_balances = AsyncMock(return_value={"option_buying_power": 1000.0})
    mock_broker.place_order_from_action = AsyncMock(return_value={"order_id": "123"})
    
    mock_db = MagicMock()
    mock_db.get_setting = AsyncMock(return_value="0.05") # 5% of 1000 = $50 max risk
    mock_db.all_open_trades = AsyncMock(return_value=[])
    mock_db.write_log = AsyncMock()

    wrapper = AsyncBrokerWrapper(mock_broker, mock_db)
    
    # This action has $350 of risk (exceeds $50 limit)
    action = TradeAction(
        strategy_id="CS75",
        symbol="AAPL",
        order_class="multileg",
        legs=[
            {"option_symbol": "AAPL260620P00150000", "side": "sell_to_open", "quantity": 1},
            {"option_symbol": "AAPL260620P00145000", "side": "buy_to_open", "quantity": 1}
        ],
        price=1.50,
        side="sell",
        quantity=1,
        width=5.0
    )
    
    with pytest.raises(SafetyValidationError, match="Order rejected by Safety Gateway"):
        await wrapper.place_order_from_action(action)
        
    mock_broker.place_order_from_action.assert_not_called()
