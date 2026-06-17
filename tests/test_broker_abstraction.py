from __future__ import annotations

import pytest
from hermes.broker.models import (
    AccountBalances,
    BrokerPosition,
    BrokerOrder,
    OptionChainLeg,
    MarketQuote,
    OrderPlacementResult,
)
from hermes.broker.tradier import TradierBroker


def test_account_balances_model():
    balances = AccountBalances(
        option_buying_power=100000.0,
        stock_buying_power=200000.0,
        total_equity=150000.0,
        cash=50000.0,
        account_type="margin",
        margin_buying_power=120000.0,
        extra_key="value"
    )
    assert balances.option_buying_power == 100000.0
    assert balances.stock_buying_power == 200000.0
    assert balances.total_equity == 150000.0
    assert balances.cash == 50000.0
    assert balances.account_type == "margin"
    assert balances.margin_buying_power == 120000.0

    assert balances["option_buying_power"] == 100000.0
    assert balances["extra_key"] == "value"
    assert balances.get("extra_key") == "value"
    assert balances.get("non_existent", "default") == "default"
    assert "option_buying_power" in balances
    assert isinstance(balances, dict)


def test_broker_position_model():
    pos = BrokerPosition(
        symbol="AAPL",
        quantity=10.0,
        cost_basis=150.0,
        date_acquired="2026-01-01",
        extra_key="value"
    )
    assert pos.symbol == "AAPL"
    assert pos.quantity == 10.0
    assert pos.cost_basis == 150.0
    assert pos.date_acquired == "2026-01-01"

    assert pos["symbol"] == "AAPL"
    assert pos["extra_key"] == "value"
    assert pos.get("non_existent", "default") == "default"
    assert isinstance(pos, dict)


def test_broker_order_model():
    order = BrokerOrder(
        order_id="12345",
        symbol="SPY",
        status="filled",
        quantity=5,
        price=1.23,
        side="buy",
        tag="HERMES_CS75",
        legs=[{"option_symbol": "SPY230519P00150000"}],
        option_symbol="SPY230519P00150000",
        extra_key="value"
    )
    assert order.order_id == "12345"
    assert order.symbol == "SPY"
    assert order.status == "filled"
    assert order.quantity == 5
    assert order.price == 1.23
    assert order.side == "buy"
    assert order.tag == "HERMES_CS75"
    assert order.legs == [{"option_symbol": "SPY230519P00150000"}]
    assert order.option_symbol == "SPY230519P00150000"

    assert order["order_id"] == "12345"
    assert order["id"] == "12345"
    assert order["leg"] == [{"option_symbol": "SPY230519P00150000"}]
    assert order["legs"] == [{"option_symbol": "SPY230519P00150000"}]
    assert order["extra_key"] == "value"
    assert isinstance(order, dict)


def test_option_chain_leg_model():
    leg = OptionChainLeg(
        symbol="SPY230519P00150000",
        strike=150.0,
        option_type="put",
        bid=1.2,
        ask=1.3,
        delta=-0.35,
        greeks={"delta": -0.35, "gamma": 0.02},
        extra_key="value"
    )
    assert leg.symbol == "SPY230519P00150000"
    assert leg.strike == 150.0
    assert leg.option_type == "put"
    assert leg.bid == 1.2
    assert leg.ask == 1.3
    assert leg.delta == -0.35
    assert leg.greeks == {"delta": -0.35, "gamma": 0.02}

    assert leg["symbol"] == "SPY230519P00150000"
    assert leg["delta"] == -0.35
    assert leg["greeks"] == {"delta": -0.35, "gamma": 0.02}
    assert leg["extra_key"] == "value"
    assert isinstance(leg, dict)


def test_market_quote_model():
    quote = MarketQuote(
        symbol="AAPL",
        price=150.0,
        bid=149.9,
        ask=150.1,
        volume=120000,
        timestamp="2026-06-17T14:00:00Z",
        extra_key="value"
    )
    assert quote.symbol == "AAPL"
    assert quote.price == 150.0
    assert quote.bid == 149.9
    assert quote.ask == 150.1
    assert quote.volume == 120000
    assert quote.timestamp == "2026-06-17T14:00:00Z"

    assert quote["symbol"] == "AAPL"
    assert quote["price"] == 150.0
    assert quote["extra_key"] == "value"
    assert isinstance(quote, dict)


def test_order_placement_result_model():
    res = OrderPlacementResult(
        order_id="BT-ORD-123",
        status="ok",
        raw_response={"status": "ok", "order_id": "BT-ORD-123"}
    )
    assert res.order_id == "BT-ORD-123"
    assert res.status == "ok"

    assert res["order_id"] == "BT-ORD-123"
    assert res["status"] == "ok"
    assert res["order"] == {"id": "BT-ORD-123", "status": "ok"}
    assert isinstance(res, dict)


class StubTradierBroker(TradierBroker):
    def __init__(self, mock_response=None):
        self.config = {}
        self.account_id = "mock_acc"
        self.dry_run = False
        self.current_date = None
        self.mock_response = mock_response or {}

    async def _get(self, path: str, params=None):
        return self.mock_response.get(path, {})

    async def _post(self, path: str, data=None):
        return self.mock_response.get(path, {})


@pytest.mark.asyncio
async def test_tradier_broker_parsing():
    mock_balances_resp = {
        "/accounts/mock_acc/balances": {
            "balances": {
                "option_buying_power": 100000.0,
                "stock_buying_power": 200000.0,
                "total_equity": 150000.0,
                "account_type": "margin",
                "margin": {
                    "option_buying_power": 100000.0,
                    "stock_buying_power": 200000.0,
                },
                "cash": {
                    "cash_available": 50000.0
                }
            }
        }
    }
    broker = StubTradierBroker(mock_balances_resp)
    balances = await broker.get_account_balances()
    assert isinstance(balances, AccountBalances)
    assert balances.option_buying_power == 100000.0
    assert balances.stock_buying_power == 200000.0
    assert balances.total_equity == 150000.0
    assert balances.cash == 50000.0
    assert balances.account_type == "margin"
    assert balances["option_buying_power"] == 100000.0

    mock_positions_resp = {
        "/accounts/mock_acc/positions": {
            "positions": {
                "position": [
                    {
                        "symbol": "AAPL",
                        "quantity": 10.0,
                        "cost_basis": 150.0,
                        "date_acquired": "2026-01-01"
                    }
                ]
            }
        }
    }
    broker = StubTradierBroker(mock_positions_resp)
    positions = await broker.get_positions()
    assert len(positions) == 1
    assert isinstance(positions[0], BrokerPosition)
    assert positions[0].symbol == "AAPL"
    assert positions[0].quantity == 10.0
    assert positions[0].cost_basis == 150.0
    assert positions[0]["symbol"] == "AAPL"
