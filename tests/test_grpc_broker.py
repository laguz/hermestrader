from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock, patch

from hermes.service2_watcher.api_grpc import start_grpc_server
from hermes.broker.grpc_client import GRPCBrokerClient
from hermes.service1_agent.trade_action import TradeAction
from hermes.broker.mock_engine import MockAsyncTradierBroker


@pytest.fixture
async def grpc_server_fixture():
    # Start on an ephemeral port
    server = await start_grpc_server(host="127.0.0.1", port=50055)
    yield server
    await server.stop(grace=0.1)


@pytest.mark.asyncio
async def test_grpc_broker_client_server_integration(grpc_server_fixture):
    # Setup client
    client = GRPCBrokerClient(config={"grpc_target": "127.0.0.1:50055"})

    # 1. Test get_positions using the default MockAsyncTradierBroker
    positions = await client.get_positions()
    assert isinstance(positions, list)

    # 2. Patch MockAsyncTradierBroker methods to verify specific returns
    mock_positions = [
        {"symbol": "TSLA", "quantity": 10.0, "cost_basis": 1500.0, "date_acquired": "2026-06-17"}
    ]
    mock_order_result = {
        "order_id": "999888",
        "status": "ok",
        "filled_quantity": 3,
        "avg_fill_price": 1.45
    }
    mock_balances = {
        "option_buying_power": 50000.0,
        "stock_buying_power": 60000.0,
        "total_equity": 110000.0,
        "cash": 45000.0,
        "account_type": "margin",
        "margin_buying_power": 60000.0
    }
    
    with patch.object(MockAsyncTradierBroker, "get_positions", new_callable=AsyncMock) as mock_get_pos, \
         patch.object(MockAsyncTradierBroker, "place_order_from_action", new_callable=AsyncMock) as mock_place_order, \
         patch.object(MockAsyncTradierBroker, "get_account_balances", new_callable=AsyncMock) as mock_get_bal, \
         patch.object(MockAsyncTradierBroker, "get_option_expirations", new_callable=AsyncMock) as mock_get_exp, \
         patch.object(MockAsyncTradierBroker, "get_option_chains", new_callable=AsyncMock) as mock_get_chain, \
         patch.object(MockAsyncTradierBroker, "get_quote", new_callable=AsyncMock) as mock_get_quote, \
         patch.object(MockAsyncTradierBroker, "get_delta", new_callable=AsyncMock) as mock_get_delta, \
         patch.object(MockAsyncTradierBroker, "get_history", new_callable=AsyncMock) as mock_get_hist, \
         patch.object(MockAsyncTradierBroker, "analyze_symbol", new_callable=AsyncMock) as mock_analyze, \
         patch.object(MockAsyncTradierBroker, "roll_to_next_month", new_callable=AsyncMock) as mock_roll:

        mock_get_pos.return_value = mock_positions
        mock_place_order.return_value = mock_order_result
        mock_get_bal.return_value = mock_balances
        mock_get_exp.return_value = ["2026-06-20"]
        mock_get_chain.return_value = [{"symbol": "AAPL260620C00150000", "strike": 150.0}]
        mock_get_quote.return_value = [{"symbol": "AAPL", "last": 175.0}]
        mock_get_delta.return_value = -0.45
        mock_get_hist.return_value = [{"date": "2026-06-16", "close": 174.0}]
        mock_analyze.return_value = {"volatility": 0.22}
        mock_roll.return_value = "AAPL260718C00150000"

        # Verify get_positions
        pos_list = await client.get_positions()
        assert len(pos_list) == 1
        assert pos_list[0]["symbol"] == "TSLA"
        assert pos_list[0]["quantity"] == 10.0
        assert pos_list[0]["cost_basis"] == 1500.0

        # Verify place_order_from_action
        action = TradeAction(
            strategy_id="CS75",
            symbol="AAPL",
            order_class="multileg",
            legs=[{"option_symbol": "AAPL260620P00150000", "quantity": 3, "side": "buy"}],
            price=1.50,
            side="buy",
            quantity=3,
            duration="day",
            tag="TEST_TAG"
        )
        res = await client.place_order_from_action(action)
        assert res["order_id"] == "999888"
        assert res["status"] == "ok"
        assert res["filled_quantity"] == 3
        assert res["avg_fill_price"] == 1.45

        # Verify get_account_balances
        balances = await client.get_account_balances()
        assert balances["option_buying_power"] == 50000.0
        assert balances["stock_buying_power"] == 60000.0

        # Verify remaining abstract methods
        expirations = await client.get_option_expirations("AAPL")
        assert expirations == ["2026-06-20"]

        chains = await client.get_option_chains("AAPL", "2026-06-20")
        assert chains[0]["symbol"] == "AAPL260620C00150000"

        quotes = await client.get_quote("AAPL")
        assert quotes[0]["last"] == 175.0

        delta = await client.get_delta("AAPL260620C00150000")
        assert delta == -0.45

        history = await client.get_history("AAPL", interval="daily")
        assert history[0]["close"] == 174.0

        analysis = await client.analyze_symbol("AAPL", period="6m")
        assert analysis["volatility"] == 0.22

        rolled = await client.roll_to_next_month("AAPL260620C00150000")
        assert rolled == "AAPL260718C00150000"

    await client.close()
