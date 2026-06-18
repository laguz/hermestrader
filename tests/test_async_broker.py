from __future__ import annotations

from unittest.mock import AsyncMock, patch
import pytest

from hermes.broker.tradier import TradierBroker
from hermes.service1_agent.core import TradeAction


@pytest.fixture
def broker_cfg():
    return {
        "tradier_access_token": "mock-token",
        "tradier_account_id": "mock-acct",
        "tradier_base_url": "https://api.tradier.com/v1",
        "dry_run": True
    }


@pytest.mark.anyio
async def test_async_broker_get_balances(broker_cfg):
    broker = TradierBroker(broker_cfg)
    
    mock_response = {
        "balances": {
            "option_buying_power": 50000.0,
            "total_equity": 60000.0,
            "cash_available": 10000.0,
            "account_type": "margin"
        }
    }
    
    with patch.object(broker, "_get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_response
        
        balances = await broker.get_account_balances()
        
        mock_get.assert_called_once_with("/accounts/mock-acct/balances")
        assert balances["option_buying_power"] == 50000.0
        assert balances["total_equity"] == 60000.0
        assert balances["account_type"] == "margin"
        
    await broker.close()


@pytest.mark.anyio
async def test_async_broker_place_order(broker_cfg):
    broker = TradierBroker(broker_cfg)
    
    action = TradeAction(
        strategy_id="CS75",
        symbol="AAPL",
        order_class="multileg",
        legs=[
            {"option_symbol": "AAPL250620P00090000", "action": "sell_to_open", "quantity": 1},
            {"option_symbol": "AAPL250620P00085000", "action": "buy_to_open", "quantity": 1}
        ],
        price=1.50,
        side="sell",
        quantity=1,
        order_type="credit",
        tag="HERMES_CS75"
    )
    
    mock_post_res = {"status": "ok", "order": {"id": 12345, "status": "ok"}}
    
    with patch.object(broker, "_post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_post_res
        
        res = await broker.place_order_from_action(action)
        
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "/accounts/mock-acct/orders"
        
        post_data = args[1]
        assert post_data["class"] == "multileg"
        assert post_data["symbol"] == "AAPL"
        assert post_data["type"] == "credit"
        assert post_data["price"] == "1.50"
        assert post_data["tag"] == "HERMES-CS75"
        assert post_data["preview"] == "true"

        # TradierBroker normalizes the raw response into an OrderPlacementResult:
        # the id is stringified, and the original payload is kept under raw_response.
        assert res["order_id"] == "12345"
        assert res["status"] == "ok"
        assert res["raw_response"]["order"]["id"] == 12345

    await broker.close()
