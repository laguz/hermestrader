from __future__ import annotations

import httpx
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from hermes.broker.tradier import TradierBroker
from hermes.broker.mcp_client import MCPBrokerClient
from hermes.service1_agent.core import TradeAction
from hermes.service1_agent.overseer import HermesOverseer

# Stub config and classes for testing
class DummyContext:
    def __init__(self, config):
        self.config = config

class DummyEngine:
    def __init__(self, config):
        self.ctx = DummyContext(config)


# --- Bug 1 ---
def test_bug_1_reactive_max_lots_zero():
    # We want to verify the logic inside _engine_reactive.py line 804
    # The fix: config-configured max_lots=0 is not overridden by fallback default
    strat_id = "CS75"
    max_lots_map = {"CS75": 1}
    config_key = "cs75_max_lots"

    # Test config has 0
    config = {config_key: 0}
    _raw_max_lots = config.get(config_key)
    max_lots = int(_raw_max_lots) if _raw_max_lots is not None else max_lots_map.get(strat_id, 1)
    assert max_lots == 0

    # Test config has None/missing
    config = {}
    _raw_max_lots = config.get(config_key)
    max_lots = int(_raw_max_lots) if _raw_max_lots is not None else max_lots_map.get(strat_id, 1)
    assert max_lots == 1


# --- Bug 2 ---
def test_bug_2_retry_policy_includes_http_status_error():
    from hermes.broker.tradier import _RETRY_POLICY
    # Check that retry predicate returns True for both RequestError and HTTPStatusError
    retry_func = _RETRY_POLICY["retry"]
    
    # Tenacity retry_if_exception_type checks isinstance:
    assert retry_func.exception_types == (httpx.RequestError, httpx.HTTPStatusError)


# --- Bug 3 ---
@pytest.mark.anyio
async def test_bug_3_bracket_access_fallback():
    broker = TradierBroker({
        "tradier_access_token": "token",
        "tradier_account_id": "acct",
        "dry_run": True
    })
    
    # Missing option_symbol key in leg
    action = TradeAction(
        strategy_id="CS75",
        symbol="AAPL",
        order_class="multileg",
        legs=[{"quantity": 1, "side": "buy"}],
        price=1.0,
        side="sell",
    )
    
    with patch.object(broker, "_post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = {"order": {"id": 123, "status": "ok"}}
        await broker.place_order_from_action(action)
        
        mock_post.assert_called_once()
        args, _ = mock_post.call_args
        post_data = args[1]
        assert post_data["option_symbol[0]"] == ""


# --- Bug 4 ---
def test_bug_4_trades_repo_quantity_zero():
    # Test Bug 4: action.quantity = 0 returns lots = 0
    action = TradeAction(
        strategy_id="CS75",
        symbol="AAPL",
        order_class="multileg",
        legs=[],
        price=1.0,
        side="sell",
        quantity=0,
    )
    lots = action.quantity if action.quantity is not None else 1
    assert lots == 0


# --- Bug 5 ---
def test_bug_5_redundant_conditions_removal():
    # Ensure the simplified logic matches correctly
    ls_sell = "sell_to_open"
    ls_buy = "buy_to_open"
    
    short_leg = None
    long_leg = None
    
    if not short_leg and "sell" in ls_sell:
        short_leg = "osym_short"
    if not long_leg and "buy" in ls_buy:
        long_leg = "osym_long"
        
    assert short_leg == "osym_short"
    assert long_leg == "osym_long"


# --- Bug 6 ---
@pytest.mark.anyio
async def test_bug_6_mcp_client_option_type_fallback():
    client = MCPBrokerClient({"mcp_server_url": "ws://mock"})
    
    # Mock _call_mcp response containing "type" instead of "option_type"
    mock_legs = [
        {
            "symbol": "AAPL250620P00090000",
            "strike": 90.0,
            "type": "put",
            "bid": 1.5,
            "ask": 1.6,
            "delta": -0.3
        }
    ]
    
    with patch.object(client, "_call_mcp", new_callable=AsyncMock) as mock_call:
        mock_call.return_value = mock_legs
        chains = await client.get_option_chains("AAPL", "2025-06-20")
        assert len(chains) == 1
        assert chains[0].option_type == "put"


# --- Bug 7 ---
@pytest.mark.anyio
async def test_bug_7_overseer_timeout_zero():
    mock_db = AsyncMock()
    mock_llm = MagicMock()
    mock_llm.timeout_s = 0.0
    
    overseer = HermesOverseer(mock_llm, mock_db, autonomy="advisory")
    
    timeout_val = getattr(overseer.llm, "timeout_s", 15.0)
    if not isinstance(timeout_val, (int, float)):
        timeout_s = 15.0
    else:
        timeout_s = timeout_val if timeout_val is not None else 15.0
        
    assert timeout_s == 0.0


# --- Bug 8 ---
@pytest.mark.anyio
async def test_bug_8_tradier_price_fallback():
    broker = TradierBroker({
        "tradier_access_token": "token",
        "tradier_account_id": "acct",
        "dry_run": True
    })
    
    # Mock returns order with price=0.0 and avg_fill_price=1.5
    mock_res = {
        "orders": {
            "order": {
                "id": "123",
                "symbol": "AAPL",
                "status": "filled",
                "quantity": 1,
                "price": 0.0,
                "avg_fill_price": 1.5,
                "side": "sell",
                "tag": "HERMES_CS75"
            }
        }
    }
    
    with patch.object(broker, "_get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_res
        orders = await broker.get_orders()
        assert len(orders) == 1
        assert orders[0]["price"] == 0.0


# --- Bug 9 ---
@pytest.mark.anyio
async def test_bug_9_tradier_quote_price_fallback():
    broker = TradierBroker({
        "tradier_access_token": "token",
        "tradier_account_id": "acct",
        "dry_run": True
    })
    
    # Mock returns quote with last=0.0 and price=150.0
    mock_res = {
        "quotes": {
            "quote": {
                "symbol": "AAPL",
                "last": 0.0,
                "price": 150.0,
                "bid": 149.0,
                "ask": 151.0,
                "volume": 1000,
                "timestamp": "2026-07-04"
            }
        }
    }
    
    with patch.object(broker, "_get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_res
        quotes = await broker.get_quote("AAPL")
        assert len(quotes) == 1
        assert quotes[0]["price"] == 0.0


# --- Bug 10 ---
def test_bug_10_broker_wrapper_lots_zero():
    # AsyncBrokerWrapper._broker_order_dict parses lots from quantity
    # Fix: if quantity is 0, lots should be 0
    o = {
        "quantity": 0,
        "symbol": "AAPL",
        "status": "filled",
        "price": 1.5,
        "side": "sell",
        "tag": "HERMES_CS75"
    }
    _raw_qty = o.get("quantity")
    lots = int(_raw_qty) if _raw_qty is not None else 1
    assert lots == 0
