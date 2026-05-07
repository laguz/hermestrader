import sys
import os
from unittest.mock import MagicMock, patch

# Create persistent mocks for sys.modules
mock_mcp = MagicMock()
mock_fastmcp = MagicMock()
mock_requests = MagicMock()
mock_numpy = MagicMock()
mock_pandas = MagicMock()
mock_tenacity = MagicMock()
mock_scipy = MagicMock()
mock_sklearn = MagicMock()

# Setup mock_tenacity
def mock_retry(*args, **kwargs):
    return lambda f: f
mock_tenacity.retry = mock_retry

# FastMCP().tool() should return a decorator that returns the function itself
def mock_tool_decorator(*args, **kwargs):
    def decorator(f):
        return f
    return decorator

# IMPORTANT: Setup mock_fastmcp.FastMCP() before it's used at module level in server.py
# In server.py: mcp = FastMCP("tradier")
mock_fastmcp.FastMCP.return_value.tool.side_effect = mock_tool_decorator

# Apply patches globally for this test file
sys.modules["mcp"] = mock_mcp
sys.modules["mcp.server"] = MagicMock()
sys.modules["mcp.server.fastmcp"] = mock_fastmcp
sys.modules["requests"] = mock_requests
sys.modules["numpy"] = mock_numpy
sys.modules["pandas"] = mock_pandas
sys.modules["tenacity"] = mock_tenacity
sys.modules["scipy"] = mock_scipy
sys.modules["scipy.signal"] = MagicMock()
sys.modules["scipy.stats"] = MagicMock()
sys.modules["sklearn"] = mock_sklearn
sys.modules["sklearn.cluster"] = MagicMock()

import pytest
import importlib

# Now import the server
import hermes.mcp.server

@pytest.fixture(autouse=True)
def clean_broker():
    """Ensure the broker is re-initialized for each test."""
    importlib.reload(hermes.mcp.server)

    if hasattr(hermes.mcp.server, "_BROKER"):
        del hermes.mcp.server._BROKER

    with patch("hermes.mcp.server.TradierBroker") as MockBroker:
        broker_instance = MockBroker.return_value
        with patch.dict(os.environ, {"TRADIER_ACCESS_TOKEN": "fake", "TRADIER_ACCOUNT_ID": "fake"}):
            yield broker_instance

def test_get_quote_forwards_symbols(clean_broker):
    broker_instance = clean_broker
    broker_instance.get_quote.return_value = [{"symbol": "AAPL", "last": 150.0}]

    result = hermes.mcp.server.get_quote("AAPL,MSFT")

    # Verify the broker was called with the correct string
    broker_instance.get_quote.assert_called_once_with("AAPL,MSFT")
    assert result == [{"symbol": "AAPL", "last": 150.0}]

def test_get_account_balances(clean_broker):
    broker_instance = clean_broker
    broker_instance.get_account_balances.return_value = {"option_buying_power": 50000.0}

    result = hermes.mcp.server.get_account_balances()

    broker_instance.get_account_balances.assert_called_once()
    assert result == {"option_buying_power": 50000.0}

def test_get_positions(clean_broker):
    broker_instance = clean_broker
    broker_instance.get_positions.return_value = [{"symbol": "AAPL", "quantity": 10}]

    result = hermes.mcp.server.get_positions()

    broker_instance.get_positions.assert_called_once()
    assert result == [{"symbol": "AAPL", "quantity": 10}]

def test_get_orders(clean_broker):
    broker_instance = clean_broker
    broker_instance.get_orders.return_value = [{"id": "123", "status": "open"}]

    result = hermes.mcp.server.get_orders()

    broker_instance.get_orders.assert_called_once()
    assert result == [{"id": "123", "status": "open"}]

def test_cancel_order(clean_broker):
    broker_instance = clean_broker
    broker_instance.cancel_order.return_value = {"status": "ok"}

    result = hermes.mcp.server.cancel_order("123")

    broker_instance.cancel_order.assert_called_once_with("123")
    assert result == {"status": "ok"}

def test_get_option_expirations(clean_broker):
    broker_instance = clean_broker
    broker_instance.get_option_expirations.return_value = ["2025-01-01"]

    result = hermes.mcp.server.get_option_expirations("AAPL")

    broker_instance.get_option_expirations.assert_called_once_with("AAPL")
    assert result == ["2025-01-01"]

def test_get_option_chain(clean_broker):
    broker_instance = clean_broker
    broker_instance.get_option_chains.return_value = [{"strike": 150}]

    result = hermes.mcp.server.get_option_chain("AAPL", "2025-01-01")

    broker_instance.get_option_chains.assert_called_once_with("AAPL", "2025-01-01")
    assert result == [{"strike": 150}]

def test_get_delta(clean_broker):
    broker_instance = clean_broker
    broker_instance.get_delta.return_value = 0.5

    result = hermes.mcp.server.get_delta("AAPL250101C00150000")

    broker_instance.get_delta.assert_called_once_with("AAPL250101C00150000")
    assert result == 0.5

def test_get_history(clean_broker):
    broker_instance = clean_broker
    broker_instance.get_history.return_value = [{"date": "2025-01-01", "close": 150.0}]

    result = hermes.mcp.server.get_history("AAPL", interval="daily")

    broker_instance.get_history.assert_called_once_with("AAPL", interval="daily", start=None, end=None)
    assert result == [{"date": "2025-01-01", "close": 150.0}]

def test_analyze_symbol(clean_broker):
    broker_instance = clean_broker
    broker_instance.analyze_symbol.return_value = {"symbol": "AAPL", "current_price": 150.0}

    result = hermes.mcp.server.analyze_symbol("AAPL", period="6m")

    broker_instance.analyze_symbol.assert_called_once_with("AAPL", period="6m")
    assert result == {"symbol": "AAPL", "current_price": 150.0}

def test_place_multileg_order(clean_broker):
    broker_instance = clean_broker
    broker_instance.place_order_from_action.return_value = {"order_id": "ML123"}

    legs = [{"option_symbol": "AAPL250101C00150000", "quantity": 1, "side": "sell_to_open"}]
    result = hermes.mcp.server.place_multileg_order("AAPL", legs, price=1.0)

    broker_instance.place_order_from_action.assert_called_once()
    action = broker_instance.place_order_from_action.call_args[0][0]
    assert action.symbol == "AAPL"
    assert action.legs == legs
    assert action.price == 1.0
    assert result == {"order_id": "ML123"}

def test_place_single_option_order(clean_broker):
    broker_instance = clean_broker
    broker_instance.place_order_from_action.return_value = {"order_id": "SO123"}

    result = hermes.mcp.server.place_single_option_order("AAPL", "AAPL250101C00150000", "sell_to_open", 1, price=1.0)

    broker_instance.place_order_from_action.assert_called_once()
    action = broker_instance.place_order_from_action.call_args[0][0]
    assert action.symbol == "AAPL"
    assert action.legs[0]["option_symbol"] == "AAPL250101C00150000"
    assert action.price == 1.0
    assert result == {"order_id": "SO123"}

def test_place_equity_order(clean_broker):
    broker_instance = clean_broker
    broker_instance.place_order_from_action.return_value = {"order_id": "EQ123"}

    result = hermes.mcp.server.place_equity_order("AAPL", "buy", 100)

    broker_instance.place_order_from_action.assert_called_once()
    action = broker_instance.place_order_from_action.call_args[0][0]
    assert action.symbol == "AAPL"
    assert action.side == "buy"
    assert action.quantity == 100
    assert result == {"order_id": "EQ123"}

def test_roll_to_next_month(clean_broker):
    broker_instance = clean_broker
    broker_instance.roll_to_next_month.return_value = "AAPL250201C00150000"

    result = hermes.mcp.server.roll_to_next_month("AAPL250101C00150000")

    broker_instance.roll_to_next_month.assert_called_once_with("AAPL250101C00150000")
    assert result == "AAPL250201C00150000"
