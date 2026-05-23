import sys
from unittest.mock import MagicMock
import pytest

# Manually mock mcp and other missing dependencies before anything else
def tool_decorator(*args, **kwargs):
    def decorator(f):
        return f
    return decorator

mcp_instance = MagicMock()
mcp_instance.tool = tool_decorator

class FastMCP:
    def __new__(cls, *args, **kwargs):
        return mcp_instance

# Setup sys.modules mocks ONLY if they are missing
try:
    import mcp
except ImportError:
    sys.modules["mcp"] = MagicMock()

try:
    import mcp.server
except ImportError:
    sys.modules["mcp.server"] = MagicMock()

try:
    import mcp.server.fastmcp
except ImportError:
    sys.modules["mcp.server.fastmcp"] = MagicMock()

try:
    import requests
except ImportError:
    sys.modules["requests"] = MagicMock()

try:
    import numpy
except ImportError:
    sys.modules["numpy"] = MagicMock()

try:
    import pandas
except ImportError:
    sys.modules["pandas"] = MagicMock()

try:
    import tenacity
except ImportError:
    sys.modules["tenacity"] = MagicMock()

try:
    import hermes.ml.pop_engine
except ImportError:
    sys.modules["hermes.ml.pop_engine"] = MagicMock()

if isinstance(sys.modules.get("mcp.server.fastmcp"), MagicMock):
    sys.modules["mcp.server.fastmcp"].FastMCP = FastMCP

# Now we can import the server
from hermes.mcp import server

def test_get_orders_happy_path(monkeypatch):
    mock_broker = MagicMock()
    expected_orders = [{"id": "order_1", "status": "filled"}]
    mock_broker.get_orders.return_value = expected_orders

    # Mock the _broker() function
    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    result = server.get_orders()

    assert result == expected_orders
    mock_broker.get_orders.assert_called_once()

def test_get_orders_empty(monkeypatch):
    mock_broker = MagicMock()
    mock_broker.get_orders.return_value = []

    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    result = server.get_orders()

    assert result == []
    mock_broker.get_orders.assert_called_once()

def test_get_orders_exception(monkeypatch):
    mock_broker = MagicMock()
    mock_broker.get_orders.side_effect = Exception("Broker error")

    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    with pytest.raises(Exception, match="Broker error"):
        server.get_orders()

@pytest.mark.parametrize("side, expected_action_side", [
    ("buy_to_open", "buy"),
    ("sell_to_open", "sell"),
    ("buy_to_close", "buy"),
    ("sell_to_close", "sell"),
])
@pytest.mark.parametrize("order_type", ["limit", "market"])
@pytest.mark.parametrize("price", [None, 1.50])
@pytest.mark.parametrize("tag", [None, "test-tag"])
def test_place_single_option_order(monkeypatch, side, expected_action_side, order_type, price, tag):
    mock_broker = MagicMock()
    mock_broker.place_order_from_action.return_value = {"status": "ok", "order_id": 123}
    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    symbol = "AAPL"
    option_symbol = "AAPL230616C00150000"
    quantity = 2

    result = server.place_single_option_order(
        symbol=symbol,
        option_symbol=option_symbol,
        side=side,
        quantity=quantity,
        price=price,
        order_type=order_type,
        tag=tag
    )

    assert result == {"status": "ok", "order_id": 123}
    mock_broker.place_order_from_action.assert_called_once()
    action = mock_broker.place_order_from_action.call_args[0][0]

    assert action.strategy_id == "mcp"
    assert action.symbol == symbol
    assert action.order_class == "option"
    assert action.legs == [{"option_symbol": option_symbol, "action": side, "quantity": quantity}]
    assert action.price == price
    assert action.side == expected_action_side
    assert action.quantity == quantity
    assert action.order_type == order_type
    assert action.duration == "day"
    assert action.tag == tag

@pytest.mark.parametrize("order_type, expected_side", [
    ("credit", "sell"),
    ("debit", "buy"),
    ("CREDIT", "sell"),
])
@pytest.mark.parametrize("duration", ["day", "gtc"])
@pytest.mark.parametrize("tag", [None, "multi-tag"])
def test_place_multileg_order(monkeypatch, order_type, expected_side, duration, tag):
    mock_broker = MagicMock()
    mock_broker.place_order_from_action.return_value = {"status": "ok", "order_id": 456}
    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    symbol = "SPY"
    legs = [
        {"option_symbol": "SPY230616C00400000", "action": "sell_to_open", "quantity": 1},
        {"option_symbol": "SPY230616C00405000", "action": "buy_to_open", "quantity": 1},
    ]
    price = 1.25

    result = server.place_multileg_order(
        symbol=symbol,
        legs=legs,
        price=price,
        order_type=order_type,
        duration=duration,
        tag=tag
    )

    assert result == {"status": "ok", "order_id": 456}
    mock_broker.place_order_from_action.assert_called_once()
    action = mock_broker.place_order_from_action.call_args[0][0]

    assert action.strategy_id == "mcp"
    assert action.symbol == symbol
    assert action.order_class == "multileg"
    assert action.legs == legs
    assert action.price == price
    assert action.side == expected_side
    assert action.order_type == order_type
    assert action.duration == duration
    assert action.tag == tag


def test_load_env_file(monkeypatch, tmp_path):
    import os
    monkeypatch.delenv("TRADIER_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("TRADIER_API_KEY", raising=False)
    monkeypatch.delenv("TRADIER_ACCOUNT_ID", raising=False)

    dummy_server_dir = tmp_path / "hermes" / "mcp"
    dummy_server_dir.mkdir(parents=True)

    dotenv_file = tmp_path / ".env"
    dotenv_file.write_text("TRADIER_API_KEY=test-token-123\nTRADIER_ACCOUNT_ID=test-acct-456\n")

    monkeypatch.setattr(server, "__file__", str(dummy_server_dir / "server.py"))

    server.load_env_file()

    assert os.environ.get("TRADIER_API_KEY") == "test-token-123"
    assert os.environ.get("TRADIER_ACCOUNT_ID") == "test-acct-456"
