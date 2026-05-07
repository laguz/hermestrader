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

# Setup sys.modules mocks
missing_deps = [
    "mcp", "mcp.server", "mcp.server.fastmcp",
    "requests", "numpy", "pandas", "tenacity",
    "hermes.ml.pop_engine"
]
for dep in missing_deps:
    sys.modules[dep] = MagicMock()

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

def test_place_equity_order_market_happy_path(monkeypatch):
    mock_broker = MagicMock()
    mock_broker.place_order_from_action.return_value = {"status": "ok", "order_id": 123}
    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    result = server.place_equity_order(symbol="AAPL", side="buy", quantity=10, order_type="market")

    assert result["status"] == "ok"
    # Verify TradeAction construction
    args, _ = mock_broker.place_order_from_action.call_args
    action = args[0]
    assert action.symbol == "AAPL"
    assert action.side == "buy"
    assert action.quantity == 10
    assert action.order_type == "market"
    assert action.order_class == "equity"
    assert action.legs == [{"side": "buy", "quantity": 10}]

def test_place_equity_order_limit_happy_path(monkeypatch):
    mock_broker = MagicMock()
    mock_broker.place_order_from_action.return_value = {"status": "ok", "order_id": 124}
    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    result = server.place_equity_order(symbol="tsla", side="SELL", quantity=5, order_type="LIMIT", price=250.0)

    assert result["status"] == "ok"
    args, _ = mock_broker.place_order_from_action.call_args
    action = args[0]
    assert action.symbol == "TSLA"
    assert action.side == "sell"
    assert action.quantity == 5
    assert action.order_type == "limit"
    assert action.price == 250.0

def test_place_equity_order_invalid_side():
    with pytest.raises(ValueError, match="Invalid side: invalid_side"):
        server.place_equity_order(symbol="AAPL", side="invalid_side", quantity=10)

def test_place_equity_order_invalid_quantity():
    with pytest.raises(ValueError, match="Invalid quantity: 0"):
        server.place_equity_order(symbol="AAPL", side="buy", quantity=0)
    with pytest.raises(ValueError, match="Invalid quantity: -1"):
        server.place_equity_order(symbol="AAPL", side="buy", quantity=-1)

def test_place_equity_order_invalid_order_type():
    with pytest.raises(ValueError, match="Invalid order_type: invalid_type"):
        server.place_equity_order(symbol="AAPL", side="buy", quantity=10, order_type="invalid_type")

def test_place_equity_order_missing_price_for_limit():
    with pytest.raises(ValueError, match="Price is required for limit orders"):
        server.place_equity_order(symbol="AAPL", side="buy", quantity=10, order_type="limit")
