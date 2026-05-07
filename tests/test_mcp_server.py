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

def test_get_history_defaults(monkeypatch):
    mock_broker = MagicMock()
    expected_data = [{"date": "2023-01-01", "close": 150.0}]
    mock_broker.get_history.return_value = expected_data

    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    result = server.get_history(symbol="AAPL")

    assert result == expected_data
    mock_broker.get_history.assert_called_once_with("AAPL", interval="daily", start=None, end=None)

def test_get_history_custom_params(monkeypatch):
    mock_broker = MagicMock()
    expected_data = [{"date": "2023-01-01", "close": 150.0}]
    mock_broker.get_history.return_value = expected_data

    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    result = server.get_history(symbol="AAPL", interval="weekly", start="2023-01-01", end="2023-01-07")

    assert result == expected_data
    mock_broker.get_history.assert_called_once_with("AAPL", interval="weekly", start="2023-01-01", end="2023-01-07")
