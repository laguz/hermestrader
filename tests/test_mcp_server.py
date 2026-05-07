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

def test_get_option_chain_happy_path(monkeypatch):
    mock_broker = MagicMock()
    expected_chain = [{"symbol": "AAPL230616C00150000", "strike": 150.0}]
    mock_broker.get_option_chains.return_value = expected_chain

    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    result = server.get_option_chain("AAPL", "2023-06-16")

    assert result == expected_chain
    mock_broker.get_option_chains.assert_called_once_with("AAPL", "2023-06-16")

def test_get_option_chain_empty(monkeypatch):
    mock_broker = MagicMock()
    mock_broker.get_option_chains.return_value = []

    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    result = server.get_option_chain("AAPL", "2023-06-16")

    assert result == []
    mock_broker.get_option_chains.assert_called_once_with("AAPL", "2023-06-16")

def test_get_option_chain_exception(monkeypatch):
    mock_broker = MagicMock()
    mock_broker.get_option_chains.side_effect = Exception("Chain error")

    monkeypatch.setattr(server, "_broker", lambda: mock_broker)

    with pytest.raises(Exception, match="Chain error"):
        server.get_option_chain("AAPL", "2023-06-16")
