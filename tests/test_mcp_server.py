import sys
from unittest.mock import MagicMock, patch
import pytest

@pytest.fixture(scope="module", autouse=True)
def mock_environment():
    """
    Fixture to mock missing dependencies and allow importing hermes.mcp.server.
    Cleans up sys.modules after tests.
    """
    # Mock mcp.server.fastmcp.FastMCP before any import to prevent real MCP initialization
    # and to allow the tool decorators to work correctly by returning the original function.
    mock_fastmcp_inst = MagicMock()
    mock_fastmcp_class = MagicMock(return_value=mock_fastmcp_inst)

    def mock_tool_decorator(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    mock_fastmcp_inst.tool = mock_tool_decorator

    # List of modules to mock because they are missing in the test environment.
    mock_modules = {
        "mcp": MagicMock(),
        "mcp.server": MagicMock(),
        "mcp.server.fastmcp": MagicMock(),
        "requests": MagicMock(),
        "tenacity": MagicMock(),
        "numpy": MagicMock(),
        "pandas": MagicMock(),
        "scipy": MagicMock(),
        "scipy.signal": MagicMock(),
        "scipy.stats": MagicMock(),
        "sklearn": MagicMock(),
        "sklearn.cluster": MagicMock(),
        "matplotlib": MagicMock(),
        "matplotlib.pyplot": MagicMock(),
        "ollama": MagicMock(),
    }

    # Save original sys.modules
    original_modules = sys.modules.copy()

    # Apply mocks
    sys.modules.update(mock_modules)
    sys.modules["mcp.server.fastmcp"].FastMCP = mock_fastmcp_class

    # Ensure hermes modules are reloaded under the mock context
    for mod in list(sys.modules.keys()):
        if mod.startswith("hermes"):
            del sys.modules[mod]

    import hermes.mcp.server

    yield hermes.mcp.server

    # Restore original sys.modules
    sys.modules.clear()
    sys.modules.update(original_modules)

def test_cancel_order_delegates_to_broker(mock_environment):
    """
    Verify that the cancel_order MCP tool correctly delegates to the broker's
    cancel_order method with the provided order_id.
    """
    server = mock_environment
    mock_broker = MagicMock()
    # Mock the internal _broker() helper that returns the TradierBroker instance.
    with patch.object(server, "_broker", return_value=mock_broker) as mock_broker_getter:
        order_id = "test-order-999"
        expected_response = {"status": "ok", "order_id": order_id}
        mock_broker.cancel_order.return_value = expected_response

        # Execute the tool
        result = server.cancel_order(order_id)

        # Verification
        assert mock_broker_getter.called, "_broker() was not called"
        mock_broker.cancel_order.assert_called_once_with(order_id)
        assert result == expected_response

def test_cancel_order_error_handling(mock_environment):
    """
    Verify that exceptions from the broker's cancel_order propagate up.
    """
    server = mock_environment
    mock_broker = MagicMock()
    with patch.object(server, "_broker", return_value=mock_broker):
        order_id = "bad-order"
        mock_broker.cancel_order.side_effect = Exception("Broker error")

        with pytest.raises(Exception, match="Broker error"):
            server.cancel_order(order_id)

def test_get_account_balances_delegates_to_broker(mock_environment):
    """
    Verify that the get_account_balances MCP tool correctly delegates to the broker.
    """
    server = mock_environment
    mock_broker = MagicMock()
    with patch.object(server, "_broker", return_value=mock_broker):
        expected_response = {"option_buying_power": 10000.0}
        mock_broker.get_account_balances.return_value = expected_response

        result = server.get_account_balances()

        mock_broker.get_account_balances.assert_called_once()
        assert result == expected_response
