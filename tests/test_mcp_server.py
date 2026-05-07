from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# MOCK ENVIRONMENT
#
# The test environment is missing several core dependencies (mcp, requests,
# pandas, numpy, etc.). To allow the 'hermes.mcp.server' module to be
# imported and unit-tested, we must mock these modules in sys.modules.
#
# NOTE: This approach is a workaround for the restricted environment and
# should be replaced by proper dependency management in a standard dev setup.
# ---------------------------------------------------------------------------

def setup_mock_modules():
    # Mock MCP
    mcp_mock = MagicMock()
    def mock_tool_decorator(*args, **kwargs):
        def decorator(func):
            return func
        return decorator
    mcp_mock.server.fastmcp.FastMCP.return_value.tool.side_effect = mock_tool_decorator

    # Mock other missing dependencies
    mocks = {
        "mcp": mcp_mock,
        "mcp.server": mcp_mock.server,
        "mcp.server.fastmcp": mcp_mock.server.fastmcp,
        "requests": MagicMock(),
        "pandas": MagicMock(),
        "numpy": MagicMock(),
        "tenacity": MagicMock(),
        "scipy": MagicMock(),
        "scipy.signal": MagicMock(),
        "scipy.stats": MagicMock(),
        "sklearn": MagicMock(),
        "sklearn.cluster": MagicMock(),
    }

    # Track which modules we added so we can potentially clean them up
    added_modules = []
    for name, m in mocks.items():
        if name not in sys.modules:
            sys.modules[name] = m
            added_modules.append(name)
    return added_modules

# Apply mocks before importing the target module
setup_mock_modules()

# Clean up before importing to ensure we're testing the version with our mocks applied
if "hermes.mcp.server" in sys.modules:
    del sys.modules["hermes.mcp.server"]

import hermes.mcp.server as mcp_server
import pytest

@pytest.fixture
def mock_broker():
    """Patches the _broker function in the MCP server to return a mock broker."""
    with patch("hermes.mcp.server._broker") as mock_func:
        mock_instance = MagicMock()
        mock_func.return_value = mock_instance
        yield mock_instance

# ---------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------

def test_get_account_balances(mock_broker):
    """Verify get_account_balances correctly delegates to the broker."""
    expected_balances = {
        "option_buying_power": 10000.0,
        "total_equity": 15000.0,
        "cash": 5000.0,
        "account_type": "margin"
    }
    mock_broker.get_account_balances.return_value = expected_balances

    result = mcp_server.get_account_balances()
    assert result == expected_balances
    mock_broker.get_account_balances.assert_called_once()

def test_get_positions(mock_broker):
    """Verify get_positions correctly delegates to the broker."""
    expected_positions = [{"symbol": "AAPL", "quantity": 10}]
    mock_broker.get_positions.return_value = expected_positions

    result = mcp_server.get_positions()
    assert result == expected_positions
    mock_broker.get_positions.assert_called_once()

def test_get_orders(mock_broker):
    """Verify get_orders correctly delegates to the broker."""
    expected_orders = [{"order_id": "123", "status": "filled"}]
    mock_broker.get_orders.return_value = expected_orders

    result = mcp_server.get_orders()
    assert result == expected_orders
    mock_broker.get_orders.assert_called_once()

def test_get_quote(mock_broker):
    """Verify get_quote correctly delegates to the broker."""
    expected_quote = [{"symbol": "AAPL", "last": 150.0}]
    mock_broker.get_quote.return_value = expected_quote

    result = mcp_server.get_quote("AAPL")
    assert result == expected_quote
    mock_broker.get_quote.assert_called_with("AAPL")
