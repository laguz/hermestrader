"""Unit tests for the Tradier MCP server tools.

Uses a module-scoped fixture to mock missing dependencies (mcp, numpy, pandas,
requests, etc.) so that the server module can be imported and tested without
polluting the global sys.modules state for other test files.
"""
from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(scope="module", autouse=True)
def mock_dependencies():
    """Patch sys.modules with mocks for dependencies missing in the test environment."""
    # Create mocks for all missing modules
    mocks = {
        "mcp": MagicMock(),
        "mcp.server": MagicMock(),
        "mcp.server.fastmcp": MagicMock(),
        "numpy": MagicMock(),
        "pandas": MagicMock(),
        "requests": MagicMock(),
        "tenacity": MagicMock(),
        "scipy": MagicMock(),
        "scipy.signal": MagicMock(),
        "scipy.stats": MagicMock(),
        "sklearn": MagicMock(),
        "sklearn.cluster": MagicMock(),
    }

    # Configure FastMCP to work as a decorator that returns the function itself
    def mock_tool_decorator(*args, **kwargs):
        return lambda f: f
    mocks["mcp.server.fastmcp"].FastMCP.return_value.tool.side_effect = mock_tool_decorator

    with patch.dict(sys.modules, mocks):
        yield


@pytest.fixture
def mcp_server():
    """Import and return the mcp server module after mocks are applied."""
    # We must import inside the fixture so that it happens after sys.modules is patched
    import hermes.mcp.server as server
    return server


def test_get_positions_delegates_to_broker(mcp_server):
    """Verify that get_positions tool correctly calls the broker and returns its data."""
    from tests._stubs import StubBroker

    # Setup
    expected_positions = [
        {"symbol": "AAPL", "quantity": 100, "cost_basis": 150.0, "date_acquired": "2023-01-01"},
        {"symbol": "TSLA", "quantity": 50, "cost_basis": 200.0, "date_acquired": "2023-01-02"}
    ]
    broker = StubBroker(positions=expected_positions)

    # Inject the stub broker
    mcp_server._BROKER = broker

    # Execute
    result = mcp_server.get_positions()

    # Verify
    assert result == expected_positions
    assert len(result) == 2
    assert result[0]["symbol"] == "AAPL"


def test_get_account_balances_delegates_to_broker(mcp_server):
    """Verify that get_account_balances tool correctly calls the broker and returns its data."""
    from tests._stubs import StubBroker

    broker = StubBroker(option_buying_power=50000.0)
    mcp_server._BROKER = broker

    result = mcp_server.get_account_balances()

    assert result["option_buying_power"] == 50000.0
    assert result["account_type"] == "margin"
