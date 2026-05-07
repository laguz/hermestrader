"""Unit tests for the Tradier MCP server tools.

NOTE: This test file uses sys.modules mocking to stand in for third-party
dependencies (mcp, requests, pandas, etc.) that are missing in the restricted
development environment. This approach allows for unit testing the logic
wrappers without requiring the full environment setup.
"""
from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

# --- Environment Mocking Start ---
# These mocks must be established before hermes.mcp.server is imported.

class MockFastMCP:
    """Mock for mcp.server.fastmcp.FastMCP class."""
    def __init__(self, *args, **kwargs):
        pass
    def tool(self, *args, **kwargs):
        def decorator(func):
            # The tool decorator just returns the function in this mock.
            return func
        return decorator
    def run(self, *args, **kwargs):
        pass

# Setup sys.modules mocks for all missing dependencies found during collection.
_MOCK_MODULES = [
    "mcp",
    "mcp.server",
    "mcp.server.fastmcp",
    "requests",
    "pandas",
    "numpy",
    "tenacity",
    "scipy",
    "scipy.signal",
    "scipy.stats",
    "sklearn",
    "sklearn.cluster",
    "xgboost",
]

for mod in _MOCK_MODULES:
    if mod not in sys.modules:
        sys.modules[mod] = MagicMock()

# Specifically wire up FastMCP class in its mock module.
sys.modules["mcp.server.fastmcp"].FastMCP = MockFastMCP

# Reload the module under test to ensure it picks up the mocks if it was
# partially imported by previous test attempts.
if "hermes.mcp.server" in sys.modules:
    import importlib
    importlib.reload(sys.modules["hermes.mcp.server"])
# --- Environment Mocking End ---

import pytest
from hermes.mcp.server import roll_to_next_month

def test_roll_to_next_month_calls_broker(monkeypatch):
    """Verify that the tool wrapper calls the broker's roll_to_next_month method."""
    # 1. Setup
    mock_broker_instance = MagicMock()
    mock_broker_instance.roll_to_next_month.return_value = "AAPL250718P00150000"

    # Mock the internal _broker() getter to return our mock instance.
    monkeypatch.setattr("hermes.mcp.server._broker", lambda: mock_broker_instance)

    input_symbol = "AAPL250620P00150000"

    # 2. Execute
    result = roll_to_next_month(input_symbol)

    # 3. Assert
    assert result == "AAPL250718P00150000"
    mock_broker_instance.roll_to_next_month.assert_called_once_with(input_symbol)

def test_roll_to_next_month_propagates_errors(monkeypatch):
    """Verify that errors from the broker are propagated through the wrapper."""
    # 1. Setup
    mock_broker_instance = MagicMock()
    mock_broker_instance.roll_to_next_month.side_effect = ValueError("Invalid symbol")

    monkeypatch.setattr("hermes.mcp.server._broker", lambda: mock_broker_instance)

    # 2. Execute & 3. Assert
    with pytest.raises(ValueError, match="Invalid symbol"):
        roll_to_next_month("NOT-A-SYMBOL")
