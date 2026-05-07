
import sys
from unittest.mock import MagicMock, patch

# Mock mcp before importing hermes.mcp.server
mcp_mock = MagicMock()
mcp_instance_mock = MagicMock()
mcp_instance_mock.tool.return_value = lambda x: x
mcp_mock.server.fastmcp.FastMCP.return_value = mcp_instance_mock

sys.modules["mcp"] = mcp_mock
sys.modules["mcp.server"] = mcp_mock.server
sys.modules["mcp.server.fastmcp"] = mcp_mock.server.fastmcp

# Mock other heavy dependencies that might be imported by TradierBroker
sys.modules["requests"] = MagicMock()
sys.modules["tenacity"] = MagicMock()
sys.modules["numpy"] = MagicMock()
sys.modules["pandas"] = MagicMock()
sys.modules["hermes.ml.pop_engine"] = MagicMock()

import hermes.mcp.server as mcp_server

def test_get_delta_success():
    """Test get_delta returns the value from the broker."""
    with patch("hermes.mcp.server._broker") as mock_broker_func:
        mock_broker = mock_broker_func.return_value
        mock_broker.get_delta.return_value = 0.5

        result = mcp_server.get_delta("AAPL240621C00150000")

        assert result == 0.5
        mock_broker.get_delta.assert_called_once_with("AAPL240621C00150000")

def test_get_delta_zero():
    """Test get_delta returns 0.0 when the broker returns 0.0."""
    with patch("hermes.mcp.server._broker") as mock_broker_func:
        mock_broker = mock_broker_func.return_value
        mock_broker.get_delta.return_value = 0.0

        result = mcp_server.get_delta("INVALID")

        assert result == 0.0
        mock_broker.get_delta.assert_called_once_with("INVALID")
