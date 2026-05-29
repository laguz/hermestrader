import os
import asyncio
import logging
from unittest.mock import AsyncMock, patch, MagicMock
from hermes.broker.mcp_client import MCPBrokerClient
from hermes.service1_agent.core import TradeAction

logger = logging.getLogger("test_mcp_client")


async def test_mcp_client_lazy_initialization():
    client = MCPBrokerClient()
    assert client._session is None
    
    mock_session = AsyncMock()
    mock_session.call_tool = AsyncMock()
    
    mock_response = MagicMock()
    mock_content = MagicMock()
    mock_content.text = '{"status": "ok", "balances": {"option_buying_power": 50000.0}}'
    mock_response.content = [mock_content]
    mock_session.call_tool.return_value = mock_response

    with patch("mcp.client.stdio.stdio_client") as mock_stdio, \
         patch("mcp.ClientSession") as mock_client_session_class:
         
        mock_client_session_class.return_value = mock_session
        mock_stdio.return_value.__aenter__.return_value = (MagicMock(), MagicMock())
        
        res = await client.get_account_balances()
        
        assert res == {"status": "ok", "balances": {"option_buying_power": 50000.0}}
        mock_session.call_tool.assert_awaited_once_with("get_account_balances", arguments={})
        
        await client.close()
        assert client._session is None
        assert client._ctx is None


async def test_mcp_client_place_multileg():
    client = MCPBrokerClient()
    mock_session = AsyncMock()
    mock_response = MagicMock()
    mock_content = MagicMock()
    mock_content.text = '{"order": {"id": "12345", "status": "ok"}}'
    mock_response.content = [mock_content]
    mock_session.call_tool.return_value = mock_response

    with patch("mcp.client.stdio.stdio_client") as mock_stdio, \
         patch("mcp.ClientSession") as mock_client_session_class:
         
        mock_client_session_class.return_value = mock_session
        mock_stdio.return_value.__aenter__.return_value = (MagicMock(), MagicMock())
        
        action = TradeAction(
            strategy_id="CS75",
            symbol="AAPL",
            order_class="multileg",
            legs=[
                {"option_symbol": "AAPL260619P00150000", "side": "sell", "quantity": 2},
                {"option_symbol": "AAPL260619P00145000", "side": "buy", "quantity": 2}
            ],
            price=1.50,
            side="sell",
            order_type="credit",
            duration="day",
            tag="HERMES_CS75"
        )
        
        res = await client.place_order_from_action(action)
        assert res == {"order": {"id": "12345", "status": "ok"}}
        mock_session.call_tool.assert_awaited_once_with(
            "place_multileg_order",
            arguments={
                "symbol": "AAPL",
                "legs": [
                    {"option_symbol": "AAPL260619P00150000", "quantity": 2, "action": "sell"},
                    {"option_symbol": "AAPL260619P00145000", "quantity": 2, "action": "buy"}
                ],
                "price": 1.50,
                "order_type": "credit",
                "duration": "day",
                "tag": "HERMES_CS75"
            }
        )
        await client.close()


async def test_mcp_client_subprocess_lifecycle():
    env_override = {
        "TRADIER_ACCESS_TOKEN": "mock-token",
        "TRADIER_ACCOUNT_ID": "mock-account",
        "TRADIER_BASE_URL": "https://sandbox.tradier.com/v1"
    }
    with patch.dict(os.environ, env_override):
        client = MCPBrokerClient()
        try:
            await asyncio.wait_for(client.get_option_expirations("AAPL"), timeout=10.0)
        except Exception as e:
            logger.info("Subprocess error as expected: %s", e)
        finally:
            await client.close()


async def test_mcp_client_concatenated_json_parsing():
    client = MCPBrokerClient()
    mock_session = AsyncMock()
    mock_response = MagicMock()
    
    block1 = MagicMock()
    block1.text = '{"date": "2025-04-21", "close": 193.16}'
    block2 = MagicMock()
    block2.text = '{"date": "2025-04-22", "close": 199.74}'
    mock_response.content = [block1, block2]
    mock_session.call_tool.return_value = mock_response

    with patch("mcp.client.stdio.stdio_client") as mock_stdio, \
         patch("mcp.ClientSession") as mock_client_session_class:
         
        mock_client_session_class.return_value = mock_session
        mock_stdio.return_value.__aenter__.return_value = (MagicMock(), MagicMock())
        
        res = await client.get_history("AAPL")
        
        assert isinstance(res, list)
        assert len(res) == 2
        assert res[0] == {"date": "2025-04-21", "close": 193.16}
        assert res[1] == {"date": "2025-04-22", "close": 199.74}
        
        await client.close()


async def test_mcp_client_recreates_session_on_loop_change():
    client = MCPBrokerClient()
    mock_session1 = AsyncMock()
    mock_session2 = AsyncMock()
    
    mock_response = MagicMock()
    mock_content = MagicMock()
    mock_content.text = '{"status": "ok"}'
    mock_response.content = [mock_content]
    
    mock_session1.call_tool.return_value = mock_response
    mock_session2.call_tool.return_value = mock_response

    with patch("mcp.client.stdio.stdio_client") as mock_stdio, \
         patch("mcp.ClientSession") as mock_client_session_class:
         
        mock_client_session_class.side_effect = [mock_session1, mock_session2]
        mock_stdio.return_value.__aenter__.return_value = (MagicMock(), MagicMock())
        
        res1 = await client.get_account_balances()
        assert res1 == {"status": "ok"}
        assert client._loop == asyncio.get_running_loop()
        
        # Simulate loop change by manually setting client._loop to a different object
        client._loop = object()
        
        res2 = await client.get_account_balances()
        assert res2 == {"status": "ok"}
        assert client._loop == asyncio.get_running_loop()
        
        assert mock_session1.__aexit__.called
        assert mock_session2.call_tool.called
        
        await client.close()

