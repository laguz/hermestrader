import os
import asyncio
import logging
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from hermes.broker.mcp_client import MCPBrokerClient
from hermes.service1_agent.core import TradeAction

logger = logging.getLogger("test_mcp_client")


async def test_mcp_client_lazy_initialization():
    client = MCPBrokerClient()
    assert client._session is None
    
    mock_session = AsyncMock()
    mock_session.call_tool = AsyncMock()
    
    mock_response = MagicMock()
    mock_response.structuredContent = None  # exercise the text-block fallback
    mock_content = MagicMock()
    mock_content.text = '{"status": "ok", "balances": {"option_buying_power": 50000.0}}'
    mock_response.content = [mock_content]
    mock_session.call_tool.return_value = mock_response

    with patch("mcp.client.stdio.stdio_client") as mock_stdio, \
         patch("mcp.ClientSession") as mock_client_session_class:
         
        mock_client_session_class.return_value = mock_session
        mock_stdio.return_value.__aenter__.return_value = (MagicMock(), MagicMock())
        
        res = await client.get_account_balances()
        
        assert res["status"] == "ok"
        assert res["balances"] == {"option_buying_power": 50000.0}
        mock_session.call_tool.assert_awaited_once_with("get_account_balances", arguments={})
        
        await client.close()
        assert client._session is None
        assert client._ctx is None


async def test_mcp_client_place_multileg():
    client = MCPBrokerClient()
    mock_session = AsyncMock()
    mock_response = MagicMock()
    mock_response.structuredContent = None  # exercise the text-block fallback
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
        assert res["order"] == {"id": "12345", "status": "ok"}
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


async def test_mcp_client_concurrent_tasks_survive_real_timeout_reset():
    """Regression for the production incident: ReactiveController's
    order-monitor loop task (polling get_orders() ~1s) and the main
    event-consumer loop task both call into one shared MCPBrokerClient. This
    uses the real subprocess (not mocks — AsyncMock's __aenter__/__aexit__
    don't exercise anyio's real cancel-scope task-affinity check at all), so
    a hung call from either Task must reset cleanly without ever raising
    anyio's 'Attempted to exit cancel scope in a different task than it was
    entered in' — which is what wedged the client (and, transitively,
    MARKET_DATA/CLOCK_TICK processing) in production."""
    env_override = {
        "TRADIER_ACCESS_TOKEN": "mock-token",
        "TRADIER_ACCOUNT_ID": "mock-account",
        "TRADIER_BASE_URL": "https://sandbox.tradier.com/v1"
    }
    with patch.dict(os.environ, env_override):
        client = MCPBrokerClient()
        client._CALL_TIMEOUT_S = 0.05
        try:
            async def poller_task_call():
                return await client.get_option_expirations("AAPL")

            async def consumer_task_call():
                return await client.get_account_balances()

            # Two independently-scheduled Tasks, mirroring the order-monitor
            # loop task vs. event-consumer loop task in production — neither
            # is the Task that bootstraps the session, since bootstrap only
            # happens lazily inside whichever call the owner loop processes
            # first.
            results = await asyncio.gather(
                asyncio.create_task(poller_task_call()),
                asyncio.create_task(consumer_task_call()),
                return_exceptions=True,
            )
            for res in results:
                assert isinstance(res, asyncio.TimeoutError), (
                    f"expected a clean TimeoutError, got {res!r}"
                )
        finally:
            # close() must complete without the cross-task anyio RuntimeError
            # propagating out.
            await client.close()
        assert client._session is None
        assert client._ctx is None


async def test_mcp_client_partial_bootstrap_timeout_tears_down_immediately():
    """Regression: if the stdio subprocess spawns fine (ctx.__aenter__
    succeeds) but the MCP handshake (session.initialize()) hangs and times
    out, the partially-built session/ctx must be torn down right then, in the
    Task that created them — not dropped as bare local variables for
    Python's asyncgen GC hook to close later from an unrelated Task (which is
    exactly what trips anyio's cross-task cancel-scope RuntimeError). Spies
    on the real ClientSession.__aexit__ instead of scraping for the error
    message, since whether Python's cyclic GC actually runs (and when) isn't
    deterministic within a test, but whether _bootstrap explicitly tore down
    its partial state is."""
    from mcp import ClientSession

    exit_calls = []
    real_aexit = ClientSession.__aexit__

    async def spy_aexit(self, *args, **kwargs):
        exit_calls.append(self)
        return await real_aexit(self, *args, **kwargs)

    async def hangs_forever(self, *args, **kwargs):
        await asyncio.sleep(3600)

    env_override = {
        "TRADIER_ACCESS_TOKEN": "mock-token",
        "TRADIER_ACCOUNT_ID": "mock-account",
        "TRADIER_BASE_URL": "https://sandbox.tradier.com/v1"
    }
    with patch.dict(os.environ, env_override), \
         patch.object(ClientSession, "__aexit__", spy_aexit), \
         patch.object(ClientSession, "initialize", hangs_forever):
        client = MCPBrokerClient()
        # Real subprocess spawn (ctx.__aenter__) comfortably finishes well
        # under this; the patched initialize() never will.
        client._CALL_TIMEOUT_S = 2.0
        try:
            with pytest.raises(asyncio.TimeoutError):
                await client.get_account_balances()

            assert exit_calls, (
                "session.initialize() timed out but ClientSession.__aexit__ "
                "was never called — the partially-built session was left "
                "for garbage collection instead of torn down immediately"
            )
        finally:
            await client.close()


async def test_mcp_client_multiblock_object_fallback():
    """When structured content is absent, each content block decodes
    independently into its own object."""
    client = MCPBrokerClient()
    mock_session = AsyncMock()
    mock_response = MagicMock()
    mock_response.structuredContent = None

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


async def test_mcp_client_prefers_structured_content():
    """Structured content is the lossless payload and must be preferred over
    the text blocks. This is what the live FastMCP server returns: the real
    value wrapped under a single "result" key."""
    client = MCPBrokerClient()
    mock_session = AsyncMock()
    mock_response = MagicMock()
    mock_response.structuredContent = {
        "result": ["2026-05-29", "2026-06-01", "2026-06-05"]
    }
    # Text blocks intentionally hold the corrupting one-string-per-block form
    # that the old concatenation logic mangled into integers.
    mock_response.content = [
        MagicMock(text="2026-05-29"),
        MagicMock(text="2026-06-01"),
        MagicMock(text="2026-06-05"),
    ]
    mock_session.call_tool.return_value = mock_response

    with patch("mcp.client.stdio.stdio_client") as mock_stdio, \
         patch("mcp.ClientSession") as mock_client_session_class:

        mock_client_session_class.return_value = mock_session
        mock_stdio.return_value.__aenter__.return_value = (MagicMock(), MagicMock())

        res = await client.get_option_expirations("IWM")

        assert res == ["2026-05-29", "2026-06-01", "2026-06-05"]
        assert all(isinstance(d, str) for d in res)
        await client.close()


async def test_mcp_client_expirations_string_blocks_fallback():
    """Regression: option expirations arrive as one bare date string per
    content block. Without structured content, they must still decode to a
    list of date strings — not the integers the old concatenation produced
    (which made every expiry fail strptime and look like 'no DTE match')."""
    client = MCPBrokerClient()
    mock_session = AsyncMock()
    mock_response = MagicMock()
    mock_response.structuredContent = None
    mock_response.content = [
        MagicMock(text="2026-05-29"),
        MagicMock(text="2026-06-01"),
        MagicMock(text="2026-06-05"),
    ]
    mock_session.call_tool.return_value = mock_response

    with patch("mcp.client.stdio.stdio_client") as mock_stdio, \
         patch("mcp.ClientSession") as mock_client_session_class:

        mock_client_session_class.return_value = mock_session
        mock_stdio.return_value.__aenter__.return_value = (MagicMock(), MagicMock())

        res = await client.get_option_expirations("IWM")

        assert res == ["2026-05-29", "2026-06-01", "2026-06-05"]
        assert "2026-06-05" in res
        await client.close()


async def test_mcp_client_bounds_a_hanging_call_and_resets_session():
    """MCPBrokerClient._call_mcp had no timeout of its own, so a stalled
    sandbox response to session.call_tool() hung the stdio round-trip
    forever. That previously wedged every caller that funnels through here
    (sync_positions, order placement, ML history sync, ...). A timeout must
    bound the call AND reset the session so the next call gets a fresh
    subprocess instead of retrying the same dead pipe."""
    client = MCPBrokerClient()
    client._CALL_TIMEOUT_S = 0.05
    mock_session = AsyncMock()

    async def hangs_forever(*args, **kwargs):
        await asyncio.sleep(3600)

    mock_session.call_tool = hangs_forever

    with patch("mcp.client.stdio.stdio_client") as mock_stdio, \
         patch("mcp.ClientSession") as mock_client_session_class:

        mock_client_session_class.return_value = mock_session
        mock_stdio.return_value.__aenter__.return_value = (MagicMock(), MagicMock())

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(client.get_account_balances(), timeout=1.0)

        assert client._session is None
        assert client._ctx is None


async def test_mcp_client_recreates_session_on_loop_change():
    client = MCPBrokerClient()
    mock_session1 = AsyncMock()
    mock_session2 = AsyncMock()
    
    mock_response = MagicMock()
    mock_response.structuredContent = None  # exercise the text-block fallback
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
        assert res1["status"] == "ok"
        assert client._loop == asyncio.get_running_loop()
        
        # Simulate loop change by manually setting client._loop to a different object
        client._loop = object()
        
        res2 = await client.get_account_balances()
        assert res2["status"] == "ok"
        assert client._loop == asyncio.get_running_loop()
        
        assert mock_session1.__aexit__.called
        assert mock_session2.call_tool.called

        await client.close()


async def test_mcp_client_multiplexes_concurrent_calls_over_one_session():
    """Two invariants, one shared session.

    (1) Bootstrap stays single-flight: concurrent tasks must not race past
    the session-is-None check and each spawn their own stdio subprocess —
    that's the orphaned-session bug that produced anyio's cross-task
    cancel-scope RuntimeError.

    (2) Calls themselves must OVERLAP, not queue single-file. MCP is JSON-RPC
    with per-request ids (ClientSession routes each response to its own
    stream) and the FastMCP server dispatches every request on its own task,
    so serializing call_tool was pure loss — in production the one-at-a-time
    channel meant every caller in the process (order-monitor polling
    get_orders ~1s, the tick pipeline, reactive MARKET_DATA handlers, ML
    history sync) queued behind ~1s-per-call throughput, and MARKET_DATA
    processing blew its 90s budget waiting for the channel rather than doing
    work. An earlier version of this test asserted max_concurrent == 1,
    pinning that starvation as expected behavior."""
    client = MCPBrokerClient()

    created_sessions = []
    concurrent_calls = 0
    max_concurrent = 0

    def make_session():
        session = AsyncMock()

        async def call_tool(*args, **kwargs):
            nonlocal concurrent_calls, max_concurrent
            concurrent_calls += 1
            max_concurrent = max(max_concurrent, concurrent_calls)
            await asyncio.sleep(0.05)
            concurrent_calls -= 1
            resp = MagicMock()
            resp.structuredContent = {"result": {"status": "ok"}}
            return resp

        session.call_tool = call_tool
        return session

    class _SlowStdioCtx:
        async def __aenter__(self):
            await asyncio.sleep(0.05)  # simulate slow subprocess bootstrap
            return (MagicMock(), MagicMock())

        async def __aexit__(self, *_exc):
            return None

    def stdio_client_factory(*_args, **_kwargs):
        return _SlowStdioCtx()

    def client_session_factory(*_args, **_kwargs):
        session = make_session()
        created_sessions.append(session)
        return session

    with patch("mcp.client.stdio.stdio_client", side_effect=stdio_client_factory), \
         patch("mcp.ClientSession", side_effect=client_session_factory):

        await asyncio.gather(
            client.get_account_balances(),
            client.get_account_balances(),
        )

    assert len(created_sessions) == 1, (
        f"expected exactly one shared session, got {len(created_sessions)} — "
        "concurrent tasks raced past the session-is-None check "
        "and each bootstrapped their own stdio session"
    )
    assert max_concurrent == 2, (
        "concurrent call_tool invocations were serialized instead of "
        "multiplexed over the shared session — this re-creates the "
        "single-file channel that starved MARKET_DATA processing"
    )

    await client.close()


async def test_mcp_client_timeout_reset_is_generation_guarded():
    """A hung call must reset the session without clobbering the *replacement*
    session a later call is already using. The reset command carries the
    generation the timed-out caller actually held; if the owner has since
    torn down and re-bootstrapped, a stale reset must be a no-op — otherwise
    every straggler timing out from an old generation would keep killing the
    fresh session out from under healthy callers."""
    client = MCPBrokerClient()
    client._CALL_TIMEOUT_S = 0.1

    created_sessions = []

    def make_session(hang: bool):
        session = AsyncMock()

        async def call_tool(*args, **kwargs):
            if hang:
                await asyncio.sleep(3600)
            resp = MagicMock()
            resp.structuredContent = {"result": {"status": "ok"}}
            return resp

        session.call_tool = call_tool
        return session

    def client_session_factory(*_args, **_kwargs):
        # First session hangs every call; the replacement works.
        session = make_session(hang=len(created_sessions) == 0)
        created_sessions.append(session)
        return session

    with patch("mcp.client.stdio.stdio_client") as mock_stdio, \
         patch("mcp.ClientSession", side_effect=client_session_factory):
        mock_stdio.return_value.__aenter__.return_value = (MagicMock(), MagicMock())

        with pytest.raises(asyncio.TimeoutError):
            await client.get_account_balances()
        assert client._session is None, (
            "timed-out call did not reset the session"
        )

        res = await client.get_account_balances()
        assert res["status"] == "ok"
        assert len(created_sessions) == 2

        # A stale reset for generation 1 must not tear down generation 2.
        stale_reset: asyncio.Future = asyncio.get_running_loop().create_future()
        await client._queue.put(("reset", 1, stale_reset))
        await stale_reset
        assert client._session is not None, (
            "a stale-generation reset tore down the healthy replacement session"
        )

        await client.close()

