import asyncio
import json
import pytest
from unittest.mock import AsyncMock, patch
from hermes.events.bus import EventBus, MarketDataEvent
from hermes.broker.tradier_stream import TradierStreamClient
from hermes.service1_agent.core import CascadingEngine, AbstractStrategy
from ._stubs import StubDB, StubBroker

class DummyStrategy(AbstractStrategy):
    PRIORITY = 1
    NAME = "DUMMY"

    def __init__(self, broker, db, money_manager=None, ic_builder=None, config=None):
        super().__init__(broker, db, money_manager, ic_builder, config)
        self.manage_positions_called = False
        self.execute_entries_called = False
        self.last_watchlist = None

    def manage_positions(self) -> list:
        self.manage_positions_called = True
        return []

    def execute_entries(self, watchlist) -> list:
        self.execute_entries_called = True
        self.last_watchlist = list(watchlist)
        return []

class MockWS:
    def __init__(self):
        self.sent_messages = []
        self.message_queue = asyncio.Queue()
        self.open = True

    async def send(self, message):
        self.sent_messages.append(message)

    async def close(self):
        self.open = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return await self.message_queue.get()
        except asyncio.CancelledError:
            raise StopAsyncIteration

@pytest.fixture(autouse=True)
def allow_offhours(monkeypatch):
    monkeypatch.setenv("HERMES_ALLOW_OFFHOURS_TRADES", "true")

@pytest.mark.asyncio
async def test_stream_client_to_engine_flow():
    # 1. Setup Event Bus
    bus = EventBus()
    bus.start()

    # 2. Setup Stubs & Engine
    db = StubDB()
    broker = StubBroker()
    
    strategy = DummyStrategy(broker, db)
    engine = CascadingEngine(
        broker=broker,
        db=db,
        strategies=[strategy],
        approval_mode=False,
        event_bus=bus
    )

    # 3. Setup TradierStreamClient with mocked session & websocket
    stream_client = TradierStreamClient(
        token="mock_token",
        account_id="mock_account",
        base_url="https://sandbox.tradier.com/v1",
        event_bus=bus,
        watchlist=["AAPL"]
    )

    # Mock _create_session
    stream_client._create_session = AsyncMock(return_value={
        "url": "wss://ws.tradier.com/v1/markets/events",
        "sessionid": "mock_session_id"
    })

    mock_ws = MockWS()

    # Use patch to mock websockets.connect
    with patch("websockets.connect", return_value=mock_ws):
        await stream_client.start()
        
        # Give start loop a moment to run and send subscription
        await asyncio.sleep(0.05)
        
        # Verify subscription was sent
        assert len(mock_ws.sent_messages) == 1
        sub_data = json.loads(mock_ws.sent_messages[0])
        assert sub_data["symbols"] == ["AAPL"]
        assert sub_data["sessionid"] == "mock_session_id"

        # Emit mock quote message through websocket queue
        quote_msg = json.dumps({
            "type": "quote",
            "symbol": "AAPL",
            "bid": "150.00",
            "ask": "150.10",
            "last": "150.05"
        })
        await mock_ws.message_queue.put(quote_msg)

        # Allow time for event dispatching and handler execution
        await asyncio.sleep(0.1)

        # Verify quote cache updated in engine
        assert "AAPL" in engine._quote_cache
        assert engine._quote_cache["AAPL"]["price"] == 150.05
        assert engine._quote_cache["AAPL"]["bid"] == "150.00"
        assert engine._quote_cache["AAPL"]["ask"] == "150.10"

        # Verify strategy callbacks triggered
        assert strategy.manage_positions_called is True
        assert strategy.execute_entries_called is True
        assert strategy.last_watchlist == ["AAPL"]

        # Clean up
        await stream_client.stop()
        await bus.stop()
