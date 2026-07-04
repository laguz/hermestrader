import asyncio
import pytest
from unittest.mock import AsyncMock, patch
from hermes.events.bus import EventBus, MarketDataEvent, OrderFillEvent
from hermes.broker.mock_stream import MockStreamClient
from hermes.service1_agent.core import CascadingEngine, AbstractStrategy
from hermes.service1_agent.trade_action import TradeAction
from ._stubs import StubDB, StubBroker

class DummyStrategy(AbstractStrategy):
    PRIORITY = 1
    NAME = "DUMMY"

    def __init__(self, broker, db, money_manager=None, ic_builder=None, config=None):
        super().__init__(broker, db, money_manager, ic_builder, config)
        self.manage_positions_called = False
        self.execute_entries_called = False
        self.last_watchlist = None

    async def manage_positions(self) -> list:
        self.manage_positions_called = True
        return []

    async def execute_entries(self, watchlist) -> list:
        self.execute_entries_called = True
        self.last_watchlist = list(watchlist)
        return []

@pytest.fixture(autouse=True)
def allow_offhours(monkeypatch):
    monkeypatch.setenv("HERMES_ALLOW_OFFHOURS_TRADES", "true")

@pytest.mark.asyncio
async def test_mock_stream_client_quote_flow():
    # 1. Setup Event Bus
    bus = EventBus()
    bus.start()

    # 2. Setup DB stub
    db = StubDB()
    with patch.object(db, "all_watchlist_symbols", new_callable=AsyncMock) as mock_wl, \
         patch.object(db, "tracked_option_symbols", new_callable=AsyncMock) as mock_tracked:
        
        mock_wl.return_value = ["AAPL", "MSFT"]
        mock_tracked.return_value = set()

        # 3. Setup MockStreamClient
        client = MockStreamClient(event_bus=bus, watchlist=["AAPL", "MSFT"], db=db)

        # 4. Listen to MarketDataEvents
        events_received = []
        def on_market_data(event: MarketDataEvent):
            events_received.append(event)

        bus.subscribe(MarketDataEvent, on_market_data)

        await client.start()
        
        try:
            # Wait for some quote messages to be received (at least 2 quotes for AAPL/MSFT)
            for _ in range(30):
                if len(events_received) >= 2:
                    break
                await asyncio.sleep(0.1)

            assert len(events_received) >= 2
            symbols = {e.symbol for e in events_received}
            assert "AAPL" in symbols or "MSFT" in symbols
        finally:
            await client.stop()
            await bus.stop()

@pytest.mark.asyncio
async def test_reactive_order_monitor_flow():
    # 1. Setup Event Bus
    bus = EventBus()
    bus.start()

    # 2. Setup Stubs & Engine
    db = StubDB()
    with patch.object(db, "all_watchlist_symbols", new_callable=AsyncMock) as mock_all_wl:
        mock_all_wl.return_value = ["AAPL"]
        
        broker = StubBroker()
        
        # We want to mock get_orders to return a working order initially,
        # and then transition to filled.
        mock_orders = [
            {
                "id": "ord-1234",
                "symbol": "AAPL",
                "side": "buy",
                "quantity": 10,
                "status": "open",
                "price": 150.0
            }
        ]
        broker.get_orders = AsyncMock(return_value=mock_orders)
        
        strategy = DummyStrategy(broker, db)
        engine = CascadingEngine(
            broker=broker,
            db=db,
            strategies=[strategy],
            approval_mode=False,
            event_bus=bus
        )

        # 3. Track events received
        order_fills = []
        def on_order_fill(event: OrderFillEvent):
            order_fills.append(event)
            
        bus.subscribe(OrderFillEvent, on_order_fill)

        # Ensure order monitor is running
        engine._ensure_order_monitor()
        await asyncio.sleep(0.05)

        # 4. Trigger order execution
        action = TradeAction(
            strategy_id="DUMMY",
            symbol="AAPL",
            order_class="single",
            legs=[],
            price=150.0,
            side="buy",
            quantity=10,
            duration="day",
            tag="HERMES-DUMMY"
        )
        
        # Simulate place_order_from_action returning success with order_id
        broker.place_order_from_action = AsyncMock(return_value={"order_id": "ord-1234", "status": "ok"})
        
        await engine._execute_or_queue(action, action_type="entry")
        
        # Verify order was added to _tracked_orders
        assert "ord-1234" in engine._tracked_orders
        
        # Now change status to filled in get_orders
        mock_orders[0]["status"] = "filled"
        
        # Wait for order monitor loop to poll it, emit the event, and engine to finish processing entries
        for _ in range(50):
            if len(order_fills) > 0 and strategy.execute_entries_called:
                break
            await asyncio.sleep(0.1)
            
        assert len(order_fills) == 1
        assert order_fills[0].broker_order_id == "ord-1234"
        assert order_fills[0].status == "filled"
        
        # Verify order removed from tracking list
        assert "ord-1234" not in engine._tracked_orders
        
        # Verify strategy entries reactively evaluated post order fill
        assert strategy.execute_entries_called is True
        
        # Clean up
        if engine._order_monitor_task:
            engine._order_monitor_task.cancel()
        await bus.stop()


@pytest.mark.asyncio
async def test_dry_run_entry_resolves_pending_order_instead_of_leaking_capacity():
    """dry_run must resolve the row record_pending_order() just wrote, the same
    way a real broker rejection would — otherwise it sits at status=PENDING
    forever (count_pending_orders has no staleness filter), permanently
    occupying that (strategy, symbol, side, expiry) capacity slot even though
    nothing was ever sent to the broker. Regression for "[MM] BLOCKED ... at
    capacity" recurring on every tick with no order ever reaching the broker.
    """
    bus = EventBus()
    bus.start()
    try:
        db = StubDB()
        broker = StubBroker()
        broker.dry_run = True

        strategy = DummyStrategy(broker, db)
        engine = CascadingEngine(
            broker=broker,
            db=db,
            strategies=[strategy],
            approval_mode=False,
            event_bus=bus,
        )

        action = TradeAction(
            strategy_id="CS7",
            symbol="IWM",
            order_class="multileg",
            legs=[
                {"side": "sell_to_open", "quantity": 1, "option_symbol": "IWM260709P00292000"},
                {"side": "buy_to_open", "quantity": 1, "option_symbol": "IWM260709P00291000"},
            ],
            price=0.24,
            side="sell",
            quantity=1,
            duration="day",
            order_type="credit",
            expiry="2026-07-09",
            width=1.0,
            strategy_params={"side_type": "put"},
            tag="HERMES_CS7",
        )

        with patch.object(db.trades, "record_order_response", new_callable=AsyncMock) as mock_resolve:
            await engine._execute_or_queue(action, action_type="entry")

        mock_resolve.assert_awaited_once()
        resolved_action, resolved_response = mock_resolve.await_args.args
        assert resolved_action is action
        assert "errors" in resolved_response
    finally:
        await bus.stop()
