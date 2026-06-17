import asyncio
import pytest
from unittest.mock import AsyncMock, patch
from hermes.events.bus import EventBus, MarketDataEvent, OrderFillEvent
from hermes.service1_agent.core import CascadingEngine, AbstractStrategy
from ._stubs import StubDB, StubBroker

class DummyStrategy(AbstractStrategy):
    PRIORITY = 1
    NAME = "DUMMY"

    def __init__(self, broker, db, money_manager=None, ic_builder=None, config=None):
        super().__init__(broker, db, money_manager, ic_builder, config)
        self.strategy_id = "DUMMY"
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
async def test_market_data_sr_crossing_reactive_entries():
    bus = EventBus()
    bus.start()

    db = StubDB()
    broker = StubBroker()
    db.set_watchlist("DUMMY", ["AAPL"])

    strategy = DummyStrategy(broker, db)
    engine = CascadingEngine(
        broker=broker,
        db=db,
        strategies=[strategy],
        approval_mode=False,
        event_bus=bus,
        config={"max_orders_per_tick": 5}
    )

    try:
        # First quote to establish baseline price in cache (does not cross)
        ev1 = MarketDataEvent(symbol="AAPL", price=89.0)
        await engine.handle_market_data(ev1)
        assert not strategy.execute_entries_called

        # Second quote that does not cross any S/R level (remains below 90.0)
        ev2 = MarketDataEvent(symbol="AAPL", price=89.5)
        await engine.handle_market_data(ev2)
        assert not strategy.execute_entries_called

        # Third quote that crosses 90.0 (support level returned by StubBroker)
        ev3 = MarketDataEvent(symbol="AAPL", price=91.0)
        await engine.handle_market_data(ev3)
        
        # Verify strategy entries were triggered reactively
        assert strategy.execute_entries_called is True
        assert strategy.last_watchlist == ["AAPL"]
    finally:
        await bus.stop()

@pytest.mark.asyncio
async def test_order_fill_reactive_sync():
    bus = EventBus()
    bus.start()

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

    engine.sync_positions = AsyncMock()
    engine.reconcile_orphans = AsyncMock()
    engine.mm = AsyncMock()

    event = OrderFillEvent(
        broker_order_id="TEST-ORDER-123",
        symbol="AAPL",
        side="buy",
        quantity=10,
        price=150.0,
        status="filled"
    )

    try:
        await engine.handle_order_fill(event)
        
        engine.sync_positions.assert_called_once()
        engine.mm.sync_broker_orders.assert_called_once()
        engine.reconcile_orphans.assert_called_once()
    finally:
        await bus.stop()

@pytest.mark.asyncio
async def test_set_trigger_wakes_up_asyncio_event():
    import hermes.service1_agent.main as main
    
    # Initialize the global async trigger event
    main._ASYNC_TRIGGER_EVENT = asyncio.Event()
    main._TRIGGER_EVENT.clear()
    
    assert not main._TRIGGER_EVENT.is_set()
    assert not main._ASYNC_TRIGGER_EVENT.is_set()
    
    main.set_trigger()
    
    assert main._TRIGGER_EVENT.is_set()
    await asyncio.sleep(0.01)
    assert main._ASYNC_TRIGGER_EVENT.is_set()
    
    # Reset
    main._TRIGGER_EVENT.clear()
    main._ASYNC_TRIGGER_EVENT = None


@pytest.mark.asyncio
async def test_quote_updates_shared_cache():
    bus = EventBus()
    bus.start()
    
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
    
    event_data = {"bid": 100.0, "ask": 101.0, "last": 100.5}
    ev = MarketDataEvent(symbol="AAPL", price=100.5, volume=1000, data=event_data)
    
    try:
        await engine.handle_market_data(ev)
        
        # Now query the cache in broker wrapper
        quotes = await engine.broker.get_quote("AAPL")
        assert len(quotes) == 1
        assert quotes[0]["price"] == 100.5
        assert quotes[0]["bid"] == 100.0
        assert quotes[0]["ask"] == 101.0
        assert quotes[0]["volume"] == 1000
    finally:
        await bus.stop()


@pytest.mark.asyncio
async def test_order_fill_triggers_management_and_entries():
    bus = EventBus()
    bus.start()
    
    db = StubDB()
    broker = StubBroker()
    db.set_watchlist("DUMMY", ["AAPL"])
    
    strategy = DummyStrategy(broker, db)
    engine = CascadingEngine(
        broker=broker,
        db=db,
        strategies=[strategy],
        approval_mode=False,
        event_bus=bus
    )
    
    engine.sync_positions = AsyncMock()
    engine.reconcile_orphans = AsyncMock()
    engine.mm = AsyncMock()
    
    # Spy/mock process_management and process_entries
    engine.process_management = AsyncMock(return_value=[])
    engine.process_entries = AsyncMock(return_value=0)
    
    event = OrderFillEvent(
        broker_order_id="TEST-ORDER-123",
        symbol="AAPL",
        side="buy",
        quantity=10,
        price=150.0,
        status="filled"
    )
    
    try:
        await engine.handle_order_fill(event)
        
        # Verify syncs were done
        engine.sync_positions.assert_called_once()
        engine.mm.sync_broker_orders.assert_called_once()
        engine.reconcile_orphans.assert_called_once()
        
        # Verify reactive management and entries were run
        engine.process_management.assert_called_once()
        engine.process_entries.assert_called_once_with(["AAPL"])
    finally:
        await bus.stop()


@pytest.mark.asyncio
async def test_order_fill_event_triggers_main_wakeup():
    import hermes.service1_agent.main as main
    from hermes.events.bus import EventBus, OrderFillEvent
    
    # Initialize the global async trigger event
    main._ASYNC_TRIGGER_EVENT = asyncio.Event()
    main._TRIGGER_EVENT.clear()
    
    # Set up bus
    bus = EventBus()
    bus.start()
    
    # Subscribe the lambda like main.py does
    bus.subscribe(OrderFillEvent, lambda ev: main.set_trigger())
    
    try:
        # Emit OrderFillEvent
        event = OrderFillEvent(
            broker_order_id="TEST-ORDER-123",
            symbol="AAPL",
            side="buy",
            quantity=10,
            price=150.0,
            status="filled"
        )
        bus.emit(event)
        
        # Give event bus time to process and call set_trigger
        await asyncio.sleep(0.05)
        
        # Verify the trigger was set
        assert main._TRIGGER_EVENT.is_set()
        assert main._ASYNC_TRIGGER_EVENT.is_set()
    finally:
        await bus.stop()
        # Reset global trigger settings
        main._TRIGGER_EVENT.clear()
        main._ASYNC_TRIGGER_EVENT = None

