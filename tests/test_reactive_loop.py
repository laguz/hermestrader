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
