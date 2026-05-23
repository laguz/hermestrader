import asyncio
import pytest
from datetime import datetime
from hermes.events.bus import EventBus, Event, MarketDataEvent

@pytest.fixture
def event_bus():
    bus = EventBus()
    bus.start()
    yield bus
    # Note: we don't await bus.stop() here directly since pytest fixtures aren't async by default
    # but we can clean up in tests if needed.
    if bus._task and not bus._task.done():
        bus._task.cancel()

@pytest.mark.asyncio
async def test_event_bus_subscription_and_dispatch():
    bus = EventBus()
    bus.start()
    
    received_events = []

    async def my_handler(event: MarketDataEvent):
        received_events.append(event)

    bus.subscribe(MarketDataEvent, my_handler)

    # Emit the event
    test_event = MarketDataEvent(symbol="SPY", price=500.0)
    bus.emit(test_event)

    # Yield to event loop to allow processing
    await asyncio.sleep(0.01)

    assert len(received_events) == 1
    assert received_events[0].symbol == "SPY"
    assert received_events[0].price == 500.0
    
    await bus.stop()

@pytest.mark.asyncio
async def test_event_bus_multiple_handlers():
    bus = EventBus()
    bus.start()
    
    counter = {"h1": 0, "h2": 0}

    async def handler_one(event: MarketDataEvent):
        counter["h1"] += 1

    async def handler_two(event: MarketDataEvent):
        counter["h2"] += 1

    bus.subscribe(MarketDataEvent, handler_one)
    bus.subscribe(MarketDataEvent, handler_two)

    bus.emit(MarketDataEvent(symbol="QQQ", price=400.0))
    await asyncio.sleep(0.01)

    assert counter["h1"] == 1
    assert counter["h2"] == 1
    
    await bus.stop()

@pytest.mark.asyncio
async def test_event_bus_handler_exception_isolation():
    bus = EventBus()
    bus.start()
    
    success_count = [0]

    async def failing_handler(event: MarketDataEvent):
        raise ValueError("Simulated handler failure")

    async def successful_handler(event: MarketDataEvent):
        success_count[0] += 1

    bus.subscribe(MarketDataEvent, failing_handler)
    bus.subscribe(MarketDataEvent, successful_handler)

    # Emit event. Even though the first handler fails, the second should succeed 
    # and the bus itself should not crash.
    bus.emit(MarketDataEvent(symbol="IWM", price=200.0))
    await asyncio.sleep(0.01)

    assert success_count[0] == 1
    assert not bus._task.done()  # Task loop should still be running
    
    await bus.stop()
