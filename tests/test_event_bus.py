import asyncio
import pytest
from hermes.events.bus import EventBus, MarketDataEvent

@pytest.fixture
def event_bus():
    bus = EventBus()
    bus.start()
    yield bus
    # Note: we don't await bus.stop() here directly since pytest fixtures aren't async by default
    # but we can clean up in tests if needed.
    if bus._task and not bus._task.done():
        bus._task.cancel()

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

async def test_event_bus_bounds_concurrent_dispatch():
    """A handler slower than the event cadence must not spawn unbounded
    dispatch tasks — _dispatch_sem should cap how many run at once and let
    the rest queue instead of piling up as live tasks/threads."""
    bus = EventBus(max_concurrent_dispatch=2)
    bus.start()

    in_flight = {"current": 0, "max_seen": 0}
    release = asyncio.Event()

    async def slow_handler(event: MarketDataEvent):
        in_flight["current"] += 1
        in_flight["max_seen"] = max(in_flight["max_seen"], in_flight["current"])
        await release.wait()
        in_flight["current"] -= 1

    bus.subscribe(MarketDataEvent, slow_handler)

    for i in range(5):
        bus.emit(MarketDataEvent(symbol="SPY", price=float(i)))

    await asyncio.sleep(0.05)
    assert in_flight["max_seen"] == 2  # capped, not 5

    release.set()
    await asyncio.sleep(0.05)
    assert in_flight["current"] == 0

    await bus.stop()


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


async def test_event_bus_holds_strong_refs_to_dispatch_tasks():
    # The event loop keeps only weak references to tasks: a bare
    # create_task() in _process_events could be GC'd mid-flight, dropping
    # the event, leaking a semaphore permit, and hanging stop()'s join().
    bus = EventBus()
    bus.start()

    release = asyncio.Event()
    handled = []

    async def slow_handler(event: MarketDataEvent):
        await release.wait()
        handled.append(event)

    bus.subscribe(MarketDataEvent, slow_handler)
    bus.emit(MarketDataEvent(symbol="SPY", price=500.0))
    await asyncio.sleep(0.01)

    # While the handler is in flight the bus must hold the dispatch task.
    assert len(bus._dispatch_tasks) == 1

    release.set()
    await bus.stop()

    assert handled and handled[0].symbol == "SPY"
    assert not bus._dispatch_tasks  # done-callback discarded the reference
