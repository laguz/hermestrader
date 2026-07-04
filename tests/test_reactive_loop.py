import asyncio
import json
import pytest
from unittest.mock import AsyncMock
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
    db.watchlist.set_watchlist("DUMMY", ["AAPL"])

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
    db.watchlist.set_watchlist("DUMMY", ["AAPL"])
    
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


@pytest.mark.asyncio
async def test_market_data_management_sweep_skips_strategies_with_no_open_position():
    """Production incident: every market-data tick ran manage_positions() —
    a full broker-backed quote sweep — for EVERY strategy, then threw away
    results that didn't match the ticked symbol. In paper mode (Tradier
    sandbox, materially slower/spikier than production) stacking that many
    broker round-trips per tick routinely blew past the 90s durable-event
    timeout, so every single MARKET_DATA event timed out back-to-back.
    manage_positions() must only run for strategies actually holding a
    position in the ticked symbol.
    """
    bus = EventBus()
    bus.start()

    db = StubDB()
    broker = StubBroker()
    # Only HAS_AAPL holds a position in AAPL; HAS_OTHER holds one in a
    # different symbol and must not be swept on an AAPL tick.
    db.set_open_trades("HAS_AAPL", [
        {"id": 1, "strategy_id": "HAS_AAPL", "symbol": "AAPL", "side_type": "put",
         "width": 5.0, "entry_credit": 1.0, "lots": 1, "expiry": "2026-06-20"},
    ])
    db.set_open_trades("HAS_OTHER", [
        {"id": 2, "strategy_id": "HAS_OTHER", "symbol": "MSFT", "side_type": "put",
         "width": 5.0, "entry_credit": 1.0, "lots": 1, "expiry": "2026-06-20"},
    ])

    class NamedDummyStrategy(DummyStrategy):
        def __init__(self, strategy_id, *a, **kw):
            super().__init__(*a, **kw)
            self.strategy_id = strategy_id

    has_aapl = NamedDummyStrategy("HAS_AAPL", broker, db)
    has_other = NamedDummyStrategy("HAS_OTHER", broker, db)
    no_positions = NamedDummyStrategy("NO_POSITIONS", broker, db)

    engine = CascadingEngine(
        broker=broker, db=db,
        strategies=[has_aapl, has_other, no_positions],
        approval_mode=False, event_bus=bus,
    )

    try:
        ev = MarketDataEvent(symbol="AAPL", price=100.0)
        await engine.handle_market_data(ev)

        assert has_aapl.manage_positions_called is True
        assert has_other.manage_positions_called is False
        assert no_positions.manage_positions_called is False
    finally:
        await bus.stop()


@pytest.mark.asyncio
async def test_publish_event_bounds_the_durable_stream():
    """Production incident: xadd() had no maxlen, so the durable Redis stream
    grew forever — every acked/processed event stayed in Redis regardless,
    with maxmemory-policy=noeviction. Live measured 130k stale entries
    (~44MB); paper, whose MARKET_DATA events were timing out and thus
    piling up faster, measured 817k (~292MB). xadd must cap the stream so
    it can't grow unboundedly."""
    db = StubDB()
    broker = StubBroker()
    engine = CascadingEngine(broker=broker, db=db, strategies=[])

    xadd_calls = []

    class _FakeClient:
        async def xadd(self, name, fields, **kwargs):
            xadd_calls.append((name, fields, kwargs))
            return "1-1"

    class _FakeIpc:
        is_connected = True
        client = _FakeClient()

    engine.ctx.ipc_client = _FakeIpc()

    # publish_event awaits a future keyed by msg_id that only the (not
    # running, in this test) consumer loop would normally resolve — resolve
    # it ourselves from a background task so the awaited xadd call above it
    # is what's actually under test.
    async def _resolve_soon():
        await asyncio.sleep(0)
        for f in engine.reactive._pending_futures.values():
            if not f.done():
                f.set_result("ok")

    asyncio.create_task(_resolve_soon())
    await engine.reactive.publish_event("MARKET_DATA", {"event": {"symbol": "AAPL"}})

    assert len(xadd_calls) == 1
    _, _, kwargs = xadd_calls[0]
    assert kwargs.get("maxlen") == engine.reactive._STREAM_MAXLEN
    assert kwargs.get("approximate") is True


class _FakeDurableRedisClient:
    """Minimal stand-in for the ipc_client.client used by the durable Redis
    Streams consumer loop: delivers one message on the first xreadgroup call,
    then blocks like the real (bounded) call would on subsequent polls."""

    def __init__(self, msg_id: str, event_type: str, payload: dict):
        self._pending = [(msg_id, {"event_type": event_type, "payload": json.dumps(payload)})]
        self.xack_calls: list = []

    async def xgroup_create(self, *args, **kwargs):
        return True

    async def xreadgroup(self, *, block, **_kwargs):
        if self._pending:
            messages, self._pending = self._pending, []
            return [("hermes_event_stream", messages)]
        await asyncio.sleep(block / 1000.0)
        return None

    async def xack(self, _stream, _group, msg_id):
        self.xack_calls.append(msg_id)
        return 1


class _FakeIpcClient:
    def __init__(self, client):
        self.is_connected = True
        self.client = client


@pytest.mark.asyncio
async def test_durable_loop_bounds_a_deadlocked_process_event():
    """Production incident: _process_event re-enters the same EventBus it
    came from (it emits an Execute*Command and awaits its future) — a burst
    of concurrent events can exhaust the bus's dispatch semaphore with outer
    handlers all blocked on their own redis round-trip, deadlocking the
    single-threaded durable consumer loop forever with no exception and 0%
    CPU. The stuck message was also never acked, so it replayed and
    re-deadlocked on every subsequent restart.

    _process_event must be bounded so one stuck message can never wedge
    every message behind it, and it must still get acked (so it isn't
    replayed forever) and resolve the caller's pending future instead of
    leaving it hanging.
    """
    db = StubDB()
    broker = StubBroker()
    bus = EventBus()
    bus.start()

    engine = CascadingEngine(broker=broker, db=db, strategies=[], event_bus=bus)
    engine.reactive._PROCESS_EVENT_TIMEOUT_S = 0.05

    async def hangs_forever(event_type, payload):
        await asyncio.sleep(3600)

    engine.reactive._process_event = hangs_forever

    fake_client = _FakeDurableRedisClient(
        msg_id="1-1", event_type="MARKET_DATA",
        payload={"event": {"symbol": "AAPL", "price": 100.0}},
    )
    engine.ctx.ipc_client = _FakeIpcClient(fake_client)

    pending_fut = asyncio.get_running_loop().create_future()
    engine.reactive._pending_futures["1-1"] = pending_fut

    loop_task = asyncio.create_task(engine.reactive._redis_event_consumer_loop())
    try:
        # Comfortably past the 0.05s test timeout but nowhere near the real
        # 90s default — if the bound didn't work, this would just time out
        # the *test* instead of the durable loop ever recovering.
        await asyncio.sleep(0.5)

        assert pending_fut.done(), "pending future should resolve once _process_event is bounded"
        with pytest.raises(TimeoutError):
            pending_fut.result()

        # Acked despite the timeout — otherwise this exact message replays
        # (and re-deadlocks) forever on every future restart.
        assert fake_client.xack_calls == ["1-1"]
        assert "1-1" not in engine.reactive._pending_futures
    finally:
        loop_task.cancel()
        try:
            await loop_task
        except asyncio.CancelledError:
            pass
        await bus.stop()

