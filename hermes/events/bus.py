import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, List, Type, TypeVar, Awaitable, Optional

logger = logging.getLogger("hermes.events")

# --- Event Definitions ---

@dataclass
class Event:
    """Base class for all events in the Hermes system."""
    timestamp: datetime = field(init=False)

    def __post_init__(self):
        self.timestamp = datetime.utcnow()
        try:
            self.future = asyncio.get_running_loop().create_future()
        except RuntimeError:
            self.future = None

@dataclass
class MarketDataEvent(Event):
    """Fired when new market data (quotes/bars) arrives."""
    symbol: str
    price: float
    volume: int = 0
    data: Dict[str, Any] = field(default_factory=dict)

@dataclass
class OrderFillEvent(Event):
    """Fired when an order is partially or completely filled."""
    broker_order_id: str
    symbol: str
    side: str
    quantity: int
    price: float
    status: str

@dataclass
class ReviewRequestEvent(Event):
    """Fired by the rules engine to request AI review of a proposed trade."""
    strategy_id: str
    symbol: str
    trade_action: Any  # TradeAction
    context_data: Dict[str, Any] = field(default_factory=dict)
    # 'entry' | 'management' | 'ai' — carried through review so the eventual
    # execution routes pure closes correctly instead of defaulting to 'entry'.
    action_type: str = "entry"
    approval_id: Optional[int] = None

@dataclass
class AIApprovalEvent(Event):
    """Fired by the AI Overseer after reviewing a proposed trade."""
    strategy_id: str
    symbol: str
    verdict: str  # APPROVE, VETO, MODIFY
    rationale: str
    modifications: Dict[str, Any] = field(default_factory=dict)
    original_action: Any = None
    # Preserved from the originating ReviewRequestEvent / submit() call.
    action_type: str = "entry"
    approval_id: Optional[int] = None


@dataclass
class ClockTickEvent(Event):
    """Fired periodically by the Scheduler to trigger tick loops and safety sweeps."""
    pass

@dataclass
class MlRetrainTick(Event):
    """Fired by the Scheduler to trigger ML retraining checks."""
    force: bool = False

@dataclass
class CacheWarmTick(Event):
    """Fired by the Scheduler to trigger cache pre-warming."""
    pass

@dataclass
class ChartRefreshTick(Event):
    """Fired by the Scheduler to trigger chart vision analysis."""
    pass


@dataclass
class TickStartedEvent(Event):
    """Fired to trigger tick loops and safety sweeps."""
    watchlist: List[str]


@dataclass
class ExecuteTickCommand(Event):
    watchlist: List[str]


@dataclass
class ExecuteClockTickCommand(Event):
    event: ClockTickEvent


@dataclass
class ExecuteAIApprovalCommand(Event):
    event: AIApprovalEvent


@dataclass
class ExecuteMarketDataCommand(Event):
    event: MarketDataEvent


@dataclass
class ExecuteOrderFillCommand(Event):
    event: OrderFillEvent


@dataclass
class SubmitTradeActionsCommand(Event):
    actions: List[Any]
    action_type: str = "entry"
    approval_id: Optional[int] = None
    execute_directly: bool = False


@dataclass
class EvaluateReactiveExitEvent(Event):
    symbol: str
    mgmt_actions: List[Any]


@dataclass
class ProcessReactiveEntriesEvent(Event):
    symbol: str


@dataclass
class OrderTrackedEvent(Event):
    order_id: str
    symbol: str
    side: str
    quantity: int


@dataclass
class SyncPositionsCommand(Event):
    pass


@dataclass
class ReconcileOrphansCommand(Event):
    pass


@dataclass
class ProcessManagementCommand(Event):
    pass


@dataclass
class ProcessEntriesCommand(Event):
    watchlist: List[str]


E = TypeVar("E", bound=Event)
EventHandler = Callable[[E], Awaitable[None]]

# --- Event Bus ---

class EventBus:
    """
    A lightweight asynchronous event bus.
    Routes events to registered handler coroutines.
    """
    def __init__(self):
        self._subscribers: Dict[Type[Event], List[EventHandler]] = {}
        self._queue: asyncio.Queue[Event] = asyncio.Queue()
        self._task: asyncio.Task | None = None

    def subscribe(self, event_type: Type[E], handler: EventHandler) -> None:
        """Register an async handler for a specific event type."""
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(handler)
        logger.debug(f"Subscribed {handler.__name__} to {event_type.__name__}")

    def emit(self, event: Event) -> None:
        """Publish an event to the bus without blocking."""
        self._queue.put_nowait(event)
        try:
            self.start()
        except RuntimeError:
            pass

    async def _process_events(self) -> None:
        """Background task loop that consumes events and dispatches them."""
        logger.info("EventBus processing loop started.")
        while True:
            try:
                event = await self._queue.get()
                event_type = type(event)
                handlers = self._subscribers.get(event_type, [])
                
                if not handlers:
                    logger.debug(f"No subscribers for event: {event_type.__name__}")
                    self._queue.task_done()
                    continue
                
                asyncio.create_task(self._dispatch_event(event, handlers))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in EventBus processing loop: {e}", exc_info=True)

    async def _dispatch_event(self, event: Event, handlers: List[EventHandler]) -> None:
        try:
            tasks = []
            for handler in handlers:
                tasks.append(asyncio.create_task(self._safe_invoke(handler, event)))
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            self._queue.task_done()

    async def _safe_invoke(self, handler: EventHandler, event: Event) -> None:
        """Invokes a handler and catches any exceptions to prevent bus crashes."""
        try:
            await handler(event)
        except Exception as e:
            logger.error(f"Error in handler {handler.__name__} for {type(event).__name__}: {e}", exc_info=True)

    def start(self) -> None:
        """Starts the background event processing loop."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._process_events())

    async def stop(self) -> None:
        """Stops the event loop gracefully after processing the remaining queue."""
        if self._task:
            await self._queue.join()  # Wait for all items to be processed
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            logger.info("EventBus processing loop stopped.")
