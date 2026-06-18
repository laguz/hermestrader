import asyncio
import logging
from datetime import datetime
from typing import List, Optional, Any
from hermes.events.bus import MarketDataEvent

logger = logging.getLogger("hermes.broker.mock_stream")

class MockStreamClient:
    """
    Mock stream client that periodically generates mock quotes for the watchlist
    and emits them to the event bus, replicating the behavior of the real stream client.
    """
    def __init__(self, event_bus, watchlist: List[str], db: Optional[Any] = None, target: Optional[str] = None):
        # Keep target parameter for compatibility with GRPCStreamClient signature
        self.event_bus = event_bus
        self.watchlist = list(watchlist) if watchlist else []
        self.db = db
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("MockStreamClient started.")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("MockStreamClient stopped.")

    def update_watchlist(self, watchlist: List[str]) -> None:
        self.watchlist = list(watchlist)
        logger.debug("MockStreamClient watchlist updated: %s", self.watchlist)

    async def _run_loop(self) -> None:
        while self._running:
            try:
                # Refresh watchlist symbols from DB to mirror what api_grpc.py was doing
                watchlist_syms = set(self.watchlist)
                if self.db:
                    try:
                        wl = await self.db.watchlist.all_watchlist_symbols()
                        tracked = await self.db.trades.tracked_option_symbols()
                        watchlist_syms.update(wl)
                        watchlist_syms.update(tracked)
                    except Exception:
                        pass
                
                if not watchlist_syms:
                    watchlist_syms = {"SPY"}

                for sym in sorted(list(watchlist_syms)):
                    if not self._running:
                        break
                    
                    # Generate a mock quote event
                    raw_data = {
                        "symbol": sym,
                        "price": 500.0,
                        "bid": 499.9,
                        "ask": 500.1,
                        "volume": 1000000,
                        "timestamp": datetime.utcnow().isoformat(),
                        "type": "quote"
                    }
                    event = MarketDataEvent(
                        symbol=sym,
                        price=500.0,
                        volume=1000000,
                        data=raw_data
                    )
                    self.event_bus.emit(event)
                
                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in MockStreamClient loop: %s", exc)
                await asyncio.sleep(5.0)
