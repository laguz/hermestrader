import asyncio
import json
import logging
import websockets
import httpx
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger("hermes.broker.stream")


class TradierStreamClient:
    """
    Asynchronous WebSocket client for Tradier's streaming market data.
    Establishes a streaming session and subscribes to real-time quotes/trades.
    Emits MarketDataEvents to the central EventBus.
    """
    def __init__(self, token: str, account_id: str, base_url: str, event_bus, watchlist: List[str]):
        self.token = token
        self.account_id = account_id
        # Normalize base URL (remove trailing slash)
        self.base_url = base_url.rstrip("/")
        self.event_bus = event_bus
        self.watchlist: Set[str] = set(watchlist)
        self.session_id: Optional[str] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._task: Optional[asyncio.Task] = None
        self._running: bool = False

    async def _create_session(self) -> Dict[str, Any]:
        """Creates a streaming session via Tradier REST API."""
        url = f"{self.base_url}/markets/events/session"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json"
        }
        logger.debug("Requesting streaming session from %s", url)
        async with httpx.AsyncClient() as client:
            r = await client.post(url, headers=headers)
            r.raise_for_status()
            data = r.json()
            return data.get("stream") or {}

    async def start(self) -> None:
        """Starts the WebSocket connection in a background task."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("TradierStreamClient background task started.")

    async def stop(self) -> None:
        """Stops the WebSocket connection gracefully."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("TradierStreamClient background task stopped.")

    async def _run_loop(self) -> None:
        """Background loop that connects and processes messages."""
        retry_delay = 1.0
        while self._running:
            try:
                session_data = await self._create_session()
                ws_url = session_data.get("url")
                self.session_id = session_data.get("sessionid")
                if not ws_url or not self.session_id:
                    logger.error("Failed to create streaming session: %s", session_data)
                    await asyncio.sleep(5)
                    continue

                url_with_session = f"{ws_url}?sessionid={self.session_id}"
                logger.info("Connecting to Tradier WebSocket at %s", ws_url)
                async with websockets.connect(url_with_session) as ws:
                    self._ws = ws
                    retry_delay = 1.0  # reset retry delay on clean connection
                    
                    await self._send_subscription()

                    async for message in ws:
                        if not self._running:
                            break
                        await self._handle_message(message)
            except websockets.exceptions.ConnectionClosed as ecc:
                logger.warning("Tradier WebSocket connection closed (%s). Reconnecting...", ecc)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Error in Tradier WebSocket stream: %s", e, exc_info=True)
            
            if self._running:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60.0)

    async def _send_subscription(self) -> None:
        if not self._ws or not self.session_id:
            return
        symbols = sorted(list(self.watchlist))
        if not symbols:
            logger.info("No symbols to subscribe to.")
            return
        sub_payload = {
            "symbols": symbols,
            "filter": ["quote", "trade"],
            "sessionid": self.session_id,
            "linebreak": True
        }
        await self._ws.send(json.dumps(sub_payload))
        logger.info("Subscribed to Tradier WebSocket stream for %d symbols", len(symbols))

    async def _handle_message(self, message: str) -> None:
        """Parses the WebSocket message and emits a MarketDataEvent."""
        try:
            data = json.loads(message)
            event_type = data.get("type")
            symbol = data.get("symbol")
            if not event_type or not symbol:
                return

            from hermes.events.bus import MarketDataEvent
            price = 0.0
            volume = 0
            if event_type == "quote":
                bid = float(data.get("bid") or 0.0)
                ask = float(data.get("ask") or 0.0)
                price = (bid + ask) / 2.0 if (bid > 0 and ask > 0) else float(data.get("last") or 0.0)
            elif event_type == "trade":
                price = float(data.get("price") or 0.0)
                volume = int(data.get("size") or 0)

            if price > 0:
                event = MarketDataEvent(
                    symbol=symbol,
                    price=price,
                    volume=volume,
                    data=data
                )
                self.event_bus.emit(event)
        except Exception as e:
            logger.error("Failed to parse WebSocket message: %s", e, exc_info=True)

    def update_watchlist(self, new_watchlist: List[str]) -> None:
        """Dynamically updates the watchlist symbols and resubscribes."""
        new_set = set(new_watchlist)
        if self.watchlist != new_set:
            self.watchlist = new_set
            if self._ws and self._ws.open and self.session_id:
                asyncio.create_task(self._send_subscription())
