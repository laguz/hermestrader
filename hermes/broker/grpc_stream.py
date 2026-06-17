import asyncio
import logging
from typing import Any, Dict, List, Optional
import grpc

from hermes.protos import broker_pb2, broker_pb2_grpc
from hermes.events.bus import MarketDataEvent

logger = logging.getLogger("hermes.broker.grpc_stream")


class GRPCStreamClient:
    """
    Subscribes to the Watcher's gRPC `StreamQuotes` endpoint
    and translates incoming quotes into local MarketDataEvent instances
    published to the EventBus.
    """
    def __init__(self, target: str, event_bus, watchlist: List[str] = None):
        self.target = target
        self.event_bus = event_bus
        self.watchlist = list(watchlist) if watchlist else []
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._channel: Optional[grpc.aio.Channel] = None

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("GRPCStreamClient background task started.")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._channel:
            try:
                await self._channel.close()
            except Exception:
                pass
            self._channel = None
        logger.info("GRPCStreamClient background task stopped.")

    async def _run_loop(self) -> None:
        retry_delay = 1.0
        while self._running:
            try:
                logger.info("Connecting to gRPC quotes stream at %s", self.target)
                self._channel = grpc.aio.insecure_channel(self.target)
                stub = broker_pb2_grpc.BrokerServiceStub(self._channel)
                
                # Call StreamQuotes
                stream = stub.StreamQuotes(broker_pb2.Empty())
                
                retry_delay = 1.0  # Reset retry delay on successful stream connect
                
                async for quote in stream:
                    if not self._running:
                        break
                    
                    # Convert to MarketDataEvent
                    # Ensure bid/ask are populated
                    bid = quote.bid if quote.bid > 0 else quote.price
                    ask = quote.ask if quote.ask > 0 else quote.price
                    
                    raw_data = {
                        "symbol": quote.symbol,
                        "price": quote.price,
                        "bid": bid,
                        "ask": ask,
                        "volume": quote.volume,
                        "timestamp": quote.timestamp,
                        "type": "quote"
                    }
                    
                    event = MarketDataEvent(
                        symbol=quote.symbol,
                        price=quote.price,
                        volume=quote.volume,
                        data=raw_data
                    )
                    self.event_bus.emit(event)
            except grpc.RpcError as rpc_err:
                logger.warning("gRPC quotes stream encountered error: %s. Reconnecting...", rpc_err)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Unexpected error in gRPC quotes stream: %s", exc, exc_info=True)
            
            if self._channel:
                try:
                    await self._channel.close()
                except Exception:
                    pass
                self._channel = None
                
            if self._running:
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 60.0)

    def update_watchlist(self, new_watchlist: List[str]) -> None:
        """Compatibility signature. Subscriptions are managed dynamically on the watcher via DB."""
        self.watchlist = list(new_watchlist)
