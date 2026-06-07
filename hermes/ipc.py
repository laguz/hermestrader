"""
hermes/ipc.py — Lightweight asynchronous Inter-Process Communication.
Exposes standard Publish/Subscribe patterns over Redis, with a mock fallback
if Redis is unreachable or disabled, ensuring tests and local non-Redis setups
continue to work seamlessly.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable, Coroutine, Dict, List, Optional
import redis.asyncio as aioredis

logger = logging.getLogger("hermes.ipc")

class AsyncIPC:
    def __init__(self, redis_dsn: str):
        self.redis_dsn = redis_dsn
        self.client: Optional[aioredis.Redis] = None
        self.is_connected = False
        self._pubsub: Optional[aioredis.client.PubSub] = None
        self._listener_task: Optional[asyncio.Task] = None
        self._local_subscribers: Dict[str, List[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]]] = {}

    async def connect(self) -> bool:
        """Attempt to connect to Redis. Returns True if successful, False if falling back to Mock."""
        import sys
        if "pytest" in sys.modules:
            self.is_connected = False
            self.client = None
            logger.info("Test environment detected; bypassing real Redis connection in AsyncIPC")
            return False

        try:
            # Short timeout so startup doesn't hang if Redis is down
            self.client = aioredis.from_url(
                self.redis_dsn,
                socket_timeout=2.0,
                socket_connect_timeout=2.0,
                decode_responses=True
            )
            # Ping to verify active connection
            await self.client.ping()
            self.is_connected = True
            logger.info("Connected to Redis IPC at %s", self.redis_dsn)
            return True
        except Exception as exc:
            self.is_connected = False
            self.client = None
            logger.warning("Redis IPC unavailable (using local mock fallback): %s", exc)
            return False

    async def publish(self, channel: str, data: Dict[str, Any]) -> int:
        """Publish a JSON payload to a channel. Returns number of receivers."""
        payload = json.dumps(data)
        if self.is_connected and self.client is not None:
            try:
                receivers = await self.client.publish(channel, payload)
                logger.debug("IPC published to Redis channel %s: %s (receivers=%d)", channel, data, receivers)
                return receivers
            except Exception as exc:
                logger.error("Failed to publish to Redis channel %s: %s", channel, exc)
        
        # Local mock publish fallback (useful for same-process thread triggers or testing)
        logger.debug("IPC published to Mock channel %s: %s", channel, data)
        receivers = 0
        if channel in self._local_subscribers:
            handlers = list(self._local_subscribers[channel])
            for handler in handlers:
                asyncio.create_task(handler(data))
                receivers += 1
        return receivers

    async def subscribe(self, channel: str, callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]) -> None:
        """Register an async callback for a channel."""
        self._local_subscribers.setdefault(channel, []).append(callback)
        
        if self.is_connected and self.client is not None:
            try:
                if self._pubsub is None:
                    self._pubsub = self.client.pubsub()
                await self._pubsub.subscribe(channel)
                logger.info("Subscribed to Redis IPC channel: %s", channel)
                
                # Start listener task if not already running
                if self._listener_task is None or self._listener_task.done():
                    self._listener_task = asyncio.create_task(self._listen_loop())
            except Exception as exc:
                logger.error("Failed to subscribe to Redis channel %s: %s", channel, exc)

    async def unsubscribe(self, channel: str, callback: Optional[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]] = None) -> None:
        """Deregister callback and unsubscribe if no callbacks remain."""
        if channel in self._local_subscribers:
            if callback:
                try:
                    self._local_subscribers[channel].remove(callback)
                except ValueError:
                    pass
            else:
                self._local_subscribers[channel] = []
                
            if not self._local_subscribers[channel]:
                del self._local_subscribers[channel]
                if self.is_connected and self._pubsub is not None:
                    try:
                        await self._pubsub.unsubscribe(channel)
                    except Exception as exc:
                        logger.error("Failed to unsubscribe from Redis channel %s: %s", channel, exc)

    async def _listen_loop(self) -> None:
        """Continuous listener loop processing incoming Redis messages."""
        logger.info("Starting Redis IPC listener loop...")
        try:
            while self.is_connected and self._pubsub is not None:
                try:
                    # Non-blocking get_message to allow task cancellation / clean shutdown
                    msg = await self._pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if msg is None:
                        continue
                    
                    channel = msg.get("channel")
                    data_str = msg.get("data")
                    if not channel or not data_str:
                        continue
                    
                    try:
                        data = json.loads(data_str)
                    except json.JSONDecodeError:
                        logger.warning("Received invalid non-JSON payload on %s: %r", channel, data_str)
                        continue
                    
                    if channel in self._local_subscribers:
                        for handler in self._local_subscribers[channel]:
                            try:
                                await handler(data)
                            except Exception as exc:
                                logger.exception("IPC subscriber callback failed on %s: %s", channel, exc)
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.error("Error in IPC listener loop: %s", exc)
                    await asyncio.sleep(1.0)
        finally:
            logger.info("Redis IPC listener loop stopped.")

    async def disconnect(self) -> None:
        """Clean up tasks and close connection."""
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        
        if self._pubsub is not None:
            try:
                await self._pubsub.close()
            except Exception:
                pass
            self._pubsub = None
            
        if self.client is not None:
            try:
                await self.client.aclose()
            except Exception:
                pass
            self.client = None
            
        self.is_connected = False
        logger.info("Disconnected from Redis IPC.")

# Global singleton client initialized with the settings DSN
from hermes.config import settings
ipc = AsyncIPC(settings.hermes_redis_dsn)
