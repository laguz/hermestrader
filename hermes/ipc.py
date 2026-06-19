"""
hermes/ipc.py — Lightweight asynchronous Inter-Process Communication.
Exposes standard Publish/Subscribe patterns over a dedicated Redis Pub/Sub broker,
with a local in-memory fallback if Redis is unreachable or disabled, ensuring
tests and local non-Redis setups continue to work seamlessly.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Callable, Coroutine, Dict, List, Optional

logger = logging.getLogger("hermes.ipc")

def _validate_channel(channel: str) -> None:
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", channel):
        raise ValueError(f"Invalid channel name: {channel}")

class AsyncIPC:
    def __init__(self, redis_dsn: Optional[str] = None):
        self.redis_dsn = redis_dsn
        self.client: Any = None
        self.is_connected = False
        self._listener_task: Optional[asyncio.Task] = None
        self._local_subscribers: Dict[str, List[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]]] = {}
        self._active_channels: set[str] = set()
        self._stop_event = asyncio.Event()

    async def connect(self, db: Optional[Any] = None, bypass_pytest_check: bool = False) -> bool:
        """Attempt to connect to Redis. Returns True if successful, False if falling back to Mock."""
        import sys
        if "pytest" in sys.modules and not bypass_pytest_check:
            self.is_connected = False
            logger.info("Test environment detected; bypassing Redis IPC connection")
            return False

        if not self.redis_dsn:
            self.is_connected = False
            logger.info("Redis DSN not configured; using local mock fallback")
            return False

        try:
            import redis.asyncio as aioredis
            self.client = aioredis.from_url(self.redis_dsn, decode_responses=True)
            await self.client.ping()
            self.is_connected = True
            logger.info("Connected to Redis IPC at %s", self.redis_dsn)
            if self._local_subscribers:
                await self._start_listener()
            return True
        except Exception as exc:
            self.is_connected = False
            self.client = None
            logger.warning("Redis IPC connection failed (using local mock fallback): %s", exc)
            return False

    async def publish(self, channel: str, data: Dict[str, Any]) -> int:
        """Publish a JSON payload to a channel. Returns number of receivers."""
        _validate_channel(channel)
        payload = json.dumps(data)
        receivers = 0

        if self.is_connected and self.client is not None:
            try:
                receivers = await self.client.publish(channel, payload)
                logger.debug("IPC published to Redis channel %s: %s (receivers=%d)", channel, data, receivers)
            except Exception as exc:
                logger.error("Failed to publish to Redis channel %s: %s", channel, exc)

        # Local mock publish fallback (useful for same-process thread triggers or testing)
        if channel in self._local_subscribers:
            handlers = list(self._local_subscribers[channel])
            for handler in handlers:
                asyncio.create_task(handler(data))
                if not self.is_connected:
                    receivers += 1
        return receivers

    async def subscribe(self, channel: str, callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]) -> None:
        """Register an async callback for a channel."""
        _validate_channel(channel)
        self._local_subscribers.setdefault(channel, []).append(callback)
        
        if self.is_connected and self.client is not None:
            if channel not in self._active_channels:
                self._active_channels.add(channel)
                await self._start_listener()

    async def unsubscribe(self, channel: str, callback: Optional[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]] = None) -> None:
        """Deregister callback and unsubscribe if no callbacks remain."""
        _validate_channel(channel)
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
                if channel in self._active_channels:
                    self._active_channels.remove(channel)
                    if self.is_connected:
                        await self._start_listener()

    async def _start_listener(self) -> None:
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            self._listener_task = None
            
        self._stop_event.clear()
        if self._active_channels:
            self._listener_task = asyncio.create_task(self._listen_loop())

    async def _listen_loop(self) -> None:
        """Continuous listener loop processing incoming Redis Pub/Sub notifications."""
        logger.info("Starting Redis IPC listener loop...")
        
        while self.is_connected and self.client is not None and self._active_channels:
            pubsub = None
            try:
                pubsub = self.client.pubsub()
                await pubsub.subscribe(*list(self._active_channels))
                logger.info("Redis IPC listening on channels: %s", list(self._active_channels))
                
                async for message in pubsub.listen():
                    if self._stop_event.is_set():
                        break
                    if message and message.get("type") == "message":
                        channel = message["channel"]
                        payload_str = message["data"]
                        logger.debug("Redis IPC received channel=%s payload=%s", channel, payload_str)
                        
                        try:
                            data = json.loads(payload_str)
                        except Exception:
                            data = {"raw_payload": payload_str}
                            
                        if channel in self._local_subscribers:
                            for handler in list(self._local_subscribers[channel]):
                                try:
                                    await handler(data)
                                except Exception as cb_exc:
                                    logger.exception("IPC subscriber callback failed on %s: %s", channel, cb_exc)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self._stop_event.is_set():
                    break
                logger.error("Error in Redis IPC listener loop: %s. Reconnecting in 5s...", exc)
                try:
                    await asyncio.sleep(5.0)
                except asyncio.CancelledError:
                    break
            finally:
                if pubsub is not None:
                    try:
                        await pubsub.close()
                    except Exception:
                        pass
        logger.info("Redis IPC listener loop stopped.")

    async def disconnect(self) -> None:
        """Clean up tasks and close connection."""
        self._stop_event.set()
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            self._listener_task = None
            
        if self.client is not None:
            try:
                await self.client.close()
            except Exception:
                pass
            self.client = None
            
        self.is_connected = False
        logger.info("Disconnected from Redis IPC.")

# Global singleton client initialized with the settings DSN
from hermes.config import settings
ipc = AsyncIPC(settings.hermes_redis_dsn)
