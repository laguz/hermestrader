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

class LocalMemoryIPCBackend:
    """In-memory IPC backend for testing and single-process mode."""
    def __init__(self):
        self._local_subscribers: Dict[str, List[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]]] = {}
        # The event loop holds only a weak reference to tasks, so a bare
        # create_task() here could be garbage-collected before the callback
        # runs. Keep a strong reference until each task completes.
        self._pending_tasks: set[asyncio.Task] = set()

    @property
    def is_connected(self) -> bool:
        return False

    async def connect(self) -> bool:
        logger.info("Local Memory IPC connected (always reports not connected to trigger fallback/mock behaviors).")
        return False

    async def publish(self, channel: str, data: Dict[str, Any]) -> int:
        _validate_channel(channel)
        receivers = 0
        if channel in self._local_subscribers:
            handlers = list(self._local_subscribers[channel])
            for handler in handlers:
                task = asyncio.create_task(handler(data))
                self._pending_tasks.add(task)
                task.add_done_callback(self._pending_tasks.discard)
                receivers += 1
        return receivers

    async def subscribe(self, channel: str, callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]) -> None:
        _validate_channel(channel)
        self._local_subscribers.setdefault(channel, []).append(callback)

    async def unsubscribe(self, channel: str, callback: Optional[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]] = None) -> None:
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

    async def disconnect(self) -> None:
        self._local_subscribers.clear()
        for task in list(self._pending_tasks):
            task.cancel()
        self._pending_tasks.clear()
        logger.info("Local Memory IPC disconnected.")


class RedisIPCBackend:
    """Strict Redis Pub/Sub backend that raises ConnectionError on failure in production/dev."""
    def __init__(self, redis_dsn: str):
        self.redis_dsn = redis_dsn
        self.client: Any = None
        self.is_connected = False
        self._listener_task: Optional[asyncio.Task] = None
        self._local_subscribers: Dict[str, List[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]]] = {}
        self._active_channels: set[str] = set()
        self._stop_event = asyncio.Event()
        # The redis-asyncio client's pooled connections (StreamReader/Writer)
        # are bound to whichever loop was running at connect() time. Callers
        # that reach this singleton from a *different* loop/thread (e.g. a
        # background thread's own asyncio.run()) must not reuse that pooled
        # connection — doing so corrupts it for every future caller,
        # including the main tick loop. Track the owning loop so publish()
        # can refuse instead of reusing it cross-loop.
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    async def connect(self) -> bool:
        if not self.redis_dsn:
            raise ConnectionError("Redis DSN not configured, but Redis IPC backend was selected.")
        try:
            import redis.asyncio as aioredis
            self.client = aioredis.from_url(self.redis_dsn, decode_responses=True)
            await self.client.ping()
            self.is_connected = True
            self._loop = asyncio.get_running_loop()
            logger.info("Connected to Redis IPC at %s", self.redis_dsn)
            if self._local_subscribers:
                await self._start_listener()
            return True
        except Exception as exc:
            self.is_connected = False
            self.client = None
            logger.error("Redis IPC connection failed: %s", exc)
            raise ConnectionError(f"Failed to connect to Redis IPC at {self.redis_dsn}: {exc}") from exc

    async def publish(self, channel: str, data: Dict[str, Any]) -> int:
        _validate_channel(channel)
        if not self.is_connected or self.client is None:
            raise ConnectionError("Cannot publish: Redis IPC is not connected.")
        if self._loop is not None and self._loop is not asyncio.get_running_loop():
            raise ConnectionError(
                "Cannot publish: Redis IPC client belongs to a different event loop "
                "(likely called from a background thread's own asyncio.run())."
            )
        payload = json.dumps(data)
        receivers = await self.client.publish(channel, payload)
        logger.debug("IPC published to Redis channel %s: %s (receivers=%d)", channel, data, receivers)
        return receivers

    async def subscribe(self, channel: str, callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]) -> None:
        _validate_channel(channel)
        self._local_subscribers.setdefault(channel, []).append(callback)
        
        if channel not in self._active_channels:
            self._active_channels.add(channel)
            if self.is_connected and self.client is not None:
                await self._start_listener()

    async def unsubscribe(self, channel: str, callback: Optional[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]] = None) -> None:
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
                    if self.is_connected and self.client is not None:
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


class AsyncIPC:
    """Wrapper that delegates to either Redis or In-Memory IPC backend depending on environment."""
    def __init__(self, redis_dsn: Optional[str] = None):
        self.redis_dsn = redis_dsn
        self.backend: Any = None

    @property
    def is_connected(self) -> bool:
        return self.backend.is_connected if self.backend else False

    @property
    def client(self) -> Any:
        return getattr(self.backend, "client", None)

    async def connect(self, db: Optional[Any] = None, bypass_pytest_check: bool = False) -> bool:
        """Attempt to connect. Returns True if Redis successful, False if local memory backend used."""
        import sys
        
        # Decide which backend to use
        is_pytest = "pytest" in sys.modules and not bypass_pytest_check
        if is_pytest or not self.redis_dsn:
            logger.info("Using Local Memory IPC backend (is_pytest=%s, DSN configured=%s)", is_pytest, bool(self.redis_dsn))
            self.backend = LocalMemoryIPCBackend()
        else:
            logger.info("Using Redis IPC backend")
            self.backend = RedisIPCBackend(self.redis_dsn)
            
        return await self.backend.connect()

    async def publish(self, channel: str, data: Dict[str, Any]) -> int:
        """Publish a JSON payload to a channel. Returns number of receivers."""
        if self.backend is None:
            raise RuntimeError("IPC is not connected. Call connect() first.")
        return await self.backend.publish(channel, data)

    async def subscribe(self, channel: str, callback: Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]) -> None:
        """Register an async callback for a channel."""
        if self.backend is None:
            self.backend = LocalMemoryIPCBackend()
        await self.backend.subscribe(channel, callback)

    async def unsubscribe(self, channel: str, callback: Optional[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]] = None) -> None:
        """Deregister callback and unsubscribe if no callbacks remain."""
        if self.backend is not None:
            await self.backend.unsubscribe(channel, callback)

    async def disconnect(self) -> None:
        """Clean up tasks and close connection."""
        if self.backend is not None:
            await self.backend.disconnect()
            self.backend = None

# Global singleton client initialized with the settings DSN
from hermes.config import settings
ipc = AsyncIPC(settings.hermes_redis_dsn)
