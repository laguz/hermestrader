"""
hermes/ipc.py — Lightweight asynchronous Inter-Process Communication.
Exposes standard Publish/Subscribe patterns over PostgreSQL LISTEN/NOTIFY, with an
in-memory fallback if PostgreSQL is unreachable or disabled, ensuring tests and
local non-PostgreSQL setups continue to work seamlessly.
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
        # Keep redis_dsn parameter for backward compatibility, but ignore it.
        self.db: Any = None
        self.is_connected = False
        self._listener_task: Optional[asyncio.Task] = None
        self._local_subscribers: Dict[str, List[Callable[[Dict[str, Any]], Coroutine[Any, Any, None]]]] = {}
        self._active_channels: set[str] = set()
        self._stop_event = asyncio.Event()

    async def connect(self, db: Optional[Any] = None, bypass_pytest_check: bool = False) -> bool:
        """Attempt to connect to PostgreSQL LISTEN/NOTIFY. Returns True if successful, False if falling back to Mock."""
        import sys
        if "pytest" in sys.modules and not bypass_pytest_check:
            self.is_connected = False
            self.db = None
            logger.info("Test environment detected; bypassing PostgreSQL LISTEN/NOTIFY in AsyncIPC")
            return False

        self.db = db
        if db and hasattr(db, "async_engine") and "postgresql" in db.async_engine.dialect.name:
            self.is_connected = True
            logger.info("Connected to PostgreSQL IPC")
            if self._local_subscribers:
                await self._start_listener()
            return True
        else:
            self.is_connected = False
            self.db = None
            logger.info("PostgreSQL IPC unavailable (using local mock fallback)")
            return False

    async def publish(self, channel: str, data: Dict[str, Any]) -> int:
        """Publish a JSON payload to a channel. Returns number of receivers."""
        _validate_channel(channel)
        payload = json.dumps(data)
        receivers = 0

        if self.is_connected and self.db is not None:
            try:
                from sqlalchemy import text as sa_text
                escaped_payload = payload.replace("'", "''")
                async with self.db.async_engine.begin() as conn:
                    await conn.execute(sa_text(f"NOTIFY {channel}, '{escaped_payload}'"))
                logger.debug("IPC published to PostgreSQL channel %s: %s", channel, data)
                receivers = 1
            except Exception as exc:
                logger.error("Failed to publish to PostgreSQL channel %s: %s", channel, exc)

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
        
        if self.is_connected and self.db is not None:
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
        self._listener_task = asyncio.create_task(self._listen_loop())

    async def _listen_loop(self) -> None:
        """Continuous listener loop processing incoming PostgreSQL notifications."""
        from sqlalchemy import text as sa_text
        logger.info("Starting PostgreSQL IPC listener loop...")
        
        while self.is_connected and self.db is not None and self._active_channels:
            try:
                async with self.db.async_engine.connect() as conn:
                    fairy = await conn.get_raw_connection()
                    driver_conn = fairy.driver_connection
                    
                    for channel in list(self._active_channels):
                        await conn.execute(sa_text(f"LISTEN {channel}"))
                    await conn.commit()
                    
                    logger.info("PostgreSQL IPC listening on channels: %s", list(self._active_channels))
                    
                    while self.is_connected and not self._stop_event.is_set():
                        async for notify in driver_conn.notifies():
                            if self._stop_event.is_set():
                                break
                            
                            channel = notify.channel
                            payload_str = notify.payload
                            logger.debug("PostgreSQL IPC received channel=%s payload=%s", channel, payload_str)
                            
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
                logger.error("Error in PostgreSQL IPC listener loop: %s. Reconnecting in 5s...", exc)
                try:
                    await asyncio.sleep(5.0)
                except asyncio.CancelledError:
                    break
        logger.info("PostgreSQL IPC listener loop stopped.")

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
            
        self.is_connected = False
        self.db = None
        logger.info("Disconnected from PostgreSQL IPC.")

# Global singleton client initialized with the settings DSN
from hermes.config import settings
ipc = AsyncIPC(settings.hermes_redis_dsn)
