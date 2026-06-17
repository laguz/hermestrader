from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, Optional

from hermes.ipc import ipc, AsyncIPC


class MessageBus:
    """Decoupled asynchronous message bus wrapping AsyncIPC."""

    def __init__(self, redis_url: Optional[str] = None):
        if redis_url:
            self._ipc = AsyncIPC(redis_url)
        else:
            self._ipc = ipc

    async def connect(self) -> None:
        """Connect to the underlying message bus."""
        await self._ipc.connect()

    async def disconnect(self) -> None:
        """Disconnect from the underlying message bus."""
        await self._ipc.disconnect()

    async def publish(self, topic: str, message: Dict[str, Any]) -> None:
        """Publish a message to a topic."""
        await self._ipc.publish(topic, message)

    async def subscribe(self, topic: str, callback: Callable[[Dict[str, Any]], Any]) -> None:
        """Subscribe to a topic with a callback."""
        async def callback_wrapper(data: Dict[str, Any]) -> None:
            if asyncio.iscoroutinefunction(callback):
                await callback(data)
            else:
                callback(data)
        await self._ipc.subscribe(topic, callback_wrapper)
