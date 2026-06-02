"""
[Service-1: Hermes-Agent-Core]
AsyncBrokerWrapper — unifies sync/async brokers behind one async interface
and routes order placement through the shared CircuitBreaker.

Lives below MoneyManager / AbstractStrategy / CascadingEngine in the import
graph: all three wrap their broker with this adapter, so it must not import
back up into them.
"""
from __future__ import annotations

import asyncio
import inspect
import logging

logger = logging.getLogger("hermes.agent.broker_wrapper")


class AsyncBrokerWrapper:
    """Wraps a synchronous or asynchronous broker to present a unified async interface."""
    def __init__(self, broker, db=None):
        self.broker = broker
        self.db = db
        from hermes.broker.circuit_breaker import CircuitBreaker
        if not hasattr(AsyncBrokerWrapper, "_shared_cb"):
            AsyncBrokerWrapper._shared_cb = CircuitBreaker()

    def __getattr__(self, name):
        if name == "place_order_from_action":
            return self._place_order_from_action_wrapped

        attr = getattr(self.broker, name)
        if callable(attr):
            async def _async_wrapper(*args, **kwargs):
                if asyncio.iscoroutinefunction(attr) or inspect.iscoroutinefunction(attr):
                    return await attr(*args, **kwargs)
                res = attr(*args, **kwargs)
                if inspect.iscoroutine(res) or asyncio.iscoroutine(res):
                    return await res
                return res
            return _async_wrapper
        return attr

    async def _place_order_from_action_wrapped(self, action):
        from hermes.broker.circuit_breaker import CircuitBreakerError
        cb = AsyncBrokerWrapper._shared_cb
        state = cb.check_state()
        if state == "OPEN":
            logger.error("Circuit breaker is OPEN. Fast-failing order placement.")
            raise CircuitBreakerError("Circuit breaker is OPEN. Orders are blocked.")

        try:
            attr = getattr(self.broker, "place_order_from_action")
            if asyncio.iscoroutinefunction(attr) or inspect.iscoroutinefunction(attr):
                res = await attr(action)
            else:
                res = attr(action)
                if inspect.iscoroutine(res) or asyncio.iscoroutine(res):
                    res = await res

            rejected = False
            if isinstance(res, dict):
                if "errors" in res or "error" in res:
                    rejected = True
                order = res.get("order")
                if isinstance(order, dict):
                    status = str(order.get("status", "")).lower()
                    if status in {"rejected", "error", "expired", "canceled", "cancelled"}:
                        rejected = True

            if rejected:
                await cb.record_failure(self.db, f"Order rejected: {res}")
                if cb.state == "OPEN":
                    raise CircuitBreakerError("Order rejected and circuit breaker tripped to OPEN.")
            else:
                cb.record_success()

            return res

        except Exception as e:
            if not isinstance(e, CircuitBreakerError):
                await cb.record_failure(self.db, f"Order placement exception: {e}")
            raise
