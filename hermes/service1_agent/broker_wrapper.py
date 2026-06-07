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
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("hermes.agent.broker_wrapper")


class BrokerCache:
    """Thread-safe/async-safe memory cache for broker operations."""
    def __init__(self):
        self.chains: Dict[tuple[str, str], tuple[float, List[Dict[str, Any]]]] = {}
        self.quotes: Dict[str, tuple[float, Dict[str, Any]]] = {}
        self.expirations: Dict[str, tuple[float, List[str]]] = {}
        self.analysis: Dict[tuple[str, str], tuple[float, Dict[str, Any]]] = {}
        self.ttl = int(os.environ.get("HERMES_CACHE_TTL_S", 120))

    def get_chain(self, symbol: str, expiry: str, now: float) -> Optional[List[Dict[str, Any]]]:
        key = (symbol, expiry)
        if key in self.chains:
            ts, val = self.chains[key]
            if now - ts < self.ttl:
                return val
        return None

    def set_chain(self, symbol: str, expiry: str, data: List[Dict[str, Any]], now: float):
        self.chains[(symbol, expiry)] = (now, data)

    def get_quote(self, symbol: str, now: float) -> Optional[Dict[str, Any]]:
        if symbol in self.quotes:
            ts, val = self.quotes[symbol]
            if now - ts < self.ttl:
                return val
        return None

    def set_quote(self, symbol: str, data: Dict[str, Any], now: float):
        self.quotes[symbol] = (now, data)

    def get_expirations(self, symbol: str, now: float) -> Optional[List[str]]:
        if symbol in self.expirations:
            ts, val = self.expirations[symbol]
            if now - ts < self.ttl:
                return val
        return None

    def set_expirations(self, symbol: str, data: List[str], now: float):
        self.expirations[symbol] = (now, data)

    def get_analysis(self, symbol: str, period: str, now: float) -> Optional[Dict[str, Any]]:
        key = (symbol, period)
        if key in self.analysis:
            ts, val = self.analysis[key]
            if now - ts < self.ttl:
                return val
        return None

    def set_analysis(self, symbol: str, period: str, data: Dict[str, Any], now: float):
        self.analysis[(symbol, period)] = (now, data)

    def clear(self):
        self.chains.clear()
        self.quotes.clear()
        self.expirations.clear()
        self.analysis.clear()


class AsyncBrokerWrapper:
    """Wraps a synchronous or asynchronous broker to present a unified async interface."""
    _shared_cache = BrokerCache()
    _last_broker_ref = None

    @classmethod
    def clear_cache(cls):
        cls._shared_cache.clear()

    def __init__(self, broker, db=None):
        self.broker = broker
        self.db = db
        # Automatically clear cache if a new broker instance is wrapped (e.g. between tests or on mode switches)
        if AsyncBrokerWrapper._last_broker_ref is not broker:
            AsyncBrokerWrapper._last_broker_ref = broker
            AsyncBrokerWrapper.clear_cache()

        from hermes.broker.circuit_breaker import CircuitBreaker
        if not hasattr(AsyncBrokerWrapper, "_shared_cb"):
            AsyncBrokerWrapper._shared_cb = CircuitBreaker()

    def _get_current_timestamp(self) -> float:
        if hasattr(self.broker, "current_date") and self.broker.current_date:
            dt = self.broker.current_date
            try:
                if isinstance(dt, datetime):
                    return dt.timestamp()
            except Exception:
                pass
        return time.time()

    async def get_option_chains(self, symbol: str, expiry: str) -> List[Dict[str, Any]]:
        cache = self._shared_cache
        now_ts = self._get_current_timestamp()
        cached = cache.get_chain(symbol, expiry, now_ts)
        if cached is not None:
            logger.debug("[CACHE-HIT] get_option_chains(%s, %s)", symbol, expiry)
            return cached

        attr = getattr(self.broker, "get_option_chains")
        if asyncio.iscoroutinefunction(attr) or inspect.iscoroutinefunction(attr):
            res = await attr(symbol, expiry)
        else:
            res = attr(symbol, expiry)
            if inspect.iscoroutine(res) or asyncio.iscoroutine(res):
                res = await res
        
        cache.set_chain(symbol, expiry, res or [], now_ts)
        return res

    async def get_option_expirations(self, symbol: str) -> List[str]:
        cache = self._shared_cache
        now_ts = self._get_current_timestamp()
        cached = cache.get_expirations(symbol, now_ts)
        if cached is not None:
            logger.debug("[CACHE-HIT] get_option_expirations(%s)", symbol)
            return cached

        attr = getattr(self.broker, "get_option_expirations")
        if asyncio.iscoroutinefunction(attr) or inspect.iscoroutinefunction(attr):
            res = await attr(symbol)
        else:
            res = attr(symbol)
            if inspect.iscoroutine(res) or asyncio.iscoroutine(res):
                res = await res
        
        cache.set_expirations(symbol, res or [], now_ts)
        return res

    async def analyze_symbol(self, symbol: str, period: str = "6m") -> Dict[str, Any]:
        cache = self._shared_cache
        now_ts = self._get_current_timestamp()
        cached = cache.get_analysis(symbol, period, now_ts)
        if cached is not None:
            logger.debug("[CACHE-HIT] analyze_symbol(%s, %s)", symbol, period)
            return cached

        attr = getattr(self.broker, "analyze_symbol")
        if asyncio.iscoroutinefunction(attr) or inspect.iscoroutinefunction(attr):
            res = await attr(symbol, period)
        else:
            res = attr(symbol, period)
            if inspect.iscoroutine(res) or asyncio.iscoroutine(res):
                res = await res
        
        cache.set_analysis(symbol, period, res or {}, now_ts)
        return res

    async def get_quote(self, symbols: str) -> List[Dict[str, Any]]:
        if not symbols:
            return []
        
        cache = self._shared_cache
        now_ts = self._get_current_timestamp()
        
        symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
        
        cached_quotes = []
        uncached_symbols = []
        
        for sym in symbol_list:
            cached_val = cache.get_quote(sym, now_ts)
            if cached_val is not None:
                cached_quotes.append(cached_val)
            else:
                uncached_symbols.append(sym)
                
        if not uncached_symbols:
            logger.debug("[CACHE-HIT] get_quote for all requested symbols: %s", symbols)
            return cached_quotes
            
        uncached_str = ",".join(uncached_symbols)
        attr = getattr(self.broker, "get_quote")
        if asyncio.iscoroutinefunction(attr) or inspect.iscoroutinefunction(attr):
            fetched = await attr(uncached_str)
        else:
            fetched = attr(uncached_str)
            if inspect.iscoroutine(fetched) or asyncio.iscoroutine(fetched):
                fetched = await fetched
                
        if fetched and isinstance(fetched, list):
            for quote in fetched:
                sym = quote.get("symbol")
                if sym:
                    cache.set_quote(sym, quote, now_ts)
                    
        result = []
        for sym in symbol_list:
            val = cache.get_quote(sym, now_ts)
            if val is not None:
                result.append(val)
            else:
                if fetched and isinstance(fetched, list):
                    for q in fetched:
                        if q.get("symbol") == sym:
                            result.append(q)
                            break
        return result

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
