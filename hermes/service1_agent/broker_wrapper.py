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

    def update_cached_quote(self, symbol: str, data: Dict[str, Any]) -> None:
        """Manually updates the quote cache with a streaming quote."""
        now_ts = self._get_current_timestamp()
        self._shared_cache.set_quote(symbol, data, now_ts)

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
        from hermes.portfolio.safety_gateway import SafetyValidationError
        cb = AsyncBrokerWrapper._shared_cb
        state = cb.check_state()
        if state == "OPEN":
            logger.error("Circuit breaker is OPEN. Fast-failing order placement.")
            raise CircuitBreakerError("Circuit breaker is OPEN. Orders are blocked.")

        # Declarative Safety Verification Gateway Checks
        config = {}
        enabled = False
        if self.db is not None and hasattr(self.db, "get_setting"):
            try:
                enabled_raw = await self.db.settings.get_setting("safety_gateway_enabled")
                if enabled_raw is not None:
                    enabled = enabled_raw.lower() == "true"

                max_risk_raw = await self.db.settings.get_setting("safety_max_risk_bp_ratio")
                if max_risk_raw is not None:
                    config["safety_max_risk_bp_ratio"] = float(max_risk_raw)
                    enabled = True
                    
                max_exp_raw = await self.db.settings.get_setting("safety_max_symbol_exposure_ratio")
                if max_exp_raw is not None:
                    config["safety_max_symbol_exposure_ratio"] = float(max_exp_raw)
                    enabled = True
                    
                max_trades_raw = await self.db.settings.get_setting("safety_max_symbol_trades")
                if max_trades_raw is not None:
                    config["safety_max_symbol_trades"] = int(max_trades_raw)
                    enabled = True
                    
                side_lock_raw = await self.db.settings.get_setting("safety_side_lock_enabled")
                if side_lock_raw is not None:
                    config["safety_side_lock_enabled"] = side_lock_raw.lower() == "true"
                    enabled = True
            except Exception as e:
                logger.warning("[SAFETY] Failed to load safety settings from DB: %s", e)

        if enabled:
            from hermes.portfolio.safety_gateway import SafetyGateway
            gateway = SafetyGateway(config)

            try:
                balances = await self.get_account_balances() or {}
            except Exception as e:
                logger.warning("[SAFETY] Failed to fetch account balances for safety checks: %s", e)
                balances = {}

            try:
                open_trades = []
                if hasattr(self.db, "all_open_trades"):
                    open_trades = await self.db.trades.all_open_trades() or []
            except Exception as e:
                logger.warning("[SAFETY] Failed to fetch open trades for safety checks: %s", e)
                open_trades = []

            report = gateway.validate_action(action, balances, open_trades)
            
            decision_msg = f"[SAFETY GATEWAY] {report.decision} trade for {action.symbol} (Tag: {action.tag}). Metrics: {report.metrics}"
            if report.violations:
                decision_msg += f". Violations: {report.violations}"
            
            logger.info(decision_msg)
            if self.db is not None and hasattr(self.db, "write_log"):
                try:
                    await self.db.logs.write_log(
                        strategy_id=action.strategy_id, 
                        msg=decision_msg, 
                        level="INFO" if report.decision == "APPROVED" else "WARNING"
                    )
                except Exception:
                    pass

            if report.decision == "REJECTED":
                raise SafetyValidationError(f"Order rejected by Safety Gateway: {'; '.join(report.violations)}")

        # Proceed to submission if approved
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
            if not isinstance(e, (CircuitBreakerError, SafetyValidationError)):
                await cb.record_failure(self.db, f"Order placement exception: {e}")
            raise

    async def get_normalized_active_orders(self) -> List[Dict[str, Any]]:
        """Fetch active orders and return them normalized for MoneyManager capacity checks.
        Returns a list of dicts:
            {
                "strategy_id": str,
                "symbol": str,
                "side_type": "put" | "call" | "unknown",
                "expiry_iso": "YYYY-MM-DD",
                "lots": int
            }
        """
        orders = await self.get_orders() or []
        if not isinstance(orders, list):
            logger.warning("[WRAPPER] get_orders returned non-list: %r", orders)
            return []
            
        normalized = []
        active_statuses = {"open", "partially_filled", "pending", "calculated", "accepted"}
        from hermes.common import OCC_RE
        
        for o in orders:
            if not isinstance(o, dict):
                continue
            status = str(o.get("status", "")).lower()
            if status not in active_statuses:
                continue

            tag = str(o.get("tag", "") or "")
            # Tradier's tag sanitiser converts '_' to '-' so 'HERMES_CS75'
            # arrives back as 'HERMES-CS75'. Normalise to hyphens/underscores for matching.
            normalised_tag = tag.replace("_", "-")
            if not normalised_tag.startswith("HERMES-"):
                continue
            strategy_id = normalised_tag[len("HERMES-"):].split("-", 1)[0]
            if not strategy_id:
                continue
            symbol = str(o.get("symbol", "")).upper()

            # Multileg orders return their legs under "leg"; single-leg option
            # orders carry option_symbol at the top level (no "leg" array).
            legs = o.get("leg") or []
            if isinstance(legs, dict):
                legs = [legs]
            if not legs:
                top_opt = o.get("option_symbol")
                if top_opt:
                    legs = [{"option_symbol": top_opt,
                             "quantity": o.get("quantity", 1)}]

            lots = int(o.get("quantity", 1) or 1)
            side_type = "unknown"
            expiry_iso = ""
            for leg in legs:
                occ_sym = str(leg.get("option_symbol", "") or "")
                m = OCC_RE.match(occ_sym)
                if not m:
                    continue
                side_type = "put" if m.group(3) == "P" else "call"
                # OCC expiry is YYMMDD in group 2 → normalise to YYYY-MM-DD
                yymmdd = m.group(2)
                expiry_iso = f"20{yymmdd[0:2]}-{yymmdd[2:4]}-{yymmdd[4:6]}"
                break

            if side_type != "unknown":
                normalized.append({
                    "strategy_id": strategy_id,
                    "symbol": symbol,
                    "side_type": side_type,
                    "expiry_iso": expiry_iso,
                    "lots": lots
                })
        return normalized
