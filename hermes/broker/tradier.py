"""
TradierBroker — the asynchronous broker implementation against the Tradier REST API.

This is the single Tradier client for the whole codebase: the trading agent
(Service-1), the watcher's analytics (Service-2), and the MCP server all use it.
It conforms to ``AbstractBroker`` and returns the dict-compatible normalized
models from ``hermes.broker.models``, built on ``httpx.AsyncClient``.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import httpx
from hermes.ml.pop_engine import find_key_levels
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

from .base import AbstractBroker
from .models import (
    AccountBalances,
    BrokerPosition,
    BrokerOrder,
    OptionChainLeg,
    MarketQuote,
    OrderPlacementResult,
)

logger = logging.getLogger("hermes.broker.tradier")

OCC_RE = re.compile(r"^([A-Z]+)(\d{6})([PC])(\d{8})$")

_RETRY_POLICY = dict(
    retry=retry_if_exception_type(httpx.RequestError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)

_TRADIER_OPTION_ACTIONS = {
    "buy_to_open", "sell_to_open", "buy_to_close", "sell_to_close",
}
_OPENING_BY_SIDE = {"buy": "buy_to_open", "sell": "sell_to_open"}
_CLOSING_BY_SIDE = {"buy": "buy_to_close", "sell": "sell_to_close"}


class TradierBroker(AbstractBroker):
    """Asynchronous Tradier REST client shaped like Hermes' broker contract."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.token = (
            self.config.get("tradier_access_token")
            or os.environ.get("TRADIER_ACCESS_TOKEN")
            or os.environ.get("TRADIER_API_KEY")
        )
        self.account_id = self.config.get("tradier_account_id") or os.environ.get("TRADIER_ACCOUNT_ID")
        self.base_url = (
            self.config.get("tradier_base_url")
            or os.environ.get("TRADIER_BASE_URL")
            or "https://api.tradier.com/v1"
        ).rstrip("/")
        if not self.token or not self.account_id:
            raise ValueError("TRADIER_ACCESS_TOKEN (or TRADIER_API_KEY) and TRADIER_ACCOUNT_ID must be set")

        self.dry_run = bool(self.config.get("dry_run", False))
        self.current_date: Optional[datetime] = None
        self.timeout = float(self.config.get("tradier_timeout_s", 10.0))
        self._client: Optional[httpx.AsyncClient] = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Accept": "application/json",
                },
                timeout=self.timeout
            )
        return self._client

    async def close(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()

    async def __aenter__(self) -> TradierBroker:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    def _raise_with_body(self, r: httpx.Response, method: str, url: str,
                          data: Optional[Dict[str, Any]] = None) -> None:
        try:
            body = r.json()
        except Exception:
            body = r.text
        logger.error("Tradier %s %s -> %d  body=%s  data=%s",
                     method, url, r.status_code, body, data)
        raise httpx.HTTPStatusError(
            f"{r.status_code} {r.reason_phrase} for {method} {url} :: {body}",
            request=r.request,
            response=r,
        )

    @retry(**_RETRY_POLICY)
    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        client = self._get_client()
        r = await client.get(path, params=params)
        if not r.is_success:
            self._raise_with_body(r, "GET", str(r.url), params)
        return r.json() or {}

    async def _post(self, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
        client = self._get_client()
        r = await client.post(path, data=data)
        if not r.is_success:
            self._raise_with_body(r, "POST", str(r.url), data)
        return r.json() or {}

    def _enforce_dry_run(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if self.dry_run:
            data["preview"] = "true"
        return data

    async def get_account_balances(self) -> AccountBalances:
        data = await self._get(f"/accounts/{self.account_id}/balances")
        b = (data.get("balances") or {})
        margin = b.get("margin") or {}
        pdt    = b.get("pdt") or {}
        cash   = b.get("cash") or {}

        def _f(val) -> float:
            try:
                return float(val or 0.0)
            except (TypeError, ValueError):
                return 0.0

        obp = (
            _f(b.get("option_buying_power"))
            or _f(margin.get("option_buying_power"))
            or _f(pdt.get("option_buying_power"))
            or _f(cash.get("cash_available"))
            or _f(b.get("total_cash"))
        )

        sbp = (
            _f(b.get("stock_buying_power"))
            or _f(margin.get("stock_buying_power"))
            or _f(pdt.get("stock_buying_power"))
        )

        logger.debug(
            "get_account_balances: account_type=%s obp=%.2f sbp=%.2f raw_keys=%s",
            b.get("account_type"), obp, sbp, list(b.keys()),
        )

        return AccountBalances(
            option_buying_power=obp,
            stock_buying_power=sbp,
            total_equity=_f(b.get("total_equity")),
            cash=_f(cash.get("cash_available") or b.get("total_cash")),
            account_type=b.get("account_type"),
            margin_buying_power=_f(margin.get("stock_buying_power") or pdt.get("stock_buying_power")),
            raw=b,
        )

    async def get_positions(self) -> List[BrokerPosition]:
        data = await self._get(f"/accounts/{self.account_id}/positions")
        positions = (data.get("positions") or {})
        if not positions or positions == "null":
            return []
        items = positions.get("position", [])
        if isinstance(items, dict):
            items = [items]
        return [
            BrokerPosition(
                symbol=p.get("symbol"),
                quantity=float(p.get("quantity", 0.0) or 0.0),
                cost_basis=float(p.get("cost_basis", 0.0) or 0.0),
                date_acquired=p.get("date_acquired"),
            )
            for p in items
        ]

    async def get_orders(self) -> List[BrokerOrder]:
        data = await self._get(f"/accounts/{self.account_id}/orders", params={"includeTags": "true"})
        orders = (data.get("orders") or {})
        if not orders or orders == "null":
            return []
        items = orders.get("order", [])
        if isinstance(items, dict):
            items = [items]
        orders_list = []
        for o in items:
            orders_list.append(
                BrokerOrder(
                    order_id=str(o.get("id") or o.get("order_id") or ""),
                    symbol=str(o.get("symbol", "")),
                    status=str(o.get("status", "")),
                    quantity=int(o.get("quantity", 1) or 1),
                    price=float(o.get("price") or o.get("avg_fill_price") or 0.0),
                    side=str(o.get("side", "")),
                    tag=str(o.get("tag", "")),
                    legs=o.get("leg") or [],
                    option_symbol=o.get("option_symbol"),
                    # Drop keys already passed explicitly so the raw-field spread
                    # can't collide ("multiple values for keyword argument").
                    **{k: v for k, v in o.items() if k not in (
                        "order_id", "symbol", "status", "quantity", "price",
                        "side", "tag", "legs", "option_symbol",
                    )}
                )
            )
        return orders_list

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        client = self._get_client()
        url = f"/accounts/{self.account_id}/orders/{order_id}"
        r = await client.delete(url)
        r.raise_for_status()
        return r.json() or {}

    async def get_option_expirations(self, symbol: str) -> List[str]:
        data = await self._get(
            "/markets/options/expirations",
            params={"symbol": symbol, "includeAllRoots": "true", "strikes": "false"},
        )
        exp = (data.get("expirations") or {}).get("date") or []
        if isinstance(exp, str):
            exp = [exp]
        return exp

    async def get_option_chains(self, symbol: str, expiry: str) -> List[OptionChainLeg]:
        data = await self._get(
            "/markets/options/chains",
            params={"symbol": symbol, "expiration": expiry, "greeks": "true"},
        )
        options = (data.get("options") or {})
        if not options or options == "null":
            return []
        items = options.get("option", [])
        if isinstance(items, dict):
            items = [items]
        legs = []
        for o in items:
            greeks = o.get("greeks") or {}
            delta = float(greeks.get("delta") or 0.0)
            legs.append(
                OptionChainLeg(
                    symbol=o.get("symbol", ""),
                    strike=float(o.get("strike", 0.0) or 0.0),
                    option_type=o.get("option_type", ""),
                    bid=float(o.get("bid", 0.0) or 0.0),
                    ask=float(o.get("ask", 0.0) or 0.0),
                    delta=delta,
                    greeks=greeks,
                    # Spread the remaining raw Tradier fields, but drop the keys
                    # already passed explicitly — the raw chain dict carries
                    # symbol/strike/option_type/bid/ask/greeks, so an unfiltered
                    # ``**o`` raises "multiple values for keyword argument" and
                    # silently empties every chain.
                    **{k: v for k, v in o.items() if k not in (
                        "symbol", "strike", "option_type", "bid", "ask",
                        "delta", "greeks",
                    )}
                )
            )
        return legs

    async def get_quote(self, symbols: str) -> List[MarketQuote]:
        data = await self._get("/markets/quotes", params={"symbols": symbols, "greeks": "true"})
        quotes = (data.get("quotes") or {})
        if not quotes or quotes == "null":
            return []
        items = quotes.get("quote", [])
        if isinstance(items, dict):
            items = [items]
        quotes_list = []
        for q in items:
            price = float(q.get("last") or q.get("price") or 0.0)
            quotes_list.append(
                MarketQuote(
                    symbol=q.get("symbol", ""),
                    price=price,
                    bid=float(q.get("bid", 0.0) or 0.0),
                    ask=float(q.get("ask", 0.0) or 0.0),
                    volume=int(q.get("volume", 0) or 0),
                    timestamp=str(q.get("timestamp") or ""),
                    # Drop keys already passed explicitly so the raw-field spread
                    # can't collide ("multiple values for keyword argument").
                    **{k: v for k, v in q.items() if k not in (
                        "symbol", "price", "bid", "ask", "volume", "timestamp",
                    )}
                )
            )
        return quotes_list

    async def get_delta(self, option_symbol: str) -> float:
        quotes = await self.get_quote(option_symbol)
        if not quotes:
            return 0.0
        greeks = (quotes[0].get("greeks") or {})
        return float(greeks.get("delta", 0.0) or 0.0)

    async def get_history(self, symbol: str, *, interval: str = "daily",
                    start: Optional[str] = None, end: Optional[str] = None) -> List[Dict[str, Any]]:
        if interval in ("1min", "5min", "15min"):
            params = {"symbol": symbol, "interval": interval}
            if start:
                params["start"] = start
            if end:
                params["end"] = end
            data = await self._get("/markets/timesales", params=params)
            ts = (data.get("series") or {})
            if not ts or ts == "null":
                return []
            items = ts.get("data", [])
            if isinstance(items, dict):
                items = [items]
            for item in items:
                if "time" in item:
                    item["date"] = item["time"]
            return items

        params = {"symbol": symbol, "interval": interval}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        data = await self._get("/markets/history", params=params)
        history = (data.get("history") or {})
        if not history or history == "null":
            return []
        
        key = "day"
        if interval == "weekly": key = "week"
        elif interval == "monthly": key = "month"
        
        items = history.get(key, [])
        if isinstance(items, dict):
            items = [items]
        return items

    async def analyze_symbol(self, symbol: str, period: str = "6m") -> Dict[str, Any]:
        months = {"1m": 1, "3m": 3, "6m": 6, "1y": 12}.get(period, 6)
        end = date.today()
        start = end - timedelta(days=months * 31)
        bars = await self.get_history(symbol, start=start.isoformat(), end=end.isoformat())
        
        if not bars:
            return {"error": f"no history for {symbol}"}
            
        df = pd.DataFrame(bars)
        if df.empty or 'close' not in df.columns or 'volume' not in df.columns:
            return {"error": f"incomplete history for {symbol}"}
            
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        df = df.dropna(subset=['close', 'volume'])
        
        if df.empty:
            return {"error": f"invalid history data for {symbol}"}

        current = float(df['close'].iloc[-1])
        key_levels = find_key_levels(df['close'], df['volume'], window=5, n_clusters=6)
        
        log_ret = np.log(df["close"] / df["close"].shift(1)).dropna()
        if len(log_ret) >= 21:
            realized_vol = float(log_ret.iloc[-21:].std() * np.sqrt(252))
        else:
            realized_vol = 0.0
        avg_vol = float(log_ret.std() * np.sqrt(252)) if len(log_ret) >= 2 else 0.0
        if not np.isfinite(realized_vol):
            realized_vol = 0.0
        if not np.isfinite(avg_vol):
            realized_vol = 0.0

        put_entries = [lvl for lvl in key_levels if lvl['type'] == 'support']
        call_entries = [lvl for lvl in key_levels if lvl['type'] == 'resistance']
        
        put_entries.sort(key=lambda x: abs(x['price'] - current))
        call_entries.sort(key=lambda x: abs(x['price'] - current))

        return {
            "symbol": symbol,
            "current_price": current,
            "current_vol": realized_vol,
            "avg_vol": avg_vol,
            "key_levels": key_levels,
            "put_entry_points": put_entries,
            "call_entry_points": call_entries,
            "samples": len(df),
            "period": period,
        }

    async def place_order_from_action(self, action) -> OrderPlacementResult:
        legs = action.legs or []
        if not legs:
            raise ValueError("TradeAction has no legs")

        order_class = (action.order_class or "multileg").lower()
        if order_class == "equity":
            res = await self._place_equity(action)
        elif order_class == "option" and len(legs) == 1:
            res = await self._place_single_option(action)
        else:
            res = await self._place_multileg(action)

        return OrderPlacementResult.from_broker_response(res)

    def _leg_action(self, leg: Dict[str, Any], default_open: bool = True) -> str:
        explicit = (leg.get("action") or "").lower().strip()
        if explicit in _TRADIER_OPTION_ACTIONS:
            return explicit
        side = (leg.get("side") or "").lower().strip()
        if side in _TRADIER_OPTION_ACTIONS:
            return side
        table = _OPENING_BY_SIDE if default_open else _CLOSING_BY_SIDE
        if side not in table:
            raise ValueError(f"Cannot map leg side={side!r} action={explicit!r}")
        return table[side]

    async def _place_multileg(self, action) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "class": "multileg",
            "symbol": action.symbol,
            "type": (action.order_type or "credit").lower(),
            "duration": (action.duration or "day").lower(),
        }
        if action.price is not None:
            data["price"] = f"{float(action.price):.2f}"
        clean_tag = self._sanitize_tag(action.tag)
        if clean_tag:
            data["tag"] = clean_tag

        for i, leg in enumerate(action.legs):
            data[f"option_symbol[{i}]"] = leg["option_symbol"]
            data[f"side[{i}]"] = self._leg_action(leg, default_open=True)
            data[f"quantity[{i}]"] = int(leg.get("quantity", action.quantity or 1))

        self._enforce_dry_run(data)
        return await self._post(f"/accounts/{self.account_id}/orders", data)

    @staticmethod
    def _coerce_single_leg_type(order_type: Optional[str], has_price: bool) -> str:
        t = (order_type or "").lower().strip()
        if t in {"market", "limit", "stop", "stop_limit"}:
            return t
        return "limit" if has_price else "market"

    @staticmethod
    def _sanitize_tag(tag: Optional[str]) -> Optional[str]:
        if not tag:
            return None
        cleaned = re.sub(r"[^A-Za-z0-9-]+", "-", tag).strip("-")
        return cleaned[:255] or None

    async def _place_single_option(self, action) -> Dict[str, Any]:
        leg = action.legs[0]
        order_type = self._coerce_single_leg_type(action.order_type,
                                                  action.price is not None)
        data: Dict[str, Any] = {
            "class": "option",
            "symbol": action.symbol,
            "option_symbol": leg["option_symbol"],
            "side": self._leg_action(leg, default_open=True),
            "quantity": int(leg.get("quantity", action.quantity or 1)),
            "type": order_type,
            "duration": (action.duration or "day").lower(),
        }
        if order_type in {"limit", "stop_limit"} and action.price is not None:
            data["price"] = f"{float(action.price):.2f}"
        clean_tag = self._sanitize_tag(action.tag)
        if clean_tag:
            data["tag"] = clean_tag
        self._enforce_dry_run(data)
        return await self._post(f"/accounts/{self.account_id}/orders", data)

    async def _place_equity(self, action) -> Dict[str, Any]:
        order_type = self._coerce_single_leg_type(action.order_type,
                                                  action.price is not None)
        data: Dict[str, Any] = {
            "class": "equity",
            "symbol": action.symbol,
            "side": (action.side or "buy").lower(),
            "quantity": int(action.quantity or 1),
            "type": order_type,
            "duration": (action.duration or "day").lower(),
        }
        if order_type in {"limit", "stop_limit"} and action.price is not None:
            data["price"] = f"{float(action.price):.2f}"
        clean_tag = self._sanitize_tag(action.tag)
        if clean_tag:
            data["tag"] = clean_tag
        self._enforce_dry_run(data)
        return await self._post(f"/accounts/{self.account_id}/orders", data)

    async def roll_to_next_month(self, option_symbol: str) -> str:
        m = OCC_RE.match(option_symbol or "")
        if not m:
            raise ValueError(f"Not an OCC option symbol: {option_symbol!r}")
        underlying, yymmdd, pc, strike = m.groups()
        current_exp = datetime.strptime(yymmdd, "%y%m%d").date()

        expirations = await self.get_option_expirations(underlying)
        future = [datetime.strptime(e, "%Y-%m-%d").date() for e in expirations
                  if datetime.strptime(e, "%Y-%m-%d").date() > current_exp]
        if not future:
            raise RuntimeError(f"No later expirations available for {underlying}")
        next_exp = min(future)
        return f"{underlying}{next_exp.strftime('%y%m%d')}{pc}{strike}"
