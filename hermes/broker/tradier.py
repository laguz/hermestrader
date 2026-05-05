"""
TradierBroker — concrete broker implementation against the Tradier REST API.

Conforms to the same surface the rest of Hermes already uses (see
`hermes/service1_agent/mock_broker.py`):

    get_account_balances() -> dict
    get_positions()        -> list[dict]
    get_option_expirations(symbol) -> list[str]   # "YYYY-MM-DD"
    get_option_chains(symbol, expiry) -> list[dict]
    get_quote(symbols)     -> list[dict]
    get_delta(option_symbol) -> float
    analyze_symbol(symbol, period="6m") -> dict
    place_order_from_action(action: TradeAction) -> dict
    roll_to_next_month(option_symbol) -> str

Configuration (env or config dict):
    TRADIER_ACCESS_TOKEN  — bearer token
    TRADIER_ACCOUNT_ID    — account number
    TRADIER_BASE_URL      — defaults to https://api.tradier.com/v1
                            (use https://sandbox.tradier.com/v1 for paper)

Docs: https://documentation.tradier.com/brokerage-api
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

logger = logging.getLogger("hermes.broker.tradier")

OCC_RE = re.compile(r"^([A-Z]+)(\d{6})([PC])(\d{8})$")

# Tenacity retry policy for idempotent GET calls.  Retries on transient
# network errors only — never on HTTP 4xx/5xx (those are Tradier rejections
# and retrying won't help).
_RETRY_POLICY = dict(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)

# Map Hermes leg side → Tradier OCC option side. A leg may carry the action
# in one of three shapes — handled in this priority order by `_leg_action`:
#   1. An explicit `action` field with the full Tradier name
#      ('buy_to_open' / 'sell_to_close' / ...).
#   2. A `side` field that already contains the full Tradier name (some
#      strategies build legs that way; the early hermestrader strategy code
#      pre-dates the `action` field and stores the full name in `side`).
#   3. A short `side` of 'buy'/'sell' — combined with `default_open` to
#      pick the matching open or close action.
_TRADIER_OPTION_ACTIONS = {
    "buy_to_open", "sell_to_open", "buy_to_close", "sell_to_close",
}
_OPENING_BY_SIDE = {"buy": "buy_to_open", "sell": "sell_to_open"}
_CLOSING_BY_SIDE = {"buy": "buy_to_close", "sell": "sell_to_close"}


class TradierBroker:
    """Thin, synchronous Tradier REST client shaped like Hermes' broker contract."""

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.token = self.config.get("tradier_access_token") or os.environ.get("TRADIER_ACCESS_TOKEN")
        self.account_id = self.config.get("tradier_account_id") or os.environ.get("TRADIER_ACCOUNT_ID")
        self.base_url = (
            self.config.get("tradier_base_url")
            or os.environ.get("TRADIER_BASE_URL")
            or "https://api.tradier.com/v1"
        ).rstrip("/")
        if not self.token or not self.account_id:
            raise ValueError("TRADIER_ACCESS_TOKEN and TRADIER_ACCOUNT_ID must be set")

        self.dry_run = bool(self.config.get("dry_run", False))
        self.current_date: Optional[datetime] = None  # Hermes uses this for time mocking

        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        })
        self._timeout = float(self.config.get("tradier_timeout_s", 10.0))

    # ------------------------------------------------------------------ HTTP
    def _raise_with_body(self, r: "requests.Response", method: str, url: str,
                         data: Optional[Dict[str, Any]] = None) -> None:
        """Raise an HTTPError that includes Tradier's response body.

        Tradier returns the actual rejection reason in the JSON body
        (e.g. {"errors":{"error":["type is invalid for class option"]}}).
        Without including it, every 400 looks identical and you cannot tell
        whether it was a bad symbol, bad type, bad side, missing price, etc.
        """
        try:
            body = r.json()
        except Exception:                                          # noqa: BLE001
            body = r.text
        # Log the failing request so we can correlate against strategy intent.
        logger.error("Tradier %s %s -> %d  body=%s  data=%s",
                     method, url, r.status_code, body, data)
        raise requests.HTTPError(
            f"{r.status_code} {r.reason} for {method} {url} :: {body}",
            response=r,
        )

    @retry(**_RETRY_POLICY)
    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        r = self._session.get(url, params=params, timeout=self._timeout)
        if not r.ok:
            self._raise_with_body(r, "GET", url, params)
        return r.json() or {}

    def _post(self, path: str, data: Dict[str, Any]) -> Dict[str, Any]:
        # NOTE: _post is intentionally NOT retried — duplicate order placement
        # is worse than a missed trade.  Connection-level errors propagate up
        # to the tick loop which will catch and log them.
        url = f"{self.base_url}{path}"
        r = self._session.post(url, data=data, timeout=self._timeout)
        if not r.ok:
            self._raise_with_body(r, "POST", url, data)
        return r.json() or {}

    def _enforce_dry_run(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Hard client-side guard: always stamp preview=true when dry_run is set.

        This is belt-and-suspenders on top of each placement helper so that any
        future order path added to this class cannot accidentally fire a live
        order — even if the caller forgets to set the flag in `data`.
        """
        if self.dry_run:
            if not data.get("preview"):
                logger.warning(
                    "dry_run=True but preview not set in order data — "
                    "forcing preview=true (order will NOT be submitted)"
                )
            data["preview"] = "true"
        return data

    # ----------------------------------------------------------------- Account
    def get_account_balances(self) -> Dict[str, Any]:
        data = self._get(f"/accounts/{self.account_id}/balances")
        b = (data.get("balances") or {})
        margin = b.get("margin") or {}
        pdt    = b.get("pdt") or {}
        cash   = b.get("cash") or {}

        # Tradier places option_buying_power in different locations depending on
        # account type:
        #   Live margin  → balances.option_buying_power (top-level)
        #   Paper/margin → balances.margin.option_buying_power
        #   PDT account  → balances.pdt.option_buying_power
        #   Cash account → balances.cash.cash_available (no dedicated OBP field)
        # Walk through the priority list and take the first non-zero value found.
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

        import logging as _logging
        _logging.getLogger("hermes.broker.tradier").debug(
            "get_account_balances: account_type=%s obp=%.2f sbp=%.2f raw_keys=%s",
            b.get("account_type"), obp, sbp, list(b.keys()),
        )

        return {
            "option_buying_power": obp,
            "stock_buying_power": sbp,
            "total_equity": _f(b.get("total_equity")),
            "cash": _f(cash.get("cash_available") or b.get("total_cash")),
            "account_type": b.get("account_type"),
            "margin_buying_power": _f(margin.get("stock_buying_power") or pdt.get("stock_buying_power")),
            "raw": b,
        }

    def get_positions(self) -> List[Dict[str, Any]]:
        data = self._get(f"/accounts/{self.account_id}/positions")
        positions = (data.get("positions") or {})
        if not positions or positions == "null":
            return []
        items = positions.get("position", [])
        if isinstance(items, dict):
            items = [items]
        return [
            {
                "symbol": p.get("symbol"),
                "quantity": float(p.get("quantity", 0.0) or 0.0),
                "cost_basis": float(p.get("cost_basis", 0.0) or 0.0),
                "date_acquired": p.get("date_acquired"),
            }
            for p in items
        ]

    def get_orders(self) -> List[Dict[str, Any]]:
        data = self._get(f"/accounts/{self.account_id}/orders", params={"includeTags": "true"})
        orders = (data.get("orders") or {})
        if not orders or orders == "null":
            return []
        items = orders.get("order", [])
        if isinstance(items, dict):
            items = [items]
        return items

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/accounts/{self.account_id}/orders/{order_id}"
        r = self._session.delete(url, timeout=self._timeout)
        r.raise_for_status()
        return r.json() or {}

    # ---------------------------------------------------------------- Markets
    def get_option_expirations(self, symbol: str) -> List[str]:
        data = self._get(
            "/markets/options/expirations",
            params={"symbol": symbol, "includeAllRoots": "true", "strikes": "false"},
        )
        exp = (data.get("expirations") or {}).get("date") or []
        if isinstance(exp, str):
            exp = [exp]
        return exp

    def get_option_chains(self, symbol: str, expiry: str) -> List[Dict[str, Any]]:
        data = self._get(
            "/markets/options/chains",
            params={"symbol": symbol, "expiration": expiry, "greeks": "true"},
        )
        options = (data.get("options") or {})
        if not options or options == "null":
            return []
        items = options.get("option", [])
        if isinstance(items, dict):
            items = [items]
        return items

    def get_quote(self, symbols: str) -> List[Dict[str, Any]]:
        data = self._get("/markets/quotes", params={"symbols": symbols, "greeks": "true"})
        quotes = (data.get("quotes") or {})
        if not quotes or quotes == "null":
            return []
        items = quotes.get("quote", [])
        if isinstance(items, dict):
            items = [items]
        return items

    def get_delta(self, option_symbol: str) -> float:
        quotes = self.get_quote(option_symbol)
        if not quotes:
            return 0.0
        greeks = (quotes[0].get("greeks") or {})
        return float(greeks.get("delta", 0.0) or 0.0)

    def get_history(self, symbol: str, *, interval: str = "daily",
                    start: Optional[str] = None, end: Optional[str] = None) -> List[Dict[str, Any]]:
        # For intraday intervals, use /markets/timesales
        if interval in ("1min", "5min", "15min"):
            params = {"symbol": symbol, "interval": interval}
            if start:
                params["start"] = start
            if end:
                params["end"] = end
            data = self._get("/markets/timesales", params=params)
            ts = (data.get("series") or {})
            if not ts or ts == "null":
                return []
            items = ts.get("data", [])
            if isinstance(items, dict):
                items = [items]
            # Normalize key names to match history endpoint (date -> time)
            for item in items:
                if "time" in item:
                    item["date"] = item["time"]
            return items

        # Standard daily/weekly/monthly history
        params = {"symbol": symbol, "interval": interval}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        data = self._get("/markets/history", params=params)
        history = (data.get("history") or {})
        if not history or history == "null":
            return []
        
        # Tradier returns the list under a key matching the interval (day, week, month)
        key = "day"
        if interval == "weekly": key = "week"
        elif interval == "monthly": key = "month"
        
        items = history.get(key, [])
        if isinstance(items, dict):
            items = [items]
        return items

    # --------------------------------------------------------------- Analysis
    def analyze_symbol(self, symbol: str, period: str = "6m") -> Dict[str, Any]:
        """
        Lightweight rules-based analysis used by strategies to pick entry zones.
        Returns current price plus put/call entry-point candidates derived from
        the empirical price distribution over the requested window.
        """
        months = {"1m": 1, "3m": 3, "6m": 6, "1y": 12}.get(period, 6)
        end = date.today()
        start = end - timedelta(days=months * 31)
        bars = self.get_history(symbol, start=start.isoformat(), end=end.isoformat())
        closes = [float(b.get("close", 0.0) or 0.0) for b in bars if b.get("close") is not None]
        if not closes:
            return {"error": f"no history for {symbol}"}

        current = closes[-1]
        sorted_closes = sorted(closes)

        def _p(pct: float) -> float:
            i = max(0, min(len(sorted_closes) - 1, int(round(pct * (len(sorted_closes) - 1)))))
            return round(sorted_closes[i], 2)

        # POP heuristic: how often price stayed beyond the candidate strike historically.
        def _pop_below(level: float) -> int:
            return int(round(100 * sum(1 for c in closes if c >= level) / len(closes)))

        def _pop_above(level: float) -> int:
            return int(round(100 * sum(1 for c in closes if c <= level) / len(closes)))

        put_entries = [
            {"price": _p(0.20), "pop": _pop_below(_p(0.20))},
            {"price": _p(0.10), "pop": _pop_below(_p(0.10))},
            {"price": _p(0.05), "pop": _pop_below(_p(0.05))},
        ]
        call_entries = [
            {"price": _p(0.80), "pop": _pop_above(_p(0.80))},
            {"price": _p(0.90), "pop": _pop_above(_p(0.90))},
            {"price": _p(0.95), "pop": _pop_above(_p(0.95))},
        ]
        return {
            "symbol": symbol,
            "current_price": current,
            "put_entry_points": put_entries,
            "call_entry_points": call_entries,
            "samples": len(closes),
            "period": period,
        }

    # -------------------------------------------------------------- Order API
    def place_order_from_action(self, action) -> Dict[str, Any]:
        """
        Submit a TradeAction. Honors `dry_run` by routing through Tradier's
        order preview endpoint instead of placing the order.
        """
        legs = action.legs or []
        if not legs:
            raise ValueError("TradeAction has no legs")

        order_class = (action.order_class or "multileg").lower()
        if order_class == "equity":
            return self._place_equity(action)
        if order_class == "option" and len(legs) == 1:
            return self._place_single_option(action)
        return self._place_multileg(action)

    def _leg_action(self, leg: Dict[str, Any], default_open: bool = True) -> str:
        explicit = (leg.get("action") or "").lower().strip()
        if explicit in _TRADIER_OPTION_ACTIONS:
            return explicit
        side = (leg.get("side") or "").lower().strip()
        # Some strategies put the full Tradier action ("sell_to_open" etc.)
        # directly in `side`; accept that without forcing every strategy to
        # use the canonical `action` field.
        if side in _TRADIER_OPTION_ACTIONS:
            return side
        table = _OPENING_BY_SIDE if default_open else _CLOSING_BY_SIDE
        if side not in table:
            raise ValueError(f"Cannot map leg side={side!r} action={explicit!r}")
        return table[side]

    def _place_multileg(self, action) -> Dict[str, Any]:
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
        return self._post(f"/accounts/{self.account_id}/orders", data)

    # Tradier's `type` field is class-scoped:
    #   class=multileg → credit | debit | even | market
    #   class=option   → market | limit | stop | stop_limit
    #   class=equity   → market | limit | stop | stop_limit
    # Strategies often build a TradeAction with order_type defaulted to
    # 'credit' (the multileg default) and then route a single leg through
    # _place_single_option — which 400s at Tradier. These helpers coerce
    # invalid types back to 'limit' so the order survives.
    @staticmethod
    def _coerce_single_leg_type(order_type: Optional[str], has_price: bool) -> str:
        t = (order_type or "").lower().strip()
        if t in {"market", "limit", "stop", "stop_limit"}:
            return t
        # 'credit' / 'debit' / 'even' / '' all fall through here. If the
        # strategy provided a price, treat it as a limit order; otherwise
        # market.
        return "limit" if has_price else "market"

    @staticmethod
    def _sanitize_tag(tag: Optional[str]) -> Optional[str]:
        """Conform a strategy-provided tag to Tradier's allowed character set.

        Tradier rejects orders whose `tag` contains anything outside
        [A-Za-z0-9-]. Strategies in this repo build tags like
        `HERMES_WHEEL` / `HERMES_CS75` — every underscore must become a
        hyphen, and anything else exotic is stripped. Truncates at 255 chars
        (Tradier's documented maximum).
        """
        if not tag:
            return None
        # Replace any run of disallowed chars with a single hyphen, trim
        # leading/trailing hyphens, cap length.
        cleaned = re.sub(r"[^A-Za-z0-9-]+", "-", tag).strip("-")
        return cleaned[:255] or None

    def _place_single_option(self, action) -> Dict[str, Any]:
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
        return self._post(f"/accounts/{self.account_id}/orders", data)

    def _place_equity(self, action) -> Dict[str, Any]:
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
        return self._post(f"/accounts/{self.account_id}/orders", data)

    # --------------------------------------------------------------- Roll API
    def roll_to_next_month(self, option_symbol: str) -> str:
        """
        Return the OCC symbol for the next monthly expiry at the same strike
        and side. Picks the smallest available expiry strictly after the
        current one. Strike is preserved exactly.
        """
        m = OCC_RE.match(option_symbol or "")
        if not m:
            raise ValueError(f"Not an OCC option symbol: {option_symbol!r}")
        underlying, yymmdd, pc, strike = m.groups()
        current_exp = datetime.strptime(yymmdd, "%y%m%d").date()

        expirations = self.get_option_expirations(underlying)
        future = [datetime.strptime(e, "%Y-%m-%d").date() for e in expirations
                  if datetime.strptime(e, "%Y-%m-%d").date() > current_exp]
        if not future:
            raise RuntimeError(f"No later expirations available for {underlying}")
        next_exp = min(future)
        return f"{underlying}{next_exp.strftime('%y%m%d')}{pc}{strike}"
