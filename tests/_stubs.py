"""Shared stub broker / stub DB used across the unit-test suite.

Why
---
Production code touches Tradier and TimescaleDB. Tests run on dev
machines that don't have either. Each test file used to build its own
ad-hoc stubs (see ``test_money_manager_sync.py``, ``test_mock_broker.py``);
this module collects the patterns into one place so new tests pick them
up cheaply.

Conventions
-----------
- ``StubBroker`` defaults to ample buying power and an empty order book.
  Tests override individual methods (``get_option_chains``, ``get_quote``,
  etc.) by setting attributes after construction.
- ``StubDB`` swallows writes; reads return whatever the test wired up.
  Use ``set_open_trades`` / ``set_open_legs`` to seed read paths.
- ``make_chain`` builds a synthetic option chain with realistic greeks.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Broker
# ---------------------------------------------------------------------------
class StubBroker:
    """Minimal broker double — enough to drive a CascadingEngine.tick.

    Override any method on an instance to alter behaviour for one test::

        broker = StubBroker()
        broker.get_option_chains = lambda sym, exp: my_chain
    """

    current_date: Optional[datetime] = None
    dry_run: bool = False

    def __init__(
        self,
        *,
        option_buying_power: float = 100_000.0,
        positions: Optional[List[Dict[str, Any]]] = None,
        orders: Optional[List[Dict[str, Any]]] = None,
        expirations: Optional[List[str]] = None,
    ):
        self._option_buying_power = option_buying_power
        self._positions = positions or []
        self._orders = orders or []
        self._expirations = expirations
        self.placed: List[Dict[str, Any]] = []  # captured place_order calls

    # ── account / positions / orders ─────────────────────────────────────────
    def get_account_balances(self) -> Dict[str, Any]:
        return {
            "option_buying_power": self._option_buying_power,
            "stock_buying_power": self._option_buying_power,
            "cash": self._option_buying_power,
            "total_equity": self._option_buying_power,
            "account_type": "margin",
        }

    def get_positions(self) -> List[Dict[str, Any]]:
        return list(self._positions)

    def get_orders(self) -> List[Dict[str, Any]]:
        return list(self._orders)

    # ── markets ──────────────────────────────────────────────────────────────
    def get_option_expirations(self, symbol: str) -> List[str]:
        if self._expirations is not None:
            return list(self._expirations)
        # Default: emit the conventional DTE buckets each strategy expects.
        today = date.today()
        return [(today + timedelta(days=d)).strftime("%Y-%m-%d")
                for d in (7, 14, 21, 30, 35, 40, 45, 60)]

    def get_option_chains(self, symbol: str, expiry: str) -> List[Dict[str, Any]]:
        return make_chain(symbol, expiry)

    def get_quote(self, symbols: str) -> List[Dict[str, Any]]:
        return [{"symbol": s.strip(), "last": 100.0, "bid": 99.95, "ask": 100.05}
                for s in symbols.split(",")]

    def get_delta(self, option_symbol: str) -> float:
        return 0.16

    def analyze_symbol(self, symbol: str, period: str = "6m") -> Dict[str, Any]:
        return {
            "symbol": symbol,
            "current_price": 100.0,
            "current_vol": 0.20,
            "avg_vol": 0.20,
            "key_levels": [
                {"price": 90.0,  "type": "support",    "strength": 5, "pop": 0.80},
                {"price": 110.0, "type": "resistance", "strength": 5, "pop": 0.80},
            ],
            "put_entry_points": [{"price": 90.0, "pop": 0.80}],
            "call_entry_points": [{"price": 110.0, "pop": 0.80}],
            "samples": 100,
            "period": period,
        }

    # ── orders ──────────────────────────────────────────────────────────────
    def place_order_from_action(self, action) -> Dict[str, Any]:
        self.placed.append({"symbol": action.symbol, "tag": action.tag})
        return {"status": "ok", "order_id": f"STUB-{len(self.placed)}"}

    def roll_to_next_month(self, option_symbol: str) -> str:
        return option_symbol  # tests override when they need a real roll


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------
class StubDB:
    """Tracks writes (logs, pending orders, settings) without touching SQL."""

    def __init__(self):
        self.logs: List[str] = []
        self.pending_orders: List[Any] = []
        self.settings: Dict[str, str] = {}
        self._open_trades: Dict[str, List[Dict[str, Any]]] = {}
        self._open_legs:   Dict[str, List[Dict[str, Any]]] = {}
        self._watchlists:  Dict[str, List[str]] = {}
        self._predictions: Dict[str, Dict[str, Any]] = {}
        self._vetoes: List[Dict[str, Any]] = []

    # ── seeding helpers ─────────────────────────────────────────────────────
    def set_open_trades(self, strategy_id: str, trades: List[Dict[str, Any]]):
        self._open_trades[strategy_id] = list(trades)

    def set_open_legs(self, strategy_id: str, symbol: str,
                      legs: List[Dict[str, Any]]):
        self._open_legs[(strategy_id, symbol)] = list(legs)

    def set_watchlist(self, strategy_id: str, syms: List[str]):
        self._watchlists[strategy_id] = list(syms)

    def set_prediction(self, symbol: str, pred: Dict[str, Any]):
        self._predictions[symbol] = dict(pred)

    # ── HermesDB surface ────────────────────────────────────────────────────
    async def write_log(self, *_args, **_kwargs):
        self.logs.append(_args[1] if len(_args) > 1 else "")

    async def upsert_positions(self, *_a, **_kw):
        pass

    async def tracked_option_symbols(self):
        return set()

    async def flag_orphans(self, *_a, **_kw):
        pass

    async def open_trades(self, strategy_id: str):
        return list(self._open_trades.get(strategy_id, []))

    async def all_open_trades(self):
        out: List[Dict[str, Any]] = []
        for trades in self._open_trades.values():
            out.extend(trades)
        return out

    async def open_legs(self, strategy_id: str, symbol: str):
        return list(self._open_legs.get((strategy_id, symbol), []))

    async def list_watchlist_detailed(self, strategy_id: str):
        return {sym: {"target_lots": None}
                for sym in self._watchlists.get(strategy_id, [])}

    async def all_watchlist_symbols(self):
        syms: List[str] = []
        for lst in self._watchlists.values():
            syms.extend(lst)
        return sorted(dict.fromkeys(syms))

    async def latest_prediction(self, symbol: str):
        return self._predictions.get(symbol)

    async def count_open_contracts(self, strategy_id: str, symbol: str, side: str,
                              expiry: str) -> int:
        # Mirror HermesDB: expiry is now required so a stub call without
        # one fails the same way production would.
        if not expiry:
            raise ValueError(
                "count_open_contracts requires an expiry (YYYY-MM-DD)"
            )
        total = 0
        for t in self._open_trades.get(strategy_id, []):
            if t.get("symbol") != symbol:
                continue
            if (t.get("side_type") or "").lower() != side.lower():
                continue
            t_expiry = t.get("expiry")
            if hasattr(t_expiry, "isoformat"):
                t_expiry = t_expiry.isoformat()
            if t_expiry != expiry:
                continue
            total += int(t.get("lots") or 0)
        return total

    async def count_pending_orders(self, strategy_id: str, symbol: str, side: str,
                              expiry: str) -> int:
        if not expiry:
            raise ValueError(
                "count_pending_orders requires an expiry (YYYY-MM-DD)"
            )
        return 0

    async def equity_position(self, symbol: str) -> int:
        return 0

    async def has_pending_approval(self, *_a, **_kw):
        return False

    async def get_setting(self, key: str, default: Optional[str] = None):
        return self.settings.get(key, default)

    async def set_setting(self, key: str, value: str):
        self.settings[key] = str(value)

    async def write_ai_decision(self, *_a, **_kw):
        pass

    async def record_pending_order(self, action):
        self.pending_orders.append(action)

    async def record_order_response(self, action, response):
        pass

    async def close_trade_from_action(self, action, response):
        pass

    async def queue_for_approval(self, action_dict, action_type="entry"):
        pass

    async def recent_logs(self, limit: int = 200) -> str:
        return "\n".join(self.logs[-limit:])

    async def record_veto(self, strategy_id, symbol, side_type, expiry,
                          rationale, ttl_seconds):
        import time
        symbol = (symbol or "").upper()
        side_type = side_type.lower() if side_type else None
        now = time.time()
        for v in self._vetoes:
            if (v["strategy_id"] == strategy_id and v["symbol"] == symbol
                    and v["side_type"] == side_type and v["expiry"] == expiry
                    and v["expires_at"] > now):
                v["hits"] += 1
                v["expires_at"] = now + ttl_seconds * v["hits"]
                v["rationale"] = rationale or v["rationale"]
                return v["hits"]
        self._vetoes.append({
            "strategy_id": strategy_id, "symbol": symbol, "side_type": side_type,
            "expiry": expiry, "rationale": rationale,
            "expires_at": now + ttl_seconds, "hits": 1,
        })
        return 1

    async def active_veto(self, strategy_id, symbol, side_type, expiry):
        import time
        symbol = (symbol or "").upper()
        side_type = side_type.lower() if side_type else None
        now = time.time()
        for v in sorted(self._vetoes, key=lambda x: x["expires_at"], reverse=True):
            if v["strategy_id"] != strategy_id or v["symbol"] != symbol:
                continue
            if v["expires_at"] <= now:
                continue
            if v["side_type"] and v["side_type"] != side_type:
                continue
            if v["expiry"] and v["expiry"] != expiry:
                continue
            return v["rationale"] or "previously vetoed"
        return None


# ---------------------------------------------------------------------------
# Synthetic option chain
# ---------------------------------------------------------------------------
def make_chain(symbol: str, expiry: str,
               *, spot: float = 100.0,
               strike_step: float = 1.0,
               width_strikes: int = 20) -> List[Dict[str, Any]]:
    """Return a synthetic chain symmetric around ``spot`` with realistic deltas.

    Greeks fall off linearly with distance — close enough for strike-selection
    tests; not intended for pricing-accuracy tests.

    OCC symbols use the canonical format so they parse cleanly through
    ``hermes.common.OCC_RE``.
    """
    yymmdd = datetime.strptime(expiry, "%Y-%m-%d").strftime("%y%m%d")
    chain: List[Dict[str, Any]] = []
    for i in range(-width_strikes, width_strikes + 1):
        strike = round(spot + i * strike_step, 2)
        atm_distance = abs(strike - spot) / max(spot, 1.0)
        for side in ("put", "call"):
            sign = -1 if side == "put" else 1
            # Linear delta proxy: 0.5 ATM, falling toward 0 deep OTM.
            delta = max(0.01, min(0.99, 0.5 - sign * (strike - spot) / (spot * 0.4)))
            if side == "put":
                delta = -delta
            # Per-side intrinsic so puts and calls aren't priced identically.
            if side == "call":
                intrinsic = max(0.0, spot - strike)
            else:
                intrinsic = max(0.0, strike - spot)
            # Time value chosen so adjacent-strike credit on the put side
            # clears the 25%-of-width threshold CS75 enforces (i.e. ~$1.50
            # per $1 strike-step movement at 10-pt distance from spot).
            time_value = max(0.10, 8.0 - atm_distance * 25.0)
            mid = intrinsic + time_value
            occ = f"{symbol}{yymmdd}{'P' if side == 'put' else 'C'}{int(round(strike * 1000)):08d}"
            chain.append({
                "symbol": occ,
                "option_type": side,
                "strike": float(strike),
                "bid": round(max(0.05, mid - 0.05), 2),
                "ask": round(mid + 0.05, 2),
                "greeks": {"delta": float(delta)},
            })
    return chain


def make_trade(strategy_id: str, symbol: str, *,
               side_type: str = "put",
               short_strike: float = 90.0,
               long_strike: float = 85.0,
               lots: int = 1,
               entry_credit: float = 1.50,
               width: float = 5.0,
               expiry: Optional[date] = None,
               days_to_expiry: int = 21,
               trade_id: int = 1) -> Dict[str, Any]:
    """Build a row matching the dict shape ``HermesDB.open_trades`` returns."""
    if expiry is None:
        expiry = date.today() + timedelta(days=days_to_expiry)
    yymmdd = expiry.strftime("%y%m%d")
    pc = "P" if side_type == "put" else "C"
    short_leg = f"{symbol}{yymmdd}{pc}{int(round(short_strike * 1000)):08d}"
    long_leg  = f"{symbol}{yymmdd}{pc}{int(round(long_strike  * 1000)):08d}"
    return {
        "id": trade_id,
        "strategy_id": strategy_id,
        "symbol": symbol,
        "side_type": side_type,
        "short_leg": short_leg,
        "long_leg": long_leg,
        "short_strike": short_strike,
        "long_strike": long_strike,
        "width": width,
        "lots": lots,
        "entry_credit": entry_credit,
        "expiry": expiry,
        "status": "OPEN",
    }
