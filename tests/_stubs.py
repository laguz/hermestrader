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

from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo


def _et_today() -> date:
    """ET trading day, matching StrategyBase.today() — not date.today().

    On a UTC-TZ runner, system-local date.today() disagrees with the ET
    trading day for part of every day; stubs that default off "today" must
    anchor to the same ET date the strategy under test computes.
    """
    return datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York")).date()


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
        today = _et_today()
        return [(today + timedelta(days=d)).strftime("%Y-%m-%d")
                for d in (7, 14, 21, 30, 35, 40, 45, 60)]

    async def get_corporate_calendar(self, symbols: str) -> Dict[str, Any]:
        return {"calendar": []}

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
# Repository-namespace views. Production code calls the DB through namespaces
# (``db.logs.write_log``, ``db.settings.get_setting``, …); HermesDB owns real
# repository objects, but StubDB keeps one flat method surface. These thin views
# forward an unknown attribute (a repo method) back to the stub's flat method,
# so ``db.trades.open_trades(...)`` resolves to ``StubDB.open_trades``.
#
# Three namespace names — ``logs``, ``settings``, ``approvals`` — collide with
# StubDB's own inspection state (a captured-log list, a settings dict, an
# approval-row list). Making those views ``list``/``dict`` *subclasses* lets the
# same attribute serve both roles: ``for m in db.logs`` still iterates captured
# messages, while ``db.logs.write_log(...)`` forwards to the stub. ``__getattr__``
# only fires for names the container itself doesn't define.
class _NSView:
    """Forwards repo-method lookups to the owning StubDB's flat surface."""
    def __init__(self, db):
        object.__setattr__(self, "_db", db)
    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_db"), name)


class _ListNSView(list):
    def __init__(self, db):
        super().__init__()
        self._db = db
    def __getattr__(self, name):          # only for attrs `list` lacks
        return getattr(self._db, name)


class _DictNSView(dict):
    def __init__(self, db):
        super().__init__()
        self._db = db
    def __getattr__(self, name):          # only for attrs `dict` lacks
        return getattr(self._db, name)


_REPO_NS_NAMES = frozenset({
    "logs", "decisions", "trades", "watchlist",
    "approvals", "settings", "timeseries", "analytics", "commands",
})


class RepoNamespaceMixin:
    """Give a flat DB double the ``db.<repo>.<method>`` namespace surface.

    Any ad-hoc test double that implements the flat DB methods (``get_setting``,
    ``write_log``, …) can inherit this to also answer namespaced calls — a
    missing ``db.settings`` / ``db.trades`` / … resolves to a view that forwards
    method lookups back to the double's flat surface. Only fires for the eight
    repo names the double doesn't already define as data.
    """
    def __getattr__(self, name):
        if name in _REPO_NS_NAMES:
            return _NSView(self)
        raise AttributeError(
            f"{type(self).__name__!r} object has no attribute {name!r}"
        )


def alias_db_namespaces(mock):
    """Point a bare ``AsyncMock``/``MagicMock`` db's repo namespaces at itself.

    After the namespace migration, production calls ``db.settings.get_setting``
    instead of ``db.get_setting``. For a bare mock, ``db.settings`` would be a
    *different* auto-child, so flat ``db.get_setting.return_value = …`` setups
    and ``db.set_setting.assert_called`` assertions stop matching. Aliasing each
    namespace back to the mock collapses ``db.<repo>.<method>`` to
    ``db.<method>`` on the same child mock — exactly the flat behaviour the
    tests were written against. Returns the mock for chaining.
    """
    from unittest.mock import AsyncMock, MagicMock
    for ns in _REPO_NS_NAMES:
        setattr(mock, ns, mock)
    if isinstance(mock, AsyncMock):
        mock.get_settings.return_value = {}
        mock.get_setting.return_value = None
        mock.engine = MagicMock()
    return mock


class StubDB:
    """Tracks writes (logs, pending orders, settings) without touching SQL."""

    def __init__(self):
        # Container-backed namespaces: usable both as inspection state and as a
        # repo namespace (see _ListNSView / _DictNSView above).
        self.logs = _ListNSView(self)
        self.settings = _DictNSView(self)
        # Default blackout days to 0 in StubDB so existing tests don't get blocked
        # by the system clock's proximity to real CPI/FOMC dates.
        for _s in ("cs75", "cs7", "tt45", "wheel", "hermesalpha"):
            self.settings[f"{_s}_event_blackout_days"] = "0"
            self.settings[f"{_s}_macro_blackout_days"] = "0"

        self.approvals = _ListNSView(self)
        # Plain repo namespaces forwarding to the flat surface.
        self.trades = _NSView(self)
        self.watchlist = _NSView(self)
        self.decisions = _NSView(self)
        self.timeseries = _NSView(self)
        self.analytics = _NSView(self)
        self.commands = _NSView(self)

        self.pending_orders: List[Any] = []
        self._open_trades: Dict[str, List[Dict[str, Any]]] = {}
        self._greeks_snapshots: List[Dict[str, Any]] = []
        self._prediction_ledger: List[Any] = []
        self._implied_vols: Dict[str, List[Tuple[date, float]]] = {}
        self._closing_trades: Dict[str, List[Dict[str, Any]]] = {}
        self._closed_trades: Dict[str, List[Dict[str, Any]]] = {}
        self._open_legs:   Dict[str, List[Dict[str, Any]]] = {}
        self._watchlists:  Dict[str, List[str]] = {}
        self._predictions: Dict[str, Dict[str, Any]] = {}
        self._vetoes: List[Dict[str, Any]] = []
        self._closed_times: Dict[tuple, datetime] = {}
        self._next_approval_id = 1

    # ── seeding helpers ─────────────────────────────────────────────────────
    def set_latest_closed_trade_time(self, strategy_id: str, symbol: str, dt: Optional[datetime]):
        if dt is None:
            self._closed_times.pop((strategy_id, symbol), None)
        else:
            self._closed_times[(strategy_id, symbol)] = dt
    def set_open_trades(self, strategy_id: str, trades: List[Dict[str, Any]]):
        self._open_trades[strategy_id] = list(trades)

    def set_closing_trades(self, strategy_id: str, trades: List[Dict[str, Any]]):
        self._closing_trades[strategy_id] = list(trades)

    def set_closed_trades(self, strategy_id: str, trades: List[Dict[str, Any]]):
        self._closed_trades[strategy_id] = list(trades)

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

    async def flag_orphans(self, orphan_symbols):
        for sym in orphan_symbols:
            self.logs.append(f"orphan position: {sym}")

    async def get_realized_edge_stats(self, strategy_id: str, window_days: int) -> Dict[str, Any]:
        trades_list = self._closed_trades.get(strategy_id, [])
        from datetime import datetime, timezone, timedelta
        from hermes.utils import utc_now

        cutoff = utc_now()
        if cutoff.tzinfo is not None:
            cutoff = cutoff.astimezone(timezone.utc).replace(tzinfo=None)
        cutoff = cutoff - timedelta(days=window_days)

        valid_trades = []
        for t in trades_list:
            if t.get("exit_price") is None:
                continue

            closed_at = t.get("closed_at")
            if closed_at is not None:
                if isinstance(closed_at, str):
                    closed_at = datetime.fromisoformat(closed_at)
                if closed_at.tzinfo is not None:
                    closed_at = closed_at.astimezone(timezone.utc).replace(tzinfo=None)
                if closed_at < cutoff:
                    continue

            pnl_val = t.get("pnl")
            if pnl_val is None:
                from hermes.db.orm import _compute_realized_pnl
                pnl_val = _compute_realized_pnl(
                    entry_credit=t.get("entry_credit"),
                    entry_debit=t.get("entry_debit"),
                    exit_price=t.get("exit_price"),
                    lots=t.get("lots", 1)
                )
            if pnl_val is not None:
                valid_trades.append({"pnl": float(pnl_val)})

        wins = [t["pnl"] for t in valid_trades if t["pnl"] > 0]
        losses = [t["pnl"] for t in valid_trades if t["pnl"] < 0]
        total_count = len(valid_trades)

        if total_count == 0:
            return {
                "win_rate": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "count": 0
            }

        win_rate = len(wins) / total_count
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = abs(sum(losses) / len(losses)) if losses else 0.0

        return {
            "win_rate": win_rate,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "count": total_count
        }

    async def open_trades(self, strategy_id: str):
        return list(self._open_trades.get(strategy_id, []))

    async def all_open_trades(self):
        out: List[Dict[str, Any]] = []
        for trades in self._open_trades.values():
            out.extend(trades)
        return out

    async def closing_trades(self, strategy_id: str):
        return list(self._closing_trades.get(strategy_id, []))

    async def count_trades_for_expiry(self, strategy_id: str, symbol: str,
                                      side: str, expiry: str) -> int:
        # Mirror HermesDB: every status (OPEN / CLOSING / CLOSED) counts.
        total = 0
        rows = (self._open_trades.get(strategy_id, [])
                + self._closing_trades.get(strategy_id, [])
                + self._closed_trades.get(strategy_id, []))
        for t in rows:
            if t.get("symbol") != symbol:
                continue
            if (t.get("side_type") or "").lower() != side.lower():
                continue
            t_expiry = t.get("expiry")
            if hasattr(t_expiry, "isoformat"):
                t_expiry = t_expiry.isoformat()
            if t_expiry == expiry:
                total += 1
        return total

    async def open_legs(self, strategy_id: str, symbol: str):
        return list(self._open_legs.get((strategy_id, symbol), []))

    async def list_watchlist(self, strategy_id: str):
        return list(self._watchlists.get(strategy_id, []))

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

    async def latest_closed_trade_time(self, strategy_id: str, symbol: str) -> Optional[datetime]:
        return self._closed_times.get((strategy_id, symbol))

    async def closed_trades_entry_features(self, limit: int = 500):
        return []

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

    async def save_greeks_snapshot(self, net_delta: float, net_vega: float, net_theta: float, ts: Optional[datetime] = None) -> None:
        if ts is None:
            ts = datetime.utcnow()
        self._greeks_snapshots.append({
            "ts": ts,
            "net_delta": net_delta,
            "net_vega": net_vega,
            "net_theta": net_theta
        })

    async def get_latest_greeks_snapshot(self) -> Optional[Dict[str, Any]]:
        if not self._greeks_snapshots:
            return None
        sorted_snaps = sorted(self._greeks_snapshots, key=lambda s: s["ts"], reverse=True)
        row = sorted_snaps[0]
        return {
            "ts": row["ts"].isoformat() if row["ts"] else None,
            "net_delta": float(row["net_delta"]),
            "net_vega": float(row["net_vega"]),
            "net_theta": float(row["net_theta"]),
        }


    async def has_pending_approval(self, strategy_id: str, symbol: str, side: str, expiry: str) -> bool:
        for app in self.approvals:
            if app["status"] in ("PENDING", "PENDING_AI_REVIEW"):
                a_dict = app["action_json"]
                app_side = (a_dict.get("strategy_params") or {}).get("side_type")
                if (app["strategy_id"] == strategy_id and app["symbol"] == symbol.upper()
                        and app_side == side and a_dict.get("expiry") == expiry):
                    return True
        return False

    async def fetch_pending(self, limit: int = 100) -> List[Dict[str, Any]]:
        # No operator commands queued by default; the agent's tick-start drain
        # is a no-op for stub-driven tick tests.
        return []

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

    async def save_implied_vol(self, symbol: str, iv: float, ts: Optional[Any] = None):
        if ts is None:
            ts = date.today()
        elif isinstance(ts, datetime):
            ts = ts.date()
        
        symbol = symbol.upper()
        if symbol not in self._implied_vols:
            self._implied_vols[symbol] = []
        # Update if duplicate date, else append
        history = self._implied_vols[symbol]
        for idx, (d, _) in enumerate(history):
            if d == ts:
                history[idx] = (ts, float(iv))
                break
        else:
            history.append((ts, float(iv)))
            history.sort()

    async def get_implied_vol_history(self, symbol: str, lookback_days: int = 365) -> List[Tuple[Any, float]]:
        symbol = symbol.upper()
        if symbol not in self._implied_vols:
            return []
        cutoff = date.today() - timedelta(days=lookback_days)
        res = []
        for d, iv in self._implied_vols[symbol]:
            if d >= cutoff:
                res.append((d, iv))
        return res

    def AsyncSession(self):
        return StubAsyncSession(self)


    async def queue_for_approval(self, action_dict, action_type="entry", status="PENDING"):
        app_id = self._next_approval_id
        self._next_approval_id += 1
        self.approvals.append({
            "id": app_id,
            "action_json": action_dict,
            "strategy_id": action_dict.get("strategy_id"),
            "symbol": action_dict.get("symbol"),
            "action_type": action_type,
            "status": status.upper(),
            "notes": None,
            "decided_at": None,
            "executed_at": None
        })
        return app_id

    async def fetch_pending_ai_review_actions(self):
        return [
            item for item in self.approvals
            if item["status"] == "PENDING_AI_REVIEW"
        ]

    async def update_approval_status(self, approval_id: int, status: str,
                               action_json=None, notes=None) -> bool:
        for item in self.approvals:
            if item["id"] == approval_id:
                item["status"] = status.upper()
                if action_json is not None:
                    item["action_json"] = action_json
                if notes is not None:
                    item["notes"] = notes
                return True
        return False

    async def fetch_approved_actions(self):
        return [
            item for item in self.approvals
            if item["status"] == "APPROVED"
        ]

    async def mark_approval_executed(self, approval_id: int, success: bool = True,
                               notes: Optional[str] = None) -> None:
        for item in self.approvals:
            if item["id"] == approval_id:
                item["status"] = "EXECUTED" if success else "FAILED"
                if notes is not None:
                    item["notes"] = notes

    async def decide_approval(self, approval_id: int, decision: str,
                        notes: Optional[str] = None) -> bool:
        decision = decision.upper()
        if decision not in ("APPROVED", "REJECTED"):
            raise ValueError(f"decision must be APPROVED or REJECTED, got {decision!r}")
        for item in self.approvals:
            if item["id"] == approval_id:
                if item["status"] != "PENDING":
                    return False
                item["status"] = decision
                if notes is not None:
                    item["notes"] = notes
                return True
        return False

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
        expiry = _et_today() + timedelta(days=days_to_expiry)
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


class StubAsyncSession:
    def __init__(self, db):
        self.db = db
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
    async def execute(self, stmt, params=None):
        return StubResult(self.db, stmt, params)
    async def commit(self):
        pass
    async def rollback(self):
        pass
    def add(self, row):
        self.db._prediction_ledger.append(row)


class StubResult:
    def __init__(self, db, stmt, params):
        self.db = db
        self.stmt = stmt
        self.params = params
        
        stmt_str = str(self.stmt)
        # Handle mock update queries in memory immediately on execution
        if "UPDATE" in stmt_str.upper():
            val = 0.0
            if "realized_outcome = 1.0" in stmt_str or (params and "realized_outcome = 1.0" in params):
                val = 1.0
            if "WHERE id =" in stmt_str or (params and "WHERE id =" in params):
                if self.db._prediction_ledger:
                    self.db._prediction_ledger[0].realized_outcome = val
            else:
                for r in self.db._prediction_ledger:
                    r.realized_outcome = val

    def scalars(self):
        return self

    def all(self):
        stmt_str = str(self.stmt)
        model_name = None
        try:
            params = self.stmt.compile().params
            for k, v in params.items():
                if "model_name" in k:
                    model_name = v
                    break
        except Exception:
            pass

        rows = self.db._prediction_ledger
        if model_name:
            rows = [r for r in rows if r.model_name == model_name]

        rows = sorted(rows, key=lambda r: r.ts if r.ts is not None else datetime.min, reverse=True)
        return rows

    def fetchall(self):
        rows = self.all()
        res = []
        for r in rows:
            symbol = getattr(r, "symbol", "")
            model_name = getattr(r, "model_name", "")
            predicted_prob = getattr(r, "predicted_prob", 0.0)
            res.append((symbol, model_name, predicted_prob))
        return res


