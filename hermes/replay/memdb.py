"""In-memory HermesDB double for replay runs.

Follows the stub-DB pattern from ``tests/_stubs.py`` (namespaced repo views
over one flat surface) but genuinely round-trips the trade lifecycle the way
``TradesRepository`` does — ``record_pending_order`` → ``record_order_response``
creates an OPEN trade row, ``close_trade_from_action`` finalizes it with an
exit price and realized P&L — so ``manage_positions`` and the capacity checks
see the same book they would against Postgres. Timestamps come from the
harness' :class:`~hermes.clock.SimulatedClock`, not the wall clock.

Nothing here opens a connection anywhere: a replay run's only writes land in
this object.
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from hermes.clock import Clock
from hermes.common import OCC_RE
from hermes.db.orm import _compute_realized_pnl

logger = logging.getLogger("hermes.replay.memdb")

_REJECT_STATUSES = {"rejected", "error", "expired", "canceled", "cancelled"}


class _NSView:
    """Forwards repo-method lookups to the owning ReplayDB's flat surface."""

    def __init__(self, db: "ReplayDB"):
        object.__setattr__(self, "_db", db)

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "_db"), name)


def _parse_order_response(response) -> tuple:
    order = (response or {}).get("order") if isinstance(response, dict) else None
    order_status = ""
    broker_order_id: Optional[str] = None
    if isinstance(order, dict):
        order_status = str(order.get("status", "")).lower()
        broker_order_id = str(order["id"]) if order.get("id") is not None else None
    rejected = ((isinstance(response, dict) and "errors" in response)
                or order_status in _REJECT_STATUSES)
    return order_status, broker_order_id, rejected


def _resolve_lots(action, default, include_close: bool = False) -> int:
    for leg in (action.legs or []):
        leg_side = (leg.get("side") or "").lower()
        if ("sell" in leg_side or "open" in leg_side
                or (include_close and "close" in leg_side)):
            try:
                return int(leg["quantity"])
            except (KeyError, TypeError, ValueError):
                break
    return default


def _derive_side_type(action, fallback_to_action_side: bool = False) -> Optional[str]:
    side_value = (action.strategy_params or {}).get("side_type")
    if not side_value or str(side_value).lower() in {"buy", "sell"}:
        side_value = None
        for leg in (action.legs or []):
            m = OCC_RE.match(str(leg.get("option_symbol", "") or ""))
            if m:
                side_value = "put" if m.group(3) == "P" else "call"
                break
        if side_value is None and fallback_to_action_side:
            side_value = action.side
    return side_value


def _extract_strike(occ_symbol: Optional[str]) -> Optional[float]:
    if not occ_symbol:
        return None
    m = OCC_RE.match(str(occ_symbol))
    if not m:
        return None
    return int(m.group(4)) / 1000.0


def _expiry_date(value) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


class ReplayDB:
    """Flat in-memory DB with the namespaced ``db.<repo>.<method>`` surface."""

    def __init__(self, clock: Clock):
        self.clock = clock
        for ns in ("logs", "decisions", "trades", "watchlist", "approvals",
                   "settings", "timeseries", "analytics", "commands"):
            setattr(self, ns, _NSView(self))

        self.log_lines: List[Dict[str, Any]] = []
        self._settings: Dict[str, str] = {}
        self._trades: List[Dict[str, Any]] = []
        self._pending: List[Dict[str, Any]] = []
        self._approvals: List[Dict[str, Any]] = []
        self._watchlists: Dict[str, List[str]] = {}
        self._predictions: Dict[str, Dict[str, Any]] = {}
        self._vetoes: List[Dict[str, Any]] = []
        self._next_trade_id = 1
        self._next_approval_id = 1

    def _now(self) -> datetime:
        return self.clock.utc_now()

    # ── logs ─────────────────────────────────────────────────────────────────
    async def write_log(self, strategy_id: str, message: str = "", level: str = "INFO"):
        self.log_lines.append({"ts": self._now(), "strategy_id": strategy_id,
                               "message": message, "level": level})

    async def flag_orphans(self, orphan_symbols):
        for sym in orphan_symbols:
            await self.write_log("ENGINE", f"orphan position: {sym}", level="WARNING")

    async def recent_logs(self, limit: int = 200) -> str:
        return "\n".join(l["message"] for l in self.log_lines[-limit:])

    # ── settings ─────────────────────────────────────────────────────────────
    async def get_setting(self, key: str, default: Optional[str] = None):
        return self._settings.get(key, default)

    async def set_setting(self, key: str, value: str):
        self._settings[key] = str(value)

    async def get_settings(self, keys) -> Dict[str, Optional[str]]:
        return {k: self._settings.get(k) for k in keys}

    # ── operator commands / approvals (idle in replay) ───────────────────────
    async def fetch_pending(self, limit: int = 100) -> List[Dict[str, Any]]:
        return []

    async def mark_applied(self, cid):
        pass

    async def mark_failed(self, cid, err):
        pass

    async def has_pending_approval(self, strategy_id, symbol, side, expiry) -> bool:
        for app in self._approvals:
            if app["status"] in ("PENDING", "PENDING_AI_REVIEW"):
                a = app["action_json"]
                app_side = (a.get("strategy_params") or {}).get("side_type")
                if (app["strategy_id"] == strategy_id
                        and app["symbol"] == (symbol or "").upper()
                        and app_side == side and a.get("expiry") == expiry):
                    return True
        return False

    async def queue_for_approval(self, action_dict, action_type="entry", status="PENDING"):
        app_id = self._next_approval_id
        self._next_approval_id += 1
        self._approvals.append({
            "id": app_id, "action_json": action_dict,
            "strategy_id": action_dict.get("strategy_id"),
            "symbol": (action_dict.get("symbol") or "").upper(),
            "action_type": action_type, "status": status.upper(),
        })
        return app_id

    async def update_approval_status(self, approval_id, status, action_json=None, notes=None):
        for item in self._approvals:
            if item["id"] == approval_id:
                item["status"] = status.upper()
                return True
        return False

    async def mark_approval_executed(self, approval_id, success=True, notes=None):
        for item in self._approvals:
            if item["id"] == approval_id:
                item["status"] = "EXECUTED" if success else "FAILED"

    async def fetch_approved_actions(self):
        return [i for i in self._approvals if i["status"] == "APPROVED"]

    async def active_veto(self, strategy_id, symbol, side_type, expiry):
        return None

    async def record_veto(self, strategy_id, symbol, side_type, expiry,
                          rationale, ttl_seconds):
        self._vetoes.append({"strategy_id": strategy_id, "symbol": symbol,
                             "side_type": side_type, "expiry": expiry,
                             "rationale": rationale})
        return 1

    # ── watchlist / predictions ──────────────────────────────────────────────
    async def list_watchlist(self, strategy_id: str) -> List[str]:
        return list(self._watchlists.get(strategy_id, []))

    async def list_watchlist_detailed(self, strategy_id: str) -> Dict[str, Any]:
        return {sym: {"target_lots": None}
                for sym in self._watchlists.get(strategy_id, [])}

    async def latest_prediction(self, symbol: str):
        return self._predictions.get(symbol)

    # ── trade lifecycle (mirrors TradesRepository semantics) ─────────────────
    async def record_pending_order(self, action) -> None:
        lots = _resolve_lots(action, default=action.quantity)
        side = _derive_side_type(action, fallback_to_action_side=True)
        self._pending.append({
            "strategy_id": action.strategy_id,
            "symbol": action.symbol,
            "side": (side or "").lower(),
            "lots": lots,
            "expiry": action.expiry,
            "status": "PENDING",
        })
        is_pure_close = bool(action.legs) and all(
            "_to_close" in (leg.get("side") or "").lower() for leg in action.legs)
        if is_pure_close:
            row = self._find_open_trade(action, statuses=("OPEN",))
            if row is not None:
                row["status"] = "CLOSING"

    def _consume_pending(self, action, side) -> None:
        for p in self._pending:
            if (p["status"] == "PENDING" and p["strategy_id"] == action.strategy_id
                    and p["symbol"] == action.symbol
                    and p["side"] == (side or "").lower()):
                p["status"] = "SUBMITTED"
                return

    def _find_open_trade(self, action, statuses=("OPEN", "CLOSING")) -> Optional[Dict[str, Any]]:
        sp = action.strategy_params or {}
        trade_id = sp.get("trade_id")
        if trade_id is not None:
            for t in self._trades:
                if t["id"] == int(trade_id) and t["status"] in statuses:
                    return t
        leg_syms = [str(leg.get("option_symbol") or "")
                    for leg in (action.legs or []) if leg.get("option_symbol")]
        for t in reversed(self._trades):
            if (t["status"] in statuses and t["symbol"] == action.symbol
                    and t["strategy_id"] == action.strategy_id
                    and t.get("short_leg") in leg_syms):
                return t
        return None

    async def record_order_response(self, action, response) -> None:
        order_status, broker_order_id, rejected = _parse_order_response(response)
        lots = _resolve_lots(
            action, default=action.quantity if action.quantity is not None else 1)
        side = _derive_side_type(action)
        self._consume_pending(action, side or action.side)
        if rejected:
            await self.write_log(
                action.strategy_id,
                f"[ORDER REJECTED] {action.symbol} side={side} qty={lots} "
                f"response={response}")
            return

        sp = action.strategy_params or {}
        short_leg = sp.get("short_leg")
        long_leg = sp.get("long_leg")
        if not short_leg or not long_leg:
            for leg in (action.legs or []):
                ls = (leg.get("side") or "").lower()
                osym = leg.get("option_symbol")
                if not osym:
                    continue
                if not short_leg and "sell" in ls:
                    short_leg = osym
                elif not long_leg and "buy" in ls:
                    long_leg = osym

        short_strike = _extract_strike(short_leg)
        long_strike = _extract_strike(long_leg)
        width = action.width
        if width is None and short_strike is not None and long_strike is not None:
            width = abs(short_strike - long_strike)

        entry_credit = None
        entry_debit = None
        ot = (action.order_type or "").lower()
        if action.price is not None:
            if ot == "credit" or (ot == "" and (action.side or "").lower() == "sell"):
                entry_credit = float(action.price)
            else:
                entry_debit = float(action.price)

        row = {
            "id": self._next_trade_id,
            "strategy_id": action.strategy_id,
            "symbol": action.symbol,
            "side_type": (side or "unknown").lower(),
            "short_leg": short_leg,
            "long_leg": long_leg,
            "short_strike": short_strike,
            "long_strike": long_strike,
            "width": float(width) if width is not None else None,
            "lots": lots,
            "entry_credit": entry_credit,
            "entry_debit": entry_debit,
            "expiry": _expiry_date(action.expiry),
            "status": "OPEN",
            "opened_at": self._now(),
            "closed_at": None,
            "exit_price": None,
            "pnl": None,
            "close_reason": None,
            "tag": getattr(action, "tag", None),
            "close_tag": None,
            "broker_order_id": broker_order_id,
            "pop": sp.get("pop"),
            "entry_features": sp.get("entry_features"),
        }
        self._next_trade_id += 1
        self._trades.append(row)
        await self.write_log(
            action.strategy_id,
            f"[ORDER ACCEPTED] {action.symbol} side={side} qty={lots} "
            f"order_id={broker_order_id} status={order_status or 'ok'}")

    async def close_trade_from_action(self, action, response) -> None:
        order_status, broker_order_id, rejected = _parse_order_response(response)
        side = _derive_side_type(action)
        self._consume_pending(action, side or action.side)
        row = self._find_open_trade(action)
        if rejected:
            # Re-arm a CLOSING trade so it's retried next tick.
            if row is not None and row["status"] == "CLOSING":
                row["status"] = "OPEN"
            await self.write_log(
                action.strategy_id,
                f"[CLOSE REJECTED] {action.symbol} response={response}")
            return
        if row is None:
            await self.write_log(
                action.strategy_id,
                f"[CLOSE ORPHAN] {action.symbol} no matching OPEN trade")
            return

        sp = action.strategy_params or {}
        exit_price = float(action.price) if action.price is not None else None
        self._finalize_close(
            row, exit_price=exit_price,
            close_reason=sp.get("close_reason") or "MANAGED_CLOSE",
            close_tag=getattr(action, "tag", None))
        await self.write_log(
            action.strategy_id,
            f"[CLOSE FILLED] {action.symbol} trade_id={row['id']} "
            f"reason={row['close_reason']} exit={exit_price} pnl={row['pnl']}")

    def _finalize_close(self, row: Dict[str, Any], *, exit_price: Optional[float],
                        close_reason: str, close_tag: Optional[str]) -> None:
        row["status"] = "CLOSED"
        row["exit_price"] = exit_price
        row["close_reason"] = close_reason
        row["close_tag"] = close_tag
        row["closed_at"] = self._now()
        row["pnl"] = _compute_realized_pnl(
            entry_credit=row.get("entry_credit"),
            entry_debit=row.get("entry_debit"),
            exit_price=exit_price, lots=row.get("lots"))

    async def upsert_positions(self, positions, active_order_legs=None,
                               opening_order_legs=None) -> None:
        """Reconcile OPEN/CLOSING rows against broker truth (production logic)."""
        broker_legs = set()
        for p in positions or []:
            sym = str(p.get("symbol", "") or "")
            if OCC_RE.match(sym) and int(round(float(p.get("quantity", 0) or 0))) != 0:
                broker_legs.add(sym)
        active = set(active_order_legs or [])
        for t in self._trades:
            if t["status"] not in ("OPEN", "CLOSING"):
                continue
            legs = {leg for leg in (t.get("short_leg"), t.get("long_leg")) if leg}
            held = bool(legs & broker_legs)
            resting = bool(legs & active)
            if t["status"] == "OPEN":
                if not held and not resting:
                    self._finalize_close(t, exit_price=t.get("exit_price"),
                                         close_reason=t.get("close_reason")
                                         or "RECONCILED_BROKER_FLAT",
                                         close_tag=t.get("close_tag"))
            else:  # CLOSING
                if not held:
                    self._finalize_close(t, exit_price=t.get("exit_price"),
                                         close_reason=t.get("close_reason")
                                         or "MANAGED_CLOSE",
                                         close_tag=t.get("close_tag"))
                elif not resting:
                    t["status"] = "OPEN"

    # ── replay-only settlement helper ────────────────────────────────────────
    def settle_expired(self, asof: date, spread_value_fn) -> int:
        """Close every OPEN/CLOSING trade whose expiry is before ``asof``.

        ``spread_value_fn(trade_row) -> Optional[float]`` prices the spread at
        settlement (per-share debit to close). Returns the number settled.
        """
        n = 0
        for t in self._trades:
            if t["status"] not in ("OPEN", "CLOSING"):
                continue
            exp = t.get("expiry")
            if exp is None or exp >= asof:
                continue
            value = spread_value_fn(t)
            self._finalize_close(t, exit_price=value, close_reason="EXPIRED",
                                 close_tag=None)
            n += 1
        return n

    # ── reads (repo surface) ─────────────────────────────────────────────────
    @staticmethod
    def _trade_view(t: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": t["id"], "strategy_id": t["strategy_id"], "symbol": t["symbol"],
            "side_type": t["side_type"], "short_leg": t["short_leg"],
            "long_leg": t["long_leg"], "short_strike": t["short_strike"],
            "long_strike": t["long_strike"], "width": t["width"],
            "lots": int(t["lots"]),
            "entry_credit": float(t["entry_credit"] or 0),
            "entry_debit": t["entry_debit"],
            "expiry": t["expiry"], "status": t["status"],
        }

    async def open_trades(self, strategy_id: str) -> List[Dict[str, Any]]:
        return [self._trade_view(t) for t in self._trades
                if t["strategy_id"] == strategy_id and t["status"] == "OPEN"]

    async def closing_trades(self, strategy_id: str) -> List[Dict[str, Any]]:
        return [self._trade_view(t) for t in self._trades
                if t["strategy_id"] == strategy_id and t["status"] == "CLOSING"]

    async def all_open_trades(self) -> List[Dict[str, Any]]:
        return [self._trade_view(t) for t in self._trades if t["status"] == "OPEN"]

    async def open_legs(self, strategy_id: str, symbol: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for t in self._trades:
            if (t["strategy_id"] != strategy_id or t["symbol"] != symbol
                    or t["status"] != "OPEN"):
                continue
            expiry_iso = t["expiry"].isoformat() if t.get("expiry") else None
            for leg_key in ("short_leg", "long_leg"):
                if t.get(leg_key):
                    out.append({"option_symbol": t[leg_key],
                                "side": t["side_type"], "expiry": expiry_iso})
        return out

    async def tracked_option_symbols(self) -> set:
        symbols = set()
        for t in self._trades:
            if t["status"] in ("OPEN", "CLOSING"):
                for leg_key in ("short_leg", "long_leg"):
                    if t.get(leg_key):
                        symbols.add(t[leg_key])
        return symbols

    async def count_open_contracts(self, strategy_id, symbol, side, expiry) -> int:
        if not expiry:
            raise ValueError("count_open_contracts requires an expiry (YYYY-MM-DD)")
        exp = _expiry_date(expiry)
        return sum(int(t["lots"] or 0) for t in self._trades
                   if t["strategy_id"] == strategy_id and t["symbol"] == symbol
                   and t["status"] == "OPEN"
                   and (t["side_type"] or "").lower() == side.lower()
                   and t.get("expiry") == exp)

    async def count_pending_orders(self, strategy_id, symbol, side, expiry) -> int:
        if not expiry:
            raise ValueError("count_pending_orders requires an expiry (YYYY-MM-DD)")
        return sum(int(p["lots"] or 0) for p in self._pending
                   if p["status"] == "PENDING" and p["strategy_id"] == strategy_id
                   and p["symbol"] == symbol and p["side"] == side.lower()
                   and p.get("expiry") == expiry)

    async def count_trades_for_expiry(self, strategy_id, symbol, side, expiry) -> int:
        exp = _expiry_date(expiry)
        if exp is None:
            raise ValueError(f"count_trades_for_expiry: invalid expiry {expiry!r}")
        return sum(1 for t in self._trades
                   if t["strategy_id"] == strategy_id and t["symbol"] == symbol
                   and (t["side_type"] or "").lower() == side.lower()
                   and t.get("expiry") == exp)

    async def expire_stale_pending_orders(self, older_than_seconds: int) -> int:
        return 0

    async def latest_closed_trade_time(self, strategy_id, symbol) -> Optional[datetime]:
        times = [t["closed_at"] for t in self._trades
                 if t["strategy_id"] == strategy_id and t["symbol"] == symbol
                 and t["status"] == "CLOSED" and t["closed_at"] is not None]
        return max(times) if times else None

    async def equity_position(self, symbol: str) -> int:
        for t in reversed(self._trades):
            if (t["symbol"] == symbol and t["side_type"] == "equity"
                    and t["status"] == "OPEN"):
                return int(t["lots"])
        return 0

    async def closed_trades_entry_features(self, limit: int = 500) -> List[Dict[str, Any]]:
        rows = [t for t in self._trades if t["status"] == "CLOSED"
                and t.get("pnl") is not None
                and t.get("entry_features") is not None]
        rows = sorted(rows, key=lambda x: x["closed_at"] or datetime.min, reverse=True)
        return [
            {
                "strategy_id": r["strategy_id"],
                "symbol": r["symbol"],
                "entry_features": r["entry_features"],
                "pnl": float(r["pnl"]),
                "closed_at": r["closed_at"]
            }
            for r in rows[:limit]
        ]

    async def get_realized_edge_stats(self, strategy_id: str, window_days: int) -> Dict[str, Any]:
        from datetime import timedelta
        cutoff = self._now() - timedelta(days=window_days)
        closed_trades = [t for t in self._trades if t["strategy_id"] == strategy_id
                         and t["status"] == "CLOSED"
                         and t.get("closed_at") is not None
                         and t["closed_at"] >= cutoff
                         and t.get("pnl") is not None]
        wins = [float(t["pnl"]) for t in closed_trades if float(t["pnl"]) > 0]
        losses = [float(t["pnl"]) for t in closed_trades if float(t["pnl"]) < 0]
        count = len(closed_trades)
        return {
            "win_rate": float(len(wins) / count) if count > 0 else 0.5,
            "avg_win": float(sum(wins) / len(wins)) if wins else 0.0,
            "avg_loss": float(sum(losses) / len(losses)) if losses else 0.0,
            "count": count,
        }

    async def save_greeks_snapshot(self, net_delta: float, net_vega: float, net_theta: float, ts_now: Optional[datetime] = None) -> None:
        pass


    # ── replay reporting surface ─────────────────────────────────────────────
    def closed_trade_rows(self) -> List[Dict[str, Any]]:
        return [dict(t) for t in self._trades if t["status"] == "CLOSED"]

    def all_trade_rows(self) -> List[Dict[str, Any]]:
        return [dict(t) for t in self._trades]

    def set_watchlist(self, strategy_id: str, symbols: List[str]) -> None:
        self._watchlists[strategy_id] = list(symbols)
