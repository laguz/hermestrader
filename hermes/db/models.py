"""
[TimescaleDB-Schema] — SQLAlchemy ORM mirror of schema.sql.
Both Service-1 (writes) and Service-2 (reads) import from this module.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    BigInteger, Boolean, Column, Date, DateTime, ForeignKey, Index, Integer,
    Numeric, Sequence, String, Text, create_engine,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from hermes.common import OCC_RE as _OCC_RE
from hermes.common import STRATEGY_PRIORITIES as _COMMON_STRATEGY_PRIORITIES

import pandas as pd


class Base(DeclarativeBase):
    pass


class Strategy(Base):
    __tablename__ = "strategies"
    strategy_id = Column(String, primary_key=True)
    priority = Column(Integer, nullable=False)
    status = Column(String, nullable=False, default="ACTIVE")
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class StrategyWatchlist(Base):
    __tablename__ = "strategy_watchlists"
    strategy_id = Column(String, ForeignKey("strategies.strategy_id", ondelete="CASCADE"),
                         primary_key=True)
    symbol = Column(String, primary_key=True)
    target_lots = Column(Integer)
    added_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class Trade(Base):
    __tablename__ = "trades"
    # `id` belongs to the schema's `BIGSERIAL` (sequence `trades_id_seq`).
    # Both this column AND `opened_at` form the composite PK because the
    # underlying TimescaleDB hypertable partitions by `opened_at`.
    # Marking the Sequence here is what tells SQLAlchemy to fetch a value
    # via RETURNING instead of inserting NULL.
    id = Column(BigInteger, Sequence("trades_id_seq"), primary_key=True,
                autoincrement=True)
    opened_at = Column(DateTime(timezone=True), default=datetime.utcnow,
                       primary_key=True)
    strategy_id = Column(String, ForeignKey("strategies.strategy_id"), nullable=False)
    symbol = Column(String, nullable=False)
    side_type = Column(String, nullable=False)
    short_leg = Column(String)
    long_leg = Column(String)
    short_strike = Column(Numeric(10, 4))
    long_strike = Column(Numeric(10, 4))
    width = Column(Numeric(10, 4))
    lots = Column(Integer, nullable=False)
    entry_credit = Column(Numeric(10, 4))
    entry_debit = Column(Numeric(10, 4))
    expiry = Column(Date)
    status = Column(String, nullable=False, default="OPEN")
    pnl = Column(Numeric(12, 2))
    closed_at = Column(DateTime(timezone=True))
    close_reason = Column(String)
    ai_authored = Column(Boolean, default=False)
    ai_rationale = Column(Text)
    broker_order_id = Column(String)
    # Strategy-tag bookkeeping. ``tag`` is the entry-order tag
    # (e.g. ``HERMES_CS75``); ``close_tag`` is the closing-order tag
    # (e.g. ``HERMES_CS75_CLOSE_TP-50``). ``exit_price`` is the closing
    # fill price; combined with ``entry_credit``/``entry_debit`` and
    # ``lots`` it gives realized P&L (see ``_compute_realized_pnl``).
    tag = Column(String)
    close_tag = Column(String)
    exit_price = Column(Numeric(10, 4))

    __table_args__ = (
        Index("idx_trades_strategy_status", "strategy_id", "status", "symbol"),
    )


class PendingOrder(Base):
    __tablename__ = "pending_orders"
    # Same shape as Trade: composite PK over the BIGSERIAL id and the
    # hypertable's partitioning column.
    id = Column(BigInteger, Sequence("pending_orders_id_seq"), primary_key=True,
                autoincrement=True)
    submitted_at = Column(DateTime(timezone=True), default=datetime.utcnow,
                          primary_key=True)
    strategy_id = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    side = Column(String, nullable=False)
    quantity = Column(Integer, nullable=False)
    payload = Column(JSONB, nullable=False)
    status = Column(String, nullable=False, default="PENDING")


class PendingApproval(Base):
    """Human-approval queue for proposed agent trades.

    The agent writes a row here (status=PENDING) instead of calling the broker
    when approval_mode is enabled.  The C2 panel approves or rejects; the
    agent's tick loop executes APPROVED rows and marks them EXECUTED.
    """
    __tablename__ = "pending_approvals"
    id = Column(BigInteger, Sequence("pending_approvals_id_seq"), primary_key=True,
                autoincrement=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    strategy_id = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    action_type = Column(String, nullable=False, default="entry")
    action_json = Column(JSONB, nullable=False)
    status = Column(String, nullable=False, default="PENDING")
    notes = Column(Text)
    decided_at = Column(DateTime(timezone=True))
    executed_at = Column(DateTime(timezone=True))
    expires_at = Column(DateTime(timezone=True))


class BotLog(Base):
    __tablename__ = "bot_logs"
    ts = Column(DateTime(timezone=True), default=datetime.utcnow, primary_key=True)
    strategy_id = Column(String, nullable=False, primary_key=True)
    level = Column(String, default="INFO")
    message = Column(Text, nullable=False)


class AIDecision(Base):
    __tablename__ = "ai_decisions"
    ts = Column(DateTime(timezone=True), default=datetime.utcnow, primary_key=True)
    strategy_id = Column(String)
    symbol = Column(String, primary_key=True)
    autonomy = Column(String, nullable=False)
    decision = Column(JSONB, nullable=False)


class Prediction(Base):
    __tablename__ = "predictions"
    ts = Column(DateTime(timezone=True), default=datetime.utcnow, primary_key=True)
    symbol = Column(String, nullable=False, primary_key=True)
    predicted_return = Column(Numeric(12, 6))
    predicted_price = Column(Numeric(12, 4))
    spot = Column(Numeric(12, 4))
    model_tag = Column(String, default="xgb-10feat-v1")


class SystemSetting(Base):
    """Small key/value table the agent and watcher both read.

    Used for shared runtime state that the watcher must be able to flip
    without restarting the agent, e.g. the live/paper trading mode and the
    rolling Tradier API health timestamps the agent writes after each tick.
    """
    __tablename__ = "system_settings"
    key = Column(String, primary_key=True)
    value = Column(Text, nullable=False, default="")
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow,
                        onupdate=datetime.utcnow)


class DailyBar(Base):
    __tablename__ = "bars_daily"
    ts = Column(DateTime(timezone=True), primary_key=True)
    symbol = Column(String, primary_key=True)
    open = Column(Numeric(12, 4))
    high = Column(Numeric(12, 4))
    low = Column(Numeric(12, 4))
    close = Column(Numeric(12, 4))
    volume = Column(BigInteger)
    vwap_close = Column(Numeric(12, 4))


class IntradayBar(Base):
    __tablename__ = "bars_intraday"
    ts = Column(DateTime(timezone=True), primary_key=True)
    symbol = Column(String, primary_key=True)
    open = Column(Numeric(12, 4))
    high = Column(Numeric(12, 4))
    low = Column(Numeric(12, 4))
    close = Column(Numeric(12, 4))
    volume = Column(BigInteger)


# ---------------------------------------------------------------------------
# Realized-PnL + tag helpers (module-level so they're trivially testable
# without a live DB).
# ---------------------------------------------------------------------------
def _close_reason_from_tag(tag: Optional[str]) -> Optional[str]:
    """Recover the close reason that a strategy embedded in its order tag.

    Strategies tag closing orders ``HERMES_<STRAT>_CLOSE_<REASON>`` (e.g.
    ``HERMES_CS75_CLOSE_TP-50``). Tradier sanitises ``_`` to ``-`` on the
    wire, so accept either separator on the round-trip.
    """
    if not tag:
        return None
    norm = str(tag).replace("-", "_")
    marker = "_CLOSE_"
    idx = norm.find(marker)
    if idx == -1:
        return None
    suffix = norm[idx + len(marker):].strip()
    return suffix or None


def _compute_realized_pnl(*, entry_credit, entry_debit,
                          exit_price, lots: int) -> Optional[float]:
    """Realized P&L on an option spread, in dollars (1 contract = 100 sh).

    For a credit spread: pnl = (entry_credit − exit_debit) × lots × 100.
    For a debit  spread: pnl = (exit_credit − entry_debit) × lots × 100.

    Returns ``None`` if the inputs are insufficient to compute (e.g. the
    closing fill price wasn't supplied) — analytics already treats NULL
    correctly, so we'd rather show "unknown" than a fabricated 0.
    """
    if exit_price is None or not lots:
        return None
    try:
        lots_i = int(lots)
        exit_f = float(exit_price)
    except (TypeError, ValueError):
        return None
    if entry_credit is not None:
        try:
            ec = float(entry_credit)
        except (TypeError, ValueError):
            return None
        return round((ec - exit_f) * lots_i * 100.0, 2)
    if entry_debit is not None:
        try:
            ed = float(entry_debit)
        except (TypeError, ValueError):
            return None
        return round((exit_f - ed) * lots_i * 100.0, 2)
    return None


# ---------------------------------------------------------------------------
# Repository — the only place SQL lives.
# ---------------------------------------------------------------------------
class HermesDB:
    """Thin repo layer; matches the surface the engine + UI consume."""

    def __init__(self, dsn: str):
        self.engine = create_engine(dsn, pool_pre_ping=True, future=True)
        self.Session = sessionmaker(self.engine, expire_on_commit=False, future=True)
        # Defensive: create any ORM-mapped tables that don't already exist.
        # schema.sql is the source of truth (TimescaleDB hypertables, indexes,
        # compression policies, continuous aggregates), but if it was never
        # applied — typical when a Postgres data volume predates the
        # schema-init mount — the watcher would 500 on every read. This call
        # ensures the bare relations exist so basic CRUD still works; running
        # `psql -f schema.sql` afterwards layers the Timescale-specific
        # features on top.
        # `checkfirst=True` is built into create_all and makes it a no-op for
        # tables that already exist.
        try:
            Base.metadata.create_all(self.engine, checkfirst=True)
        except Exception:                                       # noqa: BLE001
            # Don't crash on import — the next real query surfaces the cause.
            pass

    def init_schema(self, schema_sql_path: str) -> None:
        with open(schema_sql_path, "r", encoding="utf-8") as fh:
            sql = fh.read()
        with self.engine.begin() as conn:
            for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
                conn.exec_driver_sql(stmt + ";")

    # ---- writes -----------------------------------------------------------
    def write_log(self, strategy_id: str, message: str, level: str = "INFO") -> None:
        with self.Session() as s:
            s.add(BotLog(strategy_id=strategy_id, level=level, message=message))
            s.commit()

    def write_ai_decision(self, strategy_id: str, symbol: str,
                          autonomy: str, decision: Dict[str, Any]) -> None:
        with self.Session() as s:
            s.add(AIDecision(strategy_id=strategy_id, symbol=symbol or "*",
                             autonomy=autonomy, decision=decision))
            s.commit()

    def recent_ai_decisions(self, strategy_id: Optional[str] = None,
                            symbol: Optional[str] = None,
                            limit: int = 20) -> List[Dict[str, Any]]:
        """Return recent ai_decisions rows, newest-first.

        Optionally filter by strategy_id and/or symbol.
        Returns a list of plain dicts ready for JSON serialisation.
        """
        with self.Session() as s:
            q = s.query(AIDecision).order_by(AIDecision.ts.desc())
            if strategy_id is not None:
                q = q.filter(AIDecision.strategy_id == strategy_id)
            if symbol is not None:
                q = q.filter(AIDecision.symbol == symbol.upper())
            rows = q.limit(limit).all()
            return [
                {
                    "ts": r.ts.isoformat() if r.ts else None,
                    "strategy_id": r.strategy_id,
                    "symbol": r.symbol,
                    "autonomy": r.autonomy,
                    "decision": r.decision,
                }
                for r in rows
            ]

    def write_prediction(self, symbol: str, ret: float, price: float, spot: float = 0.0) -> None:
        with self.Session() as s:
            s.add(Prediction(symbol=symbol, predicted_return=ret, predicted_price=price, spot=spot))
            s.commit()

    def latest_prediction(self, symbol: str) -> Optional[Dict[str, Any]]:
        with self.Session() as s:
            row = s.query(Prediction).filter_by(symbol=symbol).order_by(Prediction.ts.desc()).first()
            if row:
                return {
                    "predicted_return": float(row.predicted_return or 0),
                    "predicted_price": float(row.predicted_price or 0)
                }
            return None

    def latest_predictions_batch(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Fetch the latest prediction for multiple symbols in one query."""
        if not symbols:
            return {}
        from sqlalchemy import bindparam
        from sqlalchemy import text as sa_text
        # Postgres-specific DISTINCT ON for efficient latest-per-group
        sql = sa_text("""
            SELECT DISTINCT ON (symbol)
                symbol, predicted_return, predicted_price
            FROM predictions
            WHERE symbol IN :symbols
            ORDER BY symbol, ts DESC
        """).bindparams(bindparam("symbols", expanding=True))
        results = {}
        with self.Session() as s:
            rows = s.execute(sql, {"symbols": list(symbols)}).fetchall()
            for r in rows:
                results[r.symbol] = {
                    "predicted_return": float(r.predicted_return or 0),
                    "predicted_price": float(r.predicted_price or 0)
                }
        return results

    def record_pending_order(self, action) -> None:
        # Derive the lot count from the first sell/open leg so that
        # count_pending_orders operates on the same unit (lots) as
        # count_open_contracts.  action.quantity is always 1 (one order
        # envelope); the actual lot size lives in leg["quantity"].
        lots = action.quantity  # fallback
        for leg in (action.legs or []):
            leg_side = (leg.get("side") or "").lower()
            if "sell" in leg_side or "open" in leg_side:
                try:
                    lots = int(leg["quantity"])
                except (KeyError, TypeError, ValueError):
                    pass
                break

        # Resolve the put/call side that count_pending_orders filters on.
        # Strategies populate strategy_params.side_type; if a future
        # ad-hoc action skips it, derive from the first leg's OCC symbol so
        # the row stays countable rather than silently bucketed under
        # "buy"/"sell".
        side_value = (action.strategy_params or {}).get("side_type")
        if not side_value or side_value.lower() in {"buy", "sell"}:
            side_value = None
            for leg in (action.legs or []):
                m = _OCC_RE.match(str(leg.get("option_symbol", "") or ""))
                if m:
                    side_value = "put" if m.group(3) == "P" else "call"
                    break
            if side_value is None:
                side_value = action.side

        with self.Session() as s:
            s.add(PendingOrder(
                strategy_id=action.strategy_id, symbol=action.symbol,
                side=side_value,
                quantity=lots,          # lot count, not order count
                payload={
                    "legs": action.legs, "price": action.price,
                    "tag": action.tag, "ai_authored": action.ai_authored,
                    "ai_rationale": action.ai_rationale,
                    "expiry": action.expiry,
                },
            ))
            s.commit()

    # ------------------------------------------------------------------
    # Order response → trades persistence
    #
    # When the broker accepts an order we write a Trade row immediately so
    # `count_open_contracts` reflects the new exposure on the very next
    # tick. We then drop the matching `pending_orders` row so capacity is
    # not double-counted (PendingOrder + Trade for the same fill).
    #
    # Rejections (Tradier returns ``status='rejected'`` or an HTTP error
    # surfaced as ``error`` in the parsed body) leave the trades table
    # untouched and free the pending row so the cap recovers.
    # ------------------------------------------------------------------
    _REJECT_STATUSES = {"rejected", "error", "expired", "canceled", "cancelled"}

    def record_order_response(self, action, response) -> None:
        order = (response or {}).get("order") if isinstance(response, dict) else None
        order_status = ""
        broker_order_id: Optional[str] = None
        if isinstance(order, dict):
            order_status = str(order.get("status", "")).lower()
            broker_order_id = (
                str(order["id"]) if order.get("id") is not None else None
            )

        rejected = (
            (isinstance(response, dict) and "errors" in response)
            or order_status in self._REJECT_STATUSES
        )

        # Resolve lots from the first sell/open leg (matches record_pending_order)
        lots = action.quantity or 1
        for leg in (action.legs or []):
            leg_side = (leg.get("side") or "").lower()
            if "sell" in leg_side or "open" in leg_side:
                try:
                    lots = int(leg["quantity"])
                except (KeyError, TypeError, ValueError):
                    pass
                break

        side_value = (action.strategy_params or {}).get("side_type")
        if not side_value or side_value.lower() in {"buy", "sell"}:
            side_value = None
            for leg in (action.legs or []):
                m = _OCC_RE.match(str(leg.get("option_symbol", "") or ""))
                if m:
                    side_value = "put" if m.group(3) == "P" else "call"
                    break

        self._consume_matching_pending(
            strategy_id=action.strategy_id, symbol=action.symbol,
            side=(side_value or action.side or "").lower(), lots=lots,
            terminal_status="REJECTED" if rejected else "SUBMITTED",
        )

        if rejected:
            self.write_log(
                action.strategy_id,
                f"[ORDER REJECTED] {action.symbol} side={side_value} "
                f"qty={lots} response={response}",
            )
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
                if not short_leg and ("sell" in ls or "open" in ls and "sell" in ls):
                    short_leg = osym
                elif not long_leg and ("buy" in ls or "open" in ls and "buy" in ls):
                    long_leg = osym

        short_strike = self._extract_strike(short_leg)
        long_strike = self._extract_strike(long_leg)
        width = action.width
        if width is None and short_strike is not None and long_strike is not None:
            width = abs(float(short_strike) - float(long_strike))

        expiry_date = None
        if action.expiry:
            try:
                expiry_date = datetime.strptime(str(action.expiry), "%Y-%m-%d").date()
            except (TypeError, ValueError):
                expiry_date = None

        entry_credit = None
        entry_debit = None
        ot = (action.order_type or "").lower()
        if action.price is not None:
            if ot == "credit" or (ot == "" and (action.side or "").lower() == "sell"):
                entry_credit = float(action.price)
            else:
                entry_debit = float(action.price)

        with self.Session() as s:
            s.add(Trade(
                strategy_id=action.strategy_id,
                symbol=action.symbol,
                side_type=(side_value or "unknown").lower(),
                short_leg=short_leg,
                long_leg=long_leg,
                short_strike=short_strike,
                long_strike=long_strike,
                width=float(width) if width is not None else None,
                lots=lots,
                entry_credit=entry_credit,
                entry_debit=entry_debit,
                expiry=expiry_date,
                status="OPEN",
                ai_authored=bool(getattr(action, "ai_authored", False)),
                ai_rationale=getattr(action, "ai_rationale", None),
                broker_order_id=broker_order_id,
                tag=getattr(action, "tag", None),
            ))
            s.commit()

        self.write_log(
            action.strategy_id,
            f"[ORDER ACCEPTED] {action.symbol} side={side_value} qty={lots} "
            f"order_id={broker_order_id} status={order_status or 'ok'}",
        )

    # ------------------------------------------------------------------
    # Management-close path
    #
    # When a strategy emits a closing order (CS75 _close_action, CS7
    # close, TT45 hard-21DTE etc.) we MUST NOT insert a fresh OPEN Trade
    # row — the action references an existing OPEN trade via
    # ``strategy_params['trade_id']`` and is intended to flatten it.
    #
    # ``close_trade_from_action`` updates that row in place: status →
    # CLOSED, sets ``closed_at`` / ``close_reason`` / ``close_tag`` /
    # ``exit_price`` and computes realized P&L from the credit-vs-debit
    # delta so the analytics dashboard shows the actual outcome instead
    # of a $0 row stamped ``RECONCILED_BROKER_FLAT``.
    # ------------------------------------------------------------------
    def close_trade_from_action(self, action, response) -> None:
        order = (response or {}).get("order") if isinstance(response, dict) else None
        order_status = ""
        broker_order_id: Optional[str] = None
        if isinstance(order, dict):
            order_status = str(order.get("status", "")).lower()
            broker_order_id = (
                str(order["id"]) if order.get("id") is not None else None
            )

        rejected = (
            (isinstance(response, dict) and "errors" in response)
            or order_status in self._REJECT_STATUSES
        )

        # Re-derive the lot count + side just like record_order_response so
        # _consume_matching_pending matches the right pending row.
        lots = action.quantity or 1
        for leg in (action.legs or []):
            leg_side = (leg.get("side") or "").lower()
            if "sell" in leg_side or "open" in leg_side or "close" in leg_side:
                try:
                    lots = int(leg["quantity"])
                except (KeyError, TypeError, ValueError):
                    pass
                break

        side_value = (action.strategy_params or {}).get("side_type")
        if not side_value or side_value.lower() in {"buy", "sell"}:
            side_value = None
            for leg in (action.legs or []):
                m = _OCC_RE.match(str(leg.get("option_symbol", "") or ""))
                if m:
                    side_value = "put" if m.group(3) == "P" else "call"
                    break

        self._consume_matching_pending(
            strategy_id=action.strategy_id, symbol=action.symbol,
            side=(side_value or action.side or "").lower(), lots=lots,
            terminal_status="REJECTED" if rejected else "SUBMITTED",
        )

        if rejected:
            self.write_log(
                action.strategy_id,
                f"[CLOSE REJECTED] {action.symbol} side={side_value} "
                f"qty={lots} response={response}",
            )
            return

        sp = action.strategy_params or {}
        trade_id = sp.get("trade_id")
        close_reason = sp.get("close_reason") or _close_reason_from_tag(
            getattr(action, "tag", None)) or "MANAGED_CLOSE"
        exit_price = float(action.price) if action.price is not None else None

        with self.Session() as s:
            row: Optional[Trade] = None
            if trade_id is not None:
                row = (s.query(Trade)
                       .filter(Trade.id == int(trade_id),
                               Trade.status == "OPEN")
                       .first())
            if row is None:
                # Fallback: match by leg symbols. Strategies that omit
                # trade_id (or pass a stale one after a row was already
                # closed by the reconciler) still get bookkeeping.
                leg_syms = [
                    str(leg.get("option_symbol") or "")
                    for leg in (action.legs or [])
                    if leg.get("option_symbol")
                ]
                if leg_syms:
                    row = (s.query(Trade)
                           .filter(Trade.status == "OPEN",
                                   Trade.symbol == action.symbol,
                                   Trade.strategy_id == action.strategy_id,
                                   Trade.short_leg.in_(leg_syms))
                           .order_by(Trade.opened_at.desc())
                           .first())

            if row is None:
                # Nothing to close — log and bail. We deliberately do NOT
                # insert a ghost OPEN row (that's the pre-fix bug).
                self.write_log(
                    action.strategy_id,
                    f"[CLOSE ORPHAN] {action.symbol} no matching OPEN trade for "
                    f"trade_id={trade_id} legs={[leg.get('option_symbol') for leg in (action.legs or [])]} "
                    f"order_id={broker_order_id}",
                )
                return

            row.status = "CLOSED"
            row.closed_at = datetime.utcnow()
            row.close_reason = close_reason
            row.close_tag = getattr(action, "tag", None)
            if exit_price is not None:
                row.exit_price = exit_price
            row.pnl = _compute_realized_pnl(
                entry_credit=row.entry_credit,
                entry_debit=row.entry_debit,
                exit_price=exit_price,
                lots=int(row.lots or 0),
            )
            s.commit()

        self.write_log(
            action.strategy_id,
            f"[CLOSE FILLED] {action.symbol} trade_id={trade_id} reason={close_reason} "
            f"exit={exit_price} pnl={float(row.pnl) if row.pnl is not None else None} "
            f"order_id={broker_order_id} status={order_status or 'ok'}",
        )

    def _consume_matching_pending(self, *, strategy_id: str, symbol: str,
                                  side: str, lots: int,
                                  terminal_status: str) -> None:
        """Delete the most-recent matching PendingOrder so capacity isn't
        double-counted with the freshly-written Trade row."""
        with self.Session() as s:
            row = (s.query(PendingOrder)
                   .filter(
                       PendingOrder.strategy_id == strategy_id,
                       PendingOrder.symbol == symbol,
                       PendingOrder.side == side,
                       PendingOrder.status == "PENDING",
                   )
                   .order_by(PendingOrder.submitted_at.desc())
                   .first())
            if row is None:
                return
            row.status = terminal_status
            s.commit()

    @staticmethod
    def _extract_strike(occ_symbol: Optional[str]):
        if not occ_symbol:
            return None
        m = _OCC_RE.match(str(occ_symbol))
        if not m:
            return None
        try:
            return int(m.group(4)) / 1000.0
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # Reconciler — keep the trades table in sync with broker positions.
    # Called every tick (CascadingEngine.sync_positions).
    # ------------------------------------------------------------------
    def upsert_positions(self, positions: List[Dict[str, Any]],
                         active_order_legs: Optional[Any] = None) -> None:
        """Reconcile OPEN trades against broker truth.

        A trade is considered "alive" if any of its option legs is either
        (a) a current filled broker position, or
        (b) a leg of a resting/accepted broker order. Limit orders that
        haven't filled yet do NOT show up in `get_positions`, so passing
        only positions would incorrectly close every just-submitted spread
        before it fills.
        """
        broker_qty: Dict[str, int] = {}
        for p in positions or []:
            sym = str(p.get("symbol", "") or "")
            m = _OCC_RE.match(sym)
            if not m:
                continue                          # skip equities
            try:
                qty = int(round(float(p.get("quantity", 0) or 0)))
            except (TypeError, ValueError):
                qty = 0
            if qty == 0:
                continue
            broker_qty[sym] = broker_qty.get(sym, 0) + abs(qty)

        active_legs: set = set(active_order_legs or [])
        coverage: set = set(broker_qty.keys()) | active_legs

        with self.Session() as s:
            open_trades = s.query(Trade).filter_by(status="OPEN").all()
            closed = 0
            for t in open_trades:
                legs = {leg for leg in (t.short_leg, t.long_leg) if leg}
                if legs & coverage:
                    continue
                t.status = "CLOSED"
                t.closed_at = datetime.utcnow()
                # Preserve a strategy-set reason (rare race: the management
                # close path already stamped this row but the commit
                # interleaved with this reconcile pass). Otherwise mark it
                # as a true orphan so analytics can distinguish.
                if not t.close_reason:
                    t.close_reason = "RECONCILED_BROKER_FLAT"
                closed += 1
            if closed:
                s.commit()

        self.write_log(
            "ENGINE",
            f"reconciled {len(positions or [])} broker pos; "
            f"closed_orphans={closed} positions_legs={len(broker_qty)} "
            f"resting_legs={len(active_legs)}",
        )

    # ------------------------------------------------------------------
    # Schema migrations applied at watcher boot. Idempotent.
    # ------------------------------------------------------------------
    def run_migrations(self) -> None:
        from sqlalchemy import text as sa_text
        stmts = [
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS broker_order_id TEXT",
            "CREATE INDEX IF NOT EXISTS idx_trades_open_order_id "
            "ON trades(broker_order_id) WHERE status = 'OPEN'",
            # Tag + exit-price bookkeeping for realized-PnL computation.
            # See Trade model for column docs.
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS tag TEXT",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS close_tag TEXT",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS exit_price NUMERIC(10,4)",
            # Approval expiry — auto-reject stale approvals spanning weekends / holidays.
            "ALTER TABLE pending_approvals ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ",
        ]
        with self.engine.begin() as conn:
            for sql in stmts:
                conn.execute(sa_text(sql))

    def flag_orphans(self, orphan_symbols) -> None:
        with self.Session() as s:
            for sym in orphan_symbols:
                s.add(BotLog(strategy_id="ENGINE", level="WARN",
                             message=f"orphan position: {sym}"))
            s.commit()

    # ---- strategies registry (must be populated before watchlists) -------
    # The `strategies` table is referenced by FK from `strategy_watchlists`,
    # so the row for a given strategy_id must exist before any symbol can be
    # added to that strategy's watchlist. Both services seed this on startup.
    def ensure_strategies(self, strategies: Dict[str, int]) -> None:
        """Idempotently upsert the canonical strategy registry.

        `strategies` maps strategy_id (e.g. 'CS75') -> priority (e.g. 1).
        Existing rows are left untouched; missing rows are inserted with
        status='ACTIVE'. Safe to call on every boot.
        """
        with self.Session() as s:
            existing = {r.strategy_id for r in s.query(Strategy).all()}
            for sid, priority in strategies.items():
                if sid in existing:
                    continue
                s.add(Strategy(strategy_id=sid, priority=int(priority),
                               status="ACTIVE"))
            s.commit()

    # ---- watchlist CRUD ---------------------------------------------------
    def list_watchlist(self, strategy_id: str) -> List[str]:
        with self.Session() as s:
            from sqlalchemy import text as sa_text
            rows = s.execute(sa_text(
                "SELECT symbol FROM strategy_watchlists WHERE strategy_id = :sid ORDER BY symbol"
            ), {"sid": strategy_id}).fetchall()
            return [r[0] for r in rows]

    def list_watchlist_detailed(self, strategy_id: str) -> Dict[str, Dict[str, Any]]:
        """Return symbols mapped to their metadata (target_lots, etc.).
        Resilient to missing columns during migration.
        """
        out = {}
        with self.Session() as s:
            from sqlalchemy import text as sa_text
            try:
                # Attempt to fetch with target_lots
                rows = s.execute(sa_text(
                    "SELECT symbol, target_lots FROM strategy_watchlists WHERE strategy_id = :sid"
                ), {"sid": strategy_id}).fetchall()
                for r in rows:
                    out[r[0]] = {"target_lots": r[1]}
            except Exception:
                # Fallback: table exists but column might not
                s.rollback()
                rows = s.execute(sa_text(
                    "SELECT symbol FROM strategy_watchlists WHERE strategy_id = :sid"
                ), {"sid": strategy_id}).fetchall()
                for r in rows:
                    out[r[0]] = {"target_lots": None}
        return out

    def list_all_watchlists(self) -> Dict[str, List[str]]:
        with self.Session() as s:
            from sqlalchemy import text as sa_text
            rows = s.execute(sa_text(
                "SELECT strategy_id, symbol FROM strategy_watchlists ORDER BY strategy_id, symbol"
            )).fetchall()
            out: Dict[str, List[str]] = {}
            for sid, sym in rows:
                out.setdefault(sid, []).append(sym)
            return out

    # Pulled from hermes.common so add_to_watchlist is self-sufficient —
    # a watchlist write cannot fail an FK check just because the strategies
    # table is empty.  Single source of truth lives in common.py.
    _DEFAULT_STRATEGY_PRIORITIES = _COMMON_STRATEGY_PRIORITIES

    def add_to_watchlist(self, strategy_id: str, symbol: str) -> bool:
        """Insert a single symbol. Returns True when inserted, False if it already existed.

        Self-heals the strategies table: if the row for `strategy_id` is
        missing the FK from strategy_watchlists.strategy_id would reject the
        insert, so we upsert it inline in the same transaction.
        """
        sym = (symbol or "").strip().upper()
        if not sym:
            raise ValueError("symbol must be non-empty")
        with self.Session() as s:
            # Ensure the parent row exists (FK requirement).
            if not s.query(Strategy).filter_by(strategy_id=strategy_id).first():
                priority = self._DEFAULT_STRATEGY_PRIORITIES.get(strategy_id, 99)
                s.add(Strategy(strategy_id=strategy_id, priority=priority,
                               status="ACTIVE"))
                s.flush()
            exists = (s.query(StrategyWatchlist)
                      .filter_by(strategy_id=strategy_id, symbol=sym).first())
            if exists:
                return False
            s.add(StrategyWatchlist(strategy_id=strategy_id, symbol=sym))
            s.commit()
            return True

    def remove_from_watchlist(self, strategy_id: str, symbol: str) -> bool:
        sym = (symbol or "").strip().upper()
        with self.Session() as s:
            row = (s.query(StrategyWatchlist)
                   .filter_by(strategy_id=strategy_id, symbol=sym).first())
            if not row:
                return False
            s.delete(row)
            s.commit()
            return True

    def set_watchlist(self, strategy_id: str, symbols: List[str]) -> List[str]:
        """Replace the entire watchlist for `strategy_id`. Returns the canonicalised list."""
        clean = sorted({(s or "").strip().upper() for s in symbols if (s or "").strip()})
        with self.Session() as s:
            # Same self-heal as add_to_watchlist — required before we can
            # write any row referencing this strategy_id.
            if not s.query(Strategy).filter_by(strategy_id=strategy_id).first():
                priority = self._DEFAULT_STRATEGY_PRIORITIES.get(strategy_id, 99)
                s.add(Strategy(strategy_id=strategy_id, priority=priority,
                               status="ACTIVE"))
                s.flush()
            s.query(StrategyWatchlist).filter_by(strategy_id=strategy_id).delete()
            for sym in clean:
                s.add(StrategyWatchlist(strategy_id=strategy_id, symbol=sym))
            s.commit()
        return clean

    # ---- reads ------------------------------------------------------------
    def open_trades(self, strategy_id: str) -> List[Dict[str, Any]]:
        with self.Session() as s:
            rows = s.query(Trade).filter_by(strategy_id=strategy_id, status="OPEN").all()
            return [self._trade_dict(r) for r in rows]

    def open_legs(self, strategy_id: str, symbol: str) -> List[Dict[str, Any]]:
        """Return both legs of every OPEN trade for (strategy, symbol).

        Both legs of a vertical share the same side_type (put/call), so
        callers that build {expiry → sides} sets are unaffected by the
        duplicate side; callers that need every leg by option_symbol
        (e.g. broker reconciliation) now get them.
        """
        out: List[Dict[str, Any]] = []
        for t in self.open_trades(strategy_id):
            if t["symbol"] != symbol:
                continue
            expiry_iso = t.get("expiry").isoformat() if t.get("expiry") else None
            side = t.get("side_type")
            for leg_key in ("short_leg", "long_leg"):
                opt = t.get(leg_key)
                if opt:
                    out.append({"option_symbol": opt, "side": side,
                                "expiry": expiry_iso})
        return out

    def count_open_contracts(self, strategy_id: str, symbol: str, side: str,
                              expiry: str) -> int:
        """Sum of OPEN lot count for (strategy, symbol, side, expiry).

        ``expiry`` is required (ISO ``YYYY-MM-DD``). Capacity is always
        scoped to a single option chain — the prior global mode (sum
        across all expiries when expiry was None) was removed because
        it never matched what production strategies actually wanted.
        """
        if not expiry:
            raise ValueError(
                "count_open_contracts requires an expiry (YYYY-MM-DD); the "
                "symbol-wide global mode has been removed."
            )
        try:
            exp_date = datetime.strptime(str(expiry), "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(
                f"count_open_contracts: invalid expiry {expiry!r}; "
                "expected YYYY-MM-DD"
            ) from exc
        with self.Session() as s:
            q = (s.query(Trade).filter_by(strategy_id=strategy_id, symbol=symbol, status="OPEN")
                 .filter(Trade.side_type == side, Trade.expiry == exp_date))
            rows = q.all()
            return sum(int(r.lots or 0) for r in rows)

    def count_pending_orders(self, strategy_id: str, symbol: str, side: str,
                              expiry: str) -> int:
        """Return total lot-count of pending exposure for (strategy, symbol, side, expiry).

        Checks two tables:
        * pending_orders   — orders queued for direct broker submission
        * pending_approvals— orders queued for human C2 approval (approval_mode=True)

        Both must be counted so side_aware_capacity works correctly regardless
        of whether approval_mode is on or off.  Without this, every tick looks
        like capacity is full/zero from open trades but the pending approval
        queue is invisible, causing duplicate entries every tick.

        ``expiry`` is required — counts are always scoped to a single
        option chain. The previous symbol-wide fallback was removed.
        """
        if not expiry:
            raise ValueError(
                "count_pending_orders requires an expiry (YYYY-MM-DD); the "
                "symbol-wide global mode has been removed."
            )
        side_lower = side.lower()
        with self.Session() as s:
            # 1) Directly-submitted pending orders — scoped to this chain
            po_rows = (s.query(PendingOrder)
                       .filter_by(strategy_id=strategy_id, symbol=symbol,
                                  side=side_lower, status="PENDING")
                       .all())
            po_lots = 0
            for r in po_rows:
                payload = r.payload or {}
                if payload.get("expiry") != expiry:
                    continue
                po_lots += int(r.quantity or 0)

            # 2) Approval-queued trades not yet executed — scoped to this chain
            pa_rows = (s.query(PendingApproval)
                       .filter_by(strategy_id=strategy_id, symbol=symbol,
                                  status="PENDING")
                       .all())
            pa_lots = 0
            for r in pa_rows:
                aj = r.action_json or {}
                sp = aj.get("strategy_params") or {}
                # Match side_type (put/call) stored in strategy_params
                if sp.get("side_type", "").lower() != side_lower:
                    continue
                if aj.get("expiry") != expiry:
                    continue
                # Sum lots from the first sell/open leg
                for leg in (aj.get("legs") or []):
                    leg_side = (leg.get("side") or "").lower()
                    if "sell" in leg_side or "open" in leg_side:
                        try:
                            pa_lots += int(leg["quantity"])
                        except (KeyError, TypeError, ValueError):
                            pa_lots += 1
                        break

        return po_lots + pa_lots

    def has_pending_approval(self, strategy_id: str, symbol: str,
                             side_type: Optional[str],
                             expiry: Optional[str]) -> bool:
        """Return True if an identical PENDING approval already exists.

        Matches on (strategy_id, symbol, side_type, expiry) so the engine
        never double-queues the same spread in the same tick or across ticks
        while the operator hasn't acted yet.
        """
        with self.Session() as s:
            rows = (s.query(PendingApproval)
                    .filter_by(strategy_id=strategy_id, symbol=symbol,
                               status="PENDING")
                    .all())
            for r in rows:
                aj = r.action_json or {}
                sp = aj.get("strategy_params") or {}
                if side_type is not None:
                    if sp.get("side_type", "").lower() != (side_type or "").lower():
                        continue
                if expiry is not None:
                    if aj.get("expiry") != expiry:
                        continue
                return True
        return False

    def expire_stale_pending_orders(self, older_than_seconds: int) -> int:
        """Mark PENDING orders older than `older_than_seconds` as EXPIRED.

        Orders that were cancelled externally on the broker (e.g. Tradier GTC
        cancel, day-order expiry, manual intervention) never get a fill callback,
        so their rows would sit as PENDING forever and artificially reduce
        side_aware_capacity.  This method is called at the start of each tick
        to clean up those ghosts.

        Returns the number of rows marked EXPIRED.
        """
        cutoff = datetime.utcnow() - timedelta(seconds=older_than_seconds)
        expired = 0
        with self.Session() as s:
            stale = (
                s.query(PendingOrder)
                .filter(
                    PendingOrder.status == "PENDING",
                    PendingOrder.submitted_at < cutoff,
                )
                .all()
            )
            for row in stale:
                row.status = "EXPIRED"
                expired += 1
            if expired:
                s.commit()
        return expired

    def expire_stale_approvals(self) -> int:
        """Auto-reject PENDING approvals whose expires_at has passed.

        Called each tick so trades queued before a weekend cannot execute
        Monday on stale quotes.  Returns the number of rows auto-rejected.
        """
        now = datetime.utcnow()
        expired = 0
        with self.Session() as s:
            stale = (
                s.query(PendingApproval)
                .filter(
                    PendingApproval.status == "PENDING",
                    PendingApproval.expires_at.isnot(None),
                    PendingApproval.expires_at < now,
                )
                .all()
            )
            for row in stale:
                row.status = "EXPIRED"
                row.decided_at = now
                row.notes = (row.notes or "") + " [auto-expired: stale approval past deadline]"
                expired += 1
            if expired:
                s.commit()
        return expired

    # ---- approval queue --------------------------------------------------
    def queue_for_approval(self, action_json: Dict[str, Any],
                           action_type: str = "entry",
                           expires_hours: float = 24.0) -> int:
        """Write a proposed TradeAction to the approval queue.  Returns the new row id.

        ``expires_hours`` sets how long the approval stays PENDING before the
        tick loop auto-rejects it (default 24 h).  Pass 0 to disable expiry.
        """
        expires_at = (datetime.utcnow() + timedelta(hours=expires_hours)
                      if expires_hours > 0 else None)
        with self.Session() as s:
            row = PendingApproval(
                strategy_id=action_json.get("strategy_id", "UNKNOWN"),
                symbol=action_json.get("symbol", ""),
                action_type=action_type,
                action_json=action_json,
                status="PENDING",
                expires_at=expires_at,
            )
            s.add(row)
            s.flush()
            row_id = row.id
            s.commit()
            return row_id

    def list_approvals(self, status: Optional[str] = None,
                       limit: int = 100) -> List[Dict[str, Any]]:
        with self.Session() as s:
            q = s.query(PendingApproval).order_by(PendingApproval.created_at.desc())
            if status:
                q = q.filter(PendingApproval.status == status.upper())
            rows = q.limit(limit).all()
            return [
                {
                    "id": r.id,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                    "strategy_id": r.strategy_id,
                    "symbol": r.symbol,
                    "action_type": r.action_type,
                    "action_json": r.action_json,
                    "status": r.status,
                    "notes": r.notes,
                    "decided_at": r.decided_at.isoformat() if r.decided_at else None,
                    "executed_at": r.executed_at.isoformat() if r.executed_at else None,
                    "expires_at": r.expires_at.isoformat() if r.expires_at else None,
                }
                for r in rows
            ]

    def decide_approval(self, approval_id: int, decision: str,
                        notes: Optional[str] = None) -> bool:
        """Set status to APPROVED or REJECTED.  Returns False if row not found."""
        decision = decision.upper()
        if decision not in ("APPROVED", "REJECTED"):
            raise ValueError(f"decision must be APPROVED or REJECTED, got {decision!r}")
        with self.Session() as s:
            row = s.query(PendingApproval).filter_by(id=approval_id).first()
            if row is None or row.status != "PENDING":
                return False
            row.status = decision
            row.decided_at = datetime.utcnow()
            if notes:
                row.notes = notes
            s.commit()
            return True

    def fetch_approved_actions(self) -> List[Dict[str, Any]]:
        """Return APPROVED rows ready for execution."""
        with self.Session() as s:
            rows = (s.query(PendingApproval)
                    .filter_by(status="APPROVED")
                    .order_by(PendingApproval.decided_at)
                    .all())
            return [
                {"id": r.id, "action_json": r.action_json,
                 "strategy_id": r.strategy_id, "symbol": r.symbol}
                for r in rows
            ]

    def mark_approval_executed(self, approval_id: int, success: bool = True,
                               notes: Optional[str] = None) -> None:
        with self.Session() as s:
            row = s.query(PendingApproval).filter_by(id=approval_id).first()
            if row:
                row.status = "EXECUTED" if success else "FAILED"
                row.executed_at = datetime.utcnow()
                if notes:
                    row.notes = (row.notes or "") + f"\n{notes}"
                s.commit()

    def tracked_option_symbols(self) -> set:
        with self.Session() as s:
            symbols = set()
            for r in s.query(Trade).filter_by(status="OPEN").all():
                if r.short_leg:
                    symbols.add(r.short_leg)
                if r.long_leg:
                    symbols.add(r.long_leg)
            return symbols

    def equity_position(self, symbol: str) -> int:
        with self.Session() as s:
            row = (s.query(Trade).filter_by(symbol=symbol, side_type="equity", status="OPEN")
                   .order_by(Trade.opened_at.desc()).first())
            return int(row.lots) if row else 0

    # ---- runtime settings (shared agent/watcher state) -------------------
    def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        with self.Session() as s:
            row = s.query(SystemSetting).filter_by(key=key).first()
            return row.value if row else default

    def set_setting(self, key: str, value: str) -> None:
        with self.Session() as s:
            row = s.query(SystemSetting).filter_by(key=key).first()
            if row is None:
                s.add(SystemSetting(key=key, value=str(value)))
            else:
                row.value = str(value)
                row.updated_at = datetime.utcnow()
            s.commit()

    def setting_updated_at(self, key: str) -> Optional[datetime]:
        with self.Session() as s:
            row = s.query(SystemSetting).filter_by(key=key).first()
            return row.updated_at if row else None

    def latest_log_ts(self) -> Optional[datetime]:
        """Most recent bot_logs timestamp — used as the agent's liveness signal."""
        with self.Session() as s:
            row = s.query(BotLog).order_by(BotLog.ts.desc()).first()
            return row.ts if row else None

    def recent_logs(self, limit: int = 200) -> str:
        # Render every line in US/Eastern so the Agent Activity Log on
        # the C2 dashboard reads in market time, matching the rest of
        # the UI (market session label, next_open, etc.). Postgres
        # TIMESTAMPTZ values come back as tz-aware UTC datetimes; if a
        # naive value ever sneaks in (e.g. legacy row, mis-set session
        # tz), assume UTC defensively rather than localizing wrong.
        from hermes.market_hours import ET
        from datetime import timezone as _tz
        with self.Session() as s:
            rows = s.query(BotLog).order_by(BotLog.ts.desc()).limit(limit).all()
            out = []
            for r in reversed(rows):
                ts = r.ts
                if ts is None:
                    out.append(f"--:--:-- ET [{r.strategy_id}] {r.message}")
                    continue
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=_tz.utc)
                local = ts.astimezone(ET)
                out.append(f"{local:%H:%M:%S} ET [{r.strategy_id}] {r.message}")
            return "\n".join(out)

    def daily_bars(self, symbol: str, lookback_days: int = 400) -> Optional[pd.DataFrame]:
        sql = """
            SELECT ts, open, high, low, close, volume, vwap_close
            FROM bars_daily
            WHERE symbol = %s AND ts >= now() - (%s || ' days')::interval
            ORDER BY ts
        """
        df = pd.read_sql(sql, self.engine, params=(symbol, lookback_days), parse_dates=["ts"])
        if df.empty:
            return None
        df = df.set_index("ts")
        return df

    def intraday_bars(self, symbol: str, lookback_days: int = 10) -> pd.DataFrame:
        sql = """
            SELECT ts, open, high, low, close, volume
            FROM bars_intraday
            WHERE symbol = %s AND ts >= now() - (%s || ' days')::interval
            ORDER BY ts
        """
        df = pd.read_sql(sql, self.engine, params=(symbol, lookback_days), parse_dates=["ts"])
        return df.set_index("ts") if not df.empty else df

    def save_daily_bars(self, symbol: str, df: pd.DataFrame) -> None:
        """Upsert daily bars for a symbol from a DataFrame."""
        if df.empty:
            return
        
        # Reset index if ts is the index
        if df.index.name == 'ts' or 'ts' not in df.columns:
            reset_df = df.reset_index()
            if 'ts' not in reset_df.columns and 'index' in reset_df.columns:
                reset_df = reset_df.rename(columns={'index': 'ts'})
        else:
            reset_df = df.copy()
            
        from sqlalchemy.dialects.postgresql import insert
        
        data = []
        for _, row in reset_df.iterrows():
            data.append({
                'ts': pd.to_datetime(row['ts']),
                'symbol': symbol,
                'open': row.get('open'),
                'high': row.get('high'),
                'low': row.get('low'),
                'close': row.get('close'),
                'volume': row.get('volume'),
                'vwap_close': row.get('vwap_close')
            })

        if not data:
            return

        stmt = insert(DailyBar).values(data)
        stmt = stmt.on_conflict_do_update(
            index_elements=['ts', 'symbol'],
            set_={
                'open': stmt.excluded.open,
                'high': stmt.excluded.high,
                'low': stmt.excluded.low,
                'close': stmt.excluded.close,
                'volume': stmt.excluded.volume,
                'vwap_close': stmt.excluded.vwap_close
            }
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)

    def save_intraday_bars(self, symbol: str, df: pd.DataFrame) -> None:
        """Upsert intraday bars for a symbol from a DataFrame."""
        if df.empty:
            return
            
        # Reset index if ts is the index
        if df.index.name == 'ts' or 'ts' not in df.columns:
            reset_df = df.reset_index()
            if 'ts' not in reset_df.columns and 'index' in reset_df.columns:
                reset_df = reset_df.rename(columns={'index': 'ts'})
        else:
            reset_df = df.copy()

        from sqlalchemy.dialects.postgresql import insert
        
        data = []
        for _, row in reset_df.iterrows():
            data.append({
                'ts': pd.to_datetime(row['ts']),
                'symbol': symbol,
                'open': row.get('open'),
                'high': row.get('high'),
                'low': row.get('low'),
                'close': row.get('close'),
                'volume': row.get('volume')
            })

        if not data:
            return

        stmt = insert(IntradayBar).values(data)
        stmt = stmt.on_conflict_do_update(
            index_elements=['ts', 'symbol'],
            set_={
                'open': stmt.excluded.open,
                'high': stmt.excluded.high,
                'low': stmt.excluded.low,
                'close': stmt.excluded.close,
                'volume': stmt.excluded.volume
            }
        )
        with self.engine.begin() as conn:
            conn.execute(stmt)

    def last_price(self, symbol: str) -> Optional[float]:
        with self.Session() as s:
            row = s.query(DailyBar).filter_by(symbol=symbol).order_by(DailyBar.ts.desc()).first()
            return float(row.close) if row and row.close is not None else None

    def pnl_daily(self, days: int = 60) -> List[Dict[str, Any]]:
        sql = """
          SELECT day::date, strategy_id, symbol, COALESCE(realized_pnl,0) AS realized_pnl,
                 COALESCE(closed_trades,0) AS closed_trades
          FROM pnl_daily
          WHERE day >= now() - (%s || ' days')::interval
          ORDER BY day
        """
        with self.engine.connect() as c:
            return [dict(r._mapping) for r in c.exec_driver_sql(sql, (days,))]

    def get_price_on_date(self, symbol: str, dt: date) -> Optional[float]:
        """Fetch close price of the symbol on or before the specified date."""
        if not dt:
            return None
        from datetime import datetime, time, date
        if isinstance(dt, datetime):
            dt_end = dt
        elif isinstance(dt, date):
            dt_end = datetime.combine(dt, time.max)
        else:
            dt_end = dt
        with self.Session() as s:
            row = (
                s.query(DailyBar)
                .filter(DailyBar.symbol == symbol, DailyBar.ts <= dt_end)
                .order_by(DailyBar.ts.desc())
                .first()
            )
            return float(row.close) if row and row.close is not None else None

    def get_strategy_performance_metrics(self, days: int = 30) -> Dict[str, Any]:
        """Calculate recent trading performance (PASS/FAIL/NEUTRAL) for each strategy.
        
        Evaluates trades closed within the rolling `days` window against the
        thresholds defined in soul.md.
        """
        cutoff = datetime.utcnow() - timedelta(days=days)
        
        with self.Session() as s:
            closed_trades = (
                s.query(Trade)
                .filter(Trade.status == "CLOSED", Trade.closed_at >= cutoff)
                .all()
            )
            
        metrics = {
            "CS7": {"closed_trades": 0, "passed": 0, "failed": 0, "total_pnl": 0.0, "details": []},
            "CS75": {"closed_trades": 0, "passed": 0, "failed": 0, "total_pnl": 0.0, "details": []},
            "TT45": {"closed_trades": 0, "passed": 0, "failed": 0, "total_pnl": 0.0, "details": []},
            "WHEEL": {"closed_trades": 0, "passed": 0, "failed": 0, "total_pnl": 0.0, "details": []}
        }
        
        # 1. Process option spreads: CS7, CS75, TT45
        spread_trades = [t for t in closed_trades if t.strategy_id in ("CS7", "CS75", "TT45")]
        for t in spread_trades:
            strat = t.strategy_id
            
            pnl_val = float(t.pnl) if t.pnl is not None else None
            if pnl_val is None:
                pnl_val = _compute_realized_pnl(
                    entry_credit=t.entry_credit,
                    entry_debit=t.entry_debit,
                    exit_price=t.exit_price or 0.0,
                    lots=int(t.lots or 0)
                )
            if pnl_val is None:
                continue
                
            metrics[strat]["total_pnl"] += pnl_val
            metrics[strat]["closed_trades"] += 1
            
            width_val = float(t.width) if t.width is not None else None
            if width_val is None and t.short_strike is not None and t.long_strike is not None:
                width_val = abs(float(t.short_strike) - float(t.long_strike))
                
            entry_credit_val = float(t.entry_credit) if t.entry_credit is not None else 0.0
            entry_debit_val = float(t.entry_debit) if t.entry_debit is not None else 0.0
            lots_val = int(t.lots or 1)
            
            if entry_credit_val > 0 and width_val is not None:
                risk_capital = (width_val - entry_credit_val) * lots_val * 100.0
            elif entry_debit_val > 0:
                risk_capital = entry_debit_val * lots_val * 100.0
            elif width_val is not None:
                risk_capital = width_val * lots_val * 100.0
            else:
                risk_capital = 1.0
                
            if risk_capital <= 0:
                risk_capital = width_val * lots_val * 100.0 if width_val else 1.0
                
            return_pct = pnl_val / risk_capital
            
            outcome = "NEUTRAL"
            if strat == "CS7":
                if return_pct < 0.05:
                    outcome = "FAIL"
                    metrics[strat]["failed"] += 1
                elif return_pct >= 0.10:
                    outcome = "PASS"
                    metrics[strat]["passed"] += 1
            elif strat == "CS75":
                if return_pct <= 0.07:
                    outcome = "FAIL"
                    metrics[strat]["failed"] += 1
                elif return_pct >= 0.22:
                    outcome = "PASS"
                    metrics[strat]["passed"] += 1
            elif strat == "TT45":
                if return_pct <= 0.03:
                    outcome = "FAIL"
                    metrics[strat]["failed"] += 1
                elif return_pct >= 0.05:
                    outcome = "PASS"
                    metrics[strat]["passed"] += 1
                    
            metrics[strat]["details"].append({
                "symbol": t.symbol,
                "closed_at": t.closed_at.isoformat() if t.closed_at else None,
                "pnl": pnl_val,
                "risk_capital": risk_capital,
                "return_pct": return_pct,
                "outcome": outcome
            })

        # 2. Process WHEEL turns by symbol
        wheel_closed = [t for t in closed_trades if t.strategy_id == "WHEEL"]
        if wheel_closed:
            from collections import defaultdict
            symbol_trades = defaultdict(list)
            for t in wheel_closed:
                symbol_trades[t.symbol].append(t)
                
            for symbol, trades in symbol_trades.items():
                option_pnl_sum = 0.0
                net_shares = 0
                stock_cash_flow = 0.0
                current_spot = self.last_price(symbol)
                
                for t in trades:
                    pnl_val = float(t.pnl) if t.pnl is not None else None
                    if pnl_val is None:
                        pnl_val = _compute_realized_pnl(
                            entry_credit=t.entry_credit,
                            entry_debit=t.entry_debit,
                            exit_price=t.exit_price or 0.0,
                            lots=int(t.lots or 0)
                        )
                    if pnl_val is None and t.entry_credit is not None:
                        pnl_val = float(t.entry_credit) * int(t.lots or 1) * 100.0
                    if pnl_val is not None:
                        option_pnl_sum += pnl_val
                        
                    if t.side_type == "put" and (t.close_reason == "RECONCILED_BROKER_FLAT" or (t.closed_at and t.expiry and t.closed_at.date() >= t.expiry)):
                        expiry_price = self.get_price_on_date(t.symbol, t.expiry)
                        if expiry_price is not None and expiry_price < float(t.short_strike or 0.0):
                            shares_bought = int(t.lots or 1) * 100
                            cost = float(t.short_strike) * shares_bought
                            net_shares += shares_bought
                            stock_cash_flow -= cost
                            
                    elif t.side_type == "call" and (t.close_reason == "RECONCILED_BROKER_FLAT" or (t.closed_at and t.expiry and t.closed_at.date() >= t.expiry)):
                        expiry_price = self.get_price_on_date(t.symbol, t.expiry)
                        if expiry_price is not None and expiry_price > float(t.short_strike or 0.0):
                            shares_sold = int(t.lots or 1) * 100
                            proceeds = float(t.short_strike) * shares_sold
                            net_shares -= shares_sold
                            stock_cash_flow += proceeds
                            
                if net_shares > 0 and current_spot is not None:
                    stock_value = current_spot * net_shares
                    total_turn_pnl = option_pnl_sum + stock_cash_flow + stock_value
                else:
                    total_turn_pnl = option_pnl_sum + stock_cash_flow
                    
                outcome = "PASS" if total_turn_pnl > 0.0 else ("FAIL" if total_turn_pnl < 0.0 else "NEUTRAL")
                
                if outcome == "PASS":
                    metrics["WHEEL"]["passed"] += 1
                elif outcome == "FAIL":
                    metrics["WHEEL"]["failed"] += 1
                    
                metrics["WHEEL"]["closed_trades"] += len(trades)
                metrics["WHEEL"]["total_pnl"] += total_turn_pnl
                metrics["WHEEL"]["details"].append({
                    "symbol": symbol,
                    "option_pnl": option_pnl_sum,
                    "stock_cash_flow": stock_cash_flow,
                    "net_shares": net_shares,
                    "current_spot": current_spot,
                    "total_pnl": total_turn_pnl,
                    "outcome": outcome
                })

        for strat in ("CS7", "CS75", "TT45", "WHEEL"):
            m = metrics[strat]
            if m["closed_trades"] == 0 and strat != "WHEEL":
                m["status"] = "NEUTRAL"
            elif strat == "WHEEL" and len(m["details"]) == 0:
                m["status"] = "NEUTRAL"
            else:
                if m["failed"] > m["passed"]:
                    m["status"] = "FAIL"
                elif m["passed"] > m["failed"]:
                    m["status"] = "PASS"
                else:
                    m["status"] = "NEUTRAL"
                    
        return metrics


    @staticmethod
    def _trade_dict(r: Trade) -> Dict[str, Any]:
        return {
            "id": r.id, "strategy_id": r.strategy_id, "symbol": r.symbol,
            "side_type": r.side_type, "short_leg": r.short_leg, "long_leg": r.long_leg,
            "short_strike": float(r.short_strike) if r.short_strike else None,
            "long_strike": float(r.long_strike) if r.long_strike else None,
            "width": float(r.width) if r.width else None,
            "lots": int(r.lots), "entry_credit": float(r.entry_credit or 0),
            "expiry": r.expiry, "status": r.status,
        }
