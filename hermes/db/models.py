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
from sqlalchemy.orm import DeclarativeBase, sessionmaker, reconstructor
from transitions import Machine

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
    status = Column(String, nullable=False, default="PROPOSED")
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

    STATES = ["PROPOSED", "PENDING_BROKER", "PARTIAL_FILL", "OPEN", "CLOSING", "CLOSED"]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._init_fsm()

    @reconstructor
    def _init_fsm(self):
        # Prevent recreating machine if called multiple times
        if hasattr(self, 'machine'):
            return
            
        # The FSM will bind directly to `self.status`
        initial_state = getattr(self, 'status', None) or "PROPOSED"
        self.machine = Machine(
            model=self,
            states=self.STATES,
            initial=initial_state,
            model_attribute='status',
            send_event=True,
            ignore_invalid_triggers=False
        )
        
        # Valid State Transitions
        self.machine.add_transition('submit_to_broker', 'PROPOSED', 'PENDING_BROKER')
        self.machine.add_transition('broker_reject', 'PENDING_BROKER', 'CLOSED')
        self.machine.add_transition('partial_fill', 'PENDING_BROKER', 'PARTIAL_FILL')
        self.machine.add_transition('fill', ['PENDING_BROKER', 'PARTIAL_FILL'], 'OPEN')
        self.machine.add_transition('begin_close', 'OPEN', 'CLOSING')
        self.machine.add_transition('finish_close', 'CLOSING', 'CLOSED')
        self.machine.add_transition('force_close', '*', 'CLOSED')


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
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import select, delete

def sync_to_async_dsn(dsn: str) -> str:
    if dsn.startswith("sqlite:///"):
        return dsn.replace("sqlite:///", "sqlite+aiosqlite:///", 1)
    if dsn.startswith("sqlite://"):
        return dsn.replace("sqlite://", "sqlite+aiosqlite://", 1)
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+psycopg://", 1)
    return dsn


class HermesDB:
    """Thin repo layer; matches the surface the engine + UI consume."""

    def __init__(self, dsn: str):
        # Adapt schema dynamically for SQLite compatibility
        if "sqlite" in dsn:
            from sqlalchemy import JSON
            from sqlalchemy.dialects.postgresql import JSONB
            for table in Base.metadata.tables.values():
                composite_pk = len(table.primary_key.columns) > 1
                if composite_pk:
                    for col in table.primary_key.columns:
                        if col.autoincrement:
                            col.autoincrement = False
                for col in table.columns:
                    if isinstance(col.type, JSONB):
                        col.type = JSON()

        self.engine = create_engine(dsn, pool_pre_ping=True, future=True)
        self.Session = sessionmaker(self.engine, expire_on_commit=False, future=True)

        async_dsn = sync_to_async_dsn(dsn)
        self.async_engine = create_async_engine(async_dsn, pool_pre_ping=True, future=True)
        self.AsyncSession = async_sessionmaker(self.async_engine, expire_on_commit=False, class_=AsyncSession, future=True)

        from hermes.db.timeseries import TimeSeriesEngine
        self.ts_engine = TimeSeriesEngine(self)

        try:
            Base.metadata.create_all(self.engine, checkfirst=True)
        except Exception:                                       # noqa: BLE001
            # Don't crash on import — the next real query surfaces the cause.
            pass
        self.engine.dispose()

    async def init_schema(self, schema_sql_path: str) -> None:
        with open(schema_sql_path, "r", encoding="utf-8") as fh:
            sql = fh.read()
        async with self.async_engine.begin() as conn:
            for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
                await conn.exec_driver_sql(stmt + ";")

    # ---- writes -----------------------------------------------------------
    async def write_log(self, strategy_id: str, message: str, level: str = "INFO") -> None:
        async with self.AsyncSession() as s:
            s.add(BotLog(strategy_id=strategy_id, level=level, message=message))
            await s.commit()

    async def write_ai_decision(self, strategy_id: str, symbol: str,
                          autonomy: str, decision: Dict[str, Any]) -> None:
        async with self.AsyncSession() as s:
            s.add(AIDecision(strategy_id=strategy_id, symbol=symbol or "*",
                             autonomy=autonomy, decision=decision))
            await s.commit()

    async def recent_ai_decisions(self, strategy_id: Optional[str] = None,
                            symbol: Optional[str] = None,
                            limit: int = 20) -> List[Dict[str, Any]]:
        """Return recent ai_decisions rows, newest-first.

        Optionally filter by strategy_id and/or symbol.
        Returns a list of plain dicts ready for JSON serialisation.
        """
        async with self.AsyncSession() as s:
            q = select(AIDecision).order_by(AIDecision.ts.desc())
            if strategy_id is not None:
                q = q.filter(AIDecision.strategy_id == strategy_id)
            if symbol is not None:
                q = q.filter(AIDecision.symbol == symbol.upper())
            result = await s.execute(q.limit(limit))
            rows = result.scalars().all()
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

    async def write_prediction(self, symbol: str, ret: float, price: float, spot: float = 0.0) -> None:
        async with self.AsyncSession() as s:
            s.add(Prediction(symbol=symbol, predicted_return=ret, predicted_price=price, spot=spot))
            await s.commit()

    async def latest_prediction(self, symbol: str) -> Optional[Dict[str, Any]]:
        async with self.AsyncSession() as s:
            q = select(Prediction).filter_by(symbol=symbol).order_by(Prediction.ts.desc()).limit(1)
            result = await s.execute(q)
            row = result.scalars().first()
            if row:
                return {
                    "predicted_return": float(row.predicted_return or 0),
                    "predicted_price": float(row.predicted_price or 0)
                }
            return None

    async def latest_predictions_batch(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        """Fetch the latest prediction for multiple symbols in one query."""
        if not symbols:
            return {}
        from sqlalchemy import bindparam, text as sa_text
        # Postgres-specific DISTINCT ON for efficient latest-per-group
        sql = sa_text("""
            SELECT DISTINCT ON (symbol)
                symbol, predicted_return, predicted_price
            FROM predictions
            WHERE symbol IN :symbols
            ORDER BY symbol, ts DESC
        """).bindparams(bindparam("symbols", expanding=True))
        results = {}
        async with self.AsyncSession() as s:
            result = await s.execute(sql, {"symbols": list(symbols)})
            rows = result.fetchall()
            for r in rows:
                results[r.symbol] = {
                    "predicted_return": float(r.predicted_return or 0),
                    "predicted_price": float(r.predicted_price or 0)
                }
        return results

    async def record_pending_order(self, action) -> None:
        lots = action.quantity  # fallback
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
            if side_value is None:
                side_value = action.side

        async with self.AsyncSession() as s:
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
            await s.commit()

    _REJECT_STATUSES = {"rejected", "error", "expired", "canceled", "cancelled"}

    async def record_order_response(self, action, response) -> None:
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

        await self._consume_matching_pending(
            strategy_id=action.strategy_id, symbol=action.symbol,
            side=(side_value or action.side or "").lower(), lots=lots,
            terminal_status="REJECTED" if rejected else "SUBMITTED",
        )

        if rejected:
            await self.write_log(
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

        async with self.AsyncSession() as s:
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
            await s.commit()

        await self.write_log(
            action.strategy_id,
            f"[ORDER ACCEPTED] {action.symbol} side={side_value} qty={lots} "
            f"order_id={broker_order_id} status={order_status or 'ok'}",
        )

    async def close_trade_from_action(self, action, response) -> None:
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

        await self._consume_matching_pending(
            strategy_id=action.strategy_id, symbol=action.symbol,
            side=(side_value or action.side or "").lower(), lots=lots,
            terminal_status="REJECTED" if rejected else "SUBMITTED",
        )

        if rejected:
            await self.write_log(
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

        async with self.AsyncSession() as s:
            row: Optional[Trade] = None
            if trade_id is not None:
                q = select(Trade).filter(Trade.id == int(trade_id), Trade.status == "OPEN").limit(1)
                result = await s.execute(q)
                row = result.scalars().first()
            if row is None:
                leg_syms = [
                    str(leg.get("option_symbol") or "")
                    for leg in (action.legs or [])
                    if leg.get("option_symbol")
                ]
                if leg_syms:
                    q = (select(Trade)
                           .filter(Trade.status == "OPEN",
                                   Trade.symbol == action.symbol,
                                   Trade.strategy_id == action.strategy_id,
                                   Trade.short_leg.in_(leg_syms))
                           .order_by(Trade.opened_at.desc())
                           .limit(1))
                    result = await s.execute(q)
                    row = result.scalars().first()

            if row is None:
                await self.write_log(
                    action.strategy_id,
                    f"[CLOSE ORPHAN] {action.symbol} no matching OPEN trade for "
                    f"trade_id={trade_id} legs={[leg.get('option_symbol') for leg in (action.legs or [])]} "
                    f"order_id={broker_order_id}",
                )
                return

            row.force_close()
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
            await s.commit()

        await self.write_log(
            action.strategy_id,
            f"[CLOSE FILLED] {action.symbol} trade_id={trade_id} reason={close_reason} "
            f"exit={exit_price} pnl={float(row.pnl) if row.pnl is not None else None} "
            f"order_id={broker_order_id} status={order_status or 'ok'}",
        )

    async def _consume_matching_pending(self, *, strategy_id: str, symbol: str,
                                  side: str, lots: int,
                                  terminal_status: str) -> None:
        """Delete the most-recent matching PendingOrder so capacity isn't
        double-counted with the freshly-written Trade row."""
        async with self.AsyncSession() as s:
            q = (select(PendingOrder)
                   .filter(
                       PendingOrder.strategy_id == strategy_id,
                       PendingOrder.symbol == symbol,
                       PendingOrder.side == side,
                       PendingOrder.status == "PENDING",
                   )
                   .order_by(PendingOrder.submitted_at.desc())
                   .limit(1))
            result = await s.execute(q)
            row = result.scalars().first()
            if row is None:
                return
            row.status = terminal_status
            await s.commit()

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
    async def upsert_positions(self, positions: List[Dict[str, Any]],
                          active_order_legs: Optional[Any] = None) -> None:
        """Reconcile OPEN trades against broker truth."""
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

        async with self.AsyncSession() as s:
            result = await s.execute(select(Trade).filter_by(status="OPEN"))
            open_trades = result.scalars().all()
            closed = 0
            for t in open_trades:
                legs = {leg for leg in (t.short_leg, t.long_leg) if leg}
                if legs & coverage:
                    continue
                t.force_close()
                t.closed_at = datetime.utcnow()
                if not t.close_reason:
                    t.close_reason = "RECONCILED_BROKER_FLAT"
                closed += 1
            if closed:
                await s.commit()

        await self.write_log(
            "ENGINE",
            f"reconciled {len(positions or [])} broker pos; "
            f"closed_orphans={closed} positions_legs={len(broker_qty)} "
            f"resting_legs={len(active_legs)}",
        )

    # ------------------------------------------------------------------
    # Schema migrations applied at watcher boot. Idempotent.
    # ------------------------------------------------------------------
    async def run_migrations(self) -> None:
        from sqlalchemy import text as sa_text
        stmts = [
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS broker_order_id TEXT",
            "CREATE INDEX IF NOT EXISTS idx_trades_open_order_id "
            "ON trades(broker_order_id) WHERE status = 'OPEN'",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS tag TEXT",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS close_tag TEXT",
            "ALTER TABLE trades ADD COLUMN IF NOT EXISTS exit_price NUMERIC(10,4)",
            "ALTER TABLE pending_approvals ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ",
        ]
        async with self.async_engine.begin() as conn:
            for sql in stmts:
                await conn.execute(sa_text(sql))

    async def flag_orphans(self, orphan_symbols) -> None:
        async with self.AsyncSession() as s:
            for sym in orphan_symbols:
                s.add(BotLog(strategy_id="ENGINE", level="WARN",
                             message=f"orphan position: {sym}"))
            await s.commit()

    # ---- strategies registry (must be populated before watchlists) -------
    async def ensure_strategies(self, strategies: Dict[str, int]) -> None:
        async with self.AsyncSession() as s:
            result = await s.execute(select(Strategy))
            existing = {r.strategy_id for r in result.scalars().all()}
            for sid, priority in strategies.items():
                if sid in existing:
                    continue
                s.add(Strategy(strategy_id=sid, priority=int(priority),
                               status="ACTIVE"))
            await s.commit()

    # ---- watchlist CRUD ---------------------------------------------------
    async def list_watchlist(self, strategy_id: str) -> List[str]:
        from sqlalchemy import text as sa_text
        async with self.AsyncSession() as s:
            result = await s.execute(sa_text(
                "SELECT symbol FROM strategy_watchlists WHERE strategy_id = :sid ORDER BY symbol"
            ), {"sid": strategy_id})
            rows = result.fetchall()
            return [r[0] for r in rows]

    async def list_watchlist_detailed(self, strategy_id: str) -> Dict[str, Dict[str, Any]]:
        from sqlalchemy import text as sa_text
        out = {}
        async with self.AsyncSession() as s:
            try:
                result = await s.execute(sa_text(
                    "SELECT symbol, target_lots FROM strategy_watchlists WHERE strategy_id = :sid"
                ), {"sid": strategy_id})
                rows = result.fetchall()
                for r in rows:
                    out[r[0]] = {"target_lots": r[1]}
            except Exception:
                await s.rollback()
                result = await s.execute(sa_text(
                    "SELECT symbol FROM strategy_watchlists WHERE strategy_id = :sid"
                ), {"sid": strategy_id})
                rows = result.fetchall()
                for r in rows:
                    out[r[0]] = {"target_lots": None}
        return out

    async def list_all_watchlists(self) -> Dict[str, List[str]]:
        from sqlalchemy import text as sa_text
        async with self.AsyncSession() as s:
            result = await s.execute(sa_text(
                "SELECT strategy_id, symbol FROM strategy_watchlists ORDER BY strategy_id, symbol"
            ))
            rows = result.fetchall()
            out: Dict[str, List[str]] = {}
            for sid, sym in rows:
                out.setdefault(sid, []).append(sym)
            return out

    _DEFAULT_STRATEGY_PRIORITIES = _COMMON_STRATEGY_PRIORITIES

    async def add_to_watchlist(self, strategy_id: str, symbol: str) -> bool:
        sym = (symbol or "").strip().upper()
        if not sym:
            raise ValueError("symbol must be non-empty")
        async with self.AsyncSession() as s:
            result = await s.execute(select(Strategy).filter_by(strategy_id=strategy_id).limit(1))
            if not result.scalars().first():
                priority = self._DEFAULT_STRATEGY_PRIORITIES.get(strategy_id, 99)
                s.add(Strategy(strategy_id=strategy_id, priority=priority,
                               status="ACTIVE"))
                await s.flush()
            result = await s.execute(select(StrategyWatchlist).filter_by(strategy_id=strategy_id, symbol=sym).limit(1))
            exists = result.scalars().first()
            if exists:
                return False
            s.add(StrategyWatchlist(strategy_id=strategy_id, symbol=sym))
            await s.commit()
            return True

    async def remove_from_watchlist(self, strategy_id: str, symbol: str) -> bool:
        sym = (symbol or "").strip().upper()
        async with self.AsyncSession() as s:
            result = await s.execute(select(StrategyWatchlist).filter_by(strategy_id=strategy_id, symbol=sym).limit(1))
            row = result.scalars().first()
            if not row:
                return False
            await s.delete(row)
            await s.commit()
            return True

    async def set_watchlist(self, strategy_id: str, symbols: List[str]) -> List[str]:
        clean = sorted({(s or "").strip().upper() for s in symbols if (s or "").strip()})
        async with self.AsyncSession() as s:
            result = await s.execute(select(Strategy).filter_by(strategy_id=strategy_id).limit(1))
            if not result.scalars().first():
                priority = self._DEFAULT_STRATEGY_PRIORITIES.get(strategy_id, 99)
                s.add(Strategy(strategy_id=strategy_id, priority=priority,
                               status="ACTIVE"))
                await s.flush()
            await s.execute(delete(StrategyWatchlist).filter_by(strategy_id=strategy_id))
            for sym in clean:
                s.add(StrategyWatchlist(strategy_id=strategy_id, symbol=sym))
            await s.commit()
        return clean

    # ---- reads ------------------------------------------------------------
    async def open_trades(self, strategy_id: str) -> List[Dict[str, Any]]:
        async with self.AsyncSession() as s:
            result = await s.execute(select(Trade).filter_by(strategy_id=strategy_id, status="OPEN"))
            rows = result.scalars().all()
            return [self._trade_dict(r) for r in rows]

    async def open_legs(self, strategy_id: str, symbol: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        trades = await self.open_trades(strategy_id)
        for t in trades:
            if t["symbol"] != symbol:
                continue
            expiry_val = t.get("expiry")
            expiry_iso = None
            if expiry_val:
                if isinstance(expiry_val, date):
                    expiry_iso = expiry_val.isoformat()
                else:
                    try:
                        expiry_iso = datetime.strptime(str(expiry_val), "%Y-%m-%d").date().isoformat()
                    except (ValueError, TypeError):
                        logger.warning("[DB] Skipping invalid trade expiry: %r", expiry_val)
                        continue
            side = t.get("side_type")
            for leg_key in ("short_leg", "long_leg"):
                opt = t.get(leg_key)
                if opt:
                    out.append({"option_symbol": opt, "side": side,
                                "expiry": expiry_iso})
        return out

    async def count_open_contracts(self, strategy_id: str, symbol: str, side: str,
                              expiry: str) -> int:
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
        async with self.AsyncSession() as s:
            q = (select(Trade).filter_by(strategy_id=strategy_id, symbol=symbol, status="OPEN")
                 .filter(Trade.side_type == side, Trade.expiry == exp_date))
            result = await s.execute(q)
            rows = result.scalars().all()
            return sum(int(r.lots or 0) for r in rows)

    async def count_pending_orders(self, strategy_id: str, symbol: str, side: str,
                              expiry: str) -> int:
        if not expiry:
            raise ValueError(
                "count_pending_orders requires an expiry (YYYY-MM-DD); the "
                "symbol-wide global mode has been removed."
            )
        side_lower = side.lower()
        async with self.AsyncSession() as s:
            # 1) Directly-submitted pending orders — scoped to this chain
            result = await s.execute(select(PendingOrder).filter_by(strategy_id=strategy_id, symbol=symbol, side=side_lower, status="PENDING"))
            po_rows = result.scalars().all()
            po_lots = 0
            for r in po_rows:
                payload = r.payload or {}
                if payload.get("expiry") != expiry:
                    continue
                po_lots += int(r.quantity or 0)

            # 2) Approval-queued trades not yet executed — scoped to this chain
            result = await s.execute(select(PendingApproval).filter_by(strategy_id=strategy_id, symbol=symbol, status="PENDING"))
            pa_rows = result.scalars().all()
            pa_lots = 0
            for r in pa_rows:
                aj = r.action_json or {}
                sp = aj.get("strategy_params") or {}
                if sp.get("side_type", "").lower() != side_lower:
                    continue
                if aj.get("expiry") != expiry:
                    continue
                for leg in (aj.get("legs") or []):
                    leg_side = (leg.get("side") or "").lower()
                    if "sell" in leg_side or "open" in leg_side:
                        try:
                            pa_lots += int(leg["quantity"])
                        except (KeyError, TypeError, ValueError):
                            pa_lots += 1
                        break

        return po_lots + pa_lots

    async def has_pending_approval(self, strategy_id: str, symbol: str,
                             side_type: Optional[str],
                             expiry: Optional[str]) -> bool:
        async with self.AsyncSession() as s:
            result = await s.execute(select(PendingApproval).filter_by(strategy_id=strategy_id, symbol=symbol, status="PENDING"))
            rows = result.scalars().all()
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

    async def expire_stale_pending_orders(self, older_than_seconds: int) -> int:
        cutoff = datetime.utcnow() - timedelta(seconds=older_than_seconds)
        expired = 0
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(PendingOrder)
                .filter(
                    PendingOrder.status == "PENDING",
                    PendingOrder.submitted_at < cutoff,
                )
            )
            stale = result.scalars().all()
            for row in stale:
                row.status = "EXPIRED"
                expired += 1
            if expired:
                await s.commit()
        return expired

    async def expire_stale_approvals(self) -> int:
        now = datetime.utcnow()
        expired = 0
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(PendingApproval)
                .filter(
                    PendingApproval.status == "PENDING",
                    PendingApproval.expires_at.isnot(None),
                    PendingApproval.expires_at < now,
                )
            )
            stale = result.scalars().all()
            for row in stale:
                row.status = "EXPIRED"
                row.decided_at = now
                row.notes = (row.notes or "") + " [auto-expired: stale approval past deadline]"
                expired += 1
            if expired:
                await s.commit()
        return expired

    # ---- approval queue --------------------------------------------------
    async def queue_for_approval(self, action_json: Dict[str, Any],
                           action_type: str = "entry",
                           expires_hours: float = 24.0) -> int:
        expires_at = (datetime.utcnow() + timedelta(hours=expires_hours)
                      if expires_hours > 0 else None)
        async with self.AsyncSession() as s:
            row = PendingApproval(
                strategy_id=action_json.get("strategy_id", "UNKNOWN"),
                symbol=action_json.get("symbol", ""),
                action_type=action_type,
                action_json=action_json,
                status="PENDING",
                expires_at=expires_at,
            )
            s.add(row)
            await s.flush()
            row_id = row.id
            await s.commit()
            return row_id

    async def list_approvals(self, status: Optional[str] = None,
                       limit: int = 100) -> List[Dict[str, Any]]:
        async with self.AsyncSession() as s:
            q = select(PendingApproval).order_by(PendingApproval.created_at.desc())
            if status:
                q = q.filter(PendingApproval.status == status.upper())
            result = await s.execute(q.limit(limit))
            rows = result.scalars().all()
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

    async def decide_approval(self, approval_id: int, decision: str,
                        notes: Optional[str] = None) -> bool:
        decision = decision.upper()
        if decision not in ("APPROVED", "REJECTED"):
            raise ValueError(f"decision must be APPROVED or REJECTED, got {decision!r}")
        async with self.AsyncSession() as s:
            result = await s.execute(select(PendingApproval).filter_by(id=approval_id).limit(1))
            row = result.scalars().first()
            if row is None or row.status != "PENDING":
                return False
            row.status = decision
            row.decided_at = datetime.utcnow()
            if notes:
                row.notes = notes
            await s.commit()
            return True

    async def fetch_approved_actions(self) -> List[Dict[str, Any]]:
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(PendingApproval)
                .filter_by(status="APPROVED")
                .order_by(PendingApproval.decided_at)
            )
            rows = result.scalars().all()
            return [
                {"id": r.id, "action_json": r.action_json,
                 "strategy_id": r.strategy_id, "symbol": r.symbol}
                for r in rows
            ]

    async def mark_approval_executed(self, approval_id: int, success: bool = True,
                               notes: Optional[str] = None) -> None:
        async with self.AsyncSession() as s:
            result = await s.execute(select(PendingApproval).filter_by(id=approval_id).limit(1))
            row = result.scalars().first()
            if row:
                row.status = "EXECUTED" if success else "FAILED"
                row.executed_at = datetime.utcnow()
                if notes:
                    row.notes = (row.notes or "") + f"\n{notes}"
                await s.commit()

    async def tracked_option_symbols(self) -> set:
        async with self.AsyncSession() as s:
            result = await s.execute(select(Trade).filter_by(status="OPEN"))
            rows = result.scalars().all()
            symbols = set()
            for r in rows:
                if r.short_leg:
                    symbols.add(r.short_leg)
                if r.long_leg:
                    symbols.add(r.long_leg)
            return symbols

    async def equity_position(self, symbol: str) -> int:
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(Trade)
                .filter_by(symbol=symbol, side_type="equity", status="OPEN")
                .order_by(Trade.opened_at.desc())
                .limit(1)
            )
            row = result.scalars().first()
            return int(row.lots) if row else 0

    # ---- runtime settings (shared agent/watcher state) -------------------
    async def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        async with self.AsyncSession() as s:
            result = await s.execute(select(SystemSetting).filter_by(key=key).limit(1))
            row = result.scalars().first()
            return row.value if row else default

    async def set_setting(self, key: str, value: str) -> None:
        async with self.AsyncSession() as s:
            result = await s.execute(select(SystemSetting).filter_by(key=key).limit(1))
            row = result.scalars().first()
            if row is None:
                s.add(SystemSetting(key=key, value=str(value)))
            else:
                row.value = str(value)
                row.updated_at = datetime.utcnow()
            await s.commit()

    async def setting_updated_at(self, key: str) -> Optional[datetime]:
        async with self.AsyncSession() as s:
            result = await s.execute(select(SystemSetting).filter_by(key=key).limit(1))
            row = result.scalars().first()
            return row.updated_at if row else None

    async def latest_log_ts(self) -> Optional[datetime]:
        """Most recent bot_logs timestamp — used as the agent's liveness signal."""
        async with self.AsyncSession() as s:
            result = await s.execute(select(BotLog).order_by(BotLog.ts.desc()).limit(1))
            row = result.scalars().first()
            return row.ts if row else None

    async def recent_logs(self, limit: int = 200) -> str:
        from hermes.market_hours import ET
        from datetime import timezone as _tz
        async with self.AsyncSession() as s:
            result = await s.execute(select(BotLog).order_by(BotLog.ts.desc()).limit(limit))
            rows = result.scalars().all()
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

    async def get_setting_async(self, key: str, default: Optional[str] = None) -> Optional[str]:
        return await self.get_setting(key, default)

    async def latest_log_ts_async(self) -> Optional[datetime]:
        return await self.latest_log_ts()

    async def recent_logs_async(self, limit: int = 200) -> str:
        return await self.recent_logs(limit)

    async def list_approvals_async(self, status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        return await self.list_approvals(status, limit)

    async def daily_bars(self, symbol: str, lookback_days: int = 400) -> Optional[pd.DataFrame]:
        return await self.ts_engine.daily_bars(symbol, lookback_days)

    async def intraday_bars(self, symbol: str, lookback_days: int = 10) -> pd.DataFrame:
        return await self.ts_engine.intraday_bars(symbol, lookback_days)

    async def save_daily_bars(self, symbol: str, df: pd.DataFrame) -> None:
        """Upsert daily bars for a symbol from a DataFrame."""
        await self.ts_engine.save_daily_bars(symbol, df)

    async def save_intraday_bars(self, symbol: str, df: pd.DataFrame) -> None:
        """Upsert intraday bars for a symbol from a DataFrame."""
        await self.ts_engine.save_intraday_bars(symbol, df)

    async def last_price(self, symbol: str) -> Optional[float]:
        return await self.ts_engine.last_price(symbol)

    async def pnl_daily(self, days: int = 60) -> List[Dict[str, Any]]:
        sql = """
          SELECT day::date, strategy_id, symbol, COALESCE(realized_pnl,0) AS realized_pnl,
                 COALESCE(closed_trades,0) AS closed_trades
          FROM pnl_daily
          WHERE day >= now() - (%s || ' days')::interval
          ORDER BY day
        """
        async with self.async_engine.connect() as conn:
            result = await conn.exec_driver_sql(sql, (days,))
            return [dict(r._mapping) for r in result.fetchall()]

    async def get_price_on_date(self, symbol: str, dt: date) -> Optional[float]:
        """Fetch close price of the symbol on or before the specified date."""
        return await self.ts_engine.get_price_on_date(symbol, dt)

    async def get_strategy_performance_metrics(self, days: int = 30) -> Dict[str, Any]:
        """Calculate recent trading performance (PASS/FAIL/NEUTRAL) for each strategy."""
        cutoff = datetime.utcnow() - timedelta(days=days)
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(Trade)
                .filter(Trade.status == "CLOSED", Trade.closed_at >= cutoff)
            )
            closed_trades = result.scalars().all()

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
                current_spot = await self.last_price(symbol)

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
                        expiry_price = await self.get_price_on_date(t.symbol, t.expiry)
                        if expiry_price is not None and expiry_price < float(t.short_strike or 0.0):
                            shares_bought = int(t.lots or 1) * 100
                            cost = float(t.short_strike) * shares_bought
                            net_shares += shares_bought
                            stock_cash_flow -= cost

                    elif t.side_type == "call" and (t.close_reason == "RECONCILED_BROKER_FLAT" or (t.closed_at and t.expiry and t.closed_at.date() >= t.expiry)):
                        expiry_price = await self.get_price_on_date(t.symbol, t.expiry)
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
