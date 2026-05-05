"""
[TimescaleDB-Schema] — SQLAlchemy ORM mirror of schema.sql.
Both Service-1 (writes) and Service-2 (reads) import from this module.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    BigInteger, Boolean, Column, Date, DateTime, ForeignKey, Index, Integer,
    Numeric, PrimaryKeyConstraint, Sequence, String, Text, create_engine,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

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

    def write_prediction(self, symbol: str, ret: float, price: float) -> None:
        with self.Session() as s:
            s.add(Prediction(symbol=symbol, predicted_return=ret, predicted_price=price))
            s.commit()

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
        with self.Session() as s:
            s.add(PendingOrder(
                strategy_id=action.strategy_id, symbol=action.symbol,
                side=(action.strategy_params or {}).get("side_type", action.side),
                quantity=lots,          # lot count, not order count
                payload={
                    "legs": action.legs, "price": action.price,
                    "tag": action.tag, "ai_authored": action.ai_authored,
                    "ai_rationale": action.ai_rationale,
                    "expiry": action.expiry,
                },
            ))
            s.commit()

    def record_order_response(self, action, response) -> None:
        # In production: also persist into trades on fill via webhook/poll.
        self.write_log(action.strategy_id,
                       f"order response for {action.symbol}: {response}")

    def upsert_positions(self, positions: List[Dict[str, Any]]) -> None:
        # Implementation-dependent: we rely on broker as source of truth and
        # simply log; the trades table is the bot's authoritative record.
        self.write_log("ENGINE", f"synced {len(positions)} positions")

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
            rows = (s.query(StrategyWatchlist)
                    .filter_by(strategy_id=strategy_id)
                    .order_by(StrategyWatchlist.symbol).all())
            return [r.symbol for r in rows]

    def list_all_watchlists(self) -> Dict[str, List[str]]:
        with self.Session() as s:
            rows = s.query(StrategyWatchlist).order_by(
                StrategyWatchlist.strategy_id, StrategyWatchlist.symbol).all()
            out: Dict[str, List[str]] = {}
            for r in rows:
                out.setdefault(r.strategy_id, []).append(r.symbol)
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
        out: List[Dict[str, Any]] = []
        for t in self.open_trades(strategy_id):
            if t["symbol"] != symbol:
                continue
            for leg, side in (("short_leg", t.get("side_type")), ):
                opt = t.get(leg)
                if opt:
                    out.append({"option_symbol": opt, "side": side,
                                "expiry": t.get("expiry").isoformat() if t.get("expiry") else None})
        return out

    def count_open_contracts(self, strategy_id: str, symbol: str, side: str) -> int:
        with self.Session() as s:
            rows = (s.query(Trade).filter_by(strategy_id=strategy_id, symbol=symbol, status="OPEN")
                    .filter(Trade.side_type == side).all())
            return sum(int(r.lots or 0) for r in rows)

    def count_pending_orders(self, strategy_id: str, symbol: str, side: str) -> int:
        """Return total lot-count of pending exposure for (strategy, symbol, side).

        Checks two tables:
        * pending_orders   — orders queued for direct broker submission
        * pending_approvals— orders queued for human C2 approval (approval_mode=True)

        Both must be counted so side_aware_capacity works correctly regardless
        of whether approval_mode is on or off.  Without this, every tick looks
        like capacity is full/zero from open trades but the pending approval
        queue is invisible, causing duplicate entries every tick.
        """
        side_lower = side.lower()
        with self.Session() as s:
            # 1) Directly-submitted pending orders
            po_rows = (s.query(PendingOrder)
                       .filter_by(strategy_id=strategy_id, symbol=symbol,
                                  side=side_lower, status="PENDING")
                       .all())
            po_lots = sum(int(r.quantity or 0) for r in po_rows)

            # 2) Approval-queued trades not yet executed
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

    # ---- approval queue --------------------------------------------------
    def queue_for_approval(self, action_json: Dict[str, Any],
                           action_type: str = "entry") -> int:
        """Write a proposed TradeAction to the approval queue.  Returns the new row id."""
        with self.Session() as s:
            row = PendingApproval(
                strategy_id=action_json.get("strategy_id", "UNKNOWN"),
                symbol=action_json.get("symbol", ""),
                action_type=action_type,
                action_json=action_json,
                status="PENDING",
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
                if r.short_leg: symbols.add(r.short_leg)
                if r.long_leg:  symbols.add(r.long_leg)
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
        with self.Session() as s:
            rows = s.query(BotLog).order_by(BotLog.ts.desc()).limit(limit).all()
            return "\n".join(f"{r.ts:%H:%M:%S} [{r.strategy_id}] {r.message}" for r in reversed(rows))

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
        sql = "SELECT close FROM bars_daily WHERE symbol = %s ORDER BY ts DESC LIMIT 1"
        df = pd.read_sql(sql, self.engine, params=(symbol,))
        return float(df.iloc[0]["close"]) if not df.empty else None

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
