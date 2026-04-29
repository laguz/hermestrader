"""
[TimescaleDB-Schema] — SQLAlchemy ORM mirror of schema.sql.
Both Service-1 (writes) and Service-2 (reads) import from this module.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    BigInteger, Boolean, Column, Date, DateTime, ForeignKey, Index, Integer,
    Numeric, String, Text, create_engine,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

import pandas as pd


class Base(DeclarativeBase):
    pass


class Strategy(Base):
    __tablename__ = "strategies"
    strategy_id = Column(String, primary_key=True)
    priority = Column(Integer, nullable=False)
    status = Column(String, nullable=False, default="ACTIVE")
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)


class Trade(Base):
    __tablename__ = "trades"
    id = Column(BigInteger, autoincrement=True)
    opened_at = Column(DateTime(timezone=True), default=datetime.utcnow, primary_key=True)
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
        {"primary_key": ("id", "opened_at")},
    )


class PendingOrder(Base):
    __tablename__ = "pending_orders"
    id = Column(BigInteger, autoincrement=True)
    submitted_at = Column(DateTime(timezone=True), default=datetime.utcnow, primary_key=True)
    strategy_id = Column(String, nullable=False)
    symbol = Column(String, nullable=False)
    side = Column(String, nullable=False)
    quantity = Column(Integer, nullable=False)
    payload = Column(JSONB, nullable=False)
    status = Column(String, nullable=False, default="PENDING")


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


# ---------------------------------------------------------------------------
# Repository — the only place SQL lives.
# ---------------------------------------------------------------------------
class HermesDB:
    """Thin repo layer; matches the surface the engine + UI consume."""

    def __init__(self, dsn: str):
        self.engine = create_engine(dsn, pool_pre_ping=True, future=True)
        self.Session = sessionmaker(self.engine, expire_on_commit=False, future=True)

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

    def write_prediction(self, symbol: str, ret: float, price: float) -> None:
        with self.Session() as s:
            s.add(Prediction(symbol=symbol, predicted_return=ret, predicted_price=price))
            s.commit()

    def record_pending_order(self, action) -> None:
        with self.Session() as s:
            s.add(PendingOrder(
                strategy_id=action.strategy_id, symbol=action.symbol,
                side=(action.strategy_params or {}).get("side_type", action.side),
                quantity=action.quantity,
                payload={
                    "legs": action.legs, "price": action.price,
                    "tag": action.tag, "ai_authored": action.ai_authored,
                    "ai_rationale": action.ai_rationale,
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
        with self.Session() as s:
            rows = (s.query(PendingOrder)
                    .filter_by(strategy_id=strategy_id, symbol=symbol, side=side, status="PENDING")
                    .all())
            return sum(int(r.quantity or 0) for r in rows)

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
