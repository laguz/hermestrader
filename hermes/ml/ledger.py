"""
[Prediction-Ledger]
Track every published prediction so the meta-learner and the nightly
calibration job can train on actual production outcomes rather than
synthetic backtests.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    BigInteger, Column, DateTime, Float, Integer, Sequence, String, select,
)
from sqlalchemy.dialects.postgresql import JSONB

try:
    from hermes.db.models import Base, HermesDB
except Exception:
    Base = None
    HermesDB = None

logger = logging.getLogger("hermes.ml.ledger")


if Base is not None:

    class PredictionLedger(Base):
        """Long-running ledger of every published prediction.

        Composite PK on (ts, symbol, model_name) so we can store
        multiple model heads per (symbol, ts) — the q10/q50/q90 trio
        from the quantile XGBoost predictor, for example.
        """

        __tablename__ = "prediction_ledger"

        id = Column(BigInteger, Sequence("prediction_ledger_id_seq"),
                    primary_key=True, autoincrement=True)
        ts = Column(DateTime(timezone=True), primary_key=True,
                    default=lambda: datetime.now(timezone.utc))
        symbol = Column(String, nullable=False, primary_key=True)
        model_name = Column(String, nullable=False, primary_key=True,
                            default="xgb-q50-default")
        horizon_dte = Column(Integer)

        model_hash = Column(String)
        schema_hash = Column(String)
        schema_stage = Column(String, default="raw")

        predicted_prob = Column(Float)
        predicted_prob_lo = Column(Float)
        predicted_prob_hi = Column(Float)
        predicted_return = Column(Float)
        spot = Column(Float)

        feature_vector = Column(JSONB, nullable=False, default=dict)

        realized_outcome = Column(Float)              # 1.0 = profitable, 0.0 = lost
        realized_at = Column(DateTime(timezone=True))
        realized_pnl = Column(Float)
        realized_close = Column(Float)
else:
    PredictionLedger = None


@dataclass
class LedgerRecord:
    symbol: str
    model_name: str
    horizon_dte: Optional[int]
    model_hash: Optional[str]
    schema_hash: Optional[str]
    schema_stage: str
    predicted_prob: float
    predicted_prob_lo: Optional[float]
    predicted_prob_hi: Optional[float]
    predicted_return: Optional[float]
    spot: Optional[float]
    feature_vector: Dict[str, Any] = field(default_factory=dict)
    ts: Optional[datetime] = None


async def write_record(db: "HermesDB", rec: LedgerRecord) -> None:
    """Insert a fresh prediction row.  Idempotent for a single (ts,
    symbol, model_name) tuple — the composite PK rejects duplicates.
    """
    if PredictionLedger is None:
        logger.debug("PredictionLedger ORM unavailable; skipping write")
        return
    async with db.AsyncSession() as s:
        row = PredictionLedger(
            ts=rec.ts or datetime.now(timezone.utc),
            symbol=rec.symbol.upper(),
            model_name=rec.model_name,
            horizon_dte=rec.horizon_dte,
            model_hash=rec.model_hash,
            schema_hash=rec.schema_hash,
            schema_stage=rec.schema_stage,
            predicted_prob=rec.predicted_prob,
            predicted_prob_lo=rec.predicted_prob_lo,
            predicted_prob_hi=rec.predicted_prob_hi,
            predicted_return=rec.predicted_return,
            spot=rec.spot,
            feature_vector=rec.feature_vector or {},
        )
        s.add(row)
        try:
            await s.commit()
        except Exception as exc:
            await s.rollback()
            logger.debug("ledger write skipped (likely duplicate): %s", exc)


async def fetch_for_calibration(
    db: "HermesDB",
    symbol: str,
    model_name: str,
    *,
    days: int = 90,
    require_outcome: bool = True,
) -> List[Dict[str, Any]]:
    """Pull recent rows for a (symbol, model_name) pair."""
    if PredictionLedger is None:
        return []
    async with db.AsyncSession() as s:
        q = select(PredictionLedger).filter(
            PredictionLedger.symbol == symbol.upper(),
            PredictionLedger.model_name == model_name,
            PredictionLedger.ts >= datetime.now(timezone.utc) - timedelta(days=days),
        )
        if require_outcome:
            q = q.filter(PredictionLedger.realized_outcome.is_not(None))
        
        result = await s.execute(q.order_by(PredictionLedger.ts))
        rows = result.scalars().all()
        return [
            {
                "ts": r.ts,
                "predicted_prob": float(r.predicted_prob or 0.0),
                "predicted_prob_lo": (float(r.predicted_prob_lo)
                                      if r.predicted_prob_lo is not None else None),
                "predicted_prob_hi": (float(r.predicted_prob_hi)
                                      if r.predicted_prob_hi is not None else None),
                "realized_outcome": (float(r.realized_outcome)
                                     if r.realized_outcome is not None else None),
                "realized_pnl": (float(r.realized_pnl)
                                 if r.realized_pnl is not None else None),
                "spot": float(r.spot or 0.0),
                "horizon_dte": int(r.horizon_dte or 7),
                "feature_vector": r.feature_vector or {},
            }
            for r in rows
        ]



async def backfill_prediction_outcomes(db: Any, lookback_days: int = 90) -> int:
    """Evaluate and mark realized outcomes for prediction ledger rows whose horizon has passed.

    Returns the number of rows marked.
    """
    if PredictionLedger is None:
        return 0

    from datetime import datetime, timezone, timedelta
    from sqlalchemy import select

    marked_count = 0
    async with db.AsyncSession() as session:
        now = datetime.now(timezone.utc)
        q = (
            select(PredictionLedger)
            .filter(PredictionLedger.realized_outcome.is_(None))
        )
        result = await session.execute(q)
        unmarked_rows = result.scalars().all()

        cached_bars = {}
        for row in unmarked_rows:
            horizon = row.horizon_dte or 7
            target_date = row.ts + timedelta(days=horizon)
            if target_date > now:
                continue

            sym = row.symbol.upper()
            if sym not in cached_bars:
                try:
                    df = await db.daily_bars(sym, lookback_days=lookback_days + 30)
                except Exception as exc:
                    logger.debug("failed to fetch daily bars for %s: %s", sym, exc)
                    df = None
                cached_bars[sym] = df

            df_bars = cached_bars[sym]
            if df_bars is None or df_bars.empty:
                continue

            # Align timezone awareness of target_date with df_bars.index to prevent comparison TypeError
            tz = df_bars.index.tz
            if tz is None:
                target_dt_cmp = target_date.replace(tzinfo=None)
            else:
                target_dt_cmp = target_date.astimezone(tz)

            df_future = df_bars[df_bars.index >= target_dt_cmp]
            if df_future.empty:
                continue

            first_bar_ts = df_future.index[0]
            first_bar_ts_utc = first_bar_ts.to_pydatetime()
            if first_bar_ts_utc.tzinfo is None:
                first_bar_ts_utc = first_bar_ts_utc.replace(tzinfo=timezone.utc)

            if first_bar_ts_utc > now:
                continue

            row_bar = df_future.iloc[0]
            realized_close = float(row_bar["close"])
            spot = float(row.spot) if row.spot else realized_close
            outcome = 1.0 if realized_close > spot else 0.0

            row.realized_outcome = outcome
            row.realized_close = realized_close
            row.realized_at = datetime.now(timezone.utc)
            marked_count += 1

        if marked_count > 0:
            await session.commit()
            logger.info("Marked %d new prediction outcomes", marked_count)

    return marked_count


def ensure_table(db: "HermesDB") -> None:
    """Create the prediction_ledger table if it does not yet exist.

    Uses a synchronous connection since it's run at startup.
    """
    if PredictionLedger is None:
        return
    try:
        # Use sync engine context for table creation on startup
        with db.engine.begin() as conn:
            Base.metadata.create_all(bind=conn, tables=[PredictionLedger.__table__], checkfirst=True)
    except Exception as exc:
        logger.warning("could not ensure prediction_ledger: %s", exc)


__all__ = [
    "LedgerRecord",
    "PredictionLedger",
    "write_record",
    "fetch_for_calibration",
    "backfill_prediction_outcomes",
    "ensure_table",
]
