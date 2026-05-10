"""
[Prediction-Ledger]
Track every published prediction so the meta-learner and the nightly
calibration job can train on actual production outcomes rather than
synthetic backtests.

Why this exists
---------------
Prior to this module the ``predictions`` table held only the most
recent point estimate per (symbol, ts). There was nowhere to attach
which model produced the row, which feature schema was live at the
time, or — critically — what the realised outcome turned out to be.

The ledger fixes that by recording, for every prediction:

- model_hash + schema_hash (so postmortems know exactly which code
  produced the number)
- the full feature vector (JSONB) the prediction was scored against
- the predicted probability and any confidence band
- a placeholder for the realised outcome, which the nightly evaluator
  fills in once the relevant DTE has rolled past

Every row joins cleanly with the ``trades`` and ``bars_daily`` tables
to derive realised outcomes — see ``mark_outcome`` below.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    BigInteger, Column, DateTime, Float, Integer, Sequence, String,
)
from sqlalchemy.dialects.postgresql import JSONB

try:                                                  # pragma: no cover
    from hermes.db.models import Base, HermesDB
except Exception:                                     # pragma: no cover
    Base = None                                       # type: ignore
    HermesDB = None                                   # type: ignore

logger = logging.getLogger("hermes.ml.ledger")


# ---------------------------------------------------------------------------
# ORM model — adds itself to hermes.db.models.Base if available so the
# table is created on watcher boot via Base.metadata.create_all.
# ---------------------------------------------------------------------------
if Base is not None:                                  # pragma: no branch

    class PredictionLedger(Base):                     # type: ignore[misc, valid-type]
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
    PredictionLedger = None                           # type: ignore


# ---------------------------------------------------------------------------
# Plain-Python record for callers that do not want SQLAlchemy on the path.
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------
def write_record(db: "HermesDB", rec: LedgerRecord) -> None:
    """Insert a fresh prediction row.  Idempotent for a single (ts,
    symbol, model_name) tuple — the composite PK rejects duplicates.
    """
    if PredictionLedger is None:
        logger.debug("PredictionLedger ORM unavailable; skipping write")
        return
    with db.Session() as s:
        row = PredictionLedger(                       # type: ignore[call-arg]
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
            s.commit()
        except Exception as exc:                      # noqa: BLE001
            s.rollback()
            logger.debug("ledger write skipped (likely duplicate): %s", exc)


def fetch_for_calibration(
    db: "HermesDB",
    symbol: str,
    model_name: str,
    *,
    days: int = 90,
    require_outcome: bool = True,
) -> List[Dict[str, Any]]:
    """Pull recent rows for a (symbol, model_name) pair.

    The nightly calibration job feeds the ``predicted_prob`` /
    ``realized_outcome`` columns from this query into IsotonicCalibrator.
    """
    if PredictionLedger is None:
        return []
    with db.Session() as s:
        q = s.query(PredictionLedger).filter(
            PredictionLedger.symbol == symbol.upper(),
            PredictionLedger.model_name == model_name,
            PredictionLedger.ts >= datetime.now(timezone.utc) - timedelta(days=days),
        )
        if require_outcome:
            q = q.filter(PredictionLedger.realized_outcome.is_not(None))
        rows = q.order_by(PredictionLedger.ts).all()
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
                "feature_vector": r.feature_vector or {},
            }
            for r in rows
        ]


def mark_outcome(
    db: "HermesDB",
    symbol: str,
    model_name: str,
    ts: datetime,
    *,
    outcome: float,
    realized_close: Optional[float] = None,
    realized_pnl: Optional[float] = None,
) -> bool:
    """Backfill the realised columns once the trade horizon expires.

    The nightly evaluator computes realised outcomes by joining each
    ledger row against bars_daily / trades and calling this method.
    """
    if PredictionLedger is None:
        return False
    with db.Session() as s:
        row = (s.query(PredictionLedger)
               .filter_by(ts=ts, symbol=symbol.upper(), model_name=model_name)
               .first())
        if row is None:
            return False
        row.realized_outcome = float(outcome)
        row.realized_at = datetime.now(timezone.utc)
        if realized_close is not None:
            row.realized_close = float(realized_close)
        if realized_pnl is not None:
            row.realized_pnl = float(realized_pnl)
        s.commit()
        return True


def ensure_table(db: "HermesDB") -> None:
    """Create the prediction_ledger table if it does not yet exist.

    HermesDB.run_migrations is the canonical place to call this from on
    watcher boot; idempotent so it is also safe to call ad-hoc.
    """
    if PredictionLedger is None:
        return
    try:
        PredictionLedger.__table__.create(bind=db.engine, checkfirst=True)
    except Exception as exc:                          # noqa: BLE001
        logger.warning("could not ensure prediction_ledger: %s", exc)


__all__ = [
    "LedgerRecord",
    "PredictionLedger",
    "write_record",
    "fetch_for_calibration",
    "mark_outcome",
    "ensure_table",
]
