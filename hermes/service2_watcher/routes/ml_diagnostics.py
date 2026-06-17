"""ML diagnostics surface.

Routes
------
- ``GET  /api/ml/diagnostics``     — schema hash, per-symbol model meta,
                                     calibration curves, drift summary, and
                                     the live PredictorConfig.
- ``GET  /api/ml/feature-catalog`` — JSON view of feature_catalog.py so the
                                     dashboard can render feature provenance.
- ``GET  /api/ml/calibration``     — historical reliability curve assembled
                                     from the prediction ledger for a given
                                     (symbol, model_name).
- ``POST /api/ml/backtest``        — run the reality-check backtester
                                     against cached daily bars and return
                                     Brier/log-loss/AUC/realised P&L.

The predictor handle is set via ``set_predictor`` on agent boot; without
it the diagnostics endpoint still serves catalog info and ledger-derived
calibration so the dashboard doesn't 500.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query

from hermes.ml import feature_catalog, ledger as ledger_mod
from hermes.ml.calibration import (
    brier_score, log_loss, reliability_curve,
)

from .._app_state import db

logger = logging.getLogger("hermes.c2.ml")
router = APIRouter()


# ---------------------------------------------------------------------------
# Predictor handle — set by service1_agent boot.
# ---------------------------------------------------------------------------
_predictor: Optional[Any] = None


def set_predictor(predictor: Any) -> None:
    """Wire a live AsyncXGBPredictor into the diagnostics endpoint."""
    global _predictor
    _predictor = predictor


# ---------------------------------------------------------------------------
# /api/ml/diagnostics
# ---------------------------------------------------------------------------
@router.get("/api/ml/diagnostics")
async def diagnostics() -> Dict[str, Any]:
    """Return everything the operator needs to audit prediction health."""
    ledger_sum = await _ledger_summary()
    base: Dict[str, Any] = {
        "schema_hash": {
            "equity": feature_catalog.schema_hash("equity"),
            "options": feature_catalog.schema_hash("options"),
            "macro": feature_catalog.schema_hash("macro"),
            "meta": feature_catalog.schema_hash("meta"),
        },
        "predictor": None,
        "ledger": ledger_sum,
    }
    if _predictor is not None:
        try:
            base["predictor"] = _predictor.diagnostics()
        except Exception as exc:                          # noqa: BLE001
            logger.exception("predictor.diagnostics failed: %s", exc)
            base["predictor_error"] = str(exc)
    return base


@router.get("/api/ml/feature-catalog")
def feature_catalog_dump() -> Dict[str, Any]:
    return {
        "catalog": feature_catalog.catalog_dict(),
        "schema_hash": feature_catalog.schema_hash("raw"),
    }


# ---------------------------------------------------------------------------
# /api/ml/calibration
# ---------------------------------------------------------------------------
@router.get("/api/ml/calibration")
async def calibration_curve(
    symbol: str = Query(..., min_length=1),
    model_name: str = Query("xgb-q50-7dte"),
    days: int = Query(90, ge=7, le=365),
    n_bins: int = Query(10, ge=4, le=20),
) -> Dict[str, Any]:
    """Reliability curve plus Brier/log-loss assembled from the ledger.

    Used by the operator to confirm the calibrator is doing its job —
    well-calibrated predictions land on the diagonal.
    """
    rows = await ledger_mod.fetch_for_calibration(
        db, symbol, model_name, days=days, require_outcome=True)
    if not rows:
        return {
            "symbol": symbol.upper(),
            "model_name": model_name,
            "n": 0,
            "reliability": [],
            "brier": None,
            "log_loss": None,
        }
    preds = [r["predicted_prob"] for r in rows]
    outs = [r["realized_outcome"] for r in rows]
    return {
        "symbol": symbol.upper(),
        "model_name": model_name,
        "n": len(rows),
        "reliability": reliability_curve(preds, outs, n_bins=n_bins),
        "brier": brier_score(preds, outs),
        "log_loss": log_loss(preds, outs),
        "mean_predicted": float(sum(preds) / len(preds)),
        "mean_actual": float(sum(outs) / len(outs)),
    }


# ---------------------------------------------------------------------------
# /api/ml/backtest
# ---------------------------------------------------------------------------
@router.post("/api/ml/backtest")
def run_backtest(
    symbol: str = Query(..., min_length=1),
    horizon_dte: int = Query(7, ge=1, le=90),
    short_distance_pct: float = Query(0.05, gt=0.0, lt=0.5),
    side: str = Query("put", regex="^(put|call)$"),
    days: int = Query(400, ge=120, le=2000),
) -> Dict[str, Any]:
    """Walk-forward backtest of the default scorer against historical bars.

    Powers the "Reality Check" button on the diagnostics dashboard.
    Heavy enough that it runs synchronously — operator-initiated, not
    background-loop driven.
    """
    bars = db.daily_bars(symbol, lookback_days=days)
    spy = db.daily_bars("SPY", lookback_days=days)
    if bars is None or bars.empty or spy is None:
        raise HTTPException(404, f"insufficient bars for {symbol}")

    # Lazy-import so importing this route module doesn't drag scipy +
    # sklearn into the watcher startup path.
    from hermes.ml.backtester import Backtester

    bt = Backtester(
        bars_daily=bars,
        spy_daily=spy,
        horizon_dte=horizon_dte,
        short_distance_pct=short_distance_pct,
        side=side,
    )
    result = bt.run()
    return {
        "symbol": symbol.upper(),
        "horizon_dte": horizon_dte,
        "short_distance_pct": short_distance_pct,
        "side": side,
        "result": result.to_dict(),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def _ledger_summary() -> Dict[str, Any]:
    """Tiny aggregate of recent ledger activity — surface-area indicator."""
    if ledger_mod.PredictionLedger is None:
        return {"available": False}
    try:
        from sqlalchemy import select, func
        async with db.AsyncSession() as s:
            row_total = (await s.execute(select(func.count(ledger_mod.PredictionLedger.id)))).scalar()
            outcome_total = (await s.execute(
                select(func.count(ledger_mod.PredictionLedger.id))
                .filter(ledger_mod.PredictionLedger.realized_outcome.is_not(None))
            )).scalar()
        return {
            "available": True,
            "total_rows": int(row_total),
            "rows_with_outcome": int(outcome_total),
        }
    except Exception as exc:                              # noqa: BLE001
        logger.debug("ledger summary failed: %s", exc)
        return {"available": False, "error": str(exc)}


__all__ = ["router", "set_predictor"]
