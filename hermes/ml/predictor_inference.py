"""
[XGBoost-Feature-Engine v2] — inference collaborator.

Split out of ``xgb_features.py`` to separate scoring/forecast emission from
training and scheduling. :class:`PredictorInference` is an injected collaborator
owned by :class:`~hermes.ml.xgb_features.AsyncXGBPredictor`: it reads the
predictor's state (config, models, calibrators, feature frames) and *mutates*
the shared ``_last_pred`` cache through a back-reference, so the method bodies
moved out of the predictor unchanged. Mutation works through the read-only
forwarding property because it returns the live dict object —
``self._last_pred[sym] = {...}`` mutates the predictor's dict in place.
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Dict, List, Optional

import numpy as np
import pandas as pd

from hermes.ml import ledger as ledger_mod
from hermes.ml.predictor_config import run_maybe_async

if TYPE_CHECKING:  # pragma: no cover - typing only
    from hermes.ml.xgb_features import AsyncXGBPredictor

logger = logging.getLogger("hermes.ml.xgb")


class PredictorInference:
    """Owns the predictor's scoring + forecast-emission path."""

    def __init__(self, predictor: "AsyncXGBPredictor") -> None:
        self._p = predictor

    # ── forwarded predictor handles (single source of truth on the predictor) ──
    @property
    def db(self):
        return self._p.db

    @property
    def _cfg(self):
        return self._p._cfg

    @property
    def _models(self):
        return self._p._models

    @property
    def _calibrators(self):
        return self._p._calibrators

    @property
    def _last_pred(self):
        return self._p._last_pred

    @property
    def _get_active_symbols(self):
        return self._p._get_active_symbols

    @property
    def _feature_frame(self):
        return self._p._feature_frame

    @property
    def _model_name(self):
        return self._p._model_name

    def predict_all(self) -> List[str]:
        warnings: List[str] = []
        active = self._get_active_symbols()
        predicted = 0
        for sym in active:
            heads = self._models.get(sym, {})
            if not heads:
                continue
            base_frame = self._feature_frame(sym, drop_target=True)
            if base_frame is None or base_frame.empty:
                continue

            x_last = base_frame.iloc[[-1]]
            spot = float(run_maybe_async(self.db.last_price, sym) or 0.0)

            quantile_returns: Dict[float, float] = {}
            quantile_probs: Dict[float, float] = {}
            for (horizon, q), (model, _meta) in heads.items():
                # Always score against the primary horizon for the
                # surfaced point-prediction; we record other horizons
                # in the ledger but only the default flows out.
                if horizon != self._cfg.horizons_dte[0]:
                    continue
                yhat = float(model.predict(x_last)[0])
                offset = float(getattr(model, "_hermes_offset", 0.0))
                yhat += offset
                quantile_returns[q] = yhat
                quantile_probs[q] = self._return_to_prob(yhat, sym, horizon)

            # Apply per-symbol calibrator to the median (q50) probability.
            cal = self._calibrators.get(sym)
            if cal is not None and 0.5 in quantile_probs:
                quantile_probs[0.5] = float(
                    cal.transform([quantile_probs[0.5]])[0])

            yhat_med = quantile_returns.get(0.5, 0.0)
            prob_med = quantile_probs.get(0.5, 0.5)
            prob_lo = quantile_probs.get(self._cfg.quantiles[0])
            prob_hi = quantile_probs.get(self._cfg.quantiles[-1])
            if prob_lo is not None and prob_hi is not None and prob_lo > prob_hi:
                prob_lo, prob_hi = prob_hi, prob_lo

            predicted_price = round(spot * (1 + yhat_med), 4) if spot else 0.0

            self._last_pred[sym] = {
                "asof": datetime.now(timezone.utc),
                "predicted_return": yhat_med,
                "predicted_price": predicted_price,
                "predicted_prob": prob_med,
                "predicted_prob_lo": prob_lo,
                "predicted_prob_hi": prob_hi,
                "spot": spot,
                "quantiles": {f"q{int(q*100):02d}": v
                              for q, v in quantile_probs.items()},
                "horizon_dte": self._cfg.horizons_dte[0],
            }
            try:
                run_maybe_async(self.db.write_prediction, sym, yhat_med, predicted_price, spot)
            except Exception:                       # noqa: BLE001
                pass

            self._write_ledger_row(sym, x_last, prob_med, prob_lo, prob_hi,
                                   yhat_med, spot)
            predicted += 1

        if predicted == 0 and active:
            warnings.append("No predictions produced")
        return warnings

    def _return_to_prob(self, yhat: float, symbol: str, horizon_dte: int) -> float:
        """Convert a predicted return into a probability of finishing
        OTM, using the symbol's own current realised vol so vol regimes
        are respected (no more 0.5 + return*5)."""
        try:
            vol = float(run_maybe_async(self.db.get_setting, f"ml_current_vol__{symbol}") or 0.30)
        except Exception:                           # noqa: BLE001
            vol = 0.30
        sigma_horizon = max(0.005, vol * math.sqrt(horizon_dte / 365.0))
        z = float(yhat) / sigma_horizon
        from scipy.stats import norm
        return float(np.clip(norm.cdf(z), 0.01, 0.99))

    def _write_ledger_row(self, symbol: str, x_last: pd.DataFrame,
                          prob_med: float, prob_lo: Optional[float],
                          prob_hi: Optional[float], yhat: float,
                          spot: float) -> None:
        try:
            heads = self._models.get(symbol, {})
            meta = next(iter(heads.values()))[1] if heads else None
            feature_vec = (x_last.iloc[0].to_dict()
                           if not x_last.empty else {})
            run_maybe_async(ledger_mod.write_record, self.db, ledger_mod.LedgerRecord(
                symbol=symbol,
                model_name=self._model_name(self._cfg.horizons_dte[0], 0.5),
                horizon_dte=self._cfg.horizons_dte[0],
                model_hash=(meta.model_hash if meta else None),
                schema_hash=(meta.schema_hash if meta else None),
                schema_stage="equity",
                predicted_prob=float(prob_med),
                predicted_prob_lo=(float(prob_lo) if prob_lo is not None else None),
                predicted_prob_hi=(float(prob_hi) if prob_hi is not None else None),
                predicted_return=float(yhat),
                spot=float(spot),
                feature_vector={k: (float(v) if isinstance(v, (int, float)) else v)
                                for k, v in feature_vec.items()
                                if not isinstance(v, str)},
            ))
        except Exception as exc:                    # noqa: BLE001
            logger.debug("ledger write failed for %s: %s", symbol, exc)
