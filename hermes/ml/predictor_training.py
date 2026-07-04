"""
[XGBoost-Feature-Engine v2] — training collaborator.

Split out of ``xgb_features.py`` to separate the (slow, batch) training concern
from prediction and scheduling. :class:`PredictorTrainer` is an injected
collaborator owned by
:class:`~hermes.ml.xgb_features.AsyncXGBPredictor`: it reads the predictor's
state (config, broker, active universe, feature frames) and *mutates* the shared
model cache (``_models``) through a back-reference, so the
method bodies moved out of the predictor unchanged. Mutation works through the
read-only forwarding properties because they return the live dict objects —
``self._models.setdefault(...)`` mutates the predictor's dict in place.

The quantile-objective helpers (``_fit_with_objective`` / ``_rmse``) live here
because nothing outside training uses them; keeping them here also avoids a
circular import back into ``xgb_features``.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List

import numpy as np
import pandas as pd

from hermes.ml import persistence

if TYPE_CHECKING:  # pragma: no cover - typing only
    from hermes.ml.xgb_features import AsyncXGBPredictor

logger = logging.getLogger("hermes.ml.xgb")


class PredictorTrainer:
    """Owns the predictor's (horizon × quantile) model-training path."""

    def __init__(self, predictor: "AsyncXGBPredictor") -> None:
        self._p = predictor

    # ── forwarded predictor handles (single source of truth on the predictor) ──
    @property
    def symbols(self):
        return self._p.symbols

    @property
    def _cfg(self):
        return self._p._cfg

    @property
    def _models(self):
        return self._p._models

    @property
    def _get_active_symbols(self):
        return self._p._get_active_symbols

    @property
    def _feature_frame(self):
        return self._p._feature_frame

    @property
    def _target_frame(self):
        return self._p._target_frame

    @property
    def _model_name(self):
        return self._p._model_name

    def retrain_all(self) -> List[str]:
        warnings: List[str] = []
        try:
            import xgboost as xgb                   # type: ignore
        except ImportError:
            msg = "xgboost not installed; skipping retrain"
            logger.warning(msg)
            return [msg]

        trained = 0
        for sym in self._get_active_symbols():
            base_frame = self._feature_frame(sym)
            if base_frame is None:
                warnings.append(f"{sym}: no data")
                continue
            if len(base_frame) < 80:
                warnings.append(f"{sym}: need 80 bars, got {len(base_frame)}")
                continue

            # Train one (horizon × quantile) head per combination.
            for horizon in self._cfg.horizons_dte:
                frame = self._target_frame(sym, base_frame, horizon=horizon)
                if frame is None or len(frame) < 60:
                    warnings.append(
                        f"{sym}: insufficient rows for {horizon}DTE")
                    continue
                X = frame.drop(columns=["target"])
                y = frame["target"]
                for q in self._cfg.quantiles:
                    try:
                        model = self._fit_quantile_model(xgb, X, y, q)
                    except Exception as exc:        # noqa: BLE001
                        warnings.append(f"{sym}: q{q} fit failed: {exc}")
                        continue
                    self._models.setdefault(sym, {})[(horizon, q)] = (
                        model,
                        persistence.save_model(
                            model,
                            symbol=sym,
                            model_name=self._model_name(horizon, q),
                            target=self._cfg.target_kind,
                            sample_size=int(len(X)),
                            schema_stage="equity",
                            horizon_dte=horizon,
                            quantile=q,
                            metrics={"rmse": _rmse(model, X, y)},
                            notes=f"v2 quantile head q={q}",
                        ),
                    )
                    trained += 1

        if trained == 0 and self.symbols:
            warnings.insert(0, "No models trained")
        else:
            logger.info("retrain complete (%d heads)", trained)
        return warnings

    def _fit_quantile_model(self, xgb_mod, X: pd.DataFrame, y: pd.Series,
                            quantile: float):
        """Fit a single quantile-regression XGBoost head.

        ``reg:quantileerror`` was added in XGBoost 1.7. Older versions
        fall back to the legacy squared-error objective with a residual
        offset so the existing test environment still trains.
        """
        try:
            return _fit_with_objective(
                xgb_mod, X, y,
                objective="reg:quantileerror",
                quantile_alpha=quantile,
            )
        except Exception:
            offset = float(np.quantile(y, quantile) - np.quantile(y, 0.5))
            model = xgb_mod.XGBRegressor(
                n_estimators=400, max_depth=4, learning_rate=0.05,
                subsample=0.85, colsample_bytree=0.8, n_jobs=2,
                objective="reg:squarederror",
            )
            model.fit(X, y - offset)
            model._hermes_offset = offset           # type: ignore[attr-defined]
            return model


# ---------------------------------------------------------------------------
# Helpers (training-only)
# ---------------------------------------------------------------------------
def _fit_with_objective(xgb_mod, X: pd.DataFrame, y: pd.Series, *,
                        objective: str, quantile_alpha: float):
    model = xgb_mod.XGBRegressor(
        n_estimators=400, max_depth=4, learning_rate=0.05,
        subsample=0.85, colsample_bytree=0.8, n_jobs=2,
        objective=objective, quantile_alpha=quantile_alpha,
    )
    model.fit(X, y)
    return model


def _rmse(model, X: pd.DataFrame, y: pd.Series) -> float:
    try:
        yhat = model.predict(X)
        return float(np.sqrt(np.mean((yhat - y.values) ** 2)))
    except Exception:                               # noqa: BLE001
        return float("nan")
