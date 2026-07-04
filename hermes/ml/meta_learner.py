"""
[Stacking-Meta-Learner]
Combine delta-implied probability, XGB probability, S/R protection
score, and IV rank into a single calibrated POP.

Why this exists
---------------
The previous predict_single_pop function combined four log-odds with
hand-set weights that were (a) the same across 3M / 6M / 1Y horizons
and (b) never validated against realised outcomes. Stacking lets the
data choose the weights and produces a probability that has been
demonstrably calibrated against a held-out window.

The meta-learner is a thin wrapper around a logistic regression. We
keep it small and explainable on purpose:

- Two parameters per feature plus an intercept means tens of rows are
  enough to fit it (the prediction ledger doesn't need to be huge).
- The fitted weights are plain floats in the persisted JSON, so the
  operator can see, "this week the meta-learner trusts the protection
  score more than usual" without running SHAP.

When no fitted MetaLearner is available (cold start), we fall back to
a deterministic identity that returns ``xgb_prob`` so behaviour matches
the legacy code path.
"""
from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Sequence

import numpy as np

from hermes.ml.calibration import (
    IsotonicCalibrator,
    PlattCalibrator,
    brier_score,
    log_loss,
    load_calibrator,
)

logger = logging.getLogger("hermes.ml.meta")


# Canonical feature order for the meta-learner. Keep in sync with
# feature_catalog.META_FEATURES.
DEFAULT_FEATURES: tuple[str, ...] = (
    "delta_implied_prob",
    "xgb_prob",
    "protection_score",
    "iv_rank_365d",
    "vol_ratio",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _logit(p: float) -> float:
    p = max(min(p, 1 - 1e-6), 1e-6)
    return float(math.log(p / (1 - p)))


def _sigmoid(z: float) -> float:
    z = max(min(z, 30.0), -30.0)
    return 1.0 / (1.0 + math.exp(-z))


def _row_vector(features: Mapping[str, float],
                feature_names: Sequence[str]) -> np.ndarray:
    """Pull features in catalog order, defaulting missing to neutral."""
    out = []
    for name in feature_names:
        v = features.get(name)
        if v is None or (isinstance(v, float) and not math.isfinite(v)):
            v = 0.0 if name in {"protection_score", "vol_ratio"} else 0.5
        out.append(float(v))
    return np.array(out, dtype=float)


# ---------------------------------------------------------------------------
# MetaLearner
# ---------------------------------------------------------------------------
@dataclass
class MetaLearner:
    """Logistic regression over a fixed feature list.

    Two operating modes:

    - "untrained" (default): predict() returns features['xgb_prob']
      unchanged. This is the cold-start path and matches the legacy
      behaviour the rest of the system was tuned against.
    - "trained":  predict() returns sigmoid(intercept + Σ weight_i *
      feature_i_logit) with a downstream calibrator applied if one was
      attached.
    """

    feature_names: tuple[str, ...] = field(default_factory=lambda: DEFAULT_FEATURES)
    weights: List[float] = field(default_factory=list)
    intercept: float = 0.0
    calibrator_payload: Optional[dict] = None
    trained_at: Optional[float] = None
    metrics: Dict[str, float] = field(default_factory=dict)

    # ---- prediction -------------------------------------------------------
    def predict(self, features: Mapping[str, float]) -> float:
        if not self.weights:
            return float(features.get("xgb_prob", 0.5))

        raw = self.predict_raw(features)
        cal = load_calibrator(self.calibrator_payload)
        if cal is not None:
            return float(cal.transform([raw])[0])
        return float(raw)

    def predict_raw(self, features: Mapping[str, float]) -> float:
        if not self.weights:
            return float(features.get("xgb_prob", 0.5))
        # Logits for probability-typed features so the meta-learner is
        # in a consistent space; everything else stays linear.
        z = self.intercept
        for name, w in zip(self.feature_names, self.weights):
            v = features.get(name)
            if v is None or (isinstance(v, float) and not math.isfinite(v)):
                v = 0.5 if "prob" in name else 0.0
            if "prob" in name:
                v = _logit(float(v))
            elif "rank" in name:
                v = float(v) / 100.0
            else:
                v = float(v)
            z += w * v
        return _sigmoid(z)

    # ---- training ---------------------------------------------------------
    @classmethod
    def fit(
        cls,
        rows: Sequence[Mapping[str, float]],
        outcomes: Sequence[float],
        *,
        feature_names: Sequence[str] = DEFAULT_FEATURES,
        l2: float = 0.5,
        n_iter: int = 200,
        lr: float = 0.05,
        calibrator: str = "isotonic",
    ) -> "MetaLearner":
        """Fit a logistic regression with L2 regularisation and an
        optional downstream calibrator (isotonic by default).

        ``rows`` is a sequence of feature dicts; ``outcomes`` is the
        binary realised outcome (1.0 = profitable spread, 0.0 = lost).
        """
        if len(rows) != len(outcomes):
            raise ValueError("rows and outcomes must have the same length")
        feature_names = tuple(feature_names)

        if len(rows) < 20:
            logger.warning("meta-learner fit aborted: %d rows (<20)", len(rows))
            return cls(feature_names=feature_names)

        X = np.stack([_row_vector(r, feature_names) for r in rows])
        for j, name in enumerate(feature_names):
            if "prob" in name:
                # Apply logit transform per column so the linear model
                # operates in log-odds space for probability inputs.
                X[:, j] = np.array([_logit(v) for v in X[:, j]])
            elif "rank" in name:
                X[:, j] = X[:, j] / 100.0
        y = np.asarray(outcomes, dtype=float)

        # Plain mini-batch gradient descent — couple of dozen rows
        # typically; no need to drag in scikit-learn.
        n, d = X.shape
        w = np.zeros(d, dtype=float)
        b = 0.0
        for _ in range(n_iter):
            z = np.clip(X @ w + b, -30, 30)
            p = 1.0 / (1.0 + np.exp(-z))
            grad_w = (X.T @ (p - y)) / n + l2 * w
            grad_b = float(np.mean(p - y))
            w -= lr * grad_w
            b -= lr * grad_b

        # Compute training metrics for diagnostics.
        z = np.clip(X @ w + b, -30, 30)
        raw_probs = 1.0 / (1.0 + np.exp(-z))

        cal_payload: Optional[dict] = None
        if calibrator == "isotonic":
            cal = IsotonicCalibrator.fit(raw_probs.tolist(), y.tolist())
            cal_payload = cal.to_dict()
            calibrated = cal.transform(raw_probs)
        elif calibrator == "platt":
            cal = PlattCalibrator.fit(raw_probs.tolist(), y.tolist())
            cal_payload = cal.to_dict()
            calibrated = cal.transform(raw_probs)
        else:
            calibrated = raw_probs

        brier_raw = brier_score(raw_probs.tolist(), y.tolist())
        brier_cal = brier_score(calibrated.tolist(), y.tolist())
        if cal_payload is not None and brier_cal > brier_raw:
            logger.warning(
                "Calibration degraded Brier score (raw=%f, calibrated=%f). Falling back to uncalibrated.",
                brier_raw, brier_cal
            )
            calibrated = raw_probs
            cal_payload = None

        metrics = {
            "brier_raw": brier_score(raw_probs.tolist(), y.tolist()),
            "brier_calibrated": brier_score(calibrated.tolist(), y.tolist()),
            "log_loss": log_loss(calibrated.tolist(), y.tolist()),
            "n_train": int(n),
        }
        return cls(
            feature_names=feature_names,
            weights=[float(x) for x in w],
            intercept=float(b),
            calibrator_payload=cal_payload,
            trained_at=__import__("time").time(),
            metrics=metrics,
        )

    # ---- serialisation ----------------------------------------------------
    def to_json(self) -> str:
        return json.dumps({
            "feature_names": list(self.feature_names),
            "weights": list(self.weights),
            "intercept": float(self.intercept),
            "calibrator_payload": self.calibrator_payload,
            "trained_at": self.trained_at,
            "metrics": self.metrics,
        }, sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> "MetaLearner":
        data = json.loads(raw)
        return cls(
            feature_names=tuple(data.get("feature_names", DEFAULT_FEATURES)),
            weights=[float(x) for x in data.get("weights", [])],
            intercept=float(data.get("intercept", 0.0)),
            calibrator_payload=data.get("calibrator_payload"),
            trained_at=data.get("trained_at"),
            metrics=data.get("metrics", {}),
        )

__all__ = ["MetaLearner", "DEFAULT_FEATURES"]
