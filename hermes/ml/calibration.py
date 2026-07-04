"""
[Probability-Calibration]
Isotonic and Platt calibrators for raw model probabilities.

Why this exists
---------------
The previous augment_levels_with_pop helper turned an XGBoost predicted
return into a probability via ``0.5 + return * 5`` clipped to [0.01,
0.99]. That mapping is mathematically arbitrary — it has no provenance
in the loss the model was trained against, and no held-out data was
ever used to verify the resulting numbers behave like probabilities.

This module provides honest calibrators. Both implementations expose the
same interface:

    cal = IsotonicCalibrator.fit(probs, outcomes)
    cal.transform(new_probs)      # → calibrated probabilities

Both can be JSON-serialised so the nightly calibration job can persist
fitted parameters per symbol into the ``calibration_params`` HermesDB
table.

Calibration matters more than raw accuracy here because credit-spread
sizing relies on probabilities being honest — a Brier-tight 0.62 beats a
Brier-loose 0.71 every time.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Sequence

import numpy as np

logger = logging.getLogger("hermes.ml.calibration")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
_EPS = 1e-6


def _coerce_array(x: Iterable[float]) -> np.ndarray:
    arr = np.asarray(list(x), dtype=float)
    if arr.size == 0:
        return arr
    if not np.all(np.isfinite(arr)):
        # Replace non-finite values rather than dropping rows, so the
        # caller's ``probs`` and ``outcomes`` arrays stay aligned.
        arr = np.where(np.isfinite(arr), arr, 0.5)
    return arr


def brier_score(probs: Sequence[float], outcomes: Sequence[float]) -> float:
    """Mean squared error between predicted probabilities and outcomes."""
    p = _coerce_array(probs)
    y = _coerce_array(outcomes)
    if p.size == 0:
        return float("nan")
    return float(np.mean((p - y) ** 2))


def log_loss(probs: Sequence[float], outcomes: Sequence[float]) -> float:
    """Binary cross-entropy with a small clip to avoid log(0)."""
    p = np.clip(_coerce_array(probs), _EPS, 1 - _EPS)
    y = _coerce_array(outcomes)
    if p.size == 0:
        return float("nan")
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))



# ---------------------------------------------------------------------------
# Calibrators
# ---------------------------------------------------------------------------
@dataclass
class PlattCalibrator:
    """Logistic-regression calibrator (Platt scaling).

    Maps p ∈ [0,1] to ``sigmoid(a * logit(p) + b)``. Two parameters,
    fits in microseconds on commodity laptops, and tends to do well
    when the underlying model is already well-ranked (XGBoost usually
    is on this kind of feature set).
    """

    a: float = 1.0
    b: float = 0.0

    @classmethod
    def fit(cls, probs: Sequence[float], outcomes: Sequence[float]) -> "PlattCalibrator":
        p = np.clip(_coerce_array(probs), _EPS, 1 - _EPS)
        y = _coerce_array(outcomes)
        if p.size < 4:
            logger.warning("Platt fit aborted: only %d rows; returning identity", p.size)
            return cls()

        logit_p = np.log(p / (1 - p))
        # Newton-Raphson on the binary cross-entropy. Two parameters,
        # closed-form gradient and Hessian, converges in a handful of
        # iterations from any sane start.
        a, b = 1.0, 0.0
        for _ in range(50):
            z = a * logit_p + b
            sig = 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))
            err = sig - y
            grad_a = float(np.mean(err * logit_p))
            grad_b = float(np.mean(err))
            w = sig * (1 - sig)
            h_aa = float(np.mean(w * logit_p * logit_p)) + 1e-9
            h_bb = float(np.mean(w)) + 1e-9
            h_ab = float(np.mean(w * logit_p))
            det = h_aa * h_bb - h_ab * h_ab
            if abs(det) < 1e-12:
                break
            da = (h_bb * grad_a - h_ab * grad_b) / det
            db = (-h_ab * grad_a + h_aa * grad_b) / det
            a -= da
            b -= db
            if abs(da) < 1e-9 and abs(db) < 1e-9:
                break
        return cls(a=float(a), b=float(b))

    def transform(self, probs: Sequence[float]) -> np.ndarray:
        p = np.clip(_coerce_array(probs), _EPS, 1 - _EPS)
        z = self.a * np.log(p / (1 - p)) + self.b
        return 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))

    def to_dict(self) -> dict:
        return {"kind": "platt", "a": float(self.a), "b": float(self.b)}

    @classmethod
    def from_dict(cls, payload: dict) -> "PlattCalibrator":
        return cls(a=float(payload["a"]), b=float(payload["b"]))


@dataclass
class IsotonicCalibrator:
    """Pool-Adjacent-Violators isotonic regression.

    Non-parametric, monotone, no inductive assumption beyond
    "higher predicted probability ⇒ higher hit rate". Slightly more
    flexible than Platt and almost always preferred when there are
    >300 calibration rows available.
    """

    x_knots: List[float] = field(default_factory=list)   # sorted predicted probs
    y_knots: List[float] = field(default_factory=list)   # monotone fitted hit rates

    @classmethod
    def fit(cls, probs: Sequence[float], outcomes: Sequence[float]) -> "IsotonicCalibrator":
        p = _coerce_array(probs)
        y = _coerce_array(outcomes)
        if p.size < 4 or p.size != y.size:
            logger.warning("Isotonic fit aborted: %d rows", p.size)
            return cls()

        order = np.argsort(p)
        xs = p[order].astype(float)
        ys = y[order].astype(float)
        # Pool Adjacent Violators (Ayer et al., 1955)
        weights = np.ones_like(ys)
        i = 0
        while i < len(ys) - 1:
            if ys[i] > ys[i + 1]:
                merged_w = weights[i] + weights[i + 1]
                merged_y = (ys[i] * weights[i] + ys[i + 1] * weights[i + 1]) / merged_w
                ys[i] = merged_y
                weights[i] = merged_w
                ys = np.delete(ys, i + 1)
                xs_first = xs[i]
                xs = np.delete(xs, i + 1)
                xs[i] = xs_first
                weights = np.delete(weights, i + 1)
                if i > 0:
                    i -= 1
            else:
                i += 1
        return cls(x_knots=[float(x) for x in xs],
                   y_knots=[float(yy) for yy in ys])

    def transform(self, probs: Sequence[float]) -> np.ndarray:
        p = _coerce_array(probs)
        if not self.x_knots:
            return p
        xs = np.array(self.x_knots)
        ys = np.array(self.y_knots)
        idx = np.searchsorted(xs, p, side="right") - 1
        idx = np.clip(idx, 0, len(ys) - 1)
        return ys[idx]

    def to_dict(self) -> dict:
        return {
            "kind": "isotonic",
            "x_knots": list(self.x_knots),
            "y_knots": list(self.y_knots),
        }

    @classmethod
    def from_dict(cls, payload: dict) -> "IsotonicCalibrator":
        return cls(
            x_knots=[float(x) for x in payload.get("x_knots", [])],
            y_knots=[float(y) for y in payload.get("y_knots", [])],
        )


# ---------------------------------------------------------------------------
# Generic load helper (used by pop_engine + meta_learner)
# ---------------------------------------------------------------------------
def load_calibrator(payload: Optional[dict]):
    """Restore a calibrator from a serialised dict, or None if the
    payload is missing/malformed. Callers fall back to identity
    transform when this returns None."""
    if not payload:
        return None
    kind = payload.get("kind")
    try:
        if kind == "isotonic":
            return IsotonicCalibrator.from_dict(payload)
        if kind == "platt":
            return PlattCalibrator.from_dict(payload)
    except Exception as exc:                          # noqa: BLE001
        logger.warning("calibrator load failed: %s", exc)
    return None


__all__ = [
    "PlattCalibrator",
    "IsotonicCalibrator",
    "load_calibrator",
    "brier_score",
    "log_loss",
]
