"""
[Feature-Drift Detector]
Kolmogorov-Smirnov drift detection over a snapshot of the training
feature distribution.

Why this exists
---------------
A retrained model is only as good as the assumption that today's feature
distribution looks like the one the model was fit on. Volatility regime
shifts, broker data outages, and silent vendor schema changes all break
that assumption. Without a drift signal, the predictor reports a
healthy-looking probability long after the inputs have wandered.

This detector takes a *baseline* sample (typically the last training
window) and a *current* sample (the most recent N production rows) and
runs a per-column two-sample KS test. Results are surfaced through:

- ``DriftDetector.summary()``  → JSON-serialisable for /ml/diagnostics
- ``DriftDetector.alarms()``   → list of features whose KS exceeds a
                                 caller-supplied threshold (default 0.2)

Hermes' maintain_service skill polls .alarms() every hour and posts a
Telegram message when the list is non-empty.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("hermes.ml.drift")


@dataclass
class DriftReport:
    """One feature's KS statistic plus baseline/current summaries."""

    feature: str
    ks_statistic: float
    baseline_mean: float
    baseline_std: float
    current_mean: float
    current_std: float
    baseline_n: int
    current_n: int

    def to_dict(self) -> Dict[str, float]:
        return {
            "feature": self.feature,
            "ks": float(self.ks_statistic),
            "baseline_mean": float(self.baseline_mean),
            "baseline_std": float(self.baseline_std),
            "current_mean": float(self.current_mean),
            "current_std": float(self.current_std),
            "baseline_n": int(self.baseline_n),
            "current_n": int(self.current_n),
        }


def _ks_stat(a: np.ndarray, b: np.ndarray) -> float:
    """Two-sample KS statistic with no scipy dependency.

    We do not need the p-value here — Hermes thresholds on the KS
    distance directly, and recomputing the p-value would just import a
    chunk of scipy we already pull in for argrelextrema in pop_engine.
    """
    a = np.sort(np.asarray(a, dtype=float))
    b = np.sort(np.asarray(b, dtype=float))
    if a.size == 0 or b.size == 0:
        return 0.0
    pooled = np.concatenate([a, b])
    cdf_a = np.searchsorted(a, pooled, side="right") / a.size
    cdf_b = np.searchsorted(b, pooled, side="right") / b.size
    return float(np.max(np.abs(cdf_a - cdf_b)))


class DriftDetector:
    """KS drift detector over a fixed set of feature columns."""

    def __init__(self, features: Iterable[str]) -> None:
        self.features: List[str] = list(features)
        self._baseline: Optional[pd.DataFrame] = None

    def fit(self, frame: pd.DataFrame) -> "DriftDetector":
        """Snapshot the baseline distribution.

        Call this once at training time on the same dataframe the
        model was fit against. We keep only the listed feature
        columns to avoid retaining the entire training set in memory.
        """
        cols = [c for c in self.features if c in frame.columns]
        if not cols:
            raise ValueError("none of the requested features are present in the frame")
        self._baseline = frame[cols].copy()
        return self

    def evaluate(
        self, current: pd.DataFrame, *, max_baseline_rows: int = 5_000,
    ) -> List[DriftReport]:
        """Compare ``current`` against the baseline column-by-column."""
        if self._baseline is None:
            raise RuntimeError("DriftDetector.fit() must be called before evaluate()")
        reports: List[DriftReport] = []
        baseline = (self._baseline.tail(max_baseline_rows)
                    if len(self._baseline) > max_baseline_rows
                    else self._baseline)
        for col in self.features:
            if col not in baseline.columns or col not in current.columns:
                continue
            base = baseline[col].dropna().to_numpy()
            cur = current[col].dropna().to_numpy()
            if base.size == 0 or cur.size == 0:
                continue
            reports.append(DriftReport(
                feature=col,
                ks_statistic=_ks_stat(base, cur),
                baseline_mean=float(np.mean(base)),
                baseline_std=float(np.std(base)),
                current_mean=float(np.mean(cur)),
                current_std=float(np.std(cur)),
                baseline_n=int(base.size),
                current_n=int(cur.size),
            ))
        return reports

    def alarms(self, current: pd.DataFrame, *, threshold: float = 0.2,
               ) -> List[DriftReport]:
        """Subset of ``evaluate`` whose KS statistic exceeds ``threshold``."""
        return [r for r in self.evaluate(current) if r.ks_statistic >= threshold]


__all__ = ["DriftDetector", "DriftReport"]
