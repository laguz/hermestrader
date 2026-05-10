"""Tests for hermes.ml.drift — KS-based feature drift detection."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from hermes.ml.drift import DriftDetector, _ks_stat


# ---------------------------------------------------------------------------
# _ks_stat — unit-level
# ---------------------------------------------------------------------------
def test_ks_stat_zero_for_identical_samples():
    rng = np.random.default_rng(0)
    a = rng.normal(size=200)
    assert _ks_stat(a, a.copy()) == pytest.approx(0.0)


def test_ks_stat_large_for_disjoint_samples():
    a = np.zeros(100)
    b = np.ones(100)
    # Two delta distributions far apart → KS statistic = 1.
    assert _ks_stat(a, b) == pytest.approx(1.0, abs=1e-9)


def test_ks_stat_handles_empty_inputs():
    assert _ks_stat(np.array([]), np.array([1, 2, 3])) == 0.0
    assert _ks_stat(np.array([1, 2, 3]), np.array([])) == 0.0


# ---------------------------------------------------------------------------
# DriftDetector
# ---------------------------------------------------------------------------
def _baseline_frame(n: int = 500) -> pd.DataFrame:
    rng = np.random.default_rng(seed=2)
    return pd.DataFrame({
        "feature_a": rng.normal(0, 1, size=n),
        "feature_b": rng.normal(5, 2, size=n),
    })


def test_drift_detector_no_alarm_when_distributions_identical():
    base = _baseline_frame()
    detector = DriftDetector(["feature_a", "feature_b"]).fit(base)
    # Same distribution sampled separately — no drift.
    rng = np.random.default_rng(seed=3)
    current = pd.DataFrame({
        "feature_a": rng.normal(0, 1, size=200),
        "feature_b": rng.normal(5, 2, size=200),
    })
    alarms = detector.alarms(current, threshold=0.2)
    assert alarms == []


def test_drift_detector_alarms_when_distribution_shifts():
    base = _baseline_frame()
    detector = DriftDetector(["feature_a", "feature_b"]).fit(base)
    # Shift feature_a's mean by +3 → KS should saturate.
    rng = np.random.default_rng(seed=4)
    drifted = pd.DataFrame({
        "feature_a": rng.normal(3, 1, size=200),
        "feature_b": rng.normal(5, 2, size=200),
    })
    alarms = detector.alarms(drifted, threshold=0.2)
    assert any(r.feature == "feature_a" for r in alarms)
    # feature_b should not be in the alarm list because its distribution
    # was not perturbed.
    assert all(r.feature != "feature_b" for r in alarms)


def test_drift_detector_summary_payload_is_json_serialisable():
    base = _baseline_frame()
    detector = DriftDetector(["feature_a", "feature_b"]).fit(base)
    rng = np.random.default_rng(seed=5)
    current = pd.DataFrame({
        "feature_a": rng.normal(2, 1, size=100),
        "feature_b": rng.normal(5, 2, size=100),
    })
    summary = detector.summary(current, threshold=0.2)
    import json
    json.dumps(summary)             # must not raise
    assert summary["n_features"] == 2
    assert summary["n_alarms"] >= 1


def test_drift_detector_requires_fit_before_evaluate():
    detector = DriftDetector(["feature_a"])
    with pytest.raises(RuntimeError):
        detector.evaluate(_baseline_frame())


def test_drift_detector_skips_columns_missing_in_current():
    base = _baseline_frame()
    detector = DriftDetector(["feature_a", "missing"]).fit(
        base.assign(missing=base["feature_a"] * 0.5))
    cur = pd.DataFrame({"feature_a": np.random.normal(size=50)})
    reports = detector.evaluate(cur)
    # Only feature_a is comparable; "missing" is gracefully skipped.
    assert {r.feature for r in reports} == {"feature_a"}
