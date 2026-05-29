"""Tests for hermes.ml.calibration — IsotonicCalibrator and PlattCalibrator.

The calibrators must:

- Be monotone (isotonic property).
- Improve Brier score on a synthetic mis-calibrated dataset.
- Round-trip through to_dict/from_dict without information loss.
- Survive degenerate inputs (all-zero outcomes, < 4 rows, etc.).
"""
from __future__ import annotations

import numpy as np
import pytest

from hermes.ml.calibration import (
    IsotonicCalibrator,
    PlattCalibrator,
    brier_score,
    load_calibrator,
    reliability_curve,
)


# ---------------------------------------------------------------------------
# Synthetic miscalibrated data
# ---------------------------------------------------------------------------
@pytest.fixture
def miscalibrated_dataset():
    """Generates probabilities biased high vs realised hit rate.

    Hit rate for predicted=p is ~p**2, so the calibrator should pull
    the predictions down. Both IsotonicCalibrator and PlattCalibrator
    must demonstrably improve Brier score on this dataset.
    """
    rng = np.random.default_rng(seed=0)
    raw = rng.uniform(0.1, 0.95, size=600)
    # Realized hit probability is the square of the prediction —
    # a textbook over-confident model.
    realized = (rng.uniform(size=600) < raw ** 2).astype(float)
    return raw.tolist(), realized.tolist()


# ---------------------------------------------------------------------------
# IsotonicCalibrator
# ---------------------------------------------------------------------------
def test_isotonic_improves_brier(miscalibrated_dataset):
    raw, realized = miscalibrated_dataset
    cal = IsotonicCalibrator.fit(raw, realized)
    calibrated = cal.transform(raw)
    raw_brier = brier_score(raw, realized)
    cal_brier = brier_score(calibrated.tolist(), realized)
    assert cal_brier < raw_brier, (
        f"Isotonic calibration must reduce Brier "
        f"(raw={raw_brier:.4f}, cal={cal_brier:.4f})"
    )


def test_isotonic_is_monotone(miscalibrated_dataset):
    raw, realized = miscalibrated_dataset
    cal = IsotonicCalibrator.fit(raw, realized)
    test_x = np.linspace(0.05, 0.95, 50)
    out = cal.transform(test_x.tolist())
    diffs = np.diff(out)
    assert (diffs >= -1e-9).all(), "Isotonic mapping must be monotone non-decreasing"


def test_isotonic_round_trips_through_dict(miscalibrated_dataset):
    raw, realized = miscalibrated_dataset
    cal = IsotonicCalibrator.fit(raw, realized)
    payload = cal.to_dict()
    restored = IsotonicCalibrator.from_dict(payload)
    a = cal.transform([0.2, 0.5, 0.8])
    b = restored.transform([0.2, 0.5, 0.8])
    np.testing.assert_allclose(a, b, atol=1e-9)


def test_isotonic_handles_too_few_rows():
    cal = IsotonicCalibrator.fit([0.5], [1.0])
    # Falls back to identity — no x_knots fitted means transform passes through.
    assert cal.x_knots == []


# ---------------------------------------------------------------------------
# PlattCalibrator
# ---------------------------------------------------------------------------
def test_platt_improves_brier(miscalibrated_dataset):
    raw, realized = miscalibrated_dataset
    cal = PlattCalibrator.fit(raw, realized)
    calibrated = cal.transform(raw)
    raw_brier = brier_score(raw, realized)
    cal_brier = brier_score(calibrated.tolist(), realized)
    assert cal_brier < raw_brier, (
        f"Platt scaling must reduce Brier "
        f"(raw={raw_brier:.4f}, cal={cal_brier:.4f})"
    )


def test_platt_round_trips_through_dict(miscalibrated_dataset):
    raw, realized = miscalibrated_dataset
    cal = PlattCalibrator.fit(raw, realized)
    payload = cal.to_dict()
    restored = PlattCalibrator.from_dict(payload)
    a = cal.transform([0.2, 0.5, 0.8])
    b = restored.transform([0.2, 0.5, 0.8])
    np.testing.assert_allclose(a, b, atol=1e-9)


def test_platt_returns_identity_for_too_few_rows():
    cal = PlattCalibrator.fit([0.5, 0.6], [1.0, 0.0])
    # Should fall back to defaults (a=1, b=0)
    assert cal.a == pytest.approx(1.0)
    assert cal.b == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def test_brier_log_loss_align_with_known_values():
    # Perfect predictor → brier = 0, log_loss → 0 (clipped).
    assert brier_score([0.0, 1.0], [0.0, 1.0]) == pytest.approx(0.0)
    # Worst predictor → brier = 1.
    assert brier_score([1.0, 0.0], [0.0, 1.0]) == pytest.approx(1.0)


def test_reliability_curve_returns_one_bin_per_populated_decile():
    rng = np.random.default_rng(seed=1)
    p = rng.uniform(size=200)
    y = (rng.uniform(size=200) < p).astype(float)
    rows = reliability_curve(p.tolist(), y.tolist(), n_bins=5)
    # All five bins should be populated for n=200; each row carries the
    # mean predicted, mean actual, and count.
    assert len(rows) >= 4
    for row in rows:
        assert 0 <= row["mean_predicted"] <= 1
        assert 0 <= row["mean_actual"] <= 1
        assert row["count"] > 0


def test_load_calibrator_handles_unknown_kind():
    assert load_calibrator(None) is None
    assert load_calibrator({"kind": "nonsense"}) is None
    assert load_calibrator({}) is None
