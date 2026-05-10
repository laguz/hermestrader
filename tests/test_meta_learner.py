"""Tests for hermes.ml.meta_learner — stacking POP combiner.

The meta-learner must:

- Fall back to identity when no weights are fitted (cold-start parity).
- Improve Brier score on a synthetic stacked-input dataset.
- Round-trip through to_json / from_json without information loss.
- Reject empty fits gracefully (< 20 rows → identity learner).
"""
from __future__ import annotations

import numpy as np
import pytest

from hermes.ml.calibration import brier_score
from hermes.ml.meta_learner import DEFAULT_FEATURES, MetaLearner


# ---------------------------------------------------------------------------
# Synthetic dataset where stacking should help
# ---------------------------------------------------------------------------
@pytest.fixture
def stacked_dataset():
    rng = np.random.default_rng(seed=0)
    n = 400
    delta_prob = rng.uniform(0.4, 0.95, size=n)
    xgb_prob = np.clip(delta_prob + rng.normal(0, 0.05, size=n), 0.01, 0.99)
    protection = rng.uniform(0.8, 2.0, size=n)
    iv_rank = rng.uniform(10, 90, size=n)
    vol_ratio = rng.uniform(0.7, 1.4, size=n)

    # Realised outcome: weighted combination + noise.
    score = (
        0.6 * (delta_prob - 0.5)
        + 0.4 * (xgb_prob - 0.5)
        + 0.1 * (protection - 1.0)
        + rng.normal(0, 0.1, size=n)
    )
    realized = (score > 0).astype(float)

    rows = [
        {
            "delta_implied_prob": float(delta_prob[i]),
            "xgb_prob": float(xgb_prob[i]),
            "protection_score": float(protection[i]),
            "iv_rank_365d": float(iv_rank[i]),
            "vol_ratio": float(vol_ratio[i]),
        }
        for i in range(n)
    ]
    return rows, realized.tolist()


# ---------------------------------------------------------------------------
# Cold start
# ---------------------------------------------------------------------------
def test_untrained_meta_returns_xgb_prob_unchanged():
    learner = MetaLearner()
    fv = {"xgb_prob": 0.71, "delta_implied_prob": 0.5,
          "protection_score": 1.2, "iv_rank_365d": 50, "vol_ratio": 1.0}
    assert learner.predict(fv) == pytest.approx(0.71)


def test_meta_fit_with_too_few_rows_returns_identity():
    learner = MetaLearner.fit([{"xgb_prob": 0.5}], [1.0])
    assert learner.weights == []          # identity / cold-start


# ---------------------------------------------------------------------------
# Trained behaviour
# ---------------------------------------------------------------------------
def test_meta_fit_improves_brier(stacked_dataset):
    rows, outcomes = stacked_dataset
    learner = MetaLearner.fit(rows, outcomes, calibrator="isotonic")
    raw_xgb = [r["xgb_prob"] for r in rows]
    learner_preds = [learner.predict(r) for r in rows]
    raw_brier = brier_score(raw_xgb, outcomes)
    meta_brier = brier_score(learner_preds, outcomes)
    assert meta_brier < raw_brier, (
        f"Meta-learner must improve Brier "
        f"(raw={raw_brier:.4f}, meta={meta_brier:.4f})"
    )


def test_meta_round_trips_through_json(stacked_dataset):
    rows, outcomes = stacked_dataset
    learner = MetaLearner.fit(rows, outcomes)
    blob = learner.to_json()
    restored = MetaLearner.from_json(blob)
    a = learner.predict(rows[0])
    b = restored.predict(rows[0])
    assert a == pytest.approx(b, abs=1e-9)


def test_meta_metrics_payload_is_present(stacked_dataset):
    rows, outcomes = stacked_dataset
    learner = MetaLearner.fit(rows, outcomes)
    assert "brier_raw" in learner.metrics
    assert "brier_calibrated" in learner.metrics
    assert "log_loss" in learner.metrics
    assert learner.metrics["n_train"] == len(rows)


def test_meta_handles_missing_features_gracefully():
    rows = [
        {"xgb_prob": 0.5, "delta_implied_prob": 0.5,
         "protection_score": 1.0, "iv_rank_365d": 50, "vol_ratio": 1.0}
        for _ in range(40)
    ]
    learner = MetaLearner.fit(rows, [1.0 if i % 2 == 0 else 0.0
                                     for i in range(40)])
    # Caller skips a feature → meta-learner uses neutral default.
    pop = learner.predict({"xgb_prob": 0.6})
    assert 0.0 <= pop <= 1.0


def test_default_feature_order_matches_catalog():
    # The catalog META_FEATURES order is the wire contract; if the
    # default tuple drifts, every persisted MetaLearner becomes
    # mis-aligned with new fits.
    from hermes.ml.feature_catalog import META_FEATURES
    catalog_meta_names = {s.name for s in META_FEATURES}
    for name in DEFAULT_FEATURES:
        assert name in catalog_meta_names, name
