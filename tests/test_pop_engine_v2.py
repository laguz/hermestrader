"""Tests for the refactored pop_engine — FeatureVector API and confidence bands.

Covers:
- predict_pop returns a probability ∈ [0, 1] for typical inputs.
- predict_pop is the chain-only log-odds combiner (no XGB / meta-learner).
- predict_pop_with_band emits monotonically ordered (pop_lo, pop, pop_hi).
- augment_levels_with_pop drops the magic 0.5+return*5 mapping and
  uses the BS-style CDF transform when ``predicted_prob`` is absent.
- The legacy positional ``predict_single_pop`` shim still works.
- generate_regime_pops scores each horizon against its own xgb_pred.
"""
from __future__ import annotations

import math
from typing import List

import pytest

from hermes.ml import pop_engine
from hermes.ml.pop_engine import (
    FeatureVector,
    DEFAULT_REGIME_WEIGHTS,
    augment_levels_with_pop,
    generate_regime_pops,
    predict_pop,
    predict_pop_with_band,
    predict_single_pop,
)


# ---------------------------------------------------------------------------
# predict_pop
# ---------------------------------------------------------------------------
def test_predict_pop_returns_valid_probability():
    fv = FeatureVector(
        delta=0.20, xgb_prob=0.7, current_vol=0.25, avg_vol=0.22,
        protection_score=1.4, iv_rank=55, side="put", period="3M",
    )
    pop = predict_pop(fv)
    assert 0.0 <= pop <= 1.0


def test_predict_pop_is_the_legacy_combiner():
    """Chain-only POP: predict_pop is exactly the log-odds combiner."""
    fv = FeatureVector(delta=0.20, xgb_prob=0.7, side="put", period="3M")
    legacy = pop_engine._legacy_combiner(fv)
    assert predict_pop(fv) == pytest.approx(legacy)


# ---------------------------------------------------------------------------
# Confidence bands
# ---------------------------------------------------------------------------
def test_band_collapses_when_quantile_heads_missing():
    fv = FeatureVector(delta=0.2, xgb_prob=0.7, side="put", period="3M")
    band = predict_pop_with_band(fv)
    assert band["pop_lo"] == band["pop"] == band["pop_hi"]


def test_band_is_ordered_low_mid_high():
    fv = FeatureVector(
        delta=0.2, xgb_prob=0.65,
        xgb_prob_lo=0.45, xgb_prob_hi=0.85,
        side="put", period="3M",
    )
    band = predict_pop_with_band(fv)
    assert band["pop_lo"] <= band["pop"] <= band["pop_hi"]


def test_band_handles_inverted_quantile_inputs():
    """Defensive: even if a buggy upstream sends lo > hi we should not panic."""
    fv = FeatureVector(
        delta=0.2, xgb_prob=0.65,
        xgb_prob_lo=0.95, xgb_prob_hi=0.35,    # intentionally inverted
        side="put", period="3M",
    )
    band = predict_pop_with_band(fv)
    assert band["pop_lo"] <= band["pop_hi"]


# ---------------------------------------------------------------------------
# augment_levels_with_pop
# ---------------------------------------------------------------------------
def test_augment_uses_predicted_prob_when_supplied():
    analysis = {
        "current_price": 100.0,
        "current_vol": 0.30,
        "avg_vol": 0.25,
        "key_levels": [
            {"price": 95.0, "type": "support", "strength": 3},
            {"price": 105.0, "type": "resistance", "strength": 2},
        ],
    }
    xgb_pred = {
        "predicted_prob": 0.72,
        "predicted_prob_lo": 0.55,
        "predicted_prob_hi": 0.85,
    }
    out = augment_levels_with_pop(analysis, xgb_pred, period="6m")
    for level in out["key_levels"]:
        assert "pop" in level and "pop_lo" in level and "pop_hi" in level
        assert 0.0 <= level["pop"] <= 1.0
        assert level["pop_lo"] <= level["pop_hi"]


def test_augment_falls_back_to_return_cdf_without_predicted_prob():
    analysis = {
        "current_price": 100.0,
        "current_vol": 0.30,
        "avg_vol": 0.25,
        "key_levels": [{"price": 95.0, "type": "support", "strength": 1}],
    }
    # No predicted_prob — only the legacy predicted_return field. The new
    # code maps it through a vol-aware CDF, NOT 0.5 + return*5.
    out = augment_levels_with_pop(analysis, {"predicted_return": 0.01},
                                   period="3m")
    pop = out["key_levels"][0]["pop"]
    assert 0.0 <= pop <= 1.0
    # Verify the new behaviour is NOT the v1 mapping. v1 would have
    # delivered xgb_prob = clip(0.5 + 0.01*5) = 0.55 directly. The new
    # CDF mapping with vol≈0.30 and default horizon=7 produces a much lower number.
    horizon_dte = 7
    sigma_horizon = 0.30 * math.sqrt(horizon_dte / 365.0)
    from scipy.stats import norm
    cdf_prob = float(norm.cdf(0.01 / sigma_horizon))
    assert abs(cdf_prob - 0.55) > 0.02, (
        "Expected new vol-aware CDF mapping, not the legacy 0.5+r*5"
    )


def test_augment_handles_missing_xgb_pred():
    analysis = {
        "current_price": 100.0,
        "current_vol": 0.30,
        "avg_vol": 0.25,
        "key_levels": [{"price": 95.0, "type": "support", "strength": 1}],
    }
    # Empty xgb_pred → neutral 0.5 probability flows through the combiner.
    out = augment_levels_with_pop(analysis, {}, period="3m")
    assert "pop" in out["key_levels"][0]


def test_augment_returns_unchanged_for_invalid_price():
    analysis = {
        "current_price": 0.0,                      # invalid sentinel
        "current_vol": 0.30,
        "key_levels": [{"price": 95.0, "type": "support"}],
    }
    out = augment_levels_with_pop(analysis, {})
    # Should not raise, and key levels should NOT have a pop attached
    # because the early-return guard fired.
    assert "pop" not in out["key_levels"][0]


# ---------------------------------------------------------------------------
# Legacy positional shim
# ---------------------------------------------------------------------------
def test_legacy_predict_single_pop_signature_still_works():
    pop = predict_single_pop(
        delta=0.20, current_vol=0.25, avg_vol=0.22,
        xgb_prob=0.7, protection_score=1.2,
        weights=DEFAULT_REGIME_WEIGHTS["3M"], side="put",
    )
    assert 0.0 <= pop <= 1.0


def test_generate_regime_pops_uses_per_horizon_xgb():
    out = generate_regime_pops(
        delta=0.2, current_vol=0.25, vol_sma_21=0.22, protection_score=1.2,
        xgb_preds={"3M": 0.7, "6M": 0.5, "1Y": 0.3},
        side="put",
    )
    assert set(out.keys()) == {"3M", "6M", "1Y"}
    # The 3M horizon used 0.7, the 1Y horizon used 0.3 — the resulting
    # POPs should be ordered the same way.
    assert out["3M"] > out["1Y"]


def test_per_symbol_regime_weights():
    """A wired regime-weight lookup can vary weights per symbol; unknown
    symbols fall back to the static defaults."""
    custom_weights = {"3M": [1.0, 2.0, 3.0, 4.0, 5.0]}

    def mock_lookup(period: str, symbol: str = "DEFAULT") -> List[float]:
        if symbol == "AAPL" and period == "3M":
            return custom_weights["3M"]
        return DEFAULT_REGIME_WEIGHTS.get(period.upper(), DEFAULT_REGIME_WEIGHTS["3M"])

    pop_engine.set_regime_weight_lookup(mock_lookup)
    try:
        # AAPL should return the customized weights
        assert pop_engine.regime_weights("3M", symbol="AAPL") == [1.0, 2.0, 3.0, 4.0, 5.0]
        # Another symbol should fall back to default
        assert pop_engine.regime_weights("3M", symbol="TSLA") == DEFAULT_REGIME_WEIGHTS["3M"]
    finally:
        # Restore the static lookup to avoid leaking state to other tests.
        pop_engine.set_regime_weight_lookup(pop_engine._static_regime_lookup)
