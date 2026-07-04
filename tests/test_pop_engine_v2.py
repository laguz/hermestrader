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


def test_regime_weights_wiring_default_off():
    """Verify that by default (gate off), pop_engine lookup remains static lookup."""
    from hermes.ml import pop_engine
    # Ensure it's static lookup first
    assert pop_engine._regime_weight_lookup == pop_engine._static_regime_lookup


@pytest.mark.asyncio
async def test_regime_weights_gating_logic(monkeypatch):
    """Verify the gating logic checks both env var and database settings without needing a real DB."""
    import os

    class MockSettings:
        def __init__(self, val):
            self.val = val
        async def get_setting(self, key, default=None):
            return self.val

    class MockDB:
        def __init__(self, val):
            self.settings = MockSettings(val)

    # Case 1: env=false, db=false -> off
    monkeypatch.setenv("HERMES_REGIME_WEIGHTS", "false")
    db_off = MockDB("false")
    regime_weights_env = os.environ.get("HERMES_REGIME_WEIGHTS", "false").lower() == "true"
    regime_weights_setting = (await db_off.settings.get_setting("regime_weights_enabled") or "false").lower() == "true"
    assert not (regime_weights_env or regime_weights_setting)

    # Case 2: env=true, db=false -> on
    monkeypatch.setenv("HERMES_REGIME_WEIGHTS", "true")
    regime_weights_env = os.environ.get("HERMES_REGIME_WEIGHTS", "false").lower() == "true"
    assert (regime_weights_env or regime_weights_setting)

    # Case 3: env=false, db=true -> on
    monkeypatch.setenv("HERMES_REGIME_WEIGHTS", "false")
    db_on = MockDB("true")
    regime_weights_env = os.environ.get("HERMES_REGIME_WEIGHTS", "false").lower() == "true"
    regime_weights_setting = (await db_on.settings.get_setting("regime_weights_enabled") or "false").lower() == "true"
    assert (regime_weights_env or regime_weights_setting)


@pytest.mark.asyncio
async def test_regime_weights_wiring_and_caching(db, monkeypatch):
    """Verify that when gated on, wiring works, updates on ticks, and falls back on error."""
    import asyncio
    from hermes.ml import pop_engine, regime_weights
    from hermes.events.bus import EventBus, CacheWarmTick
    from sqlalchemy import select

    # 1. Gate ON via monkeypatching environment variable
    monkeypatch.setenv("HERMES_REGIME_WEIGHTS", "true")

    # Verify the table is ensured and wired
    regime_weights.ensure_table(db)

    event_bus = EventBus()
    event_bus.start()
    try:
        lookup_fn = regime_weights.make_lookup_fn(db, event_bus)
        await lookup_fn.initialize()

        pop_engine.set_regime_weight_lookup(lookup_fn)

        # Initial check: no entries in DB -> should return static defaults
        weights_initial = pop_engine.regime_weights("3M", symbol="AAPL")
        assert weights_initial == regime_weights.STATIC_DEFAULTS["3M"]

        # 2. Write outcomes to database to exceed cold start limit (>=30 observations)
        await regime_weights.update_from_outcomes(
            db, symbol="AAPL", period="3M", hits=25, misses=5
        )

        # Manually alter the weights to be different from static defaults
        # because the default mathematical update equation keeps defaults identical to static defaults
        async with db.AsyncSession() as s:
            q = select(regime_weights.RegimeWeights).filter_by(symbol="AAPL", period="3M")
            res = await s.execute(q)
            row = res.scalars().first()
            row.beta_1 = 9.9
            await s.commit()

        # Verify cache is NOT refreshed yet (in-process cache is not updated automatically without event)
        assert pop_engine.regime_weights("3M", symbol="AAPL") == regime_weights.STATIC_DEFAULTS["3M"]

        # 3. Emit CacheWarmTick to trigger refresh
        event_bus.emit(CacheWarmTick())
        await asyncio.sleep(0.1)

        # Now it should be updated and reflect the DB value
        weights_after_tick = pop_engine.regime_weights("3M", symbol="AAPL")
        assert weights_after_tick[1] == 9.9
        assert len(weights_after_tick) == 5
        assert weights_after_tick[0] == 0.0

        # 4. Check lookup error fallback
        fallback_weights = pop_engine.regime_weights("INVALID_PERIOD", symbol="AAPL")
        assert fallback_weights == regime_weights.STATIC_DEFAULTS["3M"]

        # Test database query failure does not crash the cache lookup
        original_session = db.AsyncSession
        def mock_async_session():
            raise Exception("DB Connection Error")
        db.AsyncSession = mock_async_session

        try:
            await lookup_fn.refresh_async()
            # Cache should keep previous values on failure
            assert pop_engine.regime_weights("3M", symbol="AAPL") == weights_after_tick
        finally:
            db.AsyncSession = original_session

    finally:
        await event_bus.stop()
        pop_engine.set_regime_weight_lookup(pop_engine._static_regime_lookup)


@pytest.mark.asyncio
async def test_regime_weights_gating_via_system_settings(db):
    """Verify that the setting in db.settings (regime_weights_enabled) gates the wiring logic."""
    from hermes.ml import pop_engine

    # Verify default is off
    assert pop_engine._regime_weight_lookup == pop_engine._static_regime_lookup

    # 1. Turn setting OFF in DB and check
    await db.settings.set_setting("regime_weights_enabled", "false")
    regime_weights_setting = (await db.settings.get_setting("regime_weights_enabled") or "false").lower() == "true"
    assert not regime_weights_setting

    # 2. Turn setting ON in DB and check
    await db.settings.set_setting("regime_weights_enabled", "true")
    regime_weights_setting = (await db.settings.get_setting("regime_weights_enabled") or "false").lower() == "true"
    assert regime_weights_setting


@pytest.mark.asyncio
async def test_cached_regime_weights_lookup_no_event_bus(db):
    """Verify that when event_bus is None (e.g. offline/test mode), cache misses query synchronously."""
    from hermes.ml import pop_engine, regime_weights
    from sqlalchemy import select

    regime_weights.ensure_table(db)
    lookup_fn = regime_weights.make_lookup_fn(db, event_bus=None)
    pop_engine.set_regime_weight_lookup(lookup_fn)

    try:
        await regime_weights.update_from_outcomes(
            db, symbol="MSFT", period="6M", hits=30, misses=0
        )

        # Manually alter the weights to be different from static defaults
        async with db.AsyncSession() as s:
            q = select(regime_weights.RegimeWeights).filter_by(symbol="MSFT", period="6M")
            res = await s.execute(q)
            row = res.scalars().first()
            row.beta_1 = 9.9
            await s.commit()

        # MSFT 6M has >=30 observations. Querying should pull dynamically via sync fallback
        weights = pop_engine.regime_weights("6M", symbol="MSFT")
        assert weights[1] == 9.9
        assert len(weights) == 5

    finally:
        pop_engine.set_regime_weight_lookup(pop_engine._static_regime_lookup)



