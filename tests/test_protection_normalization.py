"""Strike-protection scale invariance.

``calculate_strike_protection`` used to weight S/R levels by 1/distance in
raw dollars, so the same structural setup scored ~30× apart between a $20
and a $600 underlying — systematically starving expensive symbols of
protection in the POP combiner and the WHEEL tilt. Distance is now measured
in percent-of-spot points, which coincides with the old dollar math at
spot=$100 (preserving the historical tuning) and is identical across price
scales everywhere else.
"""
from __future__ import annotations

import pytest

from hermes.ml.pop_engine import _PROTECTION_SCORE_CAP, calculate_strike_protection


def _setup(spot: float):
    """Support 3% below spot (strength 5); short strike 7% below spot."""
    levels = [{"price": spot * 0.97, "type": "support", "strength": 5}]
    short_strike = spot * 0.93
    return levels, short_strike


def test_same_structure_scores_identically_across_price_scales():
    scores = []
    for spot in (20.0, 100.0, 600.0):
        levels, short_strike = _setup(spot)
        scores.append(
            calculate_strike_protection(levels, spot, short_strike, "put_credit"))
    assert scores[0] == pytest.approx(scores[1], abs=1e-9)
    assert scores[1] == pytest.approx(scores[2], abs=1e-9)


def test_spot_100_matches_historical_dollar_tuning():
    # Support at 97 with spot 100: distance = 3 (pct points == dollars here),
    # raw = 5 × (1/3), score = 1 + 0.1 × 5/3.
    levels, short_strike = _setup(100.0)
    score = calculate_strike_protection(levels, 100.0, short_strike, "put_credit")
    assert score == pytest.approx(1.0 + 0.1 * (5.0 / 3.0), abs=1e-9)


def test_call_side_is_symmetric():
    spot = 600.0
    levels = [{"price": spot * 1.03, "type": "resistance", "strength": 5}]
    short_strike = spot * 1.07
    call_score = calculate_strike_protection(levels, spot, short_strike, "call_credit")
    put_levels, put_strike = _setup(spot)
    put_score = calculate_strike_protection(put_levels, spot, put_strike, "put_credit")
    assert call_score == pytest.approx(put_score, abs=1e-9)


def test_distance_floor_bounds_levels_hugging_spot():
    # A support 0.01% below spot saturates at the 0.1-pct-point floor, and
    # the saturated raw score (1 + 0.1×5/0.1 = 6.0 uncapped) clamps to the
    # score cap so it can't pin POP at ~1.0 through the combiner.
    spot = 600.0
    levels = [{"price": spot * 0.9999, "type": "support", "strength": 5}]
    score = calculate_strike_protection(levels, spot, spot * 0.93, "put_credit")
    assert score == pytest.approx(_PROTECTION_SCORE_CAP, abs=1e-9)


def test_score_cap_bounds_stacked_strong_clusters():
    # Several strong clusters inside the corridor must not push the score
    # past the cap no matter how they stack.
    spot = 100.0
    levels = [{"price": spot - d, "type": "support", "strength": 9}
              for d in (0.2, 0.5, 1.0, 1.5)]
    score = calculate_strike_protection(levels, spot, 93.0, "put_credit")
    assert score == pytest.approx(_PROTECTION_SCORE_CAP, abs=1e-9)


def test_typical_setup_stays_below_cap():
    # A single strength-5 support 3% out is nowhere near saturation — the
    # cap must not distort the ordinary scoring regime.
    levels, short_strike = _setup(100.0)
    score = calculate_strike_protection(levels, 100.0, short_strike, "put_credit")
    assert score < _PROTECTION_SCORE_CAP
    assert score == pytest.approx(1.0 + 0.1 * (5.0 / 3.0), abs=1e-9)


def test_levels_outside_the_corridor_contribute_nothing():
    spot = 100.0
    levels = [
        {"price": 90.0, "type": "support", "strength": 9},      # below short strike
        {"price": 103.0, "type": "resistance", "strength": 9},  # wrong side/type
    ]
    score = calculate_strike_protection(levels, spot, 93.0, "put_credit")
    assert score == pytest.approx(1.0, abs=1e-9)


def test_degenerate_spot_returns_baseline():
    levels = [{"price": 97.0, "type": "support", "strength": 5}]
    assert calculate_strike_protection(levels, 0.0, 93.0, "put_credit") == 1.0
    assert calculate_strike_protection(levels, float("nan"), 93.0, "put_credit") == 1.0
