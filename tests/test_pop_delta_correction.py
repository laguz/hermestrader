"""Lognormal delta→POP correction (d1→d2 inversion).

``1 − |Δ|`` treats the option's delta — an N(d1) quantity — as if it were
the expiry-ITM probability, which under the same Black-Scholes model is
N(d2) = N(d1 − σ√t). The gap is systematic, not noise: it overstates
put-side POP and understates call-side POP by ~3–5 points at typical
vol/DTE, exactly the asymmetry of the lognormal. When a caller supplies
``dte`` (and ideally the option's own IV as ``sigma``), the engine now
inverts the relation:

    P(OTM) = 1 − Φ(Φ⁻¹(|Δ|) + σ√t)   puts
    P(OTM) = 1 − Φ(Φ⁻¹(|Δ|) − σ√t)   calls

Without ``dte`` behaviour is byte-identical to the historical linear
baseline, so every legacy caller (dashboard overlay, regime scoring,
positional shim) is untouched.
"""
from __future__ import annotations

import math

import pytest
from scipy.stats import norm

from hermes.ml.pop_engine import FeatureVector, delta_implied_p_otm, predict_pop


def _fv(delta: float, **overrides) -> FeatureVector:
    base = dict(delta=delta, xgb_prob=0.5, current_vol=0.25, avg_vol=0.25,
                protection_score=1.0)
    base.update(overrides)
    return FeatureVector(**base)


def _closed_form(delta: float, sigma: float, dte: float, side: str) -> float:
    shift = sigma * math.sqrt(dte / 365.0)
    sign = 1.0 if side == "put" else -1.0
    return 1.0 - float(norm.cdf(norm.ppf(delta) + sign * shift))


# ── baseline compatibility ───────────────────────────────────────────────────
def test_no_dte_keeps_linear_baseline():
    for delta in (0.10, 0.20, 0.25, 0.4338):
        assert delta_implied_p_otm(_fv(delta)) == pytest.approx(1.0 - delta)
        # And through the full combiner with neutral inputs:
        assert predict_pop(_fv(delta)) == pytest.approx(1.0 - delta, abs=1e-9)


def test_degenerate_dte_or_sigma_falls_back_to_linear():
    assert delta_implied_p_otm(_fv(0.25, dte=0.0)) == pytest.approx(0.75)
    assert delta_implied_p_otm(_fv(0.25, dte=-3.0)) == pytest.approx(0.75)
    assert delta_implied_p_otm(_fv(0.25, dte=float("nan"))) == pytest.approx(0.75)
    assert delta_implied_p_otm(
        _fv(0.25, dte=45.0, sigma=0.0, current_vol=0.0)) == pytest.approx(0.75)
    assert delta_implied_p_otm(
        _fv(0.25, dte=45.0, sigma=float("nan"), current_vol=-1.0)) == pytest.approx(0.75)


# ── the correction itself ────────────────────────────────────────────────────
def test_put_pop_sits_below_linear_baseline():
    fv = _fv(0.25, side="put", dte=45.0, sigma=0.25)
    p = delta_implied_p_otm(fv)
    assert p == pytest.approx(_closed_form(0.25, 0.25, 45.0, "put"), abs=1e-9)
    assert p < 0.75                       # linear said exactly 0.75
    assert p == pytest.approx(0.7213, abs=1e-3)


def test_call_pop_sits_above_linear_baseline():
    fv = _fv(0.25, side="call", dte=45.0, sigma=0.25)
    p = delta_implied_p_otm(fv)
    assert p == pytest.approx(_closed_form(0.25, 0.25, 45.0, "call"), abs=1e-9)
    assert p > 0.75
    assert p == pytest.approx(0.7771, abs=1e-3)


def test_correction_grows_with_vol_and_dte():
    base = delta_implied_p_otm(_fv(0.25, side="put", dte=7.0, sigma=0.20))
    longer = delta_implied_p_otm(_fv(0.25, side="put", dte=45.0, sigma=0.20))
    wilder = delta_implied_p_otm(_fv(0.25, side="put", dte=45.0, sigma=0.60))
    assert 0.75 > base > longer > wilder


def test_option_iv_preferred_over_realized_vol():
    with_iv = delta_implied_p_otm(
        _fv(0.25, side="put", dte=45.0, sigma=0.60, current_vol=0.20))
    from_rv = delta_implied_p_otm(
        _fv(0.25, side="put", dte=45.0, sigma=None, current_vol=0.20))
    assert with_iv == pytest.approx(_closed_form(0.25, 0.60, 45.0, "put"), abs=1e-9)
    assert from_rv == pytest.approx(_closed_form(0.25, 0.20, 45.0, "put"), abs=1e-9)
    assert with_iv < from_rv


def test_predict_pop_neutral_inputs_equal_corrected_baseline():
    """The combiner honesty contract, dte-aware form: with neutral XGB,
    vol ratio 1 and baseline protection, POP equals the d2-implied
    probability exactly — no optimism offset on top of the correction."""
    fv = _fv(0.30, side="put", dte=30.0, sigma=0.25)
    assert predict_pop(fv) == pytest.approx(
        _closed_form(0.30, 0.25, 30.0, "put"), abs=1e-9)


def test_band_propagates_dte_fields():
    from hermes.ml.pop_engine import predict_pop_with_band
    fv = _fv(0.25, side="put", dte=45.0, sigma=0.25,
             xgb_prob=0.6, xgb_prob_lo=0.4, xgb_prob_hi=0.8)
    band = predict_pop_with_band(fv)
    assert band["pop_lo"] <= band["pop"] <= band["pop_hi"]
