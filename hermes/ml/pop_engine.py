"""
[POP-Engine v2]
Probability-of-profit (POP) calculation for credit-spread strategies.

This module is the *consumer-facing* probability surface. It accepts a
structured FeatureVector (built per-symbol at decision time) and
returns a calibrated POP for each key support / resistance level.

What changed from v1
--------------------
- ``predict_single_pop`` now accepts a FeatureVector instead of five
  positional floats. The legacy positional signature is preserved as a
  thin shim so existing strategies (cs7/cs75/wheel/tt45) keep working
  while they migrate.
- The 0.5 + return*5 magic mapping that used to live in
  ``augment_levels_with_pop`` is gone. We now ask either:
    a) the meta-learner (when one is fitted), or
    b) the legacy log-odds combiner with database-backed regime weights.
- Confidence bands (``pop_lo`` / ``pop_hi``) propagate quantile-head
  predictions through the same combiner so the dashboard can render
  uncertainty (rec #20).
- ``calculate_strike_protection`` now logs its raw-score histogram so
  silent saturation against the 0.1 distance floor is observable.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence

import numpy as np
import pandas as pd
from scipy.signal import argrelextrema
from scipy.stats import norm
from sklearn.cluster import KMeans

from hermes.ml.meta_learner import MetaLearner

logger = logging.getLogger("hermes.ml.pop")


# ---------------------------------------------------------------------------
# Regime weights — database-backed when available, with a static fallback.
# ---------------------------------------------------------------------------
DEFAULT_REGIME_WEIGHTS: Dict[str, List[float]] = {
    # [β0 (Intercept), β1 (Delta), β2 (XGB), β3 (Vol), β4 (Protection)]
    "3M": [0.0, 1.0, 0.6, 0.3, 0.4],
    "6M": [0.0, 1.0, 0.6, 0.3, 0.4],
    "1Y": [0.0, 1.0, 0.6, 0.3, 0.4],
}


# Pluggable accessor — set by the watcher boot when a HermesDB session
# is available (see hermes.service2_watcher.api.AppState). Defaults to
# the static dict so unit tests don't need a database.
def _static_regime_lookup(period: str, symbol: str = "DEFAULT") -> List[float]:
    return DEFAULT_REGIME_WEIGHTS.get(period.upper(), DEFAULT_REGIME_WEIGHTS["3M"])


_regime_weight_lookup: Any = _static_regime_lookup


def set_regime_weight_lookup(fn: Any) -> None:
    """Wire a database-backed accessor (called once on watcher boot)."""
    global _regime_weight_lookup
    _regime_weight_lookup = fn


def regime_weights(period: str, symbol: str = "DEFAULT") -> List[float]:
    try:
        return _regime_weight_lookup(period, symbol)
    except TypeError:
        return _regime_weight_lookup(period)


# ---------------------------------------------------------------------------
# Feature vector — the consumer-facing input contract.
# ---------------------------------------------------------------------------
@dataclass
class FeatureVector:
    """Feature inputs required to score a single credit-spread level.

    The dataclass format guarantees that all callers pass identical keys,
    avoiding key-mismatch runtime errors (FACT #20).
    """

    delta: float
    xgb_prob: float
    current_vol: float = 0.30
    avg_vol: float = 0.25
    protection_score: float = 1.0
    iv_rank: float = 50.0
    xgb_prob_lo: Optional[float] = None
    xgb_prob_hi: Optional[float] = None
    side: str = "put"
    period: str = "3M"
    symbol: str = "DEFAULT"
    # Days to expiry of the scored option and its own annualized IV.
    # When both are usable the delta baseline uses the lognormal d1→d2
    # inversion (see ``delta_implied_p_otm``); when absent the engine
    # keeps the historical linear 1-|delta| baseline.
    dte: Optional[float] = None
    sigma: Optional[float] = None

    def to_meta_dict(self) -> Dict[str, float]:
        """Project the features into the meta-learner's input space."""
        adjusted_xgb = float(self.xgb_prob) if self.side == "put" else (1.0 - float(self.xgb_prob))
        return {
            "delta_implied_prob": 1.0 - abs(float(self.delta)),
            "xgb_prob": adjusted_xgb,
            "protection_score": float(self.protection_score),
            "iv_rank_365d": float(self.iv_rank),
            "vol_ratio": float(self.current_vol) / max(float(self.avg_vol), 1e-5),
        }


# ---------------------------------------------------------------------------
# Active meta-learners — settable by the watcher; maps symbol -> MetaLearner.
# ---------------------------------------------------------------------------
_meta_learners: Dict[str, MetaLearner] = {}


def set_meta_learner(model: Optional[MetaLearner], symbol: str = "DEFAULT") -> None:
    """Install a fitted meta-learner for a specific symbol. Pass None to disable stacking."""
    global _meta_learners
    key = symbol.upper()
    if model is None:
        if key in _meta_learners:
            del _meta_learners[key]
    else:
        _meta_learners[key] = model


def get_meta_learner(symbol: str = "DEFAULT") -> MetaLearner:
    return _meta_learners.get(symbol.upper(), MetaLearner())



# ---------------------------------------------------------------------------
# Outcome calibrator — fitted on the book's own closed trades (predicted POP
# vs realized win/loss) and installed by the agent's heartbeat. Anything with
# a ``.transform(seq) -> array`` surface works (PlattCalibrator in practice).
# ---------------------------------------------------------------------------
_pop_calibrator: Optional[Any] = None


def set_pop_calibrator(calibrator: Optional[Any]) -> None:
    """Install the outcome-fitted POP calibrator. Pass None to clear."""
    global _pop_calibrator
    _pop_calibrator = calibrator


def get_pop_calibrator() -> Optional[Any]:
    return _pop_calibrator


# ---------------------------------------------------------------------------
# Key-level discovery (unchanged from v1, just better-tested)
# ---------------------------------------------------------------------------
def find_key_levels(
    close_series: pd.Series,
    volume_series: pd.Series,
    *,
    window: int = 5,
    n_clusters: int = 6,
) -> List[Dict[str, Any]]:
    """Find S/R levels using K-Means clustering on local pivots.

    Recommendation #13 (volume-profile point-of-control replacement) is
    queued in a follow-up PR. We retain KMeans here for surface-area
    parity with v1 so the migration is bisectable.
    """
    prices = close_series.values
    volumes = volume_series.values
    n = len(prices)
    if n == 0:
        return []

    current_price = float(prices[-1])

    max_idx = argrelextrema(prices, np.greater, order=window)[0]
    min_idx = argrelextrema(prices, np.less, order=window)[0]
    all_pivots_idx = np.sort(np.concatenate((max_idx, min_idx)))
    if len(all_pivots_idx) == 0:
        return []

    pivot_data = pd.DataFrame({
        "index": all_pivots_idx,
        "price": prices[all_pivots_idx],
        "volume": volumes[all_pivots_idx],
    })

    X = pivot_data[["price"]].values
    k = min(n_clusters, len(pivot_data))
    if k == 0:
        return []

    kmeans = KMeans(n_clusters=k, n_init=10, random_state=42)
    pivot_data["cluster"] = kmeans.fit_predict(X)

    out: List[Dict[str, Any]] = []
    for cluster_id in range(k):
        pts = pivot_data[pivot_data["cluster"] == cluster_id].copy()
        pts["weight"] = pts["volume"] * ((pts["index"] / max(n, 1)) ** 2)
        total = pts["weight"].sum()
        if total == 0:
            continue
        avg = (pts["price"] * pts["weight"]).sum() / total
        out.append({
            "price": float(avg),
            "type": "support" if avg < current_price else "resistance",
            "strength": int(len(pts)),
        })
    return out


def wilder_atr(df: pd.DataFrame, period: int = 14) -> Optional[float]:
    """Wilder ATR over the last ``period`` daily bars of an OHLC frame.

    Same math as DS0's entry-range ATR (``strategies/ds0.py::_atr``): true
    range includes overnight gaps (max of high−low, |high−prev close|,
    |low−prev close|); the seed is the simple mean of the first ``period``
    TRs, then Wilder smoothing over the rest. The frame must be sorted
    ascending by date and contain only the bars to include — callers drop
    today's partial bar themselves. Returns ``None`` when the history is
    too short or invalid rather than guessing.
    """
    if period < 1 or df is None or df.empty:
        return None
    cols = {"high", "low", "close"}
    if not cols.issubset(df.columns):
        return None
    highs = pd.to_numeric(df["high"], errors="coerce")
    lows = pd.to_numeric(df["low"], errors="coerce")
    closes = pd.to_numeric(df["close"], errors="coerce")
    ok = highs.notna() & lows.notna() & closes.notna()
    highs, lows, closes = highs[ok].values, lows[ok].values, closes[ok].values
    if len(closes) < period + 1:
        return None
    trs: List[float] = []
    prev_close = float(closes[0])
    for high, low, close in zip(highs[1:], lows[1:], closes[1:], strict=True):
        trs.append(max(float(high) - float(low),
                       abs(float(high) - prev_close),
                       abs(float(low) - prev_close)))
        prev_close = float(close)
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr if atr > 0 else None


# ---------------------------------------------------------------------------
# Strike protection
# ---------------------------------------------------------------------------
_PROTECTION_DISTANCE_FLOOR = 0.1   # percent-of-spot points; exposed for tests (rec #19)

# Upper bound on the protection score. The raw 1/distance sum is unbounded:
# a strength-9 cluster hugging spot saturates every level at the distance
# floor and adds β4·(10-1) ≈ +3.6 log-odds — POP pins near 1.0 regardless of
# delta, and the entry gate stops gating. Capping at 3.0 bounds the boost to
# β4·2 (~+13 POP points at default weights): a strong-but-plausible reading
# (e.g. two strength-10 levels 1% from spot) hits the cap; anything beyond it
# is treated as saturation, not extra signal.
_PROTECTION_SCORE_CAP = 3.0


def calculate_strike_protection(
    key_levels: Sequence[Mapping[str, Any]],
    current_price: float,
    short_strike: float,
    spread_type: str,
) -> float:
    """Numerical score representing how well a short strike is protected
    by S/R clusters. ``>=1.0`` means baseline protection; capped at
    ``_PROTECTION_SCORE_CAP`` so saturated clusters can't pin POP at 1.0.

    Level distance is measured in **percent-of-spot points**
    (``100 × |spot − level| / spot``), not raw dollars — a support 1%
    below spot means the same thing on a $20 stock as on a $600 stock.
    In dollar units the same structure scored ~30× apart by share price,
    systematically starving expensive underlyings of protection. At
    spot=$100 the two units coincide, so scores match the historical
    dollar-based tuning there. (σ√t-normalisation — pricing the distance
    in vol units — is the further refinement, but needs vol+DTE plumbed
    into every caller.)

    Logs the raw score so silent saturation against the distance floor
    becomes observable in production (rec #19).
    """
    if not np.isfinite(current_price) or current_price <= 0:
        return 1.0
    pct = 100.0 / current_price
    raw_score = 0.0
    for level in key_levels:
        ltype = level.get("type")
        price = float(level.get("price", 0.0))
        strength = float(level.get("strength", 1))
        if spread_type == "put_credit" and ltype == "support":
            if short_strike < price < current_price:
                distance = max((current_price - price) * pct, _PROTECTION_DISTANCE_FLOOR)
                raw_score += strength * (1.0 / distance)
        elif spread_type == "call_credit" and ltype == "resistance":
            if current_price < price < short_strike:
                distance = max((price - current_price) * pct, _PROTECTION_DISTANCE_FLOOR)
                raw_score += strength * (1.0 / distance)

    score = 1.0 + min(raw_score * 0.1, _PROTECTION_SCORE_CAP - 1.0)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "strike_protection raw=%.4f score=%.4f spread=%s short=%.4f spot=%.4f",
            raw_score, score, spread_type, short_strike, current_price,
        )
    return score


# ---------------------------------------------------------------------------
# Logistic combiner (legacy path, retained as fallback)
# ---------------------------------------------------------------------------
def calculate_log_odds(probability: float) -> float:
    p = float(np.clip(probability, 0.01, 0.99))
    return float(np.log(p / (1 - p)))


def delta_implied_p_otm(fv: FeatureVector) -> float:
    """Market-implied P(short strike expires OTM) from the option's delta.

    ``1 - |delta|`` treats delta — an N(d1) quantity — as if it were the
    expiry-ITM probability, which is N(d2) = N(d1 - σ√t) under the same
    Black-Scholes model the delta came from. Because d2 < d1, the linear
    form systematically overstates put-side POP and understates call-side
    POP (≈3-5 points at 25Δ / 25-vol / 45DTE) — enough to admit put
    entries that are genuinely below ``pop_target``. When the caller
    supplies ``dte`` (and ideally ``sigma``, the scored option's own IV;
    falls back to ``current_vol``) we invert the relation instead:

        P(OTM) = 1 - Φ(Φ⁻¹(|Δ|) + σ√t)   for puts
        P(OTM) = 1 - Φ(Φ⁻¹(|Δ|) - σ√t)   for calls

    Without ``dte`` this returns the historical ``1 - |delta|`` so
    existing callers (dashboard overlay, regime scoring, shims) are
    unchanged. The meta-learner's ``delta_implied_prob`` feature keeps
    the linear form on purpose — its training rows are built that way.
    """
    d = abs(float(fv.delta))
    p_linear = float(np.clip(1.0 - d, 0.01, 0.99))
    if fv.dte is None or not np.isfinite(fv.dte) or fv.dte <= 0:
        return p_linear
    sigma = fv.sigma
    if sigma is None or not np.isfinite(sigma) or sigma <= 0:
        sigma = fv.current_vol
    if sigma is None or not np.isfinite(sigma) or sigma <= 0:
        return p_linear
    shift = float(sigma) * math.sqrt(float(fv.dte) / 365.0)
    sign = 1.0 if fv.side == "put" else -1.0
    d_clipped = float(np.clip(d, 0.01, 0.99))
    p_itm = float(norm.cdf(norm.ppf(d_clipped) + sign * shift))
    return float(np.clip(1.0 - p_itm, 0.01, 0.99))


def _legacy_combiner(fv: FeatureVector) -> float:
    # The vol-ratio and protection terms are *centered* on their neutral
    # values (rv=1, protection=1) so they contribute zero log-odds in a
    # neutral market. Uncentered, they added a constant ~+0.7 log-odds that
    # systematically inflated POP ~10-20 points above the delta-implied
    # probability (a 43Δ short scored 76% "POP" in production), pushing the
    # strategies into closer strikes than their pop_target intends. With
    # neutral inputs (xgb=0.5, rv=1, prot=1) POP equals the delta-implied
    # probability exactly: 1-|delta| without dte, the d2 inversion with it.
    p_base = delta_implied_p_otm(fv)
    # max() not +epsilon in the denominator, matching to_meta_dict: equal
    # vols must give rv == 1.0 exactly so the centered term is truly zero.
    rv = fv.current_vol / max(fv.avg_vol, 1e-5)
    l_base = calculate_log_odds(p_base)
    l_xgb = calculate_log_odds(fv.xgb_prob)
    if fv.side == "call":
        l_xgb = -l_xgb
    weights = regime_weights(fv.period, fv.symbol)
    beta_0, beta_1, beta_2, beta_3, beta_4 = weights
    score = (
        beta_0
        + beta_1 * l_base
        + beta_2 * l_xgb
        + beta_3 * (rv - 1.0)
        + beta_4 * (fv.protection_score - 1.0)
    )
    return float(1.0 / (1.0 + math.exp(-max(min(score, 30.0), -30.0))))


# ---------------------------------------------------------------------------
# Public API — feature-vector form
# ---------------------------------------------------------------------------
def predict_pop(fv: FeatureVector) -> float:
    """Score a single FeatureVector.

    When a fitted meta-learner is installed, we delegate to it; the
    meta-learner has already absorbed the equivalent of the legacy
    weights through training. When the meta-learner is the cold-start
    identity, we fall through to the legacy log-odds combiner so the
    transition is bisectable and behaviour-preserving.

    The outcome calibrator (fitted on the book's own closed trades) is
    applied last, whichever path scored: it maps "engine said p" onto the
    realized win rate at that p. Refit continuously from live outcomes,
    it converges toward identity as the underlying scorer gets honest —
    so stacking it on an already-calibrated scorer is safe.
    """
    meta = get_meta_learner(fv.symbol)
    if meta.weights:
        pop = float(meta.predict(fv.to_meta_dict()))
    else:
        pop = _legacy_combiner(fv)
    calibrator = _pop_calibrator
    if calibrator is not None:
        pop = float(np.clip(calibrator.transform([pop])[0], 0.01, 0.99))
    return pop


def predict_pop_with_band(fv: FeatureVector) -> Dict[str, float]:
    """Return ``{pop, pop_lo, pop_hi}`` propagating quantile uncertainty.

    When ``xgb_prob_lo`` / ``xgb_prob_hi`` are absent the band
    collapses to the point estimate.
    """
    pop = predict_pop(fv)
    pop_lo = pop_hi = pop
    if fv.xgb_prob_lo is not None:
        lo = FeatureVector(**{**fv.__dict__, "xgb_prob": fv.xgb_prob_lo})
        pop_lo = predict_pop(lo)
    if fv.xgb_prob_hi is not None:
        hi = FeatureVector(**{**fv.__dict__, "xgb_prob": fv.xgb_prob_hi})
        pop_hi = predict_pop(hi)
    if pop_lo > pop_hi:
        pop_lo, pop_hi = pop_hi, pop_lo
    return {"pop": pop, "pop_lo": pop_lo, "pop_hi": pop_hi}


# ---------------------------------------------------------------------------
# Backwards-compatible positional API used by existing strategies
# ---------------------------------------------------------------------------
def predict_single_pop(
    delta: float,
    current_vol: float,
    avg_vol: float,
    xgb_prob: float,
    protection_score: float,
    weights: Optional[List[float]] = None,
    side: str = "put",
    symbol: str = "DEFAULT",
) -> float:
    """Legacy signature retained for cs7/cs75/wheel/tt45.

    The ``weights`` argument is now a regime-period hint: callers
    passing one of the well-known weight tuples get mapped onto the
    matching period; everything else is treated as 3M.
    """
    period = "3M"
    if weights is not None:
        for p, ref in DEFAULT_REGIME_WEIGHTS.items():
            if list(weights) == ref:
                period = p
                break
    fv = FeatureVector(
        delta=delta,
        xgb_prob=xgb_prob,
        current_vol=current_vol,
        avg_vol=avg_vol,
        protection_score=protection_score,
        side=side,
        period=period,
        symbol=symbol,
    )
    return predict_pop(fv)


def generate_regime_pops(
    delta: float,
    current_vol: float,
    vol_sma_21: float,
    protection_score: float,
    xgb_preds: Dict[str, float],
    regime_weights: Dict[str, List[float]] = DEFAULT_REGIME_WEIGHTS,
    side: str = "put",
    symbol: str = "DEFAULT",
) -> Dict[str, float]:
    """Score a strike across the three regime horizons.

    Each horizon now consumes its own ``xgb_preds[tf]`` value rather
    than the same number across all three (FACT #20 — fixed).
    """
    out: Dict[str, float] = {}
    for tf in ("3M", "6M", "1Y"):
        fv = FeatureVector(
            delta=delta,
            xgb_prob=float(xgb_preds.get(tf, 0.5)),
            current_vol=current_vol,
            avg_vol=vol_sma_21,
            protection_score=protection_score,
            side=side,
            period=tf,
            symbol=symbol,
        )
        out[tf] = predict_pop(fv)
    return out


# ---------------------------------------------------------------------------
# Augment dashboard payload — replaces the 0.5 + return*5 magic mapping.
# ---------------------------------------------------------------------------
def augment_levels_with_pop(
    analysis: Dict[str, Any],
    xgb_pred: Dict[str, Any],
    period: str = "6m",
    *,
    iv_rank: float = 50.0,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    """Inject calibrated POP plus a confidence band into key levels.

    Parameters
    ----------
    analysis:
        Dashboard-shaped analysis blob containing ``current_price``,
        ``current_vol``, ``avg_vol``, and ``key_levels``.
    xgb_pred:
        AsyncXGBPredictor.predict_latest output.  Optional keys:
        ``predicted_prob`` (calibrated probability of finishing OTM),
        ``predicted_prob_lo`` / ``predicted_prob_hi`` (quantile bands),
        ``predicted_return`` (legacy fallback).
    iv_rank:
        IV percentile (0–100). Defaults to 50 so the meta-learner sees
        a neutral input when no IV cache is wired.
    """
    current_price = float(analysis.get("current_price", 0))
    current_vol = float(analysis.get("current_vol", 0.30))
    avg_vol = float(analysis.get("avg_vol", 0.25))
    key_levels = analysis.get("key_levels", []) or []

    if not np.isfinite(current_vol) or current_vol <= 0:
        current_vol = 0.30
    if not np.isfinite(avg_vol) or avg_vol <= 0:
        avg_vol = 0.25
    if not np.isfinite(current_price) or current_price <= 0:
        return analysis

    analysis["current_vol"] = current_vol
    analysis["avg_vol"] = avg_vol
    analysis["current_price"] = current_price

    # Source the XGB probability *honestly*. Prefer the calibrated
    # quantile heads when present; fall back to a Black-Scholes-style
    # mapping of predicted_return, which beats the previous 0.5 + r*5.
    xgb_prob = _coerce_xgb_prob(xgb_pred, current_vol)
    xgb_prob_lo = xgb_pred.get("predicted_prob_lo")
    xgb_prob_hi = xgb_pred.get("predicted_prob_hi")

    # Stash the coerced probability on the analysis blob so downstream
    # consumers (the strategies' chain-delta POP gate) reuse this exact
    # number instead of re-deriving it from the raw prediction row.
    analysis["xgb_prob"] = float(xgb_prob)
    if xgb_prob_lo is not None:
        analysis["xgb_prob_lo"] = float(xgb_prob_lo)
    if xgb_prob_hi is not None:
        analysis["xgb_prob_hi"] = float(xgb_prob_hi)

    target_dte = 7 if period.lower() == "3m" else 45
    t_years = target_dte / 365
    sigma = max(0.05, current_vol)
    period_key = period.upper()

    sym = symbol or analysis.get("symbol") or xgb_pred.get("symbol") or "DEFAULT"

    for level in key_levels:
        strike = float(level.get("price", 0))
        if strike <= 0 or np.isnan(strike):
            continue

        # 1. Estimate delta + baseline probability via standard z-score.
        try:
            z = math.log(strike / current_price) / (sigma * math.sqrt(t_years))
            p_base = float(norm.cdf(abs(z)))
            if math.isnan(p_base):
                p_base = 0.84
            delta_est = 1.0 - p_base
        except Exception:
            p_base = 0.84
            delta_est = 0.16

        # 2. S/R protection score.
        side = "put" if level.get("type") == "support" else "call"
        spread_type = f"{side}_credit"
        prot_score = calculate_strike_protection(
            key_levels, current_price, strike, spread_type,
        )
        if np.isnan(prot_score):
            prot_score = 1.0
        level["protection"] = float(prot_score)

        fv = FeatureVector(
            delta=delta_est,
            xgb_prob=xgb_prob,
            xgb_prob_lo=xgb_prob_lo,
            xgb_prob_hi=xgb_prob_hi,
            current_vol=current_vol,
            avg_vol=avg_vol,
            protection_score=prot_score,
            iv_rank=iv_rank,
            side=side,
            period=period_key,
            symbol=sym,
        )
        band = predict_pop_with_band(fv)
        if math.isnan(band["pop"]):
            band["pop"] = p_base
        level["pop"] = float(band["pop"])
        level["pop_lo"] = float(band["pop_lo"])
        level["pop_hi"] = float(band["pop_hi"])
        level["p_base"] = float(p_base)

    return analysis


def _coerce_xgb_prob(xgb_pred: Mapping[str, Any], current_vol: float) -> float:
    """Pull a probability out of whatever the XGB layer reported.

    Priority:
      1. ``predicted_prob`` if the new pipeline emitted one.
      2. CDF transform of ``predicted_return`` against current vol —
         this respects the volatility regime instead of a fixed *5
         multiplier.
      3. Neutral 0.5.
    """
    p = xgb_pred.get("predicted_prob")
    if p is not None and np.isfinite(p):
        return float(np.clip(p, 0.01, 0.99))

    pred_ret = xgb_pred.get("predicted_return")
    if pred_ret is not None and np.isfinite(pred_ret):
        horizon_dte = int(xgb_pred.get("horizon_dte", 7))
        sigma_horizon = max(0.005, current_vol * math.sqrt(horizon_dte / 365.0))
        z = float(pred_ret) / sigma_horizon
        return float(np.clip(norm.cdf(z), 0.01, 0.99))

    return 0.5


__all__ = [
    "FeatureVector",
    "DEFAULT_REGIME_WEIGHTS",
    "set_regime_weight_lookup",
    "regime_weights",
    "set_meta_learner",
    "get_meta_learner",
    "set_pop_calibrator",
    "get_pop_calibrator",
    "find_key_levels",
    "calculate_strike_protection",
    "calculate_log_odds",
    "delta_implied_p_otm",
    "predict_pop",
    "predict_pop_with_band",
    "predict_single_pop",
    "generate_regime_pops",
    "augment_levels_with_pop",
    "coerce_xgb_prob",
]


# Public alias — strategies that score individual chain strikes (not just the
# dashboard's key levels) need the same honest return→probability coercion
# ``augment_levels_with_pop`` uses internally. Exposed so the Wheel's POP
# overlay shares one definition rather than re-deriving it.
coerce_xgb_prob = _coerce_xgb_prob
