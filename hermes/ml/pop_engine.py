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
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

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
_RegimeWeightLookup = Callable[[str], List[float]]


def _static_regime_lookup(period: str) -> List[float]:
    return DEFAULT_REGIME_WEIGHTS.get(period.upper(), DEFAULT_REGIME_WEIGHTS["3M"])


_regime_weight_lookup: _RegimeWeightLookup = _static_regime_lookup


def set_regime_weight_lookup(fn: _RegimeWeightLookup) -> None:
    """Wire a database-backed accessor (called once on watcher boot)."""
    global _regime_weight_lookup
    _regime_weight_lookup = fn


def regime_weights(period: str) -> List[float]:
    return _regime_weight_lookup(period)


# ---------------------------------------------------------------------------
# Feature vector — the consumer-facing input contract.
# ---------------------------------------------------------------------------
@dataclass
class FeatureVector:
    """Structured features the POP engine scores against.

    Built once per (symbol, decision-time) and reused for every key
    level and side.

    Attributes
    ----------
    delta:
        Option delta (signed) for the short strike under consideration.
        Both legacy and meta paths consume ``1 - |delta|`` as the
        baseline OTM probability.
    xgb_prob:
        Calibrated XGB probability of finishing OTM at the configured
        horizon. Comes from AsyncXGBPredictor.predict_latest after the
        per-symbol calibrator has been applied.
    xgb_prob_lo / xgb_prob_hi:
        10th- / 90th-quantile head outputs. Optional; when absent the
        confidence band collapses to the point estimate.
    current_vol / avg_vol:
        Realised volatility today and 21-day SMA of same. The legacy
        log-odds combiner uses this ratio.
    protection_score:
        S/R protection score from ``calculate_strike_protection``.
    iv_rank:
        365-day IV percentile (0–100) for the symbol.
    side:
        'put' or 'call' — flips the sign of the XGB log-odds contribution.
    period:
        '3M', '6M', or '1Y' — selects the regime weight set.
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

    def to_meta_dict(self) -> Dict[str, float]:
        """Project the features into the meta-learner's input space."""
        return {
            "delta_implied_prob": 1.0 - abs(float(self.delta)),
            "xgb_prob": float(self.xgb_prob),
            "protection_score": float(self.protection_score),
            "iv_rank_365d": float(self.iv_rank),
            "vol_ratio": float(self.current_vol) / max(float(self.avg_vol), 1e-5),
        }


# ---------------------------------------------------------------------------
# Active meta-learner — settable by the watcher; defaults to the
# untrained identity learner.
# ---------------------------------------------------------------------------
_meta_learner: MetaLearner = MetaLearner()


def set_meta_learner(model: Optional[MetaLearner]) -> None:
    """Install a fitted meta-learner.  Pass None to disable stacking."""
    global _meta_learner
    _meta_learner = model or MetaLearner()


def get_meta_learner() -> MetaLearner:
    return _meta_learner


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


# ---------------------------------------------------------------------------
# Strike protection
# ---------------------------------------------------------------------------
_PROTECTION_DISTANCE_FLOOR = 0.1   # exposed for tests; see rec #19


def calculate_strike_protection(
    key_levels: Sequence[Mapping[str, Any]],
    current_price: float,
    short_strike: float,
    spread_type: str,
) -> float:
    """Numerical score representing how well a short strike is protected
    by S/R clusters. ``>=1.0`` means baseline protection.

    Logs the raw score so silent saturation against the distance floor
    becomes observable in production (rec #19).
    """
    raw_score = 0.0
    for level in key_levels:
        ltype = level.get("type")
        price = float(level.get("price", 0.0))
        strength = float(level.get("strength", 1))
        if spread_type == "put_credit" and ltype == "support":
            if short_strike < price < current_price:
                distance = max(current_price - price, _PROTECTION_DISTANCE_FLOOR)
                raw_score += strength * (1.0 / distance)
        elif spread_type == "call_credit" and ltype == "resistance":
            if current_price < price < short_strike:
                distance = max(price - current_price, _PROTECTION_DISTANCE_FLOOR)
                raw_score += strength * (1.0 / distance)

    score = 1.0 + (raw_score * 0.1)
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


def _legacy_combiner(fv: FeatureVector) -> float:
    p_base = 1.0 - abs(fv.delta)
    rv = fv.current_vol / (fv.avg_vol + 1e-5)
    l_base = calculate_log_odds(p_base)
    l_xgb = calculate_log_odds(fv.xgb_prob)
    if fv.side == "call":
        l_xgb = -l_xgb
    weights = regime_weights(fv.period)
    beta_0, beta_1, beta_2, beta_3, beta_4 = weights
    score = (
        beta_0
        + beta_1 * l_base
        + beta_2 * l_xgb
        + beta_3 * rv
        + beta_4 * fv.protection_score
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
    """
    meta = get_meta_learner()
    if meta.weights:
        return float(meta.predict(fv.to_meta_dict()))
    return _legacy_combiner(fv)


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

    target_dte = 7 if period.lower() == "3m" else 45
    t_years = target_dte / 365
    sigma = max(0.05, current_vol)
    period_key = period.upper()

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
        except Exception:                             # noqa: BLE001
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
        sigma_daily = max(0.005, current_vol / math.sqrt(252))
        z = float(pred_ret) / sigma_daily
        return float(np.clip(norm.cdf(z), 0.01, 0.99))

    return 0.5


__all__ = [
    "FeatureVector",
    "DEFAULT_REGIME_WEIGHTS",
    "set_regime_weight_lookup",
    "regime_weights",
    "set_meta_learner",
    "get_meta_learner",
    "find_key_levels",
    "calculate_strike_protection",
    "calculate_log_odds",
    "predict_pop",
    "predict_pop_with_band",
    "predict_single_pop",
    "generate_regime_pops",
    "augment_levels_with_pop",
]
