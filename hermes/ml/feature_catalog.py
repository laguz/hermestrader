"""
[Feature-Catalog]
Single source of truth for every feature consumed by the prediction stack.

Why this exists
---------------
Prior to this module, feature names were repeated across xgb_features.py,
pop_engine.py, and the meta-learner with no contract pinning them
together. Adding a feature meant editing three files and praying nothing
silently regressed.

The catalog gives us:
- A canonical, ordered list of feature names per stage (raw, derived, meta).
- Units, source, and refresh cadence per feature so drift detection
  knows what to compare and audits know what to expect.
- A *schema hash* derived deterministically from the catalog. The
  persistence layer refuses to load a model whose stored hash differs
  from the current one — a renamed feature can no longer cause a silent
  index shift on a warm-started predictor.

This file is intentionally declarative. Touching it is a behaviour-changing
change and must go through the seven-day Brier-score parity gate before
the live branch is promoted (see SETUP_GUIDE.md).
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class FeatureSpec:
    """One row in the feature catalog.

    Attributes
    ----------
    name:
        Canonical column name. Must match the column produced by the
        FeatureEngineer / IV cache / macro source exactly.
    units:
        Human-readable units ("ratio", "pct", "z-score"). Surfaces in the
        /ml/diagnostics dashboard so a reviewer can spot a transformation
        bug without running anything.
    source:
        Origin module/method. The drift detector uses this to decide
        which dataset to KS-test against.
    cadence:
        How often the value updates. "daily", "intraday", "static".
    nullable:
        Whether NaN is a legal value. When False, the predictor refuses
        to score a row with this column missing rather than silently
        defaulting to 0.5.
    description:
        Free-text. Kept short — long-form rationale belongs in
        ARCHITECTURE.md, not here.
    """

    name: str
    units: str
    source: str
    cadence: str
    nullable: bool = False
    description: str = ""


# ---------------------------------------------------------------------------
# Raw equity-bar features (xgb_features.FeatureEngineer outputs)
# ---------------------------------------------------------------------------
RAW_EQUITY_FEATURES: Tuple[FeatureSpec, ...] = (
    FeatureSpec("overnight_gap", "ratio", "FeatureEngineer.overnight_gap", "daily",
                description="(open_t - close_{t-1}) / close_{t-1}"),
    FeatureSpec("vol_norm_5d_momentum", "z-score", "FeatureEngineer.vol_norm_5d_momentum",
                "daily", description="5-day return scaled by 20-day rolling stdev"),
    FeatureSpec("spy_beta_residual", "ratio", "FeatureEngineer.spy_beta_residual",
                "daily", description="Daily return minus beta-times-SPY return"),
    FeatureSpec("intraday_return", "ratio", "FeatureEngineer.intraday_return", "daily",
                description="(close - open) / open"),
    FeatureSpec("vwap_distance", "ratio", "FeatureEngineer.vwap_distance", "daily",
                description="(close - vwap_close) / close"),
    FeatureSpec("range_position", "ratio", "FeatureEngineer.range_position", "daily",
                description="(close - low) / (high - low)"),
    FeatureSpec("volume_zscore_20d", "z-score", "FeatureEngineer.volume_zscore",
                "daily", nullable=True,
                description="Volume relative to 20-day rolling mean/stdev"),
    FeatureSpec("last_30min_volume_pct", "ratio", "FeatureEngineer.last_30min_volume_pct",
                "intraday", nullable=True,
                description="Closing 30-minute volume share of daily volume"),
    FeatureSpec("realized_vol_5d", "annualized_vol",
                "FeatureEngineer.realized_vol_5d", "daily",
                description="5-day realised volatility, annualised"),
    FeatureSpec("day_of_week", "ordinal", "FeatureEngineer.seasonality", "daily",
                description="0=Mon … 4=Fri"),
    FeatureSpec("month", "ordinal", "FeatureEngineer.seasonality", "daily",
                description="1=Jan … 12=Dec"),
)


# ---------------------------------------------------------------------------
# Options-derived features
# ---------------------------------------------------------------------------
# Intentionally empty. These specs (iv_atm_30d / iv_rank_365d /
# iv_term_structure / iv_skew_25d) were all sourced from the IVCache in
# hermes/ml/iv_surface.py, which was never wired into the predictor: the live
# XGB pipeline trains and infers on the "equity" stage only (see
# predictor_training.py / predictor_inference.py), so the options stage fed no
# real columns into any model. The unused IVCache module and these orphaned
# specs were removed together. Re-add concrete FeatureSpec rows here once an
# options-IV feed is actually connected to FeatureEngineer.
#
# NOTE: the live meta-learner input named "iv_rank_365d" is unrelated to these
# specs — it is populated at runtime from pop_engine.FeatureVector.iv_rank and
# lives in META_FEATURES below, not here.
OPTIONS_FEATURES: Tuple[FeatureSpec, ...] = ()


# ---------------------------------------------------------------------------
# Macro / cross-asset features
# ---------------------------------------------------------------------------
# Intentionally empty. The VIX / VIX9D / MOVE / DXY specs were placeholders
# with no production data source (always null), which fed dead columns into the
# model's feature vector. They are removed until a real macro feed is wired up;
# re-add concrete FeatureSpec rows here once MacroFeed is implemented. The
# symbol is kept (empty) so consumers and the "macro" stage stay valid.
MACRO_FEATURES: Tuple[FeatureSpec, ...] = ()


# ---------------------------------------------------------------------------
# Meta-learner inputs (downstream of the raw model heads)
# ---------------------------------------------------------------------------
META_FEATURES: Tuple[FeatureSpec, ...] = (
    FeatureSpec("delta_implied_prob", "probability", "FeatureVector.delta_to_prob",
                "decision-time", description="1 - |delta| from the option chain"),
    FeatureSpec("xgb_prob", "probability", "AsyncXGBPredictor.predict_latest",
                "decision-time",
                description="Calibrated XGB probability of finishing OTM"),
    FeatureSpec("xgb_prob_lo", "probability", "AsyncXGBPredictor.predict_quantiles",
                "decision-time", nullable=True,
                description="10th-quantile head probability (lower band)"),
    FeatureSpec("xgb_prob_hi", "probability", "AsyncXGBPredictor.predict_quantiles",
                "decision-time", nullable=True,
                description="90th-quantile head probability (upper band)"),
    FeatureSpec("protection_score", "score", "pop_engine.calculate_strike_protection",
                "decision-time",
                description=">=1.0 — S/R-derived strike protection multiplier"),
    FeatureSpec("iv_rank_365d", "pct", "FeatureVector.iv_rank", "daily",
                description="IV rank passed in by strategies for the meta-learner"),
    FeatureSpec("vol_ratio", "ratio", "FeatureVector.vol_ratio", "decision-time",
                description="Current vol divided by 21-day SMA of vol"),
)


# ---------------------------------------------------------------------------
# Aggregations + helpers
# ---------------------------------------------------------------------------
ALL_RAW_FEATURES: Tuple[FeatureSpec, ...] = (
    RAW_EQUITY_FEATURES + OPTIONS_FEATURES + MACRO_FEATURES
)


def feature_names(stage: str = "all") -> List[str]:
    """Return the ordered feature list for a given stage.

    Parameters
    ----------
    stage:
        One of "equity", "options", "macro", "raw" (= equity+options+macro),
        "meta", or "all" (raw + meta). The order returned here is the
        contract every consumer must respect.
    """
    table: Dict[str, Tuple[FeatureSpec, ...]] = {
        "equity": RAW_EQUITY_FEATURES,
        "options": OPTIONS_FEATURES,
        "macro": MACRO_FEATURES,
        "raw": ALL_RAW_FEATURES,
        "meta": META_FEATURES,
        "all": ALL_RAW_FEATURES + META_FEATURES,
    }
    if stage not in table:
        raise KeyError(f"unknown feature stage {stage!r}")
    return [spec.name for spec in table[stage]]


def specs_for(stage: str = "all") -> List[FeatureSpec]:
    """Return FeatureSpec rows (not just names) for a stage."""
    table = {
        "equity": RAW_EQUITY_FEATURES,
        "options": OPTIONS_FEATURES,
        "macro": MACRO_FEATURES,
        "raw": ALL_RAW_FEATURES,
        "meta": META_FEATURES,
        "all": ALL_RAW_FEATURES + META_FEATURES,
    }
    return list(table[stage])


def schema_hash(stage: str = "raw") -> str:
    """Deterministic SHA-256 over the catalog for a stage.

    Models persisted under one schema must refuse to load when the schema
    changes — see hermes.ml.persistence.load_model. The hash covers the
    fields the model actually depends on (name + units + nullable). Free-
    text description changes do not invalidate cached models.
    """
    payload = [
        {"name": s.name, "units": s.units, "nullable": s.nullable}
        for s in specs_for(stage)
    ]
    body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(body).hexdigest()



__all__ = [
    "FeatureSpec",
    "RAW_EQUITY_FEATURES",
    "OPTIONS_FEATURES",
    "MACRO_FEATURES",
    "META_FEATURES",
    "ALL_RAW_FEATURES",
    "feature_names",
    "specs_for",
    "schema_hash",
]
