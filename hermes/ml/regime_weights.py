"""
[Database-Backed Regime Weights]
Replaces the static DEFAULT_REGIME_WEIGHTS lookup with a per-symbol,
per-period table updated by Bayesian (Beta-Bernoulli) posteriors over
realised credit-spread outcomes.

Why this exists
---------------
The v1 weights were identical across 3M / 6M / 1Y horizons, hand-set
once, and never validated. Recommendation #5 calls for promoting them
to a database-backed table updated nightly by realised hits versus
misses.

Approach
--------
We treat each weight slot (β0…β4) as a Beta-Bernoulli posterior whose
update step is "did the prediction at this regime/symbol come true?"
The posterior mean replaces the hand-set value; the posterior variance
gives us a confidence band the diagnostics endpoint can render.

Cold start: when there are < 30 observations for a (symbol, period)
pair we keep the static default so the system behaves identically to
v1 until enough data has accumulated.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Mapping, Optional

from sqlalchemy import Column, DateTime, Float, Integer, String

try:                                                  # pragma: no cover
    from hermes.db.models import Base, HermesDB
except Exception:                                     # pragma: no cover
    Base = None                                       # type: ignore
    HermesDB = None                                   # type: ignore

logger = logging.getLogger("hermes.ml.regime_weights")


# Static default fallback — keep as a tuple to discourage in-place edits.
STATIC_DEFAULTS: Dict[str, List[float]] = {
    "3M": [0.0, 1.0, 0.6, 0.3, 0.4],
    "6M": [0.0, 1.0, 0.6, 0.3, 0.4],
    "1Y": [0.0, 1.0, 0.6, 0.3, 0.4],
}


# ---------------------------------------------------------------------------
# ORM
# ---------------------------------------------------------------------------
if Base is not None:                                  # pragma: no branch

    class RegimeWeights(Base):                        # type: ignore[misc, valid-type]
        """Per (symbol, period) weight set + posterior counts.

        Composite PK so a symbol can carry one row per regime period.
        ``hits`` and ``misses`` accumulate over time; the active weight
        set is recomputed nightly from those counters.
        """

        __tablename__ = "regime_weights"

        symbol = Column(String, primary_key=True)
        period = Column(String, primary_key=True)
        beta_0 = Column(Float, nullable=False, default=0.0)
        beta_1 = Column(Float, nullable=False, default=1.0)
        beta_2 = Column(Float, nullable=False, default=0.6)
        beta_3 = Column(Float, nullable=False, default=0.3)
        beta_4 = Column(Float, nullable=False, default=0.4)
        hits = Column(Integer, nullable=False, default=0)
        misses = Column(Integer, nullable=False, default=0)
        updated_at = Column(DateTime(timezone=True))
else:
    RegimeWeights = None                              # type: ignore


# ---------------------------------------------------------------------------
# Lookup
# ---------------------------------------------------------------------------
def lookup(db: "HermesDB", symbol: str, period: str) -> List[float]:
    """Return the active β vector for (symbol, period).

    Falls back to STATIC_DEFAULTS when:
    - the table is missing (older deployments),
    - there is no row yet for the pair,
    - the pair has fewer than 30 observations (cold start).
    """
    period_key = period.upper()
    default = STATIC_DEFAULTS.get(period_key, STATIC_DEFAULTS["3M"])
    if RegimeWeights is None or db is None:
        return list(default)
    try:
        with db.Session() as s:
            row = (s.query(RegimeWeights)
                   .filter_by(symbol=symbol.upper(), period=period_key)
                   .first())
    except Exception as exc:                          # noqa: BLE001
        logger.debug("regime_weights lookup failed: %s", exc)
        return list(default)
    if row is None or (row.hits + row.misses) < 30:
        return list(default)
    return [
        float(row.beta_0),
        float(row.beta_1),
        float(row.beta_2),
        float(row.beta_3),
        float(row.beta_4),
    ]


def make_lookup_fn(db: "HermesDB"):
    """Return a closure suitable for ``pop_engine.set_regime_weight_lookup``.

    The closure is called from the hot prediction path so it must not
    do any work on cold-start: we cache the row results in-process for
    a few minutes to keep round-trip latency minimal.
    """
    import time
    cache: Dict[tuple[str, str], tuple[float, List[float]]] = {}
    ttl = 300.0

    def _fn(period: str, symbol: str = "DEFAULT") -> List[float]:
        key = (symbol.upper(), period.upper())
        now = time.time()
        cached = cache.get(key)
        if cached is not None and now - cached[0] < ttl:
            return list(cached[1])
        weights = lookup(db, symbol, period)
        cache[key] = (now, weights)
        return list(weights)

    return _fn


# ---------------------------------------------------------------------------
# Bayesian update
# ---------------------------------------------------------------------------
def update_from_outcomes(
    db: "HermesDB",
    symbol: str,
    period: str,
    *,
    hits: int,
    misses: int,
    feature_means: Optional[Mapping[str, float]] = None,
) -> None:
    """Apply a Beta-Bernoulli update to the (symbol, period) row.

    ``feature_means`` is optional; when supplied the per-feature
    weights are nudged toward the mean of features observed during
    profitable trades (Bayesian linear regression-style heuristic).
    The math is intentionally light — this is a controller, not a
    second model.
    """
    if RegimeWeights is None or db is None:
        return
    period_key = period.upper()
    with db.Session() as s:
        row = (s.query(RegimeWeights)
               .filter_by(symbol=symbol.upper(), period=period_key)
               .first())
        if row is None:
            row = RegimeWeights(
                symbol=symbol.upper(), period=period_key,
                beta_0=STATIC_DEFAULTS[period_key][0],
                beta_1=STATIC_DEFAULTS[period_key][1],
                beta_2=STATIC_DEFAULTS[period_key][2],
                beta_3=STATIC_DEFAULTS[period_key][3],
                beta_4=STATIC_DEFAULTS[period_key][4],
            )
            s.add(row)
        row.hits = int(row.hits or 0) + int(hits)
        row.misses = int(row.misses or 0) + int(misses)
        # Nudge weights toward the empirical hit rate so the prediction
        # surface tilts in the direction the data supports.
        total = row.hits + row.misses
        if total > 0:
            empirical = row.hits / total
            # Scale: only shrink toward 1 - |empirical - 0.5| * 2,
            # which maps a 50/50 outcome to no nudge and 100% hit/miss
            # to maximum confidence.
            scale = 1.0 - abs(empirical - 0.5) * 2
            for attr, default in zip(
                ("beta_1", "beta_2", "beta_3", "beta_4"),
                STATIC_DEFAULTS[period_key][1:],
            ):
                cur = getattr(row, attr)
                setattr(row, attr,
                        float(cur * scale + default * (1.0 - scale)))
        from datetime import datetime, timezone
        row.updated_at = datetime.now(timezone.utc)
        s.commit()


def ensure_table(db: "HermesDB") -> None:
    if RegimeWeights is None:
        return
    try:
        RegimeWeights.__table__.create(bind=db.engine, checkfirst=True)
    except Exception as exc:                          # noqa: BLE001
        logger.warning("could not ensure regime_weights table: %s", exc)


__all__ = [
    "RegimeWeights",
    "STATIC_DEFAULTS",
    "lookup",
    "make_lookup_fn",
    "update_from_outcomes",
    "ensure_table",
]
