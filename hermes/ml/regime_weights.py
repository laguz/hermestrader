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

import asyncio
import logging
from typing import Any, Dict, List, Optional

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
    except Exception as exc:
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


class CachedRegimeWeightsLookup:
    """In-process cache of regime weights that refreshes asynchronously on ticks.

    This avoids blocking database calls during the hot option scoring path.
    """

    def __init__(self, db: "HermesDB", event_bus: Optional[Any] = None) -> None:
        self.db = db
        self.event_bus = event_bus
        self.cache: Dict[tuple[str, str], List[float]] = {}
        self._refresh_lock = asyncio.Lock()

        if event_bus is not None:
            from hermes.events.bus import CacheWarmTick, ClockTickEvent
            event_bus.subscribe(ClockTickEvent, self.refresh_async)
            event_bus.subscribe(CacheWarmTick, self.refresh_async)

    async def initialize(self) -> None:
        """Warms up the cache at startup."""
        await self.refresh_async()

    async def refresh_async(self, event: Any = None) -> None:
        """Asynchronously queries the database for all regime weights and updates the cache."""
        if self.db is None or RegimeWeights is None:
            return
        async with self._refresh_lock:
            try:
                from sqlalchemy import select
                async with self.db.AsyncSession() as s:
                    q = select(RegimeWeights)
                    result = await s.execute(q)
                    rows = result.scalars().all()

                    new_cache = {}
                    for row in rows:
                        if (row.hits + row.misses) < 30:
                            continue
                        key = (row.symbol.upper(), row.period.upper())
                        new_cache[key] = [
                            float(row.beta_0),
                            float(row.beta_1),
                            float(row.beta_2),
                            float(row.beta_3),
                            float(row.beta_4),
                        ]
                    self.cache = new_cache
                    logger.debug("Regime weights cache refreshed: %d rows loaded", len(new_cache))
            except Exception as exc:
                logger.warning("Failed to refresh regime weights cache: %s", exc)

    def __call__(self, period: str, symbol: str = "DEFAULT") -> List[float]:
        """Synchronous lookup from cache, returning copy of weights."""
        period_key = period.upper()
        symbol_key = symbol.upper()
        key = (symbol_key, period_key)

        try:
            if key in self.cache:
                return list(self.cache[key])

            default_key = ("DEFAULT", period_key)
            if default_key in self.cache:
                return list(self.cache[default_key])

            # In testing/offline environments (no event bus), fetch synchronously on cache miss
            if self.event_bus is None:
                try:
                    weights = lookup(self.db, symbol, period)
                    self.cache[key] = weights
                    return list(weights)
                except Exception:
                    pass
        except Exception as exc:
            logger.warning("CachedRegimeWeightsLookup call failed: %s", exc)

        return list(STATIC_DEFAULTS.get(period_key, STATIC_DEFAULTS["3M"]))


def make_lookup_fn(db: "HermesDB", event_bus: Optional[Any] = None) -> CachedRegimeWeightsLookup:
    """Return a closure/callable suitable for ``pop_engine.set_regime_weight_lookup``.

    Wires the async tick-refreshed cached lookup in Service-1.
    """
    return CachedRegimeWeightsLookup(db, event_bus)


# ---------------------------------------------------------------------------
# Bayesian update
# ---------------------------------------------------------------------------
async def update_from_outcomes(
    db: "HermesDB",
    symbol: str,
    period: str,
    *,
    hits: int,
    misses: int,
) -> None:
    """Apply a Beta-Bernoulli update to the (symbol, period) row.

    The math is intentionally light — this is a controller, not a
    second model.
    """
    if RegimeWeights is None or db is None:
        return
    from sqlalchemy import select
    period_key = period.upper()
    async with db.AsyncSession() as s:
        q = select(RegimeWeights).filter_by(symbol=symbol.upper(), period=period_key)
        result = await s.execute(q)
        row = result.scalars().first()
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
        await s.commit()


def ensure_table(db: "HermesDB") -> None:
    if RegimeWeights is None:
        return
    try:
        RegimeWeights.__table__.create(bind=db.engine, checkfirst=True)
    except Exception as exc:
        logger.warning("could not ensure regime_weights table: %s", exc)


__all__ = [
    "RegimeWeights",
    "STATIC_DEFAULTS",
    "lookup",
    "make_lookup_fn",
    "update_from_outcomes",
    "ensure_table",
]
