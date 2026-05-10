"""
[IV-Surface Cache]
Snapshot the implied-volatility surface from Tradier and expose it as a
feature source.

Why this exists
---------------
xgb_features.hv_rank() served as an "IV proxy" because no implied-vol
ingestion existed. HV is a backwards-looking number, while every option
trade is forward-looking — a noisy proxy at best. Tradier's option
chain endpoint actually returns greeks (delta, gamma, vega, …) and
implied volatility per strike, so there is no good reason to keep
guessing.

This module:

1. Pulls the chain at the nearest-expiry / 30-day / 90-day bucket from
   Tradier once per trading day and stores the snapshot under
   ``~/.hermes/iv_cache/<symbol>/<date>.json``.
2. Computes ATM IV, IV term-structure (front/90d), 25-delta skew, and a
   365-day rolling IV-rank from the last year of cached snapshots.
3. Exposes those four numbers via ``IVCache.feature_row(symbol)`` so
   the FeatureEngineer can stitch them onto the daily feature frame.

The cache is filesystem-backed (not DB-backed) on purpose — option
chains are large blobs and we'd rather keep TimescaleDB lean. The cache
lives on the durable ``~/.hermes`` volume so restarts don't refetch.
"""
from __future__ import annotations

import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger("hermes.ml.iv_surface")

DEFAULT_CACHE_ROOT = Path(os.environ.get(
    "HERMES_IV_CACHE",
    str(Path.home() / ".hermes" / "iv_cache"),
))


@dataclass
class IVRow:
    """One day's IV surface summary for a symbol."""

    asof: date
    atm_iv_30d: float
    atm_iv_90d: float
    iv_term_structure: float
    skew_25d: float
    spot: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "asof": self.asof.isoformat(),
            "atm_iv_30d": float(self.atm_iv_30d),
            "atm_iv_90d": float(self.atm_iv_90d),
            "iv_term_structure": float(self.iv_term_structure),
            "skew_25d": float(self.skew_25d),
            "spot": float(self.spot),
        }


# ---------------------------------------------------------------------------
# Helpers — work with whatever Tradier returns, given that some fields
# are sometimes None or strings.
# ---------------------------------------------------------------------------
def _coerce_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v):
        return None
    return v


def _pick_atm(chain: List[Dict[str, Any]], spot: float) -> Optional[Dict[str, Any]]:
    """Return the contract closest to ATM with a usable IV greek."""
    best = None
    best_dist = float("inf")
    for c in chain:
        if (c.get("option_type") or "").lower() != "call":
            continue                                # ATM IV from calls; skew handles puts
        strike = _coerce_float(c.get("strike"))
        greeks = c.get("greeks") or {}
        iv = _coerce_float(greeks.get("mid_iv") or greeks.get("smv_vol"))
        if strike is None or iv is None or iv <= 0:
            continue
        dist = abs(strike - spot)
        if dist < best_dist:
            best_dist = dist
            best = c
    return best


def _pick_25d(chain: List[Dict[str, Any]], side: str) -> Optional[Dict[str, Any]]:
    """Return the contract whose absolute delta is nearest 0.25."""
    target = "put" if side == "put" else "call"
    best = None
    best_dist = float("inf")
    for c in chain:
        if (c.get("option_type") or "").lower() != target:
            continue
        greeks = c.get("greeks") or {}
        delta = _coerce_float(greeks.get("delta"))
        iv = _coerce_float(greeks.get("mid_iv") or greeks.get("smv_vol"))
        if delta is None or iv is None or iv <= 0:
            continue
        dist = abs(abs(delta) - 0.25)
        if dist < best_dist:
            best_dist = dist
            best = c
    return best


def _nearest_expiry(expirations: Iterable[str], target_days: int,
                     today: date) -> Optional[str]:
    """Return the expiry whose DTE is closest to ``target_days``."""
    best = None
    best_dist = float("inf")
    for e in expirations:
        try:
            d = datetime.strptime(e, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            continue
        if d <= today:
            continue
        dte = (d - today).days
        dist = abs(dte - target_days)
        if dist < best_dist:
            best_dist = dist
            best = e
    return best


# ---------------------------------------------------------------------------
# IVCache
# ---------------------------------------------------------------------------
class IVCache:
    """Filesystem-backed IV-surface store.

    Parameters
    ----------
    broker:
        Anything implementing ``get_option_expirations(symbol)``,
        ``get_option_chains(symbol, expiry)`` and ``get_quote(symbol)``.
        TradierBroker satisfies this contract.
    root:
        Override the cache root for tests.
    """

    def __init__(self, broker: Any, root: Path = DEFAULT_CACHE_ROOT) -> None:
        self.broker = broker
        self.root = root

    # ---- snapshot --------------------------------------------------------
    def snapshot(self, symbol: str, *, today: Optional[date] = None,
                 force: bool = False) -> Optional[IVRow]:
        """Fetch and cache today's IV summary, returning the parsed row.

        Returns None when Tradier delivers no usable chain — typical
        for thinly-traded names. Callers leave the corresponding feature
        as NaN, which the catalog already permits via ``nullable=True``.
        """
        symbol = symbol.upper()
        today = today or date.today()
        cache_path = self.root / symbol / f"{today.isoformat()}.json"
        if cache_path.exists() and not force:
            try:
                payload = json.loads(cache_path.read_text())
                return IVRow(
                    asof=date.fromisoformat(payload["asof"]),
                    atm_iv_30d=float(payload["atm_iv_30d"]),
                    atm_iv_90d=float(payload["atm_iv_90d"]),
                    iv_term_structure=float(payload["iv_term_structure"]),
                    skew_25d=float(payload["skew_25d"]),
                    spot=float(payload["spot"]),
                )
            except (OSError, json.JSONDecodeError, KeyError, ValueError):
                pass                                # fall through to refetch

        try:
            quotes = self.broker.get_quote(symbol)
        except Exception as exc:                    # noqa: BLE001
            logger.warning("get_quote(%s) failed: %s", symbol, exc)
            return None
        if not quotes:
            return None
        spot = _coerce_float(quotes[0].get("last") or quotes[0].get("close"))
        if spot is None:
            return None

        try:
            expirations = self.broker.get_option_expirations(symbol)
        except Exception as exc:                    # noqa: BLE001
            logger.warning("get_option_expirations(%s) failed: %s", symbol, exc)
            return None
        if not expirations:
            return None

        e30 = _nearest_expiry(expirations, 30, today)
        e90 = _nearest_expiry(expirations, 90, today)
        if e30 is None or e90 is None:
            return None

        try:
            chain30 = self.broker.get_option_chains(symbol, e30) or []
            chain90 = self.broker.get_option_chains(symbol, e90) or []
        except Exception as exc:                    # noqa: BLE001
            logger.warning("get_option_chains(%s) failed: %s", symbol, exc)
            return None

        atm30 = _pick_atm(chain30, spot)
        atm90 = _pick_atm(chain90, spot)
        if atm30 is None or atm90 is None:
            return None
        atm_iv_30d = _coerce_float(
            (atm30.get("greeks") or {}).get("mid_iv")
        ) or 0.0
        atm_iv_90d = _coerce_float(
            (atm90.get("greeks") or {}).get("mid_iv")
        ) or 0.0
        if atm_iv_30d <= 0 or atm_iv_90d <= 0:
            return None

        # 25-delta skew on the 30-day expiry
        put25 = _pick_25d(chain30, "put")
        call25 = _pick_25d(chain30, "call")
        skew = 0.0
        if put25 is not None and call25 is not None:
            iv_p = _coerce_float((put25.get("greeks") or {}).get("mid_iv")) or 0.0
            iv_c = _coerce_float((call25.get("greeks") or {}).get("mid_iv")) or 0.0
            skew = iv_p - iv_c

        row = IVRow(
            asof=today,
            atm_iv_30d=float(atm_iv_30d),
            atm_iv_90d=float(atm_iv_90d),
            iv_term_structure=float(atm_iv_30d / atm_iv_90d) if atm_iv_90d > 0 else 1.0,
            skew_25d=float(skew),
            spot=float(spot),
        )
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(row.to_dict()))
        return row

    # ---- feature row -----------------------------------------------------
    def history(self, symbol: str, *, days: int = 365) -> List[IVRow]:
        """Return cached IV rows newest-first, up to ``days`` back."""
        symbol = symbol.upper()
        d = self.root / symbol
        if not d.exists():
            return []
        cutoff = date.today() - timedelta(days=days)
        out: List[IVRow] = []
        for p in sorted(d.glob("*.json"), reverse=True):
            try:
                payload = json.loads(p.read_text())
            except (OSError, json.JSONDecodeError):
                continue
            try:
                asof = date.fromisoformat(payload["asof"])
            except (KeyError, ValueError):
                continue
            if asof < cutoff:
                break
            out.append(IVRow(
                asof=asof,
                atm_iv_30d=float(payload["atm_iv_30d"]),
                atm_iv_90d=float(payload["atm_iv_90d"]),
                iv_term_structure=float(payload["iv_term_structure"]),
                skew_25d=float(payload["skew_25d"]),
                spot=float(payload["spot"]),
            ))
        return out

    def iv_rank(self, symbol: str, *, lookback_days: int = 365) -> Optional[float]:
        """Standard IV rank: position of today's ATM IV inside the rolling
        365-day min/max window. Returns None when the cache is too thin."""
        rows = self.history(symbol, days=lookback_days)
        if len(rows) < 30:
            return None
        ivs = [r.atm_iv_30d for r in rows]
        lo, hi = min(ivs), max(ivs)
        if hi <= lo:
            return 0.0
        return float((rows[0].atm_iv_30d - lo) / (hi - lo) * 100.0)

    def feature_row(self, symbol: str) -> Dict[str, Optional[float]]:
        """Return the live feature dict the meta-learner consumes.

        Keys match hermes.ml.feature_catalog.OPTIONS_FEATURES exactly.
        """
        latest = self.snapshot(symbol)
        if latest is None:
            return {
                "iv_atm_30d": None,
                "iv_rank_365d": None,
                "iv_term_structure": None,
                "iv_skew_25d": None,
            }
        return {
            "iv_atm_30d": latest.atm_iv_30d,
            "iv_rank_365d": self.iv_rank(symbol),
            "iv_term_structure": latest.iv_term_structure,
            "iv_skew_25d": latest.skew_25d,
        }


__all__ = ["IVCache", "IVRow", "DEFAULT_CACHE_ROOT"]
