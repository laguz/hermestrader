"""Internal helpers shared across the four strategies.

Only ``parse_occ`` and ``nearest_strike`` live here — anything that needs
``MoneyManager`` / ``IronCondorBuilder`` / ``AbstractStrategy`` belongs in
``..core`` instead.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from hermes.common import OCC_RE


def parse_occ(symbol: str) -> Optional[Dict[str, Any]]:
    """Decompose an OCC option symbol like ``AAPL250620P00150000``.

    Returns ``{underlying, expiry: date, side: 'put'|'call'}`` or ``None``
    if the input doesn't match the OCC format.
    """
    m = OCC_RE.match(symbol or "")
    if not m:
        return None
    underlying, yymmdd, pc, _strike = m.groups()
    return {
        "underlying": underlying,
        "expiry": datetime.strptime(yymmdd, "%y%m%d").date(),
        "side": "put" if pc == "P" else "call",
    }


def nearest_strike(chain, option_type: str, target: float) -> Optional[Dict[str, Any]]:
    """Return the chain option whose strike is closest to ``target``.

    ``chain`` is whatever ``broker.get_option_chains`` returned;
    ``option_type`` is 'put' or 'call'. Returns ``None`` for an empty side.
    """
    candidates = [o for o in chain if o.get("option_type") == option_type]
    if not candidates:
        return None
    return min(candidates, key=lambda o: abs(float(o["strike"]) - target))


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _dte_from_expiry(expiry: Any, *, asof: Optional[datetime] = None) -> Optional[int]:
    """Days-to-expiration from an expiry (``date`` or ``YYYY-MM-DD`` string)."""
    if expiry is None:
        return None
    exp_date = None
    if hasattr(expiry, "year") and not isinstance(expiry, datetime):
        exp_date = expiry                                   # already a date
    elif isinstance(expiry, datetime):
        exp_date = expiry.date()
    else:
        try:
            exp_date = datetime.strptime(str(expiry), "%Y-%m-%d").date()
        except (TypeError, ValueError):
            return None
    today = (asof or datetime.utcnow()).date()
    return (exp_date - today).days


def entry_feature_snapshot(
    strategy_id: str,
    knobs: Optional[Dict[str, Any]],
    *,
    side_type: Optional[str] = None,
    pop: Any = None,
    short_delta: Any = None,
    width: Any = None,
    entry_credit: Any = None,
    expiry: Any = None,
    spot: Any = None,
    iv_rank: Any = None,
    ai_authored: bool = False,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Assemble the at-entry context snapshot persisted on the Trade row.

    This is pure instrumentation — it never influences a trade. The dict pairs
    the resolved tunables (``knobs``, the future bandit's *action*) with the
    market context that produced the fill (the *state*), so a closed trade's
    realized ``pnl`` becomes a labelled training row. ``None`` fields are
    dropped to keep the JSON compact and to avoid implying a measured zero.
    """
    width_f = _coerce_float(width)
    credit_f = _coerce_float(entry_credit)
    feats: Dict[str, Any] = {
        "schema": 1,
        "strategy_id": strategy_id,
        "side_type": side_type,
        "knobs": dict(knobs) if knobs else None,
        "pop": _coerce_float(pop),
        "short_delta": _coerce_float(short_delta),
        "width": width_f,
        "entry_credit": credit_f,
        "credit_width_ratio": (
            round(credit_f / width_f, 4)
            if credit_f is not None and width_f not in (None, 0.0)
            else None
        ),
        "dte": _dte_from_expiry(expiry),
        "spot": _coerce_float(spot),
        "iv_rank": _coerce_float(iv_rank),
        "ai_authored": bool(ai_authored),
    }
    if extra:
        feats.update(extra)
    return {k: v for k, v in feats.items() if v is not None}
