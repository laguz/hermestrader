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
