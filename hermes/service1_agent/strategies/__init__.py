"""Concrete strategies — one file per cascading priority.

Public surface re-exported here so existing imports continue to work::

    from hermes.service1_agent.strategies import (
        CreditSpreads75, CreditSpreads7, TastyTrade45, WheelStrategy,
    )

Each strategy declares ``PRIORITY`` (1 = highest). The ``CascadingEngine``
runs them in PRIORITY order; high-priority strategies consume capacity
before lower-priority ones see the watchlist.

Layout
------
- ``_helpers.py`` — OCC parser + ``_nearest_strike`` (used by every strategy)
- ``cs75.py``     — Priority 1, 39–45 DTE credit spreads
- ``cs7.py``      — Priority 2, 7 DTE credit spreads
- ``tt45.py``     — Priority 3, 16Δ short, 30–60 DTE
- ``wheel.py``    — Priority 4, put → assignment → call wheel

Adding a strategy: subclass ``AbstractStrategy`` from ``..core``, give it a
``PRIORITY`` ≥5 and a ``NAME``, drop it in here, and register it in
``hermes/common.py`` (``STRATEGIES`` + ``STRATEGY_PRIORITIES``) and
``hermes/service1_agent/main.py`` (``build``).
"""
from __future__ import annotations

from .cs75 import CreditSpreads75
from .cs7 import CreditSpreads7
from .tt45 import TastyTrade45
from .wheel import WheelStrategy

__all__ = [
    "CreditSpreads75",
    "CreditSpreads7",
    "TastyTrade45",
    "WheelStrategy",
]
