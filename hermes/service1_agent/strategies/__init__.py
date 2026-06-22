"""Concrete strategies — one file per cascading priority.

Public surface re-exported here so existing imports continue to work::

    from hermes.service1_agent.strategies import CreditSpreads75

Each strategy declares ``PRIORITY`` (1 = highest). The ``CascadingEngine``
runs them in PRIORITY order; high-priority strategies consume capacity
before lower-priority ones see the watchlist.

Phase 0 ships a single strategy — the highest-priority, longest-DTE / most
vetted recipe. Additional strategies are admitted only when they clear the
promotion gate (see ``REBUILD.md``).

Layout
------
- ``_helpers.py`` — OCC parser + ``_nearest_strike`` (used by every strategy)
- ``_credit_spread_base.py`` — shared POP-driven credit-spread engine
- ``cs75.py``     — Priority 1, 39–45 DTE credit spreads (config + hooks)

Adding a strategy: subclass ``AbstractStrategy`` from ``..core``, give it a
``PRIORITY`` ≥2 and a ``NAME``, drop it in here, and register it in
``hermes/common.py`` (``STRATEGIES`` + ``STRATEGY_PRIORITIES``) and
``hermes/service1_agent/agent_construction.py`` (``build``).
"""
from __future__ import annotations

from .cs75 import CreditSpreads75
from .cs7 import CreditSpreads7
from .tt45 import TastyTrade45
from .wheel import WheelStrategy
from .hermes_alpha import HermesAlpha

__all__ = [
    "CreditSpreads75",
    "CreditSpreads7",
    "TastyTrade45",
    "WheelStrategy",
    "HermesAlpha",
]
