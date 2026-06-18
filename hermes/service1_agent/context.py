"""[Service-1: Hermes-Agent-Core] — TickContext defining execution state for a single sweep."""
from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import Any, Dict, List, Set


@dataclasses.dataclass
class TickContext:
    """Carries the immutable and cached data queried at the start of a single CascadingEngine tick."""
    timestamp: datetime
    watchlist: List[str]
    banned_symbols: Set[str] = dataclasses.field(default_factory=set)
    positions: List[Dict[str, Any]] = dataclasses.field(default_factory=list)
    active_order_legs: Set[str] = dataclasses.field(default_factory=set)
