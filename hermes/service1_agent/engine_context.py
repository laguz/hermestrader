"""
[Service-1: Hermes-Agent-Core] — shared engine dependency surface.

:class:`EngineContext` is the single source of truth for the dependencies and
shared mutable state that the :class:`~hermes.service1_agent.core.CascadingEngine`
and its owned collaborators (``pipeline`` / ``reactive`` / ``ai``) all read:
the database, broker wrapper, money manager, event bus, config, clock, IPC
client, overseer, strategy roster, risk engine, the operator-tunable
``control_state`` / ``approval_mode`` / ``llm_out_of_loop`` flags, and the shared
``quote_cache``.

Both the engine and the collaborators hold a reference to **one** context, so
the ~200 ``self.engine.<dep>`` reads in the collaborators become reads against a
small, named surface instead of the whole engine. Several of these are
reconfigured at runtime (``main.py`` swaps the LLM, flips ``approval_mode``,
re-rosters ``strategies``; tests reassign ``overseer`` / ``mm`` / ``ipc_client``
/ ``control_state``), so they cannot be copied per-collaborator — the context is
the shared cell they all see. The engine keeps ``engine.<dep>`` working as a
read/write proxy onto this context for backward compatibility.

What is **not** here: per-run engine runtime state (the circuit-breaker counters
and the background-task set) and the orchestration *methods* the collaborators
still call back into the engine for. Those stay on the engine — the context is
the data surface, not a second engine.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Sequence

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .strategy_base import AbstractStrategy


class EngineContext:
    """Shared dependency + mutable-state surface for the engine + collaborators."""

    def __init__(self, *, db, broker, config: Dict[str, Any], clock, event_bus,
                 ipc_client, overseer, mm, risk_engine,
                 strategies: Sequence["AbstractStrategy"],
                 approval_mode: bool, llm_out_of_loop: bool) -> None:
        self.db = db
        self.broker = broker
        self.config = config
        self.clock = clock
        self.event_bus = event_bus
        self.ipc_client = ipc_client
        self.overseer = overseer
        self.mm = mm
        self.risk_engine = risk_engine
        # Sorted by declared PRIORITY (1 highest); the setter preserves it.
        self._strategies = sorted(strategies, key=lambda s: s.PRIORITY)
        self.approval_mode = approval_mode
        self.llm_out_of_loop = llm_out_of_loop
        # Set after construction by main.py (and tests); starts unset.
        self.control_state = None
        # Shared per-tick quote memo, mutated in place by several collaborators.
        self.quote_cache: Dict[str, Dict[str, Any]] = {}

    @property
    def strategies(self):
        return self._strategies

    @strategies.setter
    def strategies(self, val: Sequence["AbstractStrategy"]) -> None:
        # Collaborators read ``ctx.strategies`` directly; this setter only
        # preserves the PRIORITY sort invariant.
        self._strategies = sorted(val, key=lambda s: s.PRIORITY)
