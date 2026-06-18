"""
[Service-1: Hermes-Agent-Core] — shared base for the engine's owned collaborators.

``RuntimeController`` / ``ReactiveController`` / ``AIController`` were previously
mixins inherited by ``CascadingEngine``. They are now owned collaborators
(``engine.runtime`` / ``engine.reactive`` / ``engine.ai``), constructed with a
back-reference to the engine.

Unlike :class:`~hermes.service1_agent._engine_tuning.TuningController` — which
forwards a handful of read-only handles via explicit properties because it owns
its own cadence state — these three share the engine's *hot tick state*
(``queue``, ``loop_task``, ``_pending_futures``, ``_quote_cache``,
``_tracked_orders`` …) and call freely into the engine spine and one another.
Rather than forward ~18 attributes apiece, this base routes every attribute the
collaborator does not define itself to the engine: reads via ``__getattr__``,
writes via ``__setattr__``. That keeps the engine the single source of truth for
shared state and let the method bodies move out of ``core.py`` verbatim.

Tradeoff (the reason these were originally kept as mixins): attribute access on
the collaborators now goes through one extra Python-level hop instead of a
direct ``__dict__`` hit. On the reactive/runtime hot path that is a small but
real cost; if it ever shows up in profiling, cache the hot handles on the
collaborator in ``__init__``.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .core import CascadingEngine


class _EngineCollaborator:
    """Behaviour split out of the engine that shares the engine's state.

    All state lives on the engine; the collaborator holds only the
    back-reference. Any name not defined on the collaborator class resolves on
    the engine (read and write), so the engine stays the single source of truth.
    """

    def __init__(self, engine: "CascadingEngine") -> None:
        object.__setattr__(self, "_engine", engine)

    def __getattr__(self, name: str) -> Any:
        # Only invoked when normal lookup misses (i.e. not a method or _engine),
        # so it forwards shared engine state and spine methods to the engine.
        try:
            engine = object.__getattribute__(self, "_engine")
        except AttributeError:  # pragma: no cover - _engine always set in __init__
            raise AttributeError(name)
        return getattr(engine, name)

    def __setattr__(self, name: str, value: Any) -> None:
        # Collaborators hold no own state beyond _engine; every write targets the
        # shared engine state so core.py and tests observe it on the engine.
        setattr(self._engine, name, value)
