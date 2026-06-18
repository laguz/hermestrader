"""
[Service-1: Hermes-Agent-Core] — shared base for the engine's owned collaborators.

``RuntimeController`` / ``ReactiveController`` / ``AIController`` were previously
mixins inherited by ``CascadingEngine``. They are now owned collaborators
(``engine.runtime`` / ``engine.reactive`` / ``engine.ai``), constructed with a
back-reference to the engine.

They now use explicit typed references (`self.engine`) instead of dynamic magic
routing via `__getattr__` / `__setattr__`. This ensures complete static type-safety
and clarity.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .core import CascadingEngine


class _EngineCollaborator:
    """Behaviour split out of the engine that shares the engine's state explicitly.

    All state lives on the engine; the collaborator holds only the explicit
    reference `self.engine`.
    """

    def __init__(self, engine: CascadingEngine) -> None:
        self.engine = engine
