"""Base class for the focused repositories composed onto :class:`HermesDB`.

Each repository owns one slice of the persistence surface (logs, trades,
approvals, …). Rather than being mixed into ``HermesDB`` via inheritance, they
are now *owned* by it — ``HermesDB.__init__`` constructs one of each and exposes
them as attributes (``db.trades``, ``db.approvals``, …). This makes the
collaborators explicit and inspectable instead of flattened onto one MRO.

A repository reads the shared connection handles through the back-reference to
its owning ``HermesDB`` (``self._db``). The forwarding properties below let the
method bodies keep saying ``self.AsyncSession`` / ``self.ts_engine`` unchanged,
so converting a mixin to a repository is a rename, not a body rewrite. Calls
into a *sibling* repository go through the owner explicitly, e.g.
``self._db.logs.write_log(...)``.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from hermes.db.models import HermesDB


class Repository:
    """Holds a back-reference to the owning :class:`HermesDB`.

    Subclasses contribute query methods for one concern; the shared engine /
    session handles are forwarded from the owner so there is a single source of
    truth for connection state.
    """

    def __init__(self, db: "HermesDB") -> None:
        self._db = db

    # ── shared connection handles (single source of truth on the owner) ──────
    @property
    def AsyncSession(self):
        return self._db.AsyncSession

    @property
    def async_engine(self):
        return self._db.async_engine

    @property
    def ts_engine(self):
        return self._db.ts_engine

    @property
    def engine(self):
        return self._db.engine

    @property
    def Session(self):
        return self._db.Session
