"""Persistence layer — SQLAlchemy ORM + thin repository over TimescaleDB.

``models`` is the only place SQL lives. Both Service-1 (writes) and
Service-2 (reads) import ``HermesDB`` from there.

``schema.sql`` at the repo root is the source of truth for the
TimescaleDB-specific bits (hypertables on ``opened_at`` / ``submitted_at``
/ ``ts``, compression policies, retention, continuous aggregates).
``models.HermesDB.__init__`` defensively calls ``create_all(checkfirst=True)``
so plain SQLAlchemy CRUD works even if ``schema.sql`` was never applied,
but the Timescale features need ``psql -f schema.sql`` to land.

Migration scripts (``migrate_*.py``) are one-shot operator-run helpers —
not invoked at boot. Don't add new ones without operator sign-off.
"""
