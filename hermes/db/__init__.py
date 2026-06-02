"""Persistence layer — SQLAlchemy ORM + thin repository over TimescaleDB.

``models`` is the only place SQL lives. Both Service-1 (writes) and
Service-2 (reads) import ``HermesDB`` from there.

``hermes/db/schema.sql`` is the canonical DDL artifact for the
TimescaleDB-specific bits (hypertables on ``opened_at`` / ``submitted_at``
/ ``ts``, compression policies, retention, the ``pnl_daily`` view).

Schema application by environment:

* **Postgres / TimescaleDB** — Alembic owns it. ``alembic upgrade head``
  applies the baseline (``alembic/versions/0001_baseline.py``, which runs
  ``schema.sql``); on an already-populated DB, ``alembic stamp 0001``
  instead. Future schema changes are new migrations, not ad-hoc edits.
* **SQLite / dev / tests** — ``models.HermesDB.__init__`` calls
  ``create_all(checkfirst=True)`` so plain SQLAlchemy CRUD works without
  Timescale. The ORM models mirror ``schema.sql`` for this path.
"""
