"""Persistence layer — SQLAlchemy ORM + thin repository over TimescaleDB.

The **ORM** (``hermes/db/orm.py``, re-exported by ``models``) is the single
source of truth for every table and column. Both Service-1 (writes) and
Service-2 (reads) import ``HermesDB`` from ``models``.

``hermes/db/schema.sql`` is **not** a table catalog — it holds only the
TimescaleDB layer the ORM cannot express: the raw ``bars_*`` tables,
hypertable conversions, compression/retention policies, and the ``pnl_daily``
view. It is applied *after* the ORM tables exist.

Schema application by environment:

* **Postgres / TimescaleDB** — Alembic owns it. ``alembic upgrade head``
  applies the baseline (``alembic/versions/0001_baseline.py``), which creates
  the tables from ``Base.metadata`` and then applies the ``schema.sql``
  addendum. On an already-populated DB, ``alembic stamp 0001`` instead.
  Future schema changes are new migrations, not ad-hoc edits.
* **SQLite / dev / tests** — ``models.HermesDB.__init__`` calls
  ``create_all(checkfirst=True)`` so plain SQLAlchemy CRUD works without
  Timescale; the hypertable/compression DDL simply doesn't apply there.

``tests/test_schema_parity.py`` guards the one remaining seam: every
hypertable-backed ORM table has its ``create_hypertable`` line, and
``schema.sql`` never re-declares an ORM table's columns.
"""
