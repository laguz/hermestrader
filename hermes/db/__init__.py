"""Persistence layer — SQLAlchemy ORM + thin repository over TimescaleDB.

The **ORM** (``hermes/db/orm.py``, re-exported by ``models``) is the single
source of truth for every table and column. Both Service-1 (writes) and
Service-2 (reads) import ``HermesDB`` from ``models``.

``hermes/db/schema.sql`` is **not** a table catalog — it holds only the
TimescaleDB layer the ORM cannot express: the raw ``bars_*`` tables,
hypertable conversions, compression/retention policies, and the ``pnl_daily``
view. It is applied *after* the ORM tables exist.

Schema application by environment:

* **Production** — Alembic owns it. ``alembic upgrade head`` applies the
  baseline (``alembic/versions/0001_baseline.py``), which creates the tables
  from ``Base.metadata`` and then applies the ``schema.sql`` addendum. On an
  already-populated DB, ``alembic stamp 0001`` instead. Future schema changes
  are new migrations, not ad-hoc edits.
* **Throwaway DBs (tests / simulation)** — ``hermes/db/provisioning.py`` creates
  a fresh Timescale database, installs the ``timescaledb`` + ``vector``
  extensions, and lets ``models.HermesDB.__init__`` run
  ``create_all(checkfirst=True)``; the ``schema.sql`` addendum is applied on top
  when the bars_*/hypertable layer is needed. There is no SQLite fallback.

``tests/test_schema_parity.py`` guards the one remaining seam: every
hypertable-backed ORM table has its ``create_hypertable`` line, and
``schema.sql`` never re-declares an ORM table's columns.
"""
