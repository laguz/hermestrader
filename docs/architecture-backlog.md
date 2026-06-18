# Architecture Backlog

> Tracked list of structural changes worth completing. Derived from an
> architecture review (2026-06-18). These are *not* bugs — the system works.
> They are the "if I rebuilt Hermes I'd do this differently" items, turned into
> finishable tasks. Each has a Why, a Definition of Done, and rough size.
>
> Order is rough priority. Check items off as PRs land; link the PR next to the box.

## 1. Schema single-source-of-truth / drift guard
- [x] **Collapse the schema to one source of truth.**
  *(Implemented on branch `refactor/schema-single-source`; check the PR link in
  when it merges. Full suite green: 542 passed.)*
  - **Problem:** Four places describe the same schema and can silently diverge —
    `schema.sql` (canonical Timescale DDL), `models.py` ORM (mirrors it by hand,
    swaps `JSONB`→`JSON` for SQLite), `create_all` SQLite bootstrap, and Alembic
    migrations (`alembic/versions/`). The column structure is duplicated by hand
    between `schema.sql` and the ORM.
  - **Why:** A hand-mirrored schema is a prod incident waiting to happen — the
    ORM and the DDL drift and nothing fails until a real column is missing.
    A parity test only *polices* the duplication; the real fix is to delete the
    duplication so there's nothing to police.
  - **Goal — one source for columns, plus an irreducible Timescale addendum.**
    The ORM can't express hypertables, compression policies, or the `pnl_daily`
    view, so a small SQL file must keep those. Everything else collapses onto the
    ORM:
    - **ORM (`models.py`) becomes authoritative** for all table/column structure
      — names, types, nullability, indexes. (`create_all` already derives from it.)
    - **`schema.sql` shrinks to Timescale-only DDL** — `create_hypertable`,
      compression policies, `pnl_daily` view. It stops re-declaring columns, so it
      can no longer drift on them.
    - **Alembic migrations are `--autogenerate`d from the ORM** for column
      structure; the Timescale addendum is applied as an explicit op, not by
      hand-running a column-bearing `schema.sql`.
  - **Definition of Done:**
    - `schema.sql` contains no column-level `CREATE TABLE` duplication of ORM
      tables — only hypertable/compression/view DDL.
    - Postgres bring-up (`alembic upgrade head`) and SQLite bring-up (`create_all`)
      both originate from the ORM; verified to produce matching table/column sets.
    - A small CI test guards the *remaining* seam only: every ORM table that
      should be a hypertable has its `create_hypertable` line in the addendum
      (catches "added a table, forgot the Timescale bits").
    - `ARCHITECTURE.md`'s "Where the data lives" / schema-governance section is
      rewritten to describe the new single-source model.
  - **Size:** L (touches models, `schema.sql`, the Alembic baseline, and the
    SQLite/Postgres bring-up paths — bigger than a test, but it deletes the
    problem instead of monitoring it). Land behind the event-replay parity test.

## 2. Decompose `core.py` (850 lines)
- [x] **Keep splitting the engine spine.**
  *(Done — `core.py` 1149 → 556 lines. The tick-phase bodies (sync / reconcile /
  manage / entries / submit / execute_or_queue) moved to `PipelineController`
  (`_engine_pipeline.py`) and the heartbeat body to `ClockController`
  (`_engine_clock.py`); `core.py` keeps the `_run_tick_internal` spine plus
  wiring, with thin delegators preserving the engine's public surface. Full
  suite green: 542 passed.)*
  - **Problem:** Even after the mixins→controllers refactor, `core.py` is the
    largest file and the one most likely to keep accreting.
  - **Why:** The spine should orchestrate the owned collaborators (`_engine_*.py`),
    not hold logic itself. Gravity wells attract the next feature.
  - **Definition of Done:** `core.py` is pure orchestration — each tick phase
    (sync / manage / entries / overseer) delegates to a named collaborator; no
    phase body exceeds a screen. Target < ~400 lines. Behavior unchanged
    (event-replay parity test still green).
  - **Size:** M–L

## 3. Decompose `overseer.py` (817 lines)
- [ ] **Separate monolithic vs committee review paths.**
  - **Problem:** Both LLM-review modes live in one 817-line module.
  - **Why:** They're independent code paths sharing a file; splitting clarifies
    which one a change touches and shrinks the blast radius.
  - **Definition of Done:** Monolithic and committee paths live in separate
    modules behind a thin `HermesOverseer.review` facade; the committee's
    Macro / Strategy / Risk-Officer roles are individually testable. Existing
    overseer tests still pass.
  - **Size:** M

## 4. Split the ML feature surface (`xgb_features.py`, 782 lines)
- [ ] **Turn the feature file into a small package.**
  - **Problem:** Even after the pure feature-engineering extraction, one file
    holds the whole feature surface.
  - **Why:** Feature families (price/vol/IV/calendar/etc.) change independently;
    a package with per-family modules is easier to test and extend.
  - **Definition of Done:** `hermes/ml/features/` package with per-family modules
    and a thin assembler; no single module > ~250 lines. ML backtest output
    unchanged on a fixed seed/fixture.
  - **Size:** M

## 5. Make autonomous trade-origination opt-in, not load-bearing
- [ ] **Gate LLM-originated trades behind proven-in-sim.**
  - **Problem:** `autonomous` autonomy + HermesAlpha let the LLM *originate*
    positions — the riskiest, least-testable surface — and it's wired in as a
    peer strategy rather than an opt-in.
  - **Why:** advisory + enforcing (veto/modify) are high-value and easy to
    reason about; origination should have to earn its place.
  - **Definition of Done:** Decide & document the stance. Either (a) keep it but
    add a sim gate — a backtest assertion that the autonomous path clears a
    defined bar before it can run live — or (b) make it a clearly-flagged,
    default-off capability. Whichever: the decision is written down in
    `ARCHITECTURE.md` and the default config reflects it.
  - **Size:** S (decision) + M (sim gate, if chosen)
  - **NOTE:** This is the one item to discuss before building — see review notes.

## 6. (Meta) Front-load structural decisions
- [ ] **Capture the "start here next time" defaults.**
  - **Why:** Items 2–4 are all retrofits of "started as a mixin/flat/one-file and
    grew." Worth recording the target shape so new modules start there.
  - **Definition of Done:** A short "module conventions" section in `AGENTS.md`
    (owned collaborators over mixins; namespaced repos; per-family packages over
    god-files; the one-layer-up/down rule) so the next contributor starts at the
    destination.
  - **Size:** S
