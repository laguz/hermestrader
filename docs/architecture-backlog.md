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
  (`_engine_pipeline.py`) and the heartbeat body to a controller; `core.py` keeps
  the `_run_tick_internal` spine plus wiring, with thin delegators preserving the
  engine's public surface. Full suite green: 542 passed.)*
- [x] **Consolidate the collaborators (6 → 3, unified back-reference pattern).**
  *(Done — the six `_engine_*.py` collaborators collapsed to three:
  `PipelineController` (tick phases + heartbeat), `ReactiveController` (event-loop
  runtime + reactive handlers), `AIController` (overseer proposals/closes/gating +
  bandit/exit-policy tuning). All three now use the same `self.engine`
  back-reference; `_engine_base.py` and the injected-state property setters in
  `core.py` were deleted, and the duplicated `_read_banned_symbols` /
  `_watchlist_for` helpers were deduped. Public surface unchanged; full suite
  green: 542 passed.)*
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
- [x] **Separate monolithic vs committee review paths.**
  *(Done — the committee path was already in `overseer_committee.py`
  (`CommitteeReviewer`); the single-LLM path now mirrors it in
  `overseer_monolithic.py` (`MonolithicReviewer`). Both are owned collaborators
  with a back-reference to the overseer, routed to from `_consult` per
  `overseer_mode`, behind the unchanged `HermesOverseer.review` facade.
  `core.py`-style thin delegator kept for the committee's failure fallback.
  `overseer.py` 818 → 776 lines. Full suite green: 542 passed.)*
  - **Problem:** Both LLM-review modes live in one 817-line module.
  - **Why:** They're independent code paths sharing a file; splitting clarifies
    which one a change touches and shrinks the blast radius.
  - **Definition of Done:** Monolithic and committee paths live in separate
    modules behind a thin `HermesOverseer.review` facade; the committee's
    Macro / Strategy / Risk-Officer roles are individually testable. Existing
    overseer tests still pass.
  - **Size:** M
  - **NOTE — remaining largeness is a *different* concern:** `overseer.py` is
    still ~776 lines because of the out-of-loop governance/origination methods
    (`propose`, `propose_closes`, `propose_alpha_setup`,
    `propose_parameter_adjustments`, `propose_risk_restrictions`,
    `analyze_charts`). Splitting those into an `OverseerGovernance` collaborator
    is a separate, optional follow-up — not part of the monolithic/committee DoD.

## 4. Split the ML feature surface (`xgb_features.py`, 782 lines)
- [x] **Decompose the predictor (the file's real god-class).**
  *(Done — but the original premise was stale, so the item was re-scoped. The
  pure feature surface was already extracted to `hermes/ml/feature_engineer.py`
  (164 lines, one method per family: gap / momentum / beta-residual / vwap /
  range / volume / realized-vol / seasonality / hv_rank); that file is small and
  cohesive, so exploding it into a `features/` package of ~10-line modules would
  be over-engineering. The 782-line `xgb_features.py` was **not** a feature
  surface — it was the predictor (`AsyncXGBPredictor`). Split into:*
    - *`predictor_config.py` (71) — `PredictorConfig` + `run_maybe_async`.*
    - *`predictor_training.py` (186) — `PredictorTrainer.retrain_all` +
      quantile-fit helpers.*
    - *`predictor_inference.py` (179) — `PredictorInference.predict_all` +
      `_return_to_prob` / `_write_ledger_row`.*
  *`xgb_features.py` 782 → 505. Training/inference are owned collaborators with a
  back-reference to the predictor; they mutate the shared caches (`_models` /
  `_drift` / `_last_pred`) through read-only forwarding properties that return
  the live dict objects, so the method bodies moved unchanged. `_run_ml_cycle` /
  `_loop` route to `self.trainer` / `self.inference`. Scheduling, history sync,
  calibration, feature frames, and diagnostics stay on the predictor. Full suite
  green: 542 passed.)*
  - **Why:** Feature families change independently; the predictor's concerns
    (train vs. predict vs. schedule) change independently too.
  - **Definition of Done:** the predictor's training and inference concerns live
    in their own modules behind `AsyncXGBPredictor`; no single module > ~300
    lines; ML output unchanged (full ML suite green on the same fixtures).
  - **Size:** M
  - **NOTE — optional follow-up:** scheduling (`_loop` / `_run_ml_cycle` /
    `_should_predict` / `_record_status`) and history sync (`_sync_history`)
    could move to a `PredictorScheduler` if `xgb_features.py` keeps growing; not
    needed at 505 lines.

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
