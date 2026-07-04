# CLAUDE.md — HermesTrader

Guidance for Claude Code working in this repo. Keep this file short; the
detailed maps live in [`ARCHITECTURE.md`](ARCHITECTURE.md) (read first) and
[`AGENTS.md`](AGENTS.md) (conventions, MCP, entry points).

## What this is

A two-service options-trading system on a TimescaleDB backbone:

- **Service-1 — Hermes Agent** (`hermes/service1_agent/`): event-driven
  `CascadingEngine` that ticks five priority-ordered strategies
  (CS75 → CS7 → TT45 → Wheel → HermesAlpha), sized by `MoneyManager`,
  reviewed by the LLM `HermesOverseer`. **The only writer.** Places real
  broker orders.
- **Service-2 — Watcher** (`hermes/service2_watcher/`): read-only FastAPI
  operator panel (approvals, soul doctrine, paper/live toggle, P&L).

Persistence: TimescaleDB via SQLAlchemy (`hermes/db/`). Broker: Tradier REST
(`hermes/broker/tradier.py`). LLM overseer is provider-agnostic
(`hermes/llm/`).

## Safety rules (this code places real money orders)

1. Treat `core.py`, `strategies/`, `tradier.py`, and `MoneyManager` as
   safety-critical. **Add a regression test before fixing a bug** in them.
2. Never weaken `dry_run` defaults or add a path that places a live order
   without honoring the operator's `approval_mode` setting. **One gated
   exception** (the only no-human-in-the-loop path): an entry from **any**
   strategy skips the human approval queue when — and only when —
   `autonomy=='autonomous'` **and** the default-OFF `alpha_autonomous_live`
   switch is armed (the operator-facing "Auto-Execute" toggle). Even then
   `dry_run`, the paper/live toggle, the off-hours gate, and
   `PortfolioRiskEngine` still apply. The carve-out lives in
   `_engine_pipeline._execute_or_queue`; the bypass is gated on both conditions
   together — don't drop the `autonomy=='autonomous'` check or the
   `alpha_autonomous_live` gate, and never make either default to on.
3. The tick pipeline is **order-sensitive**: sync positions → sync broker
   orders → reconcile orphans → manage exits → entries in priority order →
   overseer proposals. Don't reorder it.
4. **Single-writer invariant**: only Service-1 writes the event-sourced read
   models, ledger, time series, **and `system_settings` / `pending_approvals`**.
   Service-2 is read-only except `operator_commands` (the durable queue it
   appends operator intents to), `strategy_watchlists`, `bot_logs`, and the
   `strategies` seed. Operator toggles/approvals flow through `operator_commands`,
   which `CascadingEngine.drain_operator_commands` applies in the agent process —
   never write `system_settings`/`pending_approvals` from the watcher.
   `tests/test_writer_ownership.py` enforces this.
5. Order tags round-trip as both `HERMES_<STRAT>` and `HERMES-<STRAT>`
   (Tradier rewrites `_`→`-`). Any new matcher must accept both forms.

## Working style

- Python 3.11+, type-hinted public functions. Match the surrounding code's
  idioms, naming, and comment density.
- **Default to no comments**; add one only when the *why* is non-obvious (a
  workaround, a Tradier quirk, a hidden invariant).
- Keep diffs focused — don't reformat unrelated code or add top-level packages
  without a clear reason.
- The ORM (`hermes/db/orm.py`) is the single source of truth for schema;
  `schema.sql` is only the TimescaleDB addendum. Don't add a second catalog.

## Testing

```bash
pip install -r requirements.txt && pip install pytest ruff
pytest tests -q
ruff check --select E9,F63,F7,F82 hermes tests
```

Most tests use the stub-broker / stub-DB pattern and need no database
(`tests/test_money_manager_sync.py`). DB-backed tests provision a throwaway
Timescale DB and **skip** when no server is reachable — they never fail for
lack of a DB. CI runs the full suite against a Timescale service container.

## Commits & PRs

- Imperative messages with a type prefix: `fix:`, `feat:`, `refactor:`,
  `test:`, `ci:`, `docs:`. One logical change per PR; include a **Test plan**.
- Verify the current branch before committing — this repo is actively
  multi-branch and checkout can move between turns.

## Don't touch

- `VERSION` (unless asked), `hermes/scratch/` and `scratch/` (exploratory),
  and schema migrations without operator sign-off (data is live in Timescale,
  migrations run by hand).

## Recent cleanup (context for future dead-code audits)

PR #188 removed six watcher endpoints and one repo method that had zero
callers across the UI, tests, docs, and scripts — verified, not just
grep-absent: `GET /api/balances`, `GET /api/strategies`, `GET
/api/analysis/{symbol}`, `GET`/`POST /api/admin/ml-intervals`, the whole
`GET`/`PUT /api/tunables` router (file deleted), and
`WatchlistRepository.add_to_watchlist`. `resolve()` and the `TUNABLES`
catalog in `hermes/service1_agent/tunables.py` are untouched — strategies
still read them every tick — but tunables lost their HTTP write path;
retuning a value now requires a direct `system_settings` write instead of
`PUT /api/tunables`. Don't re-flag these as "missing" API surface; if the
tunables panel or per-symbol analysis view comes back, restore from that
PR's diff rather than re-deriving it.

Kept deliberately in that same audit: `GET /api/debug` (operator triage
endpoint, no UI caller by design) and the admin instance/upgrade routes
(`scripts/upgrade_runner.sh` depends on them).

An audit in July 2026 removed the unused `mark_outcome` in `hermes/ml/ledger.py`. To close the ML measurement loop, a new batch-oriented `backfill_prediction_outcomes` function was implemented in `hermes/ml/ledger.py`, wired into `PipelineController.handle_clock_tick_internal` (the agent heartbeat) to run every tick, and reused in `scripts/nightly_calibrate.py`.
Kept deliberately as false positives / complete APIs:
- `WatchlistRepository.list_watchlist` and `TradesRepository.close_trade_from_action` — these are looked up dynamically via `getattr` in `_engine_pipeline.py` and `agent_approvals.py`.
- `date_today` in `hermes/utils.py` — matches clock library completeness.
- `SimulatedClock` in `hermes/clock.py` and `hermes/db/provisioning.py` — used exclusively by test suite fixtures.
- `__getattr__` methods in `AsyncBrokerWrapper` and `Tunables` — dynamic routing.
- `scripts/self_learning_loop.py` — retrospective closed-trade analyzer run via cron job to update operator doctrine (`soul_md`).
- All watcher routes (`routes/*.py`) and DB repositories (`repositories/*.py`) — resolved dynamically by `api.py` and `HermesDB` context.
- Vue.js components/views under `hermes/ui/src` — verified as completely mapped in `router.js` and `App.vue`.

An audit in July 2026 removed 9 dead settings fields from `HermesSettings` in `hermes/config.py` that were parsed from the environment but never read from the `settings` singleton: `hermes_env_file`, `hermes_ai_autonomy`, `hermes_soul_path`, `llm_provider`, `llm_model`, `llm_api_key`, `llm_temperature`, `llm_vision`, and `llm_timeout_s`. The `validate_autonomy` validator was also removed. Mock signatures for `xreadgroup` in `tests/test_durable_loop.py` and `tests/test_reactive_loop.py` were refactored with `*args, **kwargs` to resolve unused parameter warnings.



