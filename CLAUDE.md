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

An audit in July 2026 resolved four codebase bugs:
- Fixed mutable defaults for the `body` parameter in approvals routes ([approvals.py](file:///Users/laguz/Git/hermestrader/hermes/service2_watcher/routes/approvals.py)).
- Added `strict=True` to the watchlist analysis `zip()` call ([analytics.py](file:///Users/laguz/Git/hermestrader/hermes/service2_watcher/routes/analytics.py)).
- Removed the unused `unrealized_pnl_pct` column from the `ExitTick` model ([orm.py](file:///Users/laguz/Git/hermestrader/hermes/db/orm.py)) and dropped it from the database table via migration `0006_drop_unrealized_pnl_pct.py`.
- Fixed semicolons in comment lines in [schema.sql](file:///Users/laguz/Git/hermestrader/hermes/db/schema.sql) which had broken Alembic's baseline statement splitting.
- Removed 106 unused `# noqa: BLE001` directives (and other unused ones) using Ruff `RUF100` auto-fixing.

A follow-up audit in July 2026 (DeepSeek V4 Flash Free scan, verified by Opus) fixed 22 confirmed findings:

**Bugs fixed:**
- **Falsy-zero `or` pattern** in [risk_engine.py](file:///Users/laguz/Git/hermestrader/hermes/service1_agent/risk_engine.py) line ~150: `int(config.get(k) or default)` treated `0` as falsy, silently overriding `max_lots=0` with `1`. Fixed to use `if raw is not None` check.
- **Partial cache populate** in [risk_engine.py](file:///Users/laguz/Git/hermestrader/hermes/service1_agent/risk_engine.py) `_sync_broker_orders`: cache was cleared before the try block, leaving it partially populated on mid-loop exception. Fixed with atomic local-dict swap.
- **6 HermesAlpha tunables missing** from the catalog in [tunables.py](file:///Users/laguz/Git/hermestrader/hermes/service1_agent/tunables.py): `hermesalpha_width`, `hermesalpha_target_lots`, `hermesalpha_max_lots`, `hermesalpha_min_credit_pct`, `hermesalpha_time_exit_dte`, `hermesalpha_sl_mult` were read directly from `self.config` in `hermes_alpha.py` but absent from the tunables catalog, bypassing the operator-facing settings API. Added to catalog.

**Silent `except Exception: pass` blocks replaced with logging (20+ locations):**
- [risk_engine.py](file:///Users/laguz/Git/hermestrader/hermes/service1_agent/risk_engine.py): `obp_reserve` fetch failures now log a warning (previously silent → buying-power over-allocation risk).
- [_engine_pipeline.py](file:///Users/laguz/Git/hermestrader/hermes/service1_agent/_engine_pipeline.py): tick-error DB write failure now logged at DEBUG.
- [overseer.py](file:///Users/laguz/Git/hermestrader/hermes/service1_agent/overseer.py): `_mark_llm_ok`/`_mark_llm_error` DB failures logged at DEBUG; AI decision audit write failures logged at WARNING.
- [agent_construction.py](file:///Users/laguz/Git/hermestrader/hermes/service1_agent/agent_construction.py): LLM status DB write failures logged at DEBUG.
- [alpha_killswitch.py](file:///Users/laguz/Git/hermestrader/hermes/service1_agent/alpha_killswitch.py): killswitch audit log write failure now logged at WARNING.
- [_engine_reactive.py](file:///Users/laguz/Git/hermestrader/hermes/service1_agent/_engine_reactive.py): order monitor `create_task` failure now logged at ERROR (previously `except RuntimeError: pass` → monitor silently never started).

**Dead code removed:**
- `main.py`: removed 5 truly dead imports (`next_open`, `session_label`, `_strategy_enabled_key`, `_open_position_pnl`, `_REJECTED_ORDER_STATUSES`) and the dead `_parse_iso` function (moved inline to test).
- `agent_reactive.py`: removed duplicate `_utcnow_iso`, now imports from `main.py`.
- `ml/persistence.py`: removed dead `_HAS_JOBLIB` variable.
- `portfolio/__init__.py`: removed dead re-exports (all consumers import directly from `.safety_gateway`).
- `control_state.py`: removed unreachable `key == "overseer_mode"` in elif at line 121 (already handled at line 104).
- `alpha_killswitch.py`: simplified dead `getattr` else branch (all brokers return dicts).

An additional DeepSeek V4 scan in July 2026 resolved 10 confirmed bugs:
- **Reactive path falsy-zero `or` on max_lots**: Fixed in `_engine_reactive.py` line ~804 to use `is not None` check, ensuring `{strategy}_max_lots=0` is not overridden.
- **HTTP 429 rate-limit retry**: Added `httpx.HTTPStatusError` to `_RETRY_POLICY` in `tradier.py` so rate-limiting and other status failures are retried.
- **Bracket access on `leg["option_symbol"]`**: Used `.get("option_symbol", "")` in `tradier.py` line ~452 to avoid `KeyError` if option symbol is missing.
- **Falsy-zero `or` on `action.quantity`**: Fixed in `trades.py` line ~121 to use `is not None` check to preserve `quantity=0`.
- **Dead/redundant conditions in leg extraction**: Simplified check to just `"sell" in ls` / `"buy" in ls` in `trades.py` lines ~149,151.
- **Copy-paste bug in `option_type` fallback**: Fixed fallback check in `mcp_client.py` line ~250 to check `leg.get("type")` if `option_type` is absent.
- **Falsy-zero on timeout**: Fixed in `overseer.py` line ~146 to preserve `timeout_val=0.0` rather than defaulting to `15.0`.
- **Falsy-zero price fallbacks**: Fixed price parsing in `tradier.py` lines ~214 and ~293 to preserve `price=0.0` instead of falling back to default prices.
- **Falsy-zero lots in `_broker_order_dict`**: Fixed in `broker_wrapper.py` line ~402 to preserve `quantity=0`.

An audit in July 2026 resolved 24 confirmed bugs and codebase improvements:
- **Falsy-zero patterns**: Fixed Option Buying Power (OBP) and Stock Buying Power (SBP) chain fallbacks, `cash_available`, `llm_temperature`, `llm_timeout_s`, leg/action quantities, TradeAction price, and strategy parameters (`current_vol`/`avg_vol`/`target_delta`) using explicit `is not None` and non-empty checks.
- **SQL Injection Risks**: Parameterized database sequence query (`SELECT nextval(:seq)`) in `transaction_manager.py` and wrapped dynamic SQL database name/template statements using `psycopg.sql.SQL` and `psycopg.sql.Identifier` in `provisioning.py`.
- **Descriptive KeyError Handling**: Refactored bare choices dictionary accesses in `clients.py` to raise explicit KeyErrors instead of throwing generic errors.
- **Silent Exception Swallowing**: Added warning and error logging to IPC command publish failures in `agent.py` and `settings.py`, and to log database fetch failures in `status.py`.
- **Dead Code Cleanup**: Removed unused imports in `main.py` (datetime/timezone and 11 unused `SETTING_*` exports), `DummyEngine` class from `test_deepseek_v4_bugs.py`, and `_mm()` helper function from `test_money_manager_sync.py`.
- **Test Suite Enhancements**: Renamed misleading `test_db` fixtures to `schema_db`, `prediction_db`, and `timeseries_db`, consolidated `find_active_ic_expiry` test cases into `test_strategy_helpers.py`, and added assertions to test idempotency and robustness.

## Known false positives for AI code scanners

The following patterns are intentional and should NOT be flagged as dead code or bugs:

**Test-seam re-exports in `main.py`** — these symbols are imported into `main.py` but not used directly there. They exist so tests can monkeypatch them at a single path (`hermes.service1_agent.main.X`). The comment at lines 31–34 documents this. Current re-exports:
- `market_session` (from `hermes.market_hours`) — patched by `test_instant_approvals.py`
- `resolve_max_daily_loss`, `enforce_daily_loss_limit` (from `.agent_risk`) — imported by `test_daily_loss_kill_switch.py`
- `_execute_approved_action` (from `.agent_approvals`) — patched by `test_instant_approvals.py`, imported by `test_c2_approval_execution.py`

**`optimizer.py` `strategy_params` access** — `action.strategy_params` uses `field(default_factory=dict)` on the dataclass, making `.get()` calls safe. Not a missing-attribute bug.

**Dynamic lookups via `getattr`/`__getattr__`:**
- `WatchlistRepository.list_watchlist` — resolved dynamically via `getattr` in `_engine_pipeline.py`.
- `TradesRepository.close_trade_from_action` — resolved dynamically via `getattr` in `agent_approvals.py`.
- `AsyncBrokerWrapper.__getattr__` and `Tunables.__getattr__` — dynamic routing by design.

**Defensively complete APIs:**
- `date_today` in `hermes/utils.py` — clock library completeness.
- `SimulatedClock` in `hermes/clock.py` and `hermes/db/provisioning.py` — test fixtures only.
- `scripts/self_learning_loop.py` — cron-invoked retrospective analyzer, not dead.
- All watcher routes (`routes/*.py`) and DB repositories (`repositories/*.py`) — dynamically resolved by `api.py` and `HermesDB`.
- Vue.js components/views under `hermes/ui/src` — verified mapped in `router.js` and `App.vue`.



