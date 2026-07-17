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

**DS0 direction pairing (`strategies/ds0.py`)** — a qualified *support* arms a
**put** debit spread and a qualified *resistance* arms a **call** debit spread.
This reversion-toward-the-level pairing is operator-specified (2026-07-10,
`docs/ds0_spec.md` v2 revision note) and the opposite of the common touch-fade
idiom — it is NOT inverted legs. There is also deliberately no
price-proximity/touch trigger (the old `ds0_trigger_band` was removed at the
operator's instruction — not a missing tunable): entry qualification is
POP ≥ 0.75 plus the level sitting inside session-open ± Wilder ATR(14), and
the day-limit itself is the trigger ($0.08 since the 2026-07-17 operator
retune; `ds0_open_price`).

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

An audit in July 2026 resolved 13 confirmed bugs and codebase improvements:
- **Falsy-zero Quantity Defaults**: Fixed order response parsing in `tradier.py`, order class actions (equity/option/multileg) in `mcp_client.py`, risk limit checks in `safety_gateway.py`, quantity logs in `trades.py`, and realized P&L calculations in `orm.py` and `analytics.py` to preserve zero quantities and prevent fabricated P&L.
- **Silent Exceptions Swallowing**: Replaced silent `except Exception: pass` blocks with logged warnings/exceptions across `predictor_inference.py`, `xgb_features.py`, `feature_engineer.py`, `approvals.py`, `watchlist.py`, `strategy_base.py`, and `main.py` for increased operational observability.
- **Stale Documentation & Duplication**: Cleared stale references to `hv_rank` in `xgb_features.py` and `feature_engineer.py` docstrings, and imported `OCC_RE` from `hermes.common` in `tradier.py` to reduce code duplication.

An audit in July 2026 resolved 26 additional confirmed bugs and codebase improvements across 22 new regression tests in `tests/test_audit_bug_fixes.py`:
- **Falsy-zero checks**: Fixed `action.price` default in `money_manager.py`, `target_lots` lookup in `_credit_spread_base.py`, `lots` check in `safety_gateway.py`, orphan order `quantity` adoption in `_engine_pipeline.py`, `horizon_dte` in `ledger.py`, `dte_min`/`dte_max` checks in `hermes_alpha.py`, strategy parameter delta/short_delta in `money_manager.py`, delta check in `tt45.py`, and tracked order avg price fallback checks in `_engine_reactive.py` to use explicit `is not None` checks.
- **Silent Exception Logging**: Replaced silent `except Exception: pass` catches with warning or debug logging in IPC publishing (`commands.py`), regime weights database lookup (`regime_weights.py`), reactive agent ML triggers and watchlist caching (`agent_reactive.py`), Safety Gateway DB logging and timestamp parsing (`broker_wrapper.py`), stream DB/Websocket connection errors (`mock_stream.py`, `tradier_stream.py`), Redis IPC backend connection failures (`ipc.py`), FastAPI watcher JSON deserialization (`routes/status.py`), conftest engine teardown (`conftest.py`), overseer JSON formatting/vision snapshots (`overseer.py`), and reactive loop deserialization (`_engine_reactive.py`).




An audit in July 2026 resolved 14 confirmed bugs:
- **Falsy-zero checks**: Fixed Kelly score `delta` lookup in `optimizer.py`, `delta` and `price` fallback checks in `mcp_client.py`, and spot price check in `strategy_base.py` to use explicit `is not None` checks.
- **Silent Exception Logging**: Replaced silent `except Exception: pass` catches with warning logging in startup option tracking and shutdown IPC commands (`main.py`), database schema creation (`models.py`), `target_lots` column missing query fallback (`watchlist.py`), approvals routes IPC commands (`routes/approvals.py`), machine learning status writes (`xgb_features.py`), and ingest/analysis timestamp check failures (`_engine_pipeline.py`).

A July 2026 audit (DeepSeek scan, hand-verified against source line-by-line before
fixing anything — several of the report's claims did not match the actual code)
fixed 9 confirmed bugs with regression tests in `tests/test_verified_audit_bugs.py`.
Each test was confirmed to fail when its fix was reverted, not just pass with it in place:
- **`utcnow_iso()` bypassed the virtual clock** (`utils.py`): called `datetime.now(timezone.utc)`
  directly instead of `utc_now()`/`_GLOBAL_CLOCK`, leaking real wall-clock time into
  backtests under a `SimulatedClock`. Fixed to route through `utc_now()`.
- **`engine.overseer` accessed without a None guard** in `main.py`'s
  `_handle_settings_changed`: guarded `.stop()`/`.start()` with `if engine.overseer is
  not None` but then touched `.llm`/`.vision_enabled`/`.autonomy`/`.soul`/`.overseer_mode`
  unconditionally right after — `CascadingEngine.overseer` defaults to `None` in its
  constructor, so this is reachable, not just theoretical.
- **`stream_client.stop()` called during the startup race window** in `main.py`'s
  `_handle_mode_change`: `stream_client` is `None` from `_run_async` startup until the
  synchronous assignment further down; a `ModeChangedEvent` arriving in that window
  crashed with `AttributeError`. Added a `None` guard.
- **`hermes_alpha.py` `width` falsy-zero** (line ~117): `int(intent.get("width") or
  default_width)` silently replaced an overseer-specified `width=0` with the default —
  every sibling field on the same lines already used `is not None`.
- **`mcp/server.py` forced `dry_run=False` outside `"live"` mode** (`_broker()`, line
  ~64): weakened the operator's `hermes_dry_run` setting (default `True`) to `False`
  for paper mode and any unrecognized mode, contradicting the "never weaken `dry_run`
  defaults" safety rule above. Fixed to always honor `settings.hermes_dry_run`.
  **A pre-existing test (`test_dual_and_updates.py::test_mcp_server_broker_mode_aware_paper`)
  had asserted `dry_run is False` for paper mode — i.e. it had locked in the bug as
  expected behavior.** Updated to assert `True`. If you see other tests asserting a
  weakened `dry_run` for non-`"live"` modes, they're pinning this same bug — fix the
  assertion, not the code.
- **`analytics.py` exit_price falsy-zero fabricated P&L** (`get_strategy_performance_metrics`,
  lines ~107, 190): `exit_price=t.exit_price or 0.0` converted `None` (unresolved exit)
  into `0.0`, bypassing `_compute_realized_pnl`'s own `None` guard and reporting a
  fabricated ~$200/lot P&L for trades with no recorded exit fill.
- **Circuit breaker silently degraded when `db` is `None`** (`circuit_breaker.py`
  `_trip()`): tripped to `OPEN` but only logged/paused inside the `if db is not None`
  branch — no signal at all when `db` was `None`. Added a `logger.warning` on the `else`.
- **`OllamaCloudLLM.timeout_s` never reached the network call** (`llm/clients.py`):
  stored on `self` and accepted as a per-call `chat()` kwarg but never passed to the
  underlying `ollama.Client` — unlike `OpenAICompatibleLLM`, which does apply it. Fixed
  by passing `timeout=` at `Client` construction, and building a scoped client for a
  per-call override that differs from the instance default.
- **`timeseries.py` `reset_index` collision** (`_normalize_for_write`): if a DataFrame's
  index and a column were both named `"ts"`, `df.reset_index()` raises `ValueError`
  (pandas ≥1.5, duplicate column) instead of normalizing. Added a branch that drops the
  index in that case.

**Also applied (style-only, no behavioral change — no regression test written because
there's nothing to regress):** `risk_engine.py`'s `if action.width:` → `if action.width
is not None:` (line ~163). The pre-`if` default for `requirement_per_lot` is already
`0.0`, identical to what `width=0.0 * 100.0` produces, so this specific instance has no
observable effect; it's kept only for consistency with the codebase's established
falsy-zero convention. Don't waste time hunting for a functional difference here.

**Report claims that were checked against source and found wrong — don't re-flag:**
- `market_hours.py`'s off-hours override reading `os.environ` directly is not a bug: it's
  the *only* mechanism (no competing DB/UI toggle exists anywhere in the watcher or
  `tunables.py` catalog — verified by grep), and the docstrings at the top of the file
  and on `offhours_trading_allowed()` say so explicitly.
- `_engine_pipeline.py` line 826 is unrelated bar-ingest scheduling code, not an
  `action.width` falsy-zero bug — `action.width` doesn't appear anywhere in that file.
  (The real falsy-zero `action.width` pattern lives in `risk_engine.py`, see above.)
- The local imports in `_engine_pipeline.py`'s `handle_clock_tick_internal` (~lines
  687–733) aren't redundant — those names aren't imported at module level in that file,
  so nothing is duplicated.
- `SubmitTradeActionsCommand.execute_directly` (`events/bus.py`) is not dead: read at
  `core.py:275`, set at `_engine_ai.py:101`.

**Confirmed-real, deferred from that pass, then resolved on request (2026-07-09,
dead code / pure style — no regression tests, nothing behavioral to regress):**
`OrderTrackedEvent` removed (`events/bus.py` class + `_engine_reactive.py` import,
subscription, `handle_order_tracked`, deserialize-map entry — it was never published
anywhere; the order monitor is still started via `_engine_pipeline.py`'s orphan
adoption and its own initial broker scan); `ControlState.approved_actions` and
`refresh_approvals()` removed (populated/logged but never read — the pipeline calls
`db.approvals.fetch_approved_actions()` directly; the `trigger_approvals` IPC handler
now only calls `engine.execute_approved_actions()`); `db/orm.py` unused module-level
`logger` (+ its `logging` import) removed; unreachable `if quotes else {}` ternaries
in `tradier.py`/`mcp_client.py` `get_delta` simplified; dead `db` param dropped from
`AsyncIPC.connect` (callers in `main.py`/`api.py` updated); dead `target` param
dropped from `MockStreamClient.__init__` (the "GRPCStreamClient compatibility" its
comment claimed no longer exists anywhere); redundant local re-imports removed in
`main.py` (`threading`), `routes/status.py` (`json` ×2 — the `_json` alias now uses
the module-level import), `repositories/trades.py` (`select`, `Trade`),
`repositories/projections.py` (`Trade`, `PendingOrder`); the two f-strings inside
`logger.exception(...)` in `_engine_reactive.py` converted to lazy `%s` args;
return-type annotations added to the `hermes_alpha.py` no-cover override hooks
(`_resolve_entry_expiry`, `_completion_window`, `_close_reason`, matching the base
class signatures); `_credit_spread_base.py`'s broad `except Exception` around the
`sl_mult` tunable `float()` narrowed to `(KeyError, TypeError, ValueError)` —
`KeyError` included because `_tun` is a raw `t[key]` lookup.

A July 2026 audit (DeepSeek scan, hand-verified line-by-line — the report's three
"TRUE BUGS" turned out to be false positives, see below) resolved 1 dead-code finding
and 6 style-only falsy-zero occurrences. No regression tests were written for the
style-only fixes (same rationale as the `risk_engine.py:163` precedent above — the
fallback default already equals the `or` default, so there's no behavior to regress):
- **Dead type alias**: removed `_RegimeWeightLookup = Callable[[str, str],
  List[float]]` in `pop_engine.py` (and the now-unused `Callable` import) — zero
  references anywhere; `_regime_weight_lookup` is typed `Any`, not this alias.
- **Falsy-zero style consistency** on `TradeAction.price`/`.width` (both
  `Optional[float]`, so `0.0` is genuinely falsy — unlike the settings-read case
  below): `optimizer.py` (credit/width), `safety_gateway.py` (5 occurrences: width/
  credit/price × multileg/option/equity branches, plus `t_width`/`t_credit` from a
  dict), `risk_engine.py:236-237` (`width`/`entry_credit` in the running-open-trades
  audit dict), and `_engine_reactive.py:825` (`if action.width:` → `if action.width
  is not None:`) — the twin of the `risk_engine.py:163` instance fixed in the prior
  audit, same pattern in a different file.

**Report claims checked against source and found wrong — these are false positives,
not bugs; don't re-flag them:**
- **`routes/llm.py:57` (`temperature`) and `:61` (`timeout_s`)** — the report claims
  `float(await db.settings.get_setting(...) or default)` silently discards an
  operator-set `0.0`. It doesn't: `SettingsRepository.get_setting` returns
  `Optional[str]`, and every writer stores numbers via `str(value)` (see
  `routes/llm.py`'s `PUT` handler: `updates[SETTING_LLM_TEMPERATURE] =
  str(body.temperature)`). A stored `0.0` becomes the string `"0.0"`, which is
  **truthy** — `"0.0" or 0.2` evaluates to `"0.0"`, not `0.2`. The `or` only falls
  back on a genuinely absent setting (`None`) or an explicitly empty string, which is
  correct. The falsy-zero pattern only bites when the underlying value is a real
  numeric `0`/`0.0` (e.g. `TradeAction.price`/`.width` above); it does not apply to
  values that round-trip through `str()` on write and are read back as `Optional[str]`.
- **`predictor_inference.py:145`** (`ml_current_vol__{symbol}` vol lookup) — same
  string/DB-read shape as above, so not a live falsy-zero bug even in principle.
  Additionally, grep confirms this key has **no writer anywhere in the codebase** —
  `get_setting` always returns `None` here today, so the `or 0.30` fallback always
  fires regardless of the `or` pattern. Not a bug; if anything, evidence the setting
  is currently unused/unwired, not something to "fix" by changing the `or`.
- If another scan resurfaces these three `routes/llm.py` / `predictor_inference.py`
  lines as falsy-zero bugs, the report is confusing a DB-string read with a raw
  numeric read — verify the source type (`Optional[str]` vs `Optional[float]`) before
  trusting the pattern-match.

A July 2026 audit pass 2 (multi-agent scan, hand-verified against source before
fixing) resolved 4 confirmed bugs with regression tests in
`tests/test_july2026_audit_pass2.py`. Each test was confirmed to fail when its fix
was reverted:
- **`ledger.py` `backfill_prediction_outcomes` falsy-zero `spot`**: `spot =
  float(row.spot) if row.spot else realized_close` replaced a genuinely recorded
  `spot=0.0` with `realized_close`, making `outcome = 1.0 if realized_close > spot
  else 0.0` compare `realized_close` against itself — always `0.0`, regardless of
  real price movement. Fixed to `if row.spot is not None else`. (The sibling
  falsy-zero-shaped reads in `fetch_for_calibration`, `float(r.spot or 0.0)` /
  `float(r.predicted_prob or 0.0)`, are **not** bugs: their `or` fallback is `0.0`,
  identical to what a genuine `0.0` already produces, so there's nothing to
  regress — same precedent as `risk_engine.py:163` below.)
- **`_engine_reactive.py` `_process_event` silently dropped unrecognized
  `event_type`**: the if/elif chain covers exactly `TICK`/`CLOCK_TICK`/
  `AI_APPROVAL`/`MARKET_DATA`/`ORDER_FILL`; anything else (a malformed or
  corrupted Redis Streams payload) fell through with `res=None` and
  `fut.set_result(None)` — acked as if successfully processed, no log signal.
  Added an `else: logger.warning(...)` branch.
- **`_engine_reactive.py` `process_reactive_entries` watchlist-lookup gather had
  no exception isolation**: `_check_watchlist(s)` called `self.engine._watchlist_for`
  with no try/except, and the `asyncio.gather(...)` collecting all strategies had
  no `return_exceptions=True` — unlike the very next `gather` one function down
  (`_run_reactive_entries`), which explicitly catches per-strategy so one failure
  can't take out the rest. A single strategy's transient DB error during watchlist
  lookup aborted reactive entry evaluation for **every** strategy on that
  support/resistance trigger, not just the one that failed. Wrapped
  `_check_watchlist` in the same try/log/return-`None` pattern as its sibling.
- **`tradier.py` `cancel_order` bypassed the shared retry policy and structured
  error-body logging** that every other network call in the file uses (`_get` via
  `@retry(**_RETRY_POLICY)`, all paths via `_raise_with_body`). Added both.
  Deliberately scoped to `cancel_order` only, not order-placement (`_post`): a
  DELETE is idempotent (re-cancelling an already-cancelled order is harmless), but
  blindly retrying a POST that places a real order is not — if the first attempt
  actually succeeded broker-side and only the response was lost, a retry could
  double-place the order. **`_post`'s lack of a retry decorator is intentional,
  not a bug** — don't "fix" it by wrapping order-placing calls in `_RETRY_POLICY`;
  that trades a clean failure for a duplicate-order risk, which is strictly worse
  in a system whose whole safety posture is "never place an order the operator
  didn't intend."
- **Also fixed (no regression test — pure observability, same rationale as the
  `risk_engine.py:163`-class style-only fixes)**: `scripts/nightly_calibrate.py`'s
  `find_key_levels` call was the only one of 9 exception handlers in the file with
  no `logger.warning` on catch; added one for consistency. The sibling bare
  `except Exception: return []` around the `PredictionLedger` import in
  `_enumerate_symbols` is **not** the same anomaly — it mirrors the established
  `if Base is not None` / `if PredictionLedger is None: return` ORM-unavailable
  degrade-safely pattern used throughout `ledger.py` itself, not a silent-swallow bug.

A July 2026 follow-up (2026-07-09) fixed 1 confirmed bug with a regression test in
`tests/test_durable_loop.py` (confirmed to fail with the fix reverted):
- **Durable consumer stranded the publisher's future on pre-`_process_event`
  exceptions** (`_engine_reactive.py` `_redis_event_consumer_loop`): an exception
  raised before `_process_event` ran (corrupt stream payload failing `json.loads`,
  deserialization error) was only logged; the `finally` block then popped the future
  from `_pending_futures` unresolved, so `publish_event` sat awaiting it for the full
  `_PUBLISH_RESULT_TIMEOUT_S` (300s), holding its EventBus dispatch permit — enough
  corrupt entries in one window re-creates the 2026-07-08 all-permits-held bus
  freeze, just timeout-bounded. The consumer now resolves the pending future with
  the exception before popping. (Exceptions from *inside* `_process_event` were
  already resolved by its own except-block — that path was and is fine.)
- Also added `test_publish_event_stress_no_leaks_under_races_and_handler_errors`
  (30 concurrent publishes × mixed race outcomes × injected handler failures →
  `_pending_futures` must end empty), generalizing the single-interleaving corr_id
  race test, and removed the dead `event_bus` fixture in `tests/test_event_bus.py`
  (no test ever took it as a parameter; every test builds its own local bus).
- **Considered and deliberately not done**: merging the audit-named test files
  (`test_deepseek_v4_bugs.py`, `test_medium_bug_fixes.py`, `test_audit_bug_fixes.py`,
  `test_verified_audit_bugs.py`, `test_july2026_audit_pass2.py`) into per-subsystem
  files. These filenames are the provenance trail this document's audit entries
  reference; moving 51 tests would stale those records and risk breaking
  monkeypatch/fixture paths for organizational benefit only. Don't re-propose the
  merge without also reconciling every reference above.

A July 2026 duplicate-code audit (2026-07-09, jscpd + hand-verification; 19 clones
found at 1% overall duplication) consolidated 4 and deliberately kept the rest:
- **Consolidated** (behavior-preserving, full suite green): `xgb_features.py` `_loop`
  body was a verbatim ~40-line copy of `_run_ml_cycle` → now calls it;
  `risk_engine.py`'s balances/`obp_reserve`/open-trades fetch appeared in both the
  optimizer and priority branches → `_available_bp_and_open_trades()`; the per-action
  entry-sizing preamble (requested-lots → `max_lots` map/config override →
  `requirement_per_lot`) was duplicated between `risk_engine.py` and
  `_engine_reactive.py` — the falsy-zero `max_lots` bug had to be fixed once per copy —
  → `resolve_entry_sizing()` in `money_manager.py`, used by both; `trades.py`'s
  order-response parsing (×2), lots-resolution (×3), and side-derivation (×3) blocks
  → `_parse_order_response`/`_resolve_lots`/`_derive_side_type` helpers whose
  parameters preserve the real per-caller differences (close-leg matching only in
  `close_trade_from_action`, `action.side` fallback only in `record_pending_order`).
- **Kept deliberately — don't re-flag as dupes**: `tradier.py` `_place_option`/
  `_place_equity` shared tail (order-placing code; a refactor there trades clarity
  for risk in the file whose safety posture matters most); `approvals.py` repo
  `record_veto`/`active_veto` preambles (same query shape, *different* matching
  semantics — exact vs wildcard); `routes/approvals.py` approve/reject bodies
  (distinct logs/payloads, low value); `hermes/ipc.py` backend `subscribe`/
  `unsubscribe` (two backends implementing one interface); `trades.py::_trade_dict`
  vs `routes/analytics.py` open-trades dict (agent-side ORM vs watcher-side raw SQL —
  consolidating couples the two services); import-block "clones" in
  `agent_construction.py`/`agent_risk.py` and `api.py`/`routes/__init__.py`.

**Areas re-checked this pass with no new findings** (don't re-audit from scratch
next time unless something material changed): core tick pipeline / strategies /
`MoneyManager` / `risk_engine.py` (including re-verifying the `alpha_autonomous_live`
+ `autonomy=='autonomous'` dual-gate is still intact), the Vue UI under
`hermes/ui/src` against `router.js`/`App.vue`, all watcher API routes, `scripts/
self_learning_loop.py`, `scripts/upgrade_runner.sh`, and the broader `hermes/ml/`
surface (`pop_engine.py`, `predictor_inference.py`, `feature_engineer.py`,
`xgb_features.py`) plus `hermes/charts/provider.py`.
