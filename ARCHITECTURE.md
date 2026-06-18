# HermesTrader Architecture

> One-page map of how the trading agent and the operator panel fit together.
> If you're new to the codebase, read this first, then `AGENTS.md`, then jump
> into the code.

## Two services, one database

```
                 ┌──────────────────┐         ┌──────────────────┐
                 │   Service-1      │         │   Service-2      │
                 │   Hermes Agent   │         │   Watcher / C2   │
                 │  (tick loop)     │         │   (FastAPI)      │
                 └─────────┬────────┘         └─────────┬────────┘
                           │ writes trades,             │ reads logs,
                           │ pending orders,            │ approvals,
                           │ AI decisions, logs         │ settings
                           ▼                            ▼
                 ┌────────────────────────────────────────────┐
                 │              TimescaleDB                   │
                 │ (positions · pending_orders · approvals ·  │
                 │  bot_logs · ai_decisions · predictions ·   │
                 │  bars_daily · bars_intraday · settings)    │
                 └────────────────────────────────────────────┘
                           ▲                            ▲
                           │ chain quotes,              │ status reads,
                           │ orders, balances           │ approve/reject,
                           ▼                            │ flip mode
                 ┌──────────────────┐                   │
                 │   Tradier API    │                   │
                 │ (sandbox or live)│                   ▼
                 └──────────────────┘         ┌──────────────────┐
                                              │  Operator (you)  │
                                              │  via dashboard   │
                                              └──────────────────┘
```

**Service-1** runs the cascading strategy engine. It is fully **event-driven** and **event-sourced**: a central async `Scheduler` emits scheduled tick events (`ClockTickEvent`, `CacheWarmTick`, `MlRetrainTick`, `ChartRefreshTick`) over an in-process `EventBus`. The agent subscribes to these ticks and other incoming events (e.g., `OrderFillEvent`, `MarketDataEvent` from the broker stream client, and database settings/watchlist/approval changes published over `ipc` PG NOTIFY). It processes all engine, settings, ML prediction, and cache pre-warming logic reactively, avoiding database-polling loops. It never serves HTTP — its only outputs are broker orders and DB rows.

Every state change is appended to the `event_ledger` and projected to the
read-model tables (`trades`, `pending_orders`, `system_settings`,
`strategy_watchlists`, `pending_approvals`) in the same transaction via
`EventStoreManager.record_event`. Because state is a pure function of the log,
the read models are fully recoverable: `ProjectionsRepository.rebuild` wipes the
order/trade read models and replays the ledger to reconstruct them
(`tests/test_event_replay_parity.py` guards both live-vs-replay parity and
crash recovery).

**Service-2** is a FastAPI app that reads the same DB and exposes a control
panel: approve queued trades, edit the operator's "soul" doctrine, toggle
paper/live mode, see live P&L, etc.

Both services share one SQLAlchemy database as their single source of truth —
**TimescaleDB (Postgres)** in production, or **SQLite** for dev, tests, and the
unified simulation mode (a virtual clock replays history against the same code
paths; see `hermes/utils.py::set_virtual_time` and `backtest_engine.py`).

## Layers (top-down)

```
┌──────────────────────────────────────────────────────────────────┐
│  Process entry points                                            │
│    hermes/service1_agent/main.py    (agent run loop + wiring)    │
│    hermes/service2_watcher/api.py   (FastAPI app)                │
│    hermes/mcp/server.py             (MCP shim around Tradier)    │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  Orchestration                                                   │
│    CascadingEngine          — pipelines sync → manage → entries  │
│      (spine in core.py; runtime/reactive/ai/tuning concerns are  │
│       owned collaborators in _engine_*.py, not mixins)           │
│    HermesOverseer           — LLM review of every TradeAction;   │
│      monolithic OR multi-agent committee (overseer.py)           │
│    AsyncXGBPredictor        — background ML forecasting          │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  Domain logic (five cascading strategies — hermes/.../strategies/)│
│    CreditSpreads75   PRIORITY=1   39–45 DTE entries              │
│    CreditSpreads7    PRIORITY=2   7 DTE entries                  │
│    TastyTrade45      PRIORITY=3   16Δ short, 30–60 DTE           │
│    WheelStrategy     PRIORITY=4   put-→assignment-→call wheel    │
│    HermesAlpha       PRIORITY=5   LLM-directed credit spread     │
│  Plus shared invariants:                                         │
│    MoneyManager      — true BP, side-aware capacity, scaling     │
│    IronCondorBuilder — pairs put + call spreads on same expiry   │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  Adapters (talk to external systems)                             │
│    TradierBroker     — REST broker (orders, chains, balances)    │
│    OpenAICompatibleLLM / OllamaCloudLLM — overseer backends      │
│    HermesChartProvider — renders candlestick PNGs for vision     │
│    MockBroker / MockLLM — dev / demo / test stand-ins            │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  Persistence                                                     │
│    HermesDB (hermes/db/models.py) — connection + schema only;    │
│    query methods come from 8 repository mixins in                 │
│    hermes/db/repositories/ (logs, trades, approvals, settings,   │
│    decisions, timeseries, analytics, watchlist)                  │
│    SQLAlchemy ORM over TimescaleDB (Postgres) or SQLite          │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  Cross-cutting                                                   │
│    hermes/common.py    — STRATEGIES, OCC_RE, VALID_MODES, etc.   │
│    hermes/market_hours.py — US equity session awareness          │
└──────────────────────────────────────────────────────────────────┘
```

A given file should never reach more than one layer up or down.
If it does, that's a smell worth flagging.

## A single agent tick (Service-1)

`CascadingEngine.tick(watchlist)` runs this pipeline on every interval:

```
1. sync_positions()           ← broker.get_positions() → DB
2. mm.sync_broker_orders()    ← broker.get_orders() → in-memory cache
                                (so capacity reflects resting orders)
3. reconcile_orphans()        ← flag broker positions Hermes doesn't own
4. process_management()       ← every strategy's manage_positions() runs
   → submit(actions, "management")
5. process_entries(watchlist) ← strategies in PRIORITY order:
                                 CS75 → CS7 → TT45 → WHEEL → HermesAlpha
                                 each strategy drains the watchlist before
                                 the next one runs, so high-priority
                                 strategies see fresh capacity.
6. overseer.propose()         ← if autonomy=='autonomous', LLM may add trades
   → submit(ai_actions, "ai")
```

`HermesAlpha` (priority 5) is the one rule-free strategy: instead of a fixed
recipe it asks the overseer to pick one credit-spread *intent* from the
deduped union of every strategy's watchlist, then resolves and prices that
intent against the live chain like any other strategy.

`submit()` either:
- Queues the action in `pending_approvals` (when `approval_mode=true`), or
- Records it in `pending_orders` and calls `broker.place_order_from_action`.

The `HermesOverseer.review` hook can VETO, MODIFY, or APPROVE every action
before it reaches `submit()`. Review runs in one of two modes (the
`overseer_mode` setting): **monolithic** (one LLM call) or **committee** — a
Macro Specialist and a Strategy/Sizing Specialist run in parallel and a Risk
Officer (Chairman) synthesizes their findings into the final verdict, falling
back to monolithic if the committee call fails.

## A single watcher request (Service-2)

```
HTTP request
   ▼
FastAPI route in hermes/service2_watcher/api.py
   ▼
HermesDB read or write
   ▼
JSON response (or HTML for `/`)
```

The watcher is **read-mostly** for the agent's state. The two writes that
matter:
- `POST /api/approvals/{id}/decide` — operator approves or rejects a
  pending trade. Service-1 picks APPROVED rows up at the start of the
  next tick and submits them.
- `POST /api/settings/...` — `hermes_mode`, `agent_paused`,
  `agent_autonomy`, per-strategy enable flags. Service-1 reconciles its
  state against these settings every tick.

## Where the data lives

| Table              | Owner       | Read by      | Notes                                    |
|--------------------|-------------|--------------|------------------------------------------|
| `strategies`       | both        | both         | Registry; FK target for `strategy_watchlists` |
| `strategy_watchlists` | both     | both         | Per-strategy symbol lists                |
| `trades`           | Service-1   | both         | Filled positions; hypertable on `opened_at` |
| `pending_orders`   | Service-1   | both         | Submitted but not filled                 |
| `pending_approvals`| Service-1   | both         | Awaiting operator decision               |
| `bot_logs`         | both        | both         | Tick heartbeat + free-form audit log     |
| `ai_decisions`     | Service-1   | both         | Every overseer review (advisory or otherwise) |
| `predictions`      | Service-1   | both         | XGBoost next-bar forecasts               |
| `bars_daily`       | Service-1   | both         | Hypertable; populated by `_sync_history` |
| `bars_intraday`    | Service-1   | both         | Hypertable; intraday OHLCV               |
| `system_settings`  | both        | both         | KV store: mode, autonomy, soul.md, etc.  |

**The ORM (`hermes/db/orm.py`) is the single source of truth for tables and
columns.** There is no second hand-maintained catalog to drift against:
`create_all` provisions every table from `Base.metadata` on both backends, the
Alembic baseline generates its tables from the same metadata, and the boot-time
reconciler (`HermesDB.run_migrations`) derives its column self-heal from it too.

`schema.sql` is **not** a table catalog — it is the irreducible TimescaleDB
*addendum* the ORM cannot express: the two raw `bars_*` price tables,
hypertable conversions, compression/retention policies, and the `pnl_daily`
view. It is applied *after* the ORM tables exist.

**Alembic owns the Postgres schema**: `alembic upgrade head` applies the
baseline (`alembic/versions/0001_baseline.py`), which calls
`metadata.create_all` for the ORM tables and then runs the `schema.sql`
addendum; on an already-populated DB, `alembic stamp 0001` marks it migrated.
Future schema changes are new migrations.

`models.py` keeps a defensive `Base.metadata.create_all(checkfirst=True)` so
plain SQLAlchemy CRUD works on **SQLite** without Timescale — used for dev,
tests, **and the unified simulation mode** (HermesDB swaps `JSONB` for portable
`JSON` when the DSN is SQLite); the hypertable/compression DDL simply doesn't
apply there. `tests/test_schema_parity.py` guards the one remaining seam —
every hypertable-backed ORM table has its `create_hypertable` line, and
`schema.sql` never re-declares an ORM table.

## Where to look for what

| You want to…                                  | Look in                                          |
|-----------------------------------------------|--------------------------------------------------|
| Change how a strategy enters a trade          | the strategy's module in `hermes/service1_agent/strategies/` (`cs75.py`, `cs7.py`, `tt45.py`, `wheel.py`, `hermes_alpha.py`) |
| Change how a strategy exits a trade           | same — search `manage_positions`                 |
| Add a new strategy                            | subclass `AbstractStrategy` in `strategy_base.py`, add a module under `strategies/`, register in `common.py` (`STRATEGIES`/`STRATEGY_PRIORITIES`) and `agent_construction.build()` |
| Change the engine pipeline / event handling   | `core.py` (spine) + `_engine_*.py` collaborators |
| Change broker/LLM/engine construction or the run loop | `agent_construction.py`, `agent_*.py`, `main.py` |
| Change buying-power / capacity rules          | `MoneyManager` in `hermes/service1_agent/money_manager.py`|
| Change the broker integration                 | `hermes/broker/tradier.py`                       |
| Change the operator panel                     | `hermes/service2_watcher/api.py` + `static/`     |
| Change what the overseer asks the LLM (monolithic or committee) | `hermes/service1_agent/overseer.py`    |
| Add / change a DB query method                | the matching mixin in `hermes/db/repositories/`  |
| Add a new chart indicator                     | `hermes/charts/provider.py`                      |
| Add a new ML feature                          | `hermes/ml/xgb_features.py`                       |
| Run / extend simulation (virtual clock)       | `hermes/service1_agent/backtest_engine.py`, `hermes/utils.py::set_virtual_time` |
| Change shared constants (priorities, modes)   | `hermes/common.py`                               |
| Change market-hours / holiday handling        | `hermes/market_hours.py`                         |

## Testing

```
pip install -r requirements.txt
pip install pytest ruff
pytest tests -q
ruff check --select E9,F63,F7,F82 hermes tests
```

CI (`.github/workflows/ci.yml`) runs the same on every push and PR for
Python 3.11 and 3.12.

Tests must not require a live database — see
`tests/test_money_manager_sync.py` for the stub-broker / stub-DB pattern.
For tests that need parts of `hermes/db/models.py` without the full
SQLAlchemy stack, import from `hermes/common.py` instead (e.g. `OCC_RE`).

## Glossary

- **DTE** — Days to expiration.
- **OBP / SBP** — Option / Stock Buying Power (Tradier balance fields).
- **OCC symbol** — Standard option symbol like `AAPL250620P00150000`.
- **IC** — Iron Condor (put spread + call spread on the same expiry).
- **Mode A / Mode B** — Strategy concepts. Mode A opens both sides of an
  IC at once. Mode B completes an existing single-sided spread.
- **Cascading priority** — Strategies run in PRIORITY order (CS75=1 …
  HermesAlpha=5); higher-priority strategies consume capacity first.
- **HermesAlpha** — The rule-free strategy (priority 5): the overseer picks a
  credit-spread intent and the strategy resolves/prices it against the chain.
- **Overseer modes** — `monolithic` (single LLM review) or `committee`
  (Macro + Strategy specialists run in parallel → Risk Officer synthesizes).
- **Soul** — The operator's free-text doctrine appended to the LLM
  overseer's system prompt.
- **Autonomy levels** — `advisory` (log only), `enforcing` (LLM may
  veto/modify), `autonomous` (LLM may originate trades).
- **Approval mode** — When on, every proposed trade goes to a human queue
  before reaching the broker.
- **Simulation mode** — Replays history against the real code paths on a
  SQLite DB, driven by a virtual clock (`set_virtual_time`) so
  `utc_now()`/`date_today()` advance through the backtest window.
