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

**Service-1** runs the cascading strategy engine on a fixed tick interval
(default 5 minutes). It never serves HTTP — its only outputs are broker
orders and DB rows.

**Service-2** is a FastAPI app that reads the same DB and exposes a control
panel: approve queued trades, edit the operator's "soul" doctrine, toggle
paper/live mode, see live P&L, etc.

Both services share `TimescaleDB` as their single source of truth.

## Layers (top-down)

```
┌──────────────────────────────────────────────────────────────────┐
│  Process entry points                                            │
│    hermes/service1_agent/main.py    (agent tick loop)            │
│    hermes/service2_watcher/api.py   (FastAPI app)                │
│    hermes/mcp/server.py             (MCP shim around Tradier)    │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  Orchestration                                                   │
│    CascadingEngine          — pipelines sync → manage → entries  │
│    HermesOverseer           — LLM review of every TradeAction    │
│    AsyncXGBPredictor        — background ML forecasting          │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  Domain logic (the four cascading strategies)                    │
│    CreditSpreads75   PRIORITY=1   39–45 DTE entries              │
│    CreditSpreads7    PRIORITY=2   7 DTE entries                  │
│    TastyTrade45      PRIORITY=3   16Δ short, 30–60 DTE           │
│    WheelStrategy     PRIORITY=4   put-→assignment-→call wheel    │
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
│    HermesDB (hermes/db/models.py)                                │
│    SQLAlchemy ORM + thin repository layer over TimescaleDB       │
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
                                 CS75 → CS7 → TT45 → WHEEL
                                 each strategy drains the watchlist before
                                 the next one runs, so high-priority
                                 strategies see fresh capacity.
6. overseer.propose()         ← if autonomy=='autonomous', LLM may add trades
   → submit(ai_actions, "ai")
```

`submit()` either:
- Queues the action in `pending_approvals` (when `approval_mode=true`), or
- Records it in `pending_orders` and calls `broker.place_order_from_action`.

The `HermesOverseer.review` hook can VETO, MODIFY, or APPROVE every action
before it reaches `submit()`.

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

`schema.sql` is the source of truth for TimescaleDB hypertables, indexes,
compression policies, and continuous aggregates. `models.py` has a defensive
`Base.metadata.create_all(checkfirst=True)` so plain SQLAlchemy CRUD works
even if `schema.sql` was never applied — but the Timescale-specific bits
(compression, retention, continuous aggregates) need `psql -f schema.sql`.

## Where to look for what

| You want to…                                  | Look in                                          |
|-----------------------------------------------|--------------------------------------------------|
| Change how a strategy enters a trade          | `hermes/service1_agent/strategies.py`            |
| Change how a strategy exits a trade           | same — search `manage_positions`                 |
| Add a new strategy                            | subclass `AbstractStrategy` in `core.py`, register in `main.py` |
| Change buying-power / capacity rules          | `MoneyManager` in `hermes/service1_agent/core.py`|
| Change the broker integration                 | `hermes/broker/tradier.py`                       |
| Change the operator panel                     | `hermes/service2_watcher/api.py` + `static/`     |
| Change what the overseer asks the LLM         | `hermes/service1_agent/overseer.py`              |
| Add a new chart indicator                     | `hermes/charts/provider.py`                      |
| Add a new ML feature                          | `hermes/ml/xgb_features.py`                      |
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
- **Cascading priority** — Strategies run in PRIORITY order; higher-priority
  strategies consume capacity first.
- **Soul** — The operator's free-text doctrine appended to the LLM
  overseer's system prompt.
- **Autonomy levels** — `advisory` (log only), `enforcing` (LLM may
  veto/modify), `autonomous` (LLM may originate trades).
- **Approval mode** — When on, every proposed trade goes to a human queue
  before reaching the broker.
