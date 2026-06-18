# Agent Instructions for HermesTrader

This file gives Google Jules and other autonomous coding agents the context
they need to make safe, on-style changes to this repository.

## Project at a glance

HermesTrader is a two-service Python trading system:

- **Service-1 (`hermes/service1_agent/`)** — the agent itself. Runs a
  `CascadingEngine` that ticks every `HERMES_TICK_INTERVAL` seconds and drives
  four credit-spread strategies (CS75, CS7, TT45, Wheel). Writes through a
  `MoneyManager` that enforces buying-power and side-aware capacity limits.
- **Service-2 (`hermes/service2_watcher/`)** — FastAPI C2 (command & control)
  panel for human oversight. Approves trades, edits the agent's "soul"
  doctrine, toggles paper/live mode.

Persistence: TimescaleDB via SQLAlchemy (`hermes/db/models.py`).
Broker: Tradier REST (`hermes/broker/tradier.py`). LLM overseer is
provider-agnostic (`hermes/llm/clients.py`).

## Ground rules

1. **This system places real options orders.** Treat every change to
   `core.py`, `strategies/`, `tradier.py`, or `MoneyManager` as
   safety-critical. Add a regression test before fixing a bug.
2. **Never disable `dry_run` defaults or add a code path that can place a
   live order without `approval_mode` honoring the operator's setting.**
3. **Tag conventions matter.** Hermes tags broker orders `HERMES_<STRAT>`
   (e.g. `HERMES_CS75`). Tradier sanitises `_` → `-`, so the round-tripped
   form is `HERMES-<STRAT>`. Any new matcher must accept both.
4. **The strategy pipeline is order-sensitive.** `CascadingEngine.tick`
   syncs positions → broker orders → reconciles orphans → manages exits →
   then runs entries in priority order. Don't reorder.
5. **Don't introduce new dependencies casually.** The Dockerfile pins
   `requirements.txt`; both must stay in sync.

## Tradier MCP Server

The project includes an MCP (Model Context Protocol) server located at [server.py](file:///Users/laguz/Git/hermestrader/hermes/mcp/server.py). This server exposes the `TradierBroker` functionality so that any MCP-compliant AI client (such as Cursor, Windsurf, or Claude Desktop) can query the broker over stdio.

### Key Tools Exposed
- **Account**: `get_account_balances`, `get_positions`, `get_orders`, `cancel_order`
- **Market Data**: `get_quote`, `get_option_expirations`, `get_option_chain`, `get_delta`, `get_history`, `analyze_symbol`
- **Orders**: `place_multileg_order`, `place_single_option_order`, `place_equity_order`, `roll_to_next_month`

### Execution & Configuration
- **Run server locally**: Run `./hermes.sh mcp` to start the server. This automatically sources the workspace `.env` file and exports the necessary Tradier environment variables (`TRADIER_ACCESS_TOKEN`, `TRADIER_ACCOUNT_ID`, etc.).
- **Automatic Env Loading**: The server automatically attempts to resolve and load environment variables from the `.env` file at the project root if they are not already set in the parent process.
- **Cursor Integration**: Pre-configured in [mcp.json](file:///Users/laguz/Git/hermestrader/.cursor/mcp.json). Because it loads `.env` automatically, this configuration file does not require hardcoded secrets and is safe to commit.
- **Claude Desktop Config**:
  ```json
  "mcpServers": {
    "tradier": {
      "command": "python3",
      "args": ["-m", "hermes.mcp.server"]
    }
  }
  ```

For details on the Tradier API endpoints and resource URLs, refer to [tradier_llms.txt](file:///Users/laguz/Git/hermestrader/docs/tradier_llms.txt) (obtained from official Tradier documentation).

## Testing

```bash
pip install -r requirements.txt
pip install pytest ruff
pytest tests -q
```

CI runs the same on every PR (`.github/workflows/ci.yml`). Tests must not
require a live database — use the stub-broker / stub-DB pattern in
`tests/test_money_manager_sync.py`.

## Code style

- Python 3.11+ idioms; type-hinted public functions.
- Default to **no comments**. Add a comment only when the *why* is
  non-obvious (a workaround, a Tradier-specific quirk, a hidden invariant).
- Don't reformat unrelated code; a focused diff reviews faster.
- Follow the existing module structure — don't introduce new top-level
  packages without a clear reason.

## Commit and PR conventions

- Commit messages are imperative and start with a type prefix:
  `fix:`, `feat:`, `refactor:`, `test:`, `ci:`, `docs:`.
- One logical change per PR. The recent bug-fix PRs are good examples
  (see `git log --oneline`).
- Always include a **Test plan** section in the PR body.

## What to skip

- Don't touch `VERSION` unless explicitly asked.
- Don't edit `hermes/scratch/` — those are exploratory scripts.
- Don't migrate the schema (`hermes/db/migrate_*.py`) without operator
  sign-off; data lives in TimescaleDB and migrations are run by hand.

## Useful entry points for understanding the codebase

- `hermes/service1_agent/core.py` — `CascadingEngine` (the orchestrator).
  Read this first. It holds the pipeline spine; the runtime/reactive/ai/tuning
  method groups live in `_engine_*.py` mixins it inherits. The primitives it
  composes are split into siblings: `trade_action.py` (`TradeAction`),
  `broker_wrapper.py` (`AsyncBrokerWrapper`), `money_manager.py`
  (`MoneyManager`, `IronCondorBuilder`), and `strategy_base.py`
  (`AbstractStrategy`). All are still re-exported from `core.py` for
  backwards compatibility.
- `hermes/service1_agent/strategies/` — the five concrete strategies
  (`cs75`, `cs7`, `tt45`, `wheel`, `hermes_alpha`).
- `hermes/service1_agent/main.py` — run loop + config reconciliation; broker/
  LLM/engine construction and helpers live in `agent_construction.py` and the
  other `agent_*.py` modules it re-imports.
- `hermes/service2_watcher/api.py` — operator API surface.
