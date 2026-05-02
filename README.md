# HermesTrader

Two-service options-trading ecosystem on a TimescaleDB backbone.

| Service | Role | Writes orders? |
|---|---|---|
| **Hermes Agent** (`hermes/service1_agent/`) | Autonomous execution engine. Cascading priority: CS75 → CS7 → TastyTrade45 → Wheel. Capital-efficient Iron Condor builder. Gemma-driven Hermes Overseer for veto / modify / propose. | Yes |
| **Human Watcher** (`hermes/service2_watcher/`) | FastAPI dashboard with live logs, daily PnL, open positions, AI-enhanced entry-points. | **No (read-only)** |

## Layout

```
hermes/
├── service1_agent/
│   ├── core.py        # TradeAction, MoneyManager, IronCondorBuilder, AbstractStrategy, CascadingEngine
│   ├── strategies.py  # CS75 (P=1), CS7 (P=2), TT45 (P=3), Wheel (P=4)
│   ├── overseer.py    # Hermes AI Overseer (Gemma 3 Flash / e4b)
│   └── main.py        # entry point + tick loop
├── service2_watcher/
│   ├── api.py         # FastAPI backend (read-only)
│   └── static/dashboard.html
├── broker/
│   └── tradier.py     # TradierBroker — REST client conforming to Hermes broker contract
├── mcp/
│   └── server.py      # MCP server exposing TradierBroker tools (FastMCP, stdio)
├── ml/
│   └── xgb_features.py  # 10-feature engineer + threaded XGB predictor + HV Rank
└── db/
    ├── schema.sql     # TimescaleDB hypertables, compression, continuous aggregates
    └── models.py      # SQLAlchemy ORM + HermesDB repository
```

## Tradier broker

`hermes/broker/tradier.py` is a synchronous REST client for [Tradier
Brokerage](https://documentation.tradier.com/brokerage-api). It implements the
same surface the strategies already call (`get_account_balances`,
`get_positions`, `get_option_expirations`, `get_option_chains`, `get_quote`,
`get_delta`, `analyze_symbol`, `place_order_from_action`,
`roll_to_next_month`).

Configuration (env or constructor `config`):

- `TRADIER_ACCESS_TOKEN` — bearer token
- `TRADIER_ACCOUNT_ID` — account number
- `TRADIER_BASE_URL` — `https://api.tradier.com/v1` (live) or
  `https://sandbox.tradier.com/v1` (paper). Default: live.
- `HERMES_DRY_RUN=true` — orders are routed through Tradier's preview endpoint
  instead of being placed.

`hermes/service1_agent/main.py` auto-selects `TradierBroker` when the token
and account id are set; otherwise it falls back to `MockBroker`.

## Tradier MCP server

`hermes/mcp/server.py` exposes the Tradier broker as an MCP server (FastMCP,
stdio). Any MCP client (Claude Desktop, Cowork, custom agents) can call:
`get_account_balances`, `get_positions`, `get_orders`, `cancel_order`,
`get_quote`, `get_option_expirations`, `get_option_chain`, `get_delta`,
`get_history`, `analyze_symbol`, `place_multileg_order`,
`place_single_option_order`, `place_equity_order`, `roll_to_next_month`.

```bash
pip install "mcp[cli]" requests
export TRADIER_ACCESS_TOKEN=... TRADIER_ACCOUNT_ID=... HERMES_DRY_RUN=true
python -m hermes.mcp.server
```

Claude Desktop / Cowork config snippet:

```json
{
  "mcpServers": {
    "tradier": {
      "command": "python",
      "args": ["-m", "hermes.mcp.server"],
      "env": {
        "TRADIER_ACCESS_TOKEN": "...",
        "TRADIER_ACCOUNT_ID": "...",
        "TRADIER_BASE_URL": "https://sandbox.tradier.com/v1",
        "HERMES_DRY_RUN": "true"
      }
    }
  }
}
```

## Quick start

```bash
# 1. Start TimescaleDB and create the schema
psql "postgresql://hermes:hermes@localhost:5432/hermes" -f hermes/db/schema.sql

# 2. Install runtime deps
pip install fastapi uvicorn sqlalchemy psycopg[binary] xgboost pandas numpy

# 3. Run the watcher
export HERMES_DSN="postgresql+psycopg://hermes:hermes@localhost:5432/hermes"
export HERMES_WATCHLIST="AAPL,SPY,QQQ,NVDA,AMD,KO"
uvicorn hermes.service2_watcher.api:app --host 0.0.0.0 --port 8081

# 4. Run the agent (separate process). The broker is auto-built from the
#    Tradier env vars and the live/paper mode stored in `system_settings`,
#    which the watcher's toggle writes to.
python -m hermes.service1_agent.main
```

## Strategy rules at a glance

- **CS75** — 39-45 DTE entry; 25 % credit-to-width for 30-45 DTE, 20 % for 14-29 DTE; TP @ 50 % (DTE 21-45) or 75 % (DTE<21); SL @ 2.5×; time exit ≤ 8 DTE.
- **CS7** — 7 DTE; min credit ≥ 12 % width; TP debit ≤ 2 % width; SL @ 3× credit.
- **TastyTrade45** — 16 Δ shorts, 30-60 DTE entry, hard exit at 21 DTE, neutralize side when |Δ_short| > 0.30.
- **Wheel** — Cash-secured puts → assignment → covered calls; calls + puts balanced to `max_lots`; roll ITM if DTE < 7 (rolls ignore `max_lots`).

## Money management

`MoneyManager` enforces:

- True Available BP = `option_buying_power − min_obp_reserve`.
- Dynamic scaling when requirement > true BP.
- Side-aware sizing per (symbol, side): `max_lots − (open + pending)`.
- Iron Condor margin = single riskiest side × 100 × lots.

## XGBoost feature set

The 10-feature spec (`hermes/ml/xgb_features.py`):

1. Overnight gap
2. Vol-normalised 5-day momentum
3. SPY beta residual (60-day rolling β)
4. Intraday return
5. VWAP distance at 3:59 pm
6. Range position
7. Volume z-score (20d)
8. Last-30-min volume %
9. Realised vol (5d, annualised)
10. Seasonality (day-of-week + month)

Predictor runs in a daemon thread so the UI stays responsive. HV Rank is computed over a 365-day rolling window as the IV Rank proxy.
