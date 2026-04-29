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
├── ml/
│   └── xgb_features.py  # 10-feature engineer + threaded XGB predictor + HV Rank
└── db/
    ├── schema.sql     # TimescaleDB hypertables, compression, continuous aggregates
    └── models.py      # SQLAlchemy ORM + HermesDB repository
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

# 4. Run the agent (separate process; bring your own broker + LLM client)
python -c "from hermes.service1_agent.main import run; run(broker, llm, charts, config)"
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
