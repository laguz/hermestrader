"""HermesTrader — automated options-trading agent + operator panel.

Two services share this package:

- ``hermes.service1_agent`` — the agent itself. Long-running tick loop
  that drives four cascading credit-spread strategies through a money
  manager, an LLM overseer, and an XGBoost predictor. Output is broker
  orders + DB rows; no HTTP surface.
- ``hermes.service2_watcher`` — FastAPI command-and-control panel. Reads
  the same DB and exposes operator endpoints (approve trades, edit the
  agent's "soul" doctrine, flip paper/live mode, etc.).

Both services share ``hermes.db.models.HermesDB`` as the persistence
layer over TimescaleDB, ``hermes.broker.tradier.TradierBroker`` as the
broker adapter, ``hermes.charts.provider.HermesChartProvider`` for chart
rendering, and ``hermes.llm.clients`` for the overseer's model backends.

See ``ARCHITECTURE.md`` at the repo root for the full layering map and
``AGENTS.md`` for the safety rules that govern every change.
"""
