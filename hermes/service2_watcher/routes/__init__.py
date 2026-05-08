"""FastAPI routers for the watcher.

Each module owns a single resource surface:

- ``status``     — root + health + agent/Tradier/LLM status + balances + debug + logs
- ``approvals``  — pending-approval queue + approval-mode toggle
- ``watchlist``  — per-strategy symbol lists
- ``soul``       — operator doctrine + autonomy level
- ``agent``      — pause/resume + ML manual trigger + paper/live toggle
- ``strategies`` — per-strategy enable + per-strategy lot config
- ``llm``        — overseer LLM provider configuration
- ``analytics``  — ML predictions + closed-trade performance + analysis
- ``charts``     — chart PNGs + per-symbol LLM chart analyses

``api.py`` wires them all into the FastAPI app via ``include_router``.
Each module imports the shared ``db``, settings constants, and helpers
from ``hermes.service2_watcher._app_state`` rather than duplicating them.
"""
