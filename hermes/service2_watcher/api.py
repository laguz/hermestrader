"""[Service-2: Hermes C2 — Command & Control]

Slim FastAPI app factory. The actual endpoint logic lives in
``hermes/service2_watcher/routes/*.py`` — one router per resource:

- ``status``      — root + health + agent/Tradier/LLM status + balances + debug + logs
- ``approvals``   — pending-approval queue + approval-mode toggle
- ``watchlist``   — per-strategy symbol lists
- ``soul``        — operator doctrine + autonomy level
- ``agent``       — pause/resume + ML manual trigger + paper/live toggle
- ``strategies``  — per-strategy enable + per-strategy lot config
- ``llm``         — overseer LLM provider configuration
- ``analytics``   — ML predictions + closed-trade performance + analysis
- ``charts``      — chart PNGs + per-symbol LLM chart analyses

Run with: ``uvicorn hermes.service2_watcher.api:app``

Docker / docker-compose still target this module path; nothing changes for
the operator. Internal organisation only.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from hermes.common import STRATEGY_PRIORITIES

from ._app_state import STATIC_DIR, db
from .routes import (
    agent,
    analytics,
    approvals,
    charts,
    llm,
    soul,
    status,
    strategies,
    watchlist,
)

logger = logging.getLogger("hermes.c2.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup hook: ensure the strategies registry has rows for the four
    canonical strategies. The ``strategy_watchlists`` table FKs into this,
    so writes from the watcher would 500 without it on a fresh DB."""
    try:
        db.ensure_strategies(STRATEGY_PRIORITIES)
    except Exception as exc:                                       # noqa: BLE001
        logger.exception("ensure_strategies failed: %s", exc)
    yield


app = FastAPI(title="Hermes C2", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Routers are mounted in declaration order; FastAPI doesn't care about
# order for non-overlapping prefixes, but listing them resource-by-resource
# makes the app surface easy to scan.
app.include_router(status.router)
app.include_router(approvals.router)
app.include_router(watchlist.router)
app.include_router(soul.router)
app.include_router(agent.router)
app.include_router(strategies.router)
app.include_router(llm.router)
app.include_router(analytics.router)
app.include_router(charts.router)
