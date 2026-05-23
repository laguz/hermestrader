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
- ``admin``       — instance identity + self-update hook (Hermes-driven)

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
    admin,
    agent,
    analytics,
    approvals,
    charts,
    llm,
    ml_diagnostics,
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
    try:
        db.run_migrations()
    except Exception as exc:                                       # noqa: BLE001
        logger.exception("run_migrations failed: %s", exc)
    try:
        from hermes.utils import sync_soul_file_to_db, check_for_updates
        import threading
        sync_soul_file_to_db(db)
        threading.Thread(target=check_for_updates, daemon=True).start()
    except Exception as exc:                                       # noqa: BLE001
        logger.exception("lifespan startup update/soul sync failed: %s", exc)
    # ML-side migrations — idempotent. Each helper checkfirst=True so
    # repeated boots are no-ops.
    try:
        from hermes.ml import ledger as _ledger
        from hermes.ml import regime_weights as _regime
        _ledger.ensure_table(db)
        _regime.ensure_table(db)
    except Exception as exc:                                       # noqa: BLE001
        logger.exception("ml table-ensure failed: %s", exc)
    # Wire the database-backed regime-weight lookup into pop_engine so
    # every prediction reads the live posterior weights instead of the
    # static DEFAULT_REGIME_WEIGHTS.
    try:
        from hermes.ml import pop_engine, regime_weights as _regime
        pop_engine.set_regime_weight_lookup(_regime.make_lookup_fn(db))
    except Exception as exc:                                       # noqa: BLE001
        logger.exception("regime_weight lookup wire-up failed: %s", exc)
    # Start the agent thread
    logger.info("Lifespan starting Hermes agent background thread...")
    from hermes.service1_agent.main import start_agent_thread, _SHUTDOWN_EVENT
    agent_thread = start_agent_thread()

    yield

    # Shutdown the agent thread on exit
    logger.info("Lifespan shutting down Hermes agent background thread...")
    _SHUTDOWN_EVENT.set()


app = FastAPI(title="Hermes C2", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Ensure assets directory exists to prevent FastAPI crash before Vite compilation
(STATIC_DIR / "assets").mkdir(parents=True, exist_ok=True)
app.mount("/assets", StaticFiles(directory=str(STATIC_DIR / "assets")), name="assets")

from hermes.mcp.server import mcp
app.mount("/mcp", mcp.sse_app())

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
app.include_router(admin.router)
app.include_router(ml_diagnostics.router)

from fastapi.responses import HTMLResponse, FileResponse
from fastapi import HTTPException

@app.get("/{fallback_path:path}")
def spa_fallback(fallback_path: str):
    if (
        fallback_path.startswith("api") or
        fallback_path.startswith("mcp") or
        fallback_path.startswith("static") or
        fallback_path.startswith("assets")
    ):
        raise HTTPException(status_code=404, detail="Not Found")

    index_path = STATIC_DIR / "index.html"
    if not index_path.exists():
        return HTMLResponse(
            "<html><body>Frontend not compiled yet. Run: <code>npm run build</code> inside <code>hermes/ui</code></body></html>",
            status_code=503
        )
    return FileResponse(index_path, headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})

