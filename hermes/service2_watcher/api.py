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

import os

from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse

from hermes.common import STRATEGY_PRIORITIES

from ._app_state import STATIC_DIR, db
from .routes import (
    admin,
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
        await db.watchlist.ensure_strategies(STRATEGY_PRIORITIES)
    except Exception as exc:
        logger.exception("ensure_strategies failed: %s", exc)
    try:
        await db.run_migrations()
    except Exception as exc:
        logger.exception("run_migrations failed: %s", exc)
    try:
        from hermes.utils import sync_soul_file_to_db, check_for_updates
        import threading
        await sync_soul_file_to_db(db)
        threading.Thread(target=check_for_updates, daemon=True).start()
    except Exception as exc:
        logger.exception("lifespan startup update/soul sync failed: %s", exc)
    # Initialize and wire DB-backed regime weights lookup if enabled
    try:
        regime_weights_env = os.environ.get("HERMES_REGIME_WEIGHTS", "false").lower() == "true"
        regime_weights_setting = (await db.settings.get_setting("regime_weights_enabled") or "false").lower() == "true"
        if regime_weights_env or regime_weights_setting:
            from hermes.ml import pop_engine, regime_weights
            regime_weights.ensure_table(db)
            lookup_fn = regime_weights.make_lookup_fn(db, event_bus=None)
            if hasattr(lookup_fn, "initialize"):
                await lookup_fn.initialize()
            pop_engine.set_regime_weight_lookup(lookup_fn)
            logger.info("DB-backed regime weights lookup wired and warmed up in Watcher.")
        else:
            logger.info("DB-backed regime weights lookup is disabled in Watcher.")
    except Exception as _rw_exc:
        logger.warning("DB-backed regime weights lookup init failed in Watcher, falling back to static defaults: %s", _rw_exc)

    # Connect to Inter-Process Communication (IPC) broker
    from hermes.ipc import ipc
    await ipc.connect()

    yield

    # Clean up IPC connection on shutdown
    await ipc.disconnect()



app = FastAPI(title="Hermes C2", lifespan=lifespan)

# ── CORS configuration ───────────────────────────────────────────────────────
# Given this is a C2 panel, we default to restrictive origins.
# HERMES_CORS_ORIGINS can be a comma-separated list of allowed origins.
_env_origins = [
    o.strip()
    for o in os.environ.get("HERMES_CORS_ORIGINS", "").split(",")
    if o.strip()
]
origins = [
    "http://localhost",
    "http://localhost:8081",
    "http://127.0.0.1",
    "http://127.0.0.1:8081",
] + _env_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Ensure assets directory exists to prevent FastAPI crash before Vite compilation
(Path(STATIC_DIR) / "assets").mkdir(parents=True, exist_ok=True)
app.mount("/assets", StaticFiles(directory=str(Path(STATIC_DIR) / "assets")), name="assets")

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



@app.get("/favicon.svg")
def favicon_svg():
    path = STATIC_DIR / "favicon.svg"
    if path.exists():
        return FileResponse(path, media_type="image/svg+xml")
    raise HTTPException(status_code=404, detail="Favicon not found")

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

