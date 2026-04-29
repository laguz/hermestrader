"""
[Service-2: Human-Watcher-UI]
FastAPI backend (READ-ONLY). Cannot place orders. Pulls bot state, PnL, and
ML predictions from TimescaleDB. Pairs with static/dashboard.html.
"""
from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from hermes.db.models import HermesDB
from hermes.ml.xgb_features import AsyncXGBPredictor, FeatureEngineer

logger = logging.getLogger("hermes.watcher.api")

DSN = os.environ.get("HERMES_DSN", "postgresql+psycopg://hermes:hermes@localhost:5432/hermes")
WATCHLIST = os.environ.get("HERMES_WATCHLIST", "AAPL,SPY,QQQ,NVDA,AMD,KO").split(",")

db = HermesDB(DSN)
feat = FeatureEngineer()
predictor = AsyncXGBPredictor(db, feat, symbols=WATCHLIST)


@asynccontextmanager
async def lifespan(app: FastAPI):
    predictor.start()
    yield
    predictor.stop()


app = FastAPI(title="Hermes Human Watcher", lifespan=lifespan)
STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ---------------------------------------------------------------------------
# REST endpoints — read-only by design
# ---------------------------------------------------------------------------
@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "dashboard.html")


@app.get("/api/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "service": "human-watcher", "watchlist": WATCHLIST}


@app.get("/api/logs")
def get_logs(limit: int = 200) -> List[Dict[str, Any]]:
    raw = db.recent_logs(limit=limit).splitlines()
    return [{"line": ln} for ln in raw]


@app.get("/api/pnl")
def get_pnl(days: int = 60) -> List[Dict[str, Any]]:
    return db.pnl_daily(days=days)


@app.get("/api/positions")
def get_positions() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for sid in ("CS75", "CS7", "TT45", "WHEEL"):
        rows.extend(db.open_trades(sid))
    return rows


@app.get("/api/predictions")
def get_predictions() -> List[Dict[str, Any]]:
    out = []
    for sym in WATCHLIST:
        p = predictor.predict_latest(sym)
        if p:
            out.append({"symbol": sym, **p})
    return out


@app.get("/api/entry_points/{symbol}")
def entry_points(symbol: str) -> Dict[str, Any]:
    """
    Bot entry levels enhanced with the AI predicted price.
    The actual S/R levels come from the broker analysis service stored upstream;
    here we return the enriched envelope.
    """
    pred = predictor.predict_latest(symbol) or {}
    return {
        "symbol": symbol,
        "predicted_price": pred.get("predicted_price"),
        "predicted_return": pred.get("predicted_return"),
        "spot": pred.get("spot"),
        # The watcher reads pre-computed S/R levels stored by the agent's
        # analysis pass — kept transparent here.
        "rule_entry_points": [],
    }


# ---------------------------------------------------------------------------
# Live log stream
# ---------------------------------------------------------------------------
@app.websocket("/ws/logs")
async def ws_logs(ws: WebSocket):
    await ws.accept()
    seen: set = set()
    try:
        while True:
            for line in db.recent_logs(limit=20).splitlines():
                if line not in seen:
                    seen.add(line)
                    await ws.send_text(line)
            # cap memory
            if len(seen) > 5000:
                seen = set(list(seen)[-2000:])
            await asyncio.sleep(1.5)
    except WebSocketDisconnect:
        return
