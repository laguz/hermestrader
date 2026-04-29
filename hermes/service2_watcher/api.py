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

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from hermes.db.models import HermesDB
from hermes.ml.xgb_features import AsyncXGBPredictor, FeatureEngineer

logger = logging.getLogger("hermes.watcher.api")

DSN = os.environ.get("HERMES_DSN", "postgresql+psycopg://hermes:hermes@localhost:5432/hermes")
WATCHLIST = [s.strip().upper() for s in
             os.environ.get("HERMES_WATCHLIST", "AAPL,SPY,QQQ,NVDA,AMD,KO").split(",") if s.strip()]

# Canonical strategy registry mirrored from service1_agent/strategies.py
STRATEGIES = ("CS75", "CS7", "TT45", "WHEEL")


def _require_strategy(strategy_id: str) -> str:
    sid = strategy_id.upper()
    if sid not in STRATEGIES:
        raise HTTPException(status_code=404,
                            detail=f"unknown strategy {strategy_id!r}; allowed: {list(STRATEGIES)}")
    return sid


def _clean_symbol(sym: str) -> str:
    s = (sym or "").strip().upper()
    if not s or not all(c.isalnum() or c in "._-" for c in s) or len(s) > 16:
        raise HTTPException(status_code=400, detail=f"invalid symbol {sym!r}")
    return s

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
    return {
        "ok": True,
        "service": "human-watcher",
        "default_watchlist": WATCHLIST,
        "strategies": list(STRATEGIES),
    }


# ---------------------------------------------------------------------------
# Watchlist management — the watcher's only write surface. Each strategy has
# its own list; an empty list falls back to the default watchlist at tick time.
# ---------------------------------------------------------------------------
class WatchlistBody(BaseModel):
    symbols: List[str] = Field(default_factory=list)


class SymbolBody(BaseModel):
    symbol: str


@app.get("/api/watchlists")
def get_all_watchlists() -> Dict[str, List[str]]:
    stored = db.list_all_watchlists()
    return {sid: stored.get(sid, []) for sid in STRATEGIES}


@app.get("/api/watchlists/{strategy_id}")
def get_watchlist(strategy_id: str) -> Dict[str, Any]:
    sid = _require_strategy(strategy_id)
    symbols = db.list_watchlist(sid)
    return {
        "strategy_id": sid,
        "symbols": symbols,
        "effective": symbols or WATCHLIST,
        "using_default": not symbols,
    }


@app.put("/api/watchlists/{strategy_id}")
def replace_watchlist(strategy_id: str, body: WatchlistBody) -> Dict[str, Any]:
    sid = _require_strategy(strategy_id)
    symbols = [_clean_symbol(s) for s in body.symbols]
    saved = db.set_watchlist(sid, symbols)
    db.write_log(sid, f"watchlist replaced: {saved}")
    return {"strategy_id": sid, "symbols": saved}


@app.post("/api/watchlists/{strategy_id}")
def add_watchlist_symbol(strategy_id: str, body: SymbolBody) -> Dict[str, Any]:
    sid = _require_strategy(strategy_id)
    sym = _clean_symbol(body.symbol)
    inserted = db.add_to_watchlist(sid, sym)
    if inserted:
        db.write_log(sid, f"watchlist add: {sym}")
    return {
        "strategy_id": sid, "symbol": sym, "added": inserted,
        "symbols": db.list_watchlist(sid),
    }


@app.delete("/api/watchlists/{strategy_id}/{symbol}")
def remove_watchlist_symbol(strategy_id: str, symbol: str) -> Dict[str, Any]:
    sid = _require_strategy(strategy_id)
    sym = _clean_symbol(symbol)
    removed = db.remove_from_watchlist(sid, sym)
    if not removed:
        raise HTTPException(status_code=404, detail=f"{sym} not in {sid} watchlist")
    db.write_log(sid, f"watchlist remove: {sym}")
    return {"strategy_id": sid, "symbol": sym, "removed": True,
            "symbols": db.list_watchlist(sid)}


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
