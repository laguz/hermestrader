"""
[Service-2: Hermes C2 — Command & Control]
FastAPI backend for the human operator control panel.

Capabilities:
  - Approve / reject / view the trade approval queue
  - Edit the agent's soul (doctrine / personality)
  - Toggle individual strategies on/off
  - Pause / resume the agent tick loop
  - Switch paper ↔ live mode
  - Configure the LLM overseer
  - Read agent / Tradier health status
"""
from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from hermes.common import (
    DEFAULT_LLM_TIMEOUT_S,
    STRATEGIES,
    STRATEGY_PRIORITIES,
    VALID_AUTONOMY,
    VALID_LLM_PROVIDERS,
    VALID_MODES,
)
from hermes.market_hours import market_session, next_open
from hermes.db.models import HermesDB

logger = logging.getLogger("hermes.c2.api")

DSN = os.environ.get("HERMES_DSN", "postgresql+psycopg://hermes:hermes@localhost:5432/hermes")
WATCHLIST = [s.strip().upper() for s in
             os.environ.get("HERMES_WATCHLIST", "AAPL,SPY,QQQ,NVDA,AMD,KO").split(",") if s.strip()]

# ── Setting keys (mirrors service1_agent/main.py) ─────────────────────────────
SETTING_MODE             = "hermes_mode"
SETTING_TRADIER_OK_TS   = "tradier_last_ok_ts"
SETTING_TRADIER_ERROR   = "tradier_last_error"
SETTING_AGENT_STARTED   = "agent_started_at"
SETTING_LLM_PROVIDER    = "llm_provider"
SETTING_LLM_BASE_URL    = "llm_base_url"
SETTING_LLM_MODEL       = "llm_model"
SETTING_LLM_API_KEY     = "llm_api_key"
SETTING_LLM_TEMPERATURE = "llm_temperature"
SETTING_LLM_VISION      = "llm_vision"
SETTING_LLM_TIMEOUT     = "llm_timeout_s"
SETTING_LLM_OK_TS       = "llm_last_ok_ts"
SETTING_LLM_ERROR       = "llm_last_error"
SETTING_SOUL            = "soul_md"
SETTING_AUTONOMY        = "agent_autonomy"
SETTING_PAUSED          = "agent_paused"
SETTING_APPROVAL_MODE   = "approval_mode"
DEFAULT_LLM_BASE_URL    = "http://host.docker.internal:1234/v1"
MAX_SOUL_BYTES          = 64 * 1024
TICK_INTERVAL_S         = int(os.environ.get("HERMES_TICK_INTERVAL", 300))
STALE_AFTER_S           = max(60, TICK_INTERVAL_S * 2 + 30)


def _strategy_enabled_key(sid: str) -> str:
    return f"strategy_{sid.lower()}_enabled"


# ── Helpers ────────────────────────────────────────────────────────────────────
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _seconds_since(dt: Optional[datetime]) -> Optional[float]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (_utcnow() - dt).total_seconds()


def _read_version() -> str:
    for p in (Path(__file__).resolve().parents[2] / "VERSION", Path("/app/VERSION")):
        try:
            return p.read_text().strip()
        except (FileNotFoundError, OSError):
            continue
    return "dev"


# ── App setup ──────────────────────────────────────────────────────────────────
db = HermesDB(DSN)


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        db.ensure_strategies(STRATEGY_PRIORITIES)
    except Exception as exc:                                   # noqa: BLE001
        logger.exception("ensure_strategies failed: %s", exc)
    yield


app = FastAPI(title="Hermes C2", lifespan=lifespan)
STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ── Root ───────────────────────────────────────────────────────────────────────
@app.get("/")
def index() -> FileResponse:
    return FileResponse(
        STATIC_DIR / "dashboard.html",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


# ── Status ─────────────────────────────────────────────────────────────────────
@app.get("/api/status")
def get_status() -> Dict[str, Any]:
    last_log_ts = db.latest_log_ts()
    last_log_age = _seconds_since(last_log_ts)
    hermes_running = last_log_age is not None and last_log_age <= STALE_AFTER_S

    started_iso = db.get_setting(SETTING_AGENT_STARTED)
    started_at = _parse_iso(started_iso)
    uptime_s = _seconds_since(started_at) if hermes_running else None

    last_ok = _parse_iso(db.get_setting(SETTING_TRADIER_OK_TS))
    tradier_error = (db.get_setting(SETTING_TRADIER_ERROR) or "").strip()
    tradier_ok = (
        _seconds_since(last_ok) is not None
        and _seconds_since(last_ok) <= STALE_AFTER_S
        and not tradier_error
    )

    llm_error = (db.get_setting(SETTING_LLM_ERROR) or "").strip()
    llm_last_ok = _parse_iso(db.get_setting(SETTING_LLM_OK_TS))
    llm_ok = (
        llm_last_ok is not None
        and _seconds_since(llm_last_ok) is not None
        and _seconds_since(llm_last_ok) <= STALE_AFTER_S * 4  # LLM may not be used every tick
        and not llm_error
    )

    mode = (db.get_setting(SETTING_MODE) or "paper").lower()
    if mode not in VALID_MODES:
        mode = "paper"

    paused = (db.get_setting(SETTING_PAUSED) or "false").lower() == "true"
    approval_mode = (db.get_setting(SETTING_APPROVAL_MODE) or "true").lower() == "true"
    pending_count = len(db.list_approvals(status="PENDING", limit=500))

    strategy_enabled = {
        sid: (db.get_setting(_strategy_enabled_key(sid)) or "true").lower() != "false"
        for sid in STRATEGIES
    }

    llm_model = (db.get_setting(SETTING_LLM_MODEL) or "").strip()
    llm_provider = (db.get_setting(SETTING_LLM_PROVIDER) or "mock").strip()

    try:
        mkt = market_session()
        nxt = next_open() if not mkt["is_open"] else None
    except Exception:                                               # noqa: BLE001
        mkt = {"session": "unknown", "is_open": False,
               "et_time": "--:--", "et_date": "", "trading_day": False}
        nxt = None

    return {
        "hermes_running": hermes_running,
        "hermes_last_seen_age_s": last_log_age,
        "agent_started_at": started_iso,
        "uptime_s": uptime_s,
        "tradier_ok": tradier_ok,
        "tradier_error": tradier_error or None,
        "llm_ok": llm_ok,
        "llm_error": llm_error or None,
        "llm_model": llm_model or None,
        "llm_provider": llm_provider,
        "mode": mode,
        "paused": paused,
        "approval_mode": approval_mode,
        "pending_approvals": pending_count,
        "strategy_enabled": strategy_enabled,
        "stale_after_s": STALE_AFTER_S,
        "tick_interval_s": TICK_INTERVAL_S,
        "version": os.environ.get("HERMES_VERSION") or _read_version(),
        "market_session": mkt["session"],
        "market_is_open": mkt["is_open"],
        "market_et_time": mkt["et_time"],
        "market_trading_day": mkt["trading_day"],
        "market_next_open": nxt.isoformat() if nxt else None,
    }


# ── Approval queue ─────────────────────────────────────────────────────────────
@app.get("/api/approvals")
def list_approvals(status: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    return db.list_approvals(status=status, limit=min(limit, 500))


class ApprovalDecisionBody(BaseModel):
    notes: Optional[str] = None


@app.post("/api/approvals/{approval_id}/approve")
def approve_trade(approval_id: int, body: ApprovalDecisionBody = ApprovalDecisionBody()) -> Dict[str, Any]:
    ok = db.decide_approval(approval_id, "APPROVED", notes=body.notes)
    if not ok:
        raise HTTPException(status_code=404,
                            detail=f"Approval {approval_id} not found or not PENDING")
    db.write_log("ENGINE", f"[C2] Trade approval_id={approval_id} APPROVED by operator")
    return {"status": "approved", "id": approval_id}


@app.post("/api/approvals/{approval_id}/reject")
def reject_trade(approval_id: int, body: ApprovalDecisionBody = ApprovalDecisionBody()) -> Dict[str, Any]:
    ok = db.decide_approval(approval_id, "REJECTED", notes=body.notes)
    if not ok:
        raise HTTPException(status_code=404,
                            detail=f"Approval {approval_id} not found or not PENDING")
    db.write_log("ENGINE", f"[C2] Trade approval_id={approval_id} REJECTED by operator"
                           + (f": {body.notes}" if body.notes else ""))
    return {"status": "rejected", "id": approval_id}


# ── Bulk approve / reject ─────────────────────────────────────────────────────
class BulkDecisionBody(BaseModel):
    action: str          # "approve" | "reject"
    notes: Optional[str] = None


@app.post("/api/approvals/bulk")
def bulk_decide(body: BulkDecisionBody) -> Dict[str, Any]:
    action = body.action.lower()
    if action not in ("approve", "reject"):
        raise HTTPException(status_code=400, detail="action must be 'approve' or 'reject'")
    status = "APPROVED" if action == "approve" else "REJECTED"
    pending = db.list_approvals(status="PENDING", limit=500)
    count = 0
    for item in pending:
        if db.decide_approval(item["id"], status, notes=body.notes):
            count += 1
    db.write_log("ENGINE",
                 f"[C2] Bulk {status} — {count} trades by operator"
                 + (f": {body.notes}" if body.notes else ""))
    return {"status": status.lower(), "count": count}


# ── Approval mode toggle ───────────────────────────────────────────────────────
class ApprovalModeBody(BaseModel):
    enabled: bool


@app.put("/api/approval-mode")
def set_approval_mode(body: ApprovalModeBody) -> Dict[str, Any]:
    db.set_setting(SETTING_APPROVAL_MODE, "true" if body.enabled else "false")
    db.write_log("ENGINE", f"[C2] Approval mode {'ENABLED' if body.enabled else 'DISABLED'}")
    return {"approval_mode": body.enabled}


# ── Soul editor ────────────────────────────────────────────────────────────────
@app.get("/api/soul")
def get_soul() -> Dict[str, Any]:
    soul = db.get_setting(SETTING_SOUL) or ""
    autonomy = (db.get_setting(SETTING_AUTONOMY) or "advisory").lower()
    if autonomy not in VALID_AUTONOMY:
        autonomy = "advisory"
    return {
        "soul": soul,
        "autonomy": autonomy,
        "valid_autonomy": list(VALID_AUTONOMY),
        "max_bytes": MAX_SOUL_BYTES,
        "current_bytes": len(soul.encode()),
    }


class SoulBody(BaseModel):
    soul: Optional[str] = None
    autonomy: Optional[str] = None


@app.put("/api/soul")
def set_soul(body: SoulBody) -> Dict[str, Any]:
    if body.soul is not None:
        if len(body.soul.encode()) > MAX_SOUL_BYTES:
            raise HTTPException(status_code=400,
                                detail=f"Soul exceeds {MAX_SOUL_BYTES // 1024}KB limit")
        db.set_setting(SETTING_SOUL, body.soul)
        db.write_log("ENGINE", f"[C2] Soul updated ({len(body.soul.encode())}B)")

    if body.autonomy is not None:
        a = body.autonomy.lower().strip()
        if a not in VALID_AUTONOMY:
            raise HTTPException(status_code=400,
                                detail=f"autonomy must be one of {list(VALID_AUTONOMY)}")
        db.set_setting(SETTING_AUTONOMY, a)
        db.write_log("ENGINE", f"[C2] Autonomy set to {a}")

    return get_soul()


# ── Agent controls ─────────────────────────────────────────────────────────────
@app.post("/api/agent/pause")
def pause_agent() -> Dict[str, Any]:
    db.set_setting(SETTING_PAUSED, "true")
    db.write_log("ENGINE", "[C2] Agent PAUSED by operator")
    return {"paused": True}


@app.post("/api/agent/resume")
def resume_agent() -> Dict[str, Any]:
    db.set_setting(SETTING_PAUSED, "false")
    db.write_log("ENGINE", "[C2] Agent RESUMED by operator")
    return {"paused": False}


class ModeBody(BaseModel):
    mode: str


@app.put("/api/mode")
def set_mode(body: ModeBody) -> Dict[str, Any]:
    m = body.mode.lower().strip()
    if m not in VALID_MODES:
        raise HTTPException(status_code=400,
                            detail=f"mode must be one of {list(VALID_MODES)}")
    db.set_setting(SETTING_MODE, m)
    db.write_log("ENGINE", f"[C2] Mode switched to {m}")
    return {"mode": m}


# ── Strategy toggles ───────────────────────────────────────────────────────────
@app.get("/api/strategies")
def get_strategies() -> List[Dict[str, Any]]:
    return [
        {
            "id": sid,
            "priority": STRATEGY_PRIORITIES[sid],
            "enabled": (db.get_setting(_strategy_enabled_key(sid)) or "true").lower() != "false",
        }
        for sid in STRATEGIES
    ]


class StrategyToggleBody(BaseModel):
    enabled: bool


@app.put("/api/strategies/{strategy_id}")
def toggle_strategy(strategy_id: str, body: StrategyToggleBody) -> Dict[str, Any]:
    sid = strategy_id.upper()
    if sid not in STRATEGIES:
        raise HTTPException(status_code=404,
                            detail=f"Unknown strategy {strategy_id!r}; valid: {list(STRATEGIES)}")
    db.set_setting(_strategy_enabled_key(sid), "true" if body.enabled else "false")
    db.write_log("ENGINE",
                 f"[C2] Strategy {sid} {'ENABLED' if body.enabled else 'DISABLED'}")
    return {"id": sid, "enabled": body.enabled}


# ── LLM config ─────────────────────────────────────────────────────────────────
def _read_llm_config() -> Dict[str, Any]:
    provider = (db.get_setting(SETTING_LLM_PROVIDER) or "mock").lower()
    if provider not in VALID_LLM_PROVIDERS:
        provider = "mock"
    base_url = (db.get_setting(SETTING_LLM_BASE_URL) or DEFAULT_LLM_BASE_URL).strip()
    model = (db.get_setting(SETTING_LLM_MODEL) or "").strip()
    api_key = (db.get_setting(SETTING_LLM_API_KEY) or "").strip()
    try:
        temperature = float(db.get_setting(SETTING_LLM_TEMPERATURE) or 0.2)
    except ValueError:
        temperature = 0.2
    try:
        timeout_s = max(5.0, float(db.get_setting(SETTING_LLM_TIMEOUT) or DEFAULT_LLM_TIMEOUT_S))
    except ValueError:
        timeout_s = DEFAULT_LLM_TIMEOUT_S
    vision = (db.get_setting(SETTING_LLM_VISION) or "true").lower() != "false"
    last_ok = _parse_iso(db.get_setting(SETTING_LLM_OK_TS))
    last_err = (db.get_setting(SETTING_LLM_ERROR) or "").strip() or None
    return {
        "provider": provider,
        "base_url": base_url,
        "model": model,
        "temperature": temperature,
        "timeout_s": timeout_s,
        "vision": vision,
        "last_ok_age_s": _seconds_since(last_ok),
        "last_error": last_err,
        "valid_providers": list(VALID_LLM_PROVIDERS),
        # True/False only — the actual key is never sent to the browser.
        "has_api_key": bool(api_key),
        # Last 4 chars so the operator can confirm which key is stored
        # without exposing it. Empty string when no key is set.
        "api_key_hint": f"…{api_key[-4:]}" if len(api_key) >= 4 else ("set" if api_key else ""),
    }


class LLMConfigBody(BaseModel):
    provider: Optional[str] = None
    base_url: Optional[str] = None
    model: Optional[str] = None
    api_key: Optional[str] = None
    temperature: Optional[float] = None
    vision: Optional[bool] = None
    timeout_s: Optional[float] = None


@app.get("/api/llm")
def get_llm() -> Dict[str, Any]:
    return _read_llm_config()


@app.put("/api/llm")
def set_llm(body: LLMConfigBody) -> Dict[str, Any]:
    if body.provider is not None:
        p = body.provider.lower().strip()
        if p not in VALID_LLM_PROVIDERS:
            raise HTTPException(status_code=400,
                                detail=f"provider must be one of {list(VALID_LLM_PROVIDERS)}")
        db.set_setting(SETTING_LLM_PROVIDER, p)
        # Pre-fill the canonical cloud URL when switching to ollama_cloud so the
        # agent can connect even if the operator didn't explicitly set base_url.
        if p == "ollama_cloud" and not (body.base_url or "").strip():
            db.set_setting(SETTING_LLM_BASE_URL, "https://api.ollama.com/v1")
    if body.base_url is not None:
        url = body.base_url.strip()
        if url and not (url.startswith("http://") or url.startswith("https://")):
            raise HTTPException(status_code=400, detail="base_url must start with http(s)://")
        db.set_setting(SETTING_LLM_BASE_URL, url)
    if body.model is not None:
        db.set_setting(SETTING_LLM_MODEL, body.model.strip())
    if body.api_key is not None:
        db.set_setting(SETTING_LLM_API_KEY, body.api_key.strip())
    if body.temperature is not None:
        if not (0.0 <= body.temperature <= 2.0):
            raise HTTPException(status_code=400, detail="temperature must be in [0.0, 2.0]")
        db.set_setting(SETTING_LLM_TEMPERATURE, str(body.temperature))
    if body.vision is not None:
        db.set_setting(SETTING_LLM_VISION, "true" if body.vision else "false")
    if body.timeout_s is not None:
        if not (5.0 <= body.timeout_s <= 600.0):
            raise HTTPException(status_code=400, detail="timeout_s must be in [5, 600]")
        db.set_setting(SETTING_LLM_TIMEOUT, str(body.timeout_s))
    db.set_setting(SETTING_LLM_ERROR, "")
    db.write_log("ENGINE", "[C2] LLM config updated")
    return _read_llm_config()


# ── Recent logs (for the activity feed) ────────────────────────────────────────
@app.get("/api/logs")
def get_logs(limit: int = 100) -> List[Dict[str, Any]]:
    raw = db.recent_logs(limit=min(limit, 500))
    # recent_logs returns a plain string; parse into structured lines.
    lines = []
    for line in (raw or "").splitlines():
        lines.append({"text": line})
    return lines


@app.get("/api/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "service": "hermes-c2"}
