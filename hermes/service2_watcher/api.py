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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError

from hermes.db.models import HermesDB
from hermes.ml.xgb_features import AsyncXGBPredictor, FeatureEngineer

logger = logging.getLogger("hermes.watcher.api")

DSN = os.environ.get("HERMES_DSN", "postgresql+psycopg://hermes:hermes@localhost:5432/hermes")
WATCHLIST = [s.strip().upper() for s in
             os.environ.get("HERMES_WATCHLIST", "AAPL,SPY,QQQ,NVDA,AMD,KO").split(",") if s.strip()]

# Canonical strategy registry mirrored from service1_agent/strategies.py.
# Order is the cascading priority (CS75 highest → WHEEL lowest); both services
# seed the `strategies` table from this map on startup so watchlist writes
# never fail an FK check on a fresh DB.
STRATEGIES = ("CS75", "CS7", "TT45", "WHEEL")
STRATEGY_PRIORITIES = {"CS75": 1, "CS7": 2, "TT45": 3, "WHEEL": 4}

# Setting keys mirrored from service1_agent/main.py — keep in sync.
SETTING_MODE = "hermes_mode"
SETTING_TRADIER_OK_TS = "tradier_last_ok_ts"
SETTING_TRADIER_ERROR = "tradier_last_error"
SETTING_AGENT_STARTED_AT = "agent_started_at"

SETTING_LLM_PROVIDER = "llm_provider"
SETTING_LLM_BASE_URL = "llm_base_url"
SETTING_LLM_MODEL = "llm_model"
SETTING_LLM_API_KEY = "llm_api_key"
SETTING_LLM_TEMPERATURE = "llm_temperature"
SETTING_LLM_VISION = "llm_vision"
SETTING_LLM_TIMEOUT = "llm_timeout_s"
SETTING_LLM_OK_TS = "llm_last_ok_ts"
SETTING_LLM_ERROR = "llm_last_error"

DEFAULT_LLM_TIMEOUT_S = 120.0

# Operator doctrine + agent control — written by the watcher.
SETTING_SOUL = "soul_md"
SETTING_AUTONOMY = "agent_autonomy"
SETTING_PAUSED = "agent_paused"

VALID_MODES = ("paper", "live")
VALID_LLM_PROVIDERS = ("mock", "local")
VALID_AUTONOMY = ("advisory", "enforcing", "autonomous")

# Hard cap so a runaway client can't fill the settings table with multi-MB
# blobs. soul.md should be a focused doctrine, not an essay collection.
MAX_SOUL_BYTES = 64 * 1024

# Sensible defaults the form preloads when nothing is configured. LM Studio
# default chosen per operator preference; can still be edited in the UI.
DEFAULT_LLM_BASE_URL = "http://host.docker.internal:1234/v1"

TICK_INTERVAL_S = int(os.environ.get("HERMES_TICK_INTERVAL", 300))
# Anything older than this is "stale" — give the agent two ticks of slack so a
# single slow tick doesn't paint the dashboard red.
STALE_AFTER_S = max(60, TICK_INTERVAL_S * 2 + 30)


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def _seconds_since(dt: Optional[datetime]) -> Optional[float]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt).total_seconds()


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
    # Seed the strategies registry so the per-strategy watchlist UI can write
    # immediately on a fresh DB. Idempotent — existing rows are left alone.
    try:
        db.ensure_strategies(STRATEGY_PRIORITIES)
    except Exception as exc:                                       # noqa: BLE001
        logger.exception("ensure_strategies failed at startup: %s", exc)
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
    # The dashboard is the only page humans hit; we'd rather take the extra
    # bytes than chase a stale-HTML bug. These headers force the browser to
    # revalidate every load so a `docker compose up` is the only step needed
    # to see UI changes.
    return FileResponse(
        STATIC_DIR / "dashboard.html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )


@app.get("/api/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "service": "human-watcher",
        "default_watchlist": WATCHLIST,
        "strategies": list(STRATEGIES),
    }


# ---------------------------------------------------------------------------
# Agent + Tradier status — read from the shared settings table the agent
# updates on every tick. The watcher itself does not call Tradier; it trusts
# the agent's heartbeat as the source of truth for both signals.
# ---------------------------------------------------------------------------
def _expected_tradier_url(mode: str) -> str:
    """Mirror what the agent's _resolve_mode_credentials picks for `mode`."""
    if mode == "paper":
        return os.environ.get("TRADIER_PAPER_BASE_URL", "https://sandbox.tradier.com/v1")
    return os.environ.get("TRADIER_LIVE_BASE_URL", "https://api.tradier.com/v1")


def _mask_account(account_id: Optional[str]) -> Optional[str]:
    if not account_id:
        return None
    return f"…{account_id[-4:]}" if len(account_id) > 4 else "•" * len(account_id)


@app.get("/api/status")
def status() -> Dict[str, Any]:
    last_log_ts = db.latest_log_ts()
    last_log_age = _seconds_since(last_log_ts)
    hermes_running = last_log_age is not None and last_log_age <= STALE_AFTER_S

    started_at_iso = db.get_setting(SETTING_AGENT_STARTED_AT)
    started_at = _parse_iso(started_at_iso)
    uptime_s = _seconds_since(started_at) if hermes_running else None

    last_ok = _parse_iso(db.get_setting(SETTING_TRADIER_OK_TS))
    last_ok_age = _seconds_since(last_ok)
    tradier_error = (db.get_setting(SETTING_TRADIER_ERROR) or "").strip()
    tradier_ok = (
        last_ok_age is not None
        and last_ok_age <= STALE_AFTER_S
        and not tradier_error
    )

    mode = (db.get_setting(SETTING_MODE) or "paper").lower()
    if mode not in VALID_MODES:
        mode = "paper"

    # Reflect which credentials the agent *should* be using for this mode.
    if mode == "paper":
        account_id = os.environ.get("TRADIER_PAPER_ACCOUNT_ID") or os.environ.get("TRADIER_ACCOUNT_ID")
    else:
        account_id = os.environ.get("TRADIER_LIVE_ACCOUNT_ID") or os.environ.get("TRADIER_ACCOUNT_ID")

    return {
        # Hermes ---------------------------------------------------------
        "hermes_running": hermes_running,
        "hermes_last_seen": last_log_ts.isoformat() if last_log_ts else None,
        "hermes_last_seen_age_s": last_log_age,
        "agent_started_at": started_at_iso,
        "uptime_s": uptime_s,
        # Tradier --------------------------------------------------------
        "tradier_ok": tradier_ok,
        "tradier_last_ok": last_ok.isoformat() if last_ok else None,
        "tradier_last_ok_age_s": last_ok_age,
        "tradier_error": tradier_error or None,
        "tradier_base_url": _expected_tradier_url(mode),
        "tradier_account_masked": _mask_account(account_id),
        # Mode + tuning --------------------------------------------------
        "mode": mode,
        "stale_after_s": STALE_AFTER_S,
        "tick_interval_s": TICK_INTERVAL_S,
    }


class ModeBody(BaseModel):
    mode: str


@app.get("/api/mode")
def get_mode() -> Dict[str, Any]:
    mode = (db.get_setting(SETTING_MODE) or "paper").lower()
    if mode not in VALID_MODES:
        mode = "paper"
    return {"mode": mode, "valid": list(VALID_MODES)}


# ---------------------------------------------------------------------------
# LLM overseer management
# The watcher writes settings; the agent reads them at the start of every
# tick and rebuilds the LLM client/overseer when anything changes.
# ---------------------------------------------------------------------------
class LLMConfigBody(BaseModel):
    provider: Optional[str] = None
    base_url: Optional[str] = None
    model: Optional[str] = None
    api_key: Optional[str] = None
    temperature: Optional[float] = None
    vision: Optional[bool] = None
    timeout_s: Optional[float] = None


def _read_llm_config(include_secret: bool = False) -> Dict[str, Any]:
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
        timeout_s = max(5.0, float(db.get_setting(SETTING_LLM_TIMEOUT)
                                   or DEFAULT_LLM_TIMEOUT_S))
    except ValueError:
        timeout_s = DEFAULT_LLM_TIMEOUT_S
    vision = (db.get_setting(SETTING_LLM_VISION) or "true").lower() != "false"
    last_ok = _parse_iso(db.get_setting(SETTING_LLM_OK_TS))
    last_err = (db.get_setting(SETTING_LLM_ERROR) or "").strip() or None
    out = {
        "provider": provider,
        "base_url": base_url,
        "model": model,
        "temperature": temperature,
        "timeout_s": timeout_s,
        "vision": vision,
        "has_api_key": bool(api_key),
        "last_ok": last_ok.isoformat() if last_ok else None,
        "last_ok_age_s": _seconds_since(last_ok),
        "last_error": last_err,
        "valid_providers": list(VALID_LLM_PROVIDERS),
        "default_base_url": DEFAULT_LLM_BASE_URL,
        "default_timeout_s": DEFAULT_LLM_TIMEOUT_S,
    }
    if include_secret:
        out["api_key"] = api_key
    return out


@app.get("/api/llm")
def get_llm_config() -> Dict[str, Any]:
    return _read_llm_config()


@app.put("/api/llm")
def set_llm_config(body: LLMConfigBody) -> Dict[str, Any]:
    if body.provider is not None:
        prov = body.provider.lower().strip()
        if prov not in VALID_LLM_PROVIDERS:
            raise HTTPException(
                status_code=400,
                detail=f"invalid provider {body.provider!r}; allowed: {list(VALID_LLM_PROVIDERS)}",
            )
        db.set_setting(SETTING_LLM_PROVIDER, prov)

    if body.base_url is not None:
        url = body.base_url.strip()
        if url and not (url.startswith("http://") or url.startswith("https://")):
            raise HTTPException(status_code=400,
                                detail="base_url must start with http:// or https://")
        db.set_setting(SETTING_LLM_BASE_URL, url)

    if body.model is not None:
        db.set_setting(SETTING_LLM_MODEL, body.model.strip())

    if body.api_key is not None:
        # Empty string means "clear the key". This is the only way to remove it.
        db.set_setting(SETTING_LLM_API_KEY, body.api_key.strip())

    if body.temperature is not None:
        if not (0.0 <= float(body.temperature) <= 2.0):
            raise HTTPException(status_code=400,
                                detail="temperature must be in [0.0, 2.0]")
        db.set_setting(SETTING_LLM_TEMPERATURE, str(float(body.temperature)))

    if body.vision is not None:
        db.set_setting(SETTING_LLM_VISION, "true" if body.vision else "false")

    if body.timeout_s is not None:
        ts = float(body.timeout_s)
        if not (5.0 <= ts <= 600.0):
            raise HTTPException(status_code=400,
                                detail="timeout_s must be in [5, 600] seconds")
        db.set_setting(SETTING_LLM_TIMEOUT, str(ts))

    # Saving any field clears any previous error so the dashboard light
    # doesn't stay red after a fix.
    db.set_setting(SETTING_LLM_ERROR, "")

    db.write_log("ENGINE", "LLM config updated via watcher")
    return _read_llm_config()


@app.post("/api/llm/models")
def list_llm_models(body: Optional[LLMConfigBody] = None) -> Dict[str, Any]:
    """Return the list of models the configured local model server is serving.

    Two modes — same as /api/llm/test:
      * Body provided  → query the server at body.base_url (so the operator
                         can refresh the dropdown before saving).
      * Body omitted   → query the saved settings.

    No persistent changes — this is purely a read against the local server.
    """
    saved = _read_llm_config(include_secret=True)
    base_url = (body.base_url if body and body.base_url is not None
                else saved.get("base_url")) or ""
    base_url = base_url.strip()
    api_key = (body.api_key if body and body.api_key is not None
               else saved.get("api_key")) or None
    if isinstance(api_key, str):
        api_key = api_key.strip() or None
    timeout_s = (body.timeout_s if body and body.timeout_s is not None
                 else saved.get("timeout_s", DEFAULT_LLM_TIMEOUT_S))
    try:
        timeout_s = max(5.0, float(timeout_s))
    except (TypeError, ValueError):
        timeout_s = DEFAULT_LLM_TIMEOUT_S

    if not base_url:
        raise HTTPException(status_code=400, detail="base_url is required")
    if not (base_url.startswith("http://") or base_url.startswith("https://")):
        raise HTTPException(status_code=400,
                            detail="base_url must start with http:// or https://")

    try:
        from hermes.llm import OpenAICompatibleLLM, LLMConnectionError
        # `model` is required by the constructor but not used by /models —
        # pass a placeholder so we can build the client cheaply.
        client = OpenAICompatibleLLM(
            base_url=base_url, model="-", api_key=api_key,
            timeout_s=timeout_s,
        )
        models = client.list_models(timeout_s=min(timeout_s, 30.0))
        return {
            "base_url": base_url,
            "count": len(models),
            "models": models,
            "current": saved.get("model"),
        }
    except LLMConnectionError as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    except Exception as exc:                                    # noqa: BLE001
        logger.exception("LLM models listing failed")
        raise HTTPException(status_code=502,
                            detail=f"{type(exc).__name__}: {exc}")


@app.post("/api/llm/test")
def test_llm(body: Optional[LLMConfigBody] = None) -> Dict[str, Any]:
    """Round-trip ping of a local model.

    Two modes:
      * Body provided  → test those exact values without persisting them.
                         This is what the dashboard's "Test connection"
                         button uses so you can validate before saving.
      * Body omitted   → test the currently saved settings. Useful from
                         curl/CI to verify the running config.
    """
    saved = _read_llm_config(include_secret=True)

    # Layer the request body on top of saved config so a partial body
    # (e.g. only base_url + model) still works.
    base_url = (body.base_url if body and body.base_url is not None
                else saved.get("base_url")) or ""
    base_url = base_url.strip()
    model = (body.model if body and body.model is not None
             else saved.get("model")) or ""
    model = model.strip()
    api_key = (body.api_key if body and body.api_key is not None
               else saved.get("api_key")) or None
    if isinstance(api_key, str):
        api_key = api_key.strip() or None
    temperature = (body.temperature if body and body.temperature is not None
                   else saved.get("temperature", 0.2))
    timeout_s = (body.timeout_s if body and body.timeout_s is not None
                 else saved.get("timeout_s", DEFAULT_LLM_TIMEOUT_S))
    try:
        timeout_s = max(5.0, float(timeout_s))
    except (TypeError, ValueError):
        timeout_s = DEFAULT_LLM_TIMEOUT_S

    if not base_url or not model:
        raise HTTPException(
            status_code=400,
            detail="base_url and model are required to test the local model",
        )
    if not (base_url.startswith("http://") or base_url.startswith("https://")):
        raise HTTPException(status_code=400,
                            detail="base_url must start with http:// or https://")

    try:
        from hermes.llm import OpenAICompatibleLLM, LLMConnectionError
        client = OpenAICompatibleLLM(
            base_url=base_url, model=model,
            api_key=api_key, temperature=float(temperature),
            timeout_s=timeout_s,
        )
        result = client.ping()
        result["timeout_s"] = timeout_s
        # Successful ping is recorded against saved settings only when the
        # user is testing the persisted config (no body). When testing
        # form-only values we leave the persisted error/ok timestamps alone.
        if body is None:
            db.set_setting(SETTING_LLM_OK_TS,
                           datetime.now(timezone.utc).isoformat(timespec="seconds"))
            db.set_setting(SETTING_LLM_ERROR, "")
        return result
    except LLMConnectionError as exc:
        if body is None:
            db.set_setting(SETTING_LLM_ERROR, str(exc)[:500])
        raise HTTPException(status_code=502, detail=str(exc))
    except Exception as exc:                                    # noqa: BLE001
        logger.exception("LLM test failed")
        if body is None:
            db.set_setting(SETTING_LLM_ERROR, f"{type(exc).__name__}: {exc}")
        raise HTTPException(status_code=502,
                            detail=f"{type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# Operator doctrine — the free-text soul.md the overseer prepends to the
# system prompt on every LLM call. The agent re-reads this on every tick.
# ---------------------------------------------------------------------------
class SoulBody(BaseModel):
    content: str


@app.get("/api/soul")
def get_soul() -> Dict[str, Any]:
    content = db.get_setting(SETTING_SOUL) or ""
    updated = db.setting_updated_at(SETTING_SOUL)
    return {
        "content": content,
        "bytes": len(content.encode("utf-8")),
        "max_bytes": MAX_SOUL_BYTES,
        "updated_at": updated.isoformat() if updated else None,
    }


@app.put("/api/soul")
def set_soul(body: SoulBody) -> Dict[str, Any]:
    content = body.content or ""
    if len(content.encode("utf-8")) > MAX_SOUL_BYTES:
        raise HTTPException(
            status_code=400,
            detail=f"soul.md exceeds {MAX_SOUL_BYTES} bytes "
                   f"(got {len(content.encode('utf-8'))})",
        )
    db.set_setting(SETTING_SOUL, content)
    db.write_log("ENGINE", f"soul.md updated ({len(content)} chars)")
    return {"content": content, "bytes": len(content.encode("utf-8"))}


# ---------------------------------------------------------------------------
# Agent control — autonomy level + pause/resume. Both apply within one tick
# interval (the agent re-reads system_settings at the top of every tick).
# ---------------------------------------------------------------------------
class ControlBody(BaseModel):
    autonomy: Optional[str] = None
    paused: Optional[bool] = None


@app.get("/api/control")
def get_control() -> Dict[str, Any]:
    autonomy = (db.get_setting(SETTING_AUTONOMY) or "advisory").lower()
    if autonomy not in VALID_AUTONOMY:
        autonomy = "advisory"
    paused = (db.get_setting(SETTING_PAUSED) or "false").lower() == "true"
    return {
        "autonomy": autonomy,
        "paused": paused,
        "valid_autonomy": list(VALID_AUTONOMY),
        "applies_within_s": TICK_INTERVAL_S,
    }


@app.put("/api/control")
def set_control(body: ControlBody) -> Dict[str, Any]:
    if body.autonomy is not None:
        a = body.autonomy.lower().strip()
        if a not in VALID_AUTONOMY:
            raise HTTPException(
                status_code=400,
                detail=f"invalid autonomy {body.autonomy!r}; allowed: {list(VALID_AUTONOMY)}",
            )
        db.set_setting(SETTING_AUTONOMY, a)
        db.write_log("ENGINE", f"autonomy → {a}")
    if body.paused is not None:
        db.set_setting(SETTING_PAUSED, "true" if body.paused else "false")
        db.write_log("ENGINE", f"agent {'PAUSED' if body.paused else 'RESUMED'}")
    return get_control()


@app.put("/api/mode")
def set_mode(body: ModeBody) -> Dict[str, Any]:
    mode = (body.mode or "").lower().strip()
    if mode not in VALID_MODES:
        raise HTTPException(
            status_code=400,
            detail=f"invalid mode {body.mode!r}; allowed: {list(VALID_MODES)}",
        )
    previous = (db.get_setting(SETTING_MODE) or "").lower()
    db.set_setting(SETTING_MODE, mode)
    # Clear any stale Tradier error so the dashboard light isn't painted red
    # by an error that belonged to the previous mode's credentials.
    db.set_setting(SETTING_TRADIER_ERROR, "")
    db.write_log("ENGINE", f"watcher requested mode change: {previous or '?'} → {mode}")
    return {"mode": mode, "previous": previous or None,
            "applies_within_s": TICK_INTERVAL_S}


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


def _ensure_strategy_or_400(sid: str) -> None:
    """Defensive idempotent re-seed.

    The lifespan handler already seeds STRATEGIES on startup, but if a write
    arrives before that ran (or against a DB where the strategies table was
    truncated), the FK on strategy_watchlists.strategy_id would reject the
    insert. Doing the upsert here makes watchlist writes self-healing.
    """
    if sid in STRATEGY_PRIORITIES:
        try:
            db.ensure_strategies({sid: STRATEGY_PRIORITIES[sid]})
        except Exception as exc:                                  # noqa: BLE001
            raise HTTPException(status_code=500,
                                detail=f"could not seed strategy {sid}: {exc}")


@app.put("/api/watchlists/{strategy_id}")
def replace_watchlist(strategy_id: str, body: WatchlistBody) -> Dict[str, Any]:
    sid = _require_strategy(strategy_id)
    _ensure_strategy_or_400(sid)
    symbols = [_clean_symbol(s) for s in body.symbols]
    try:
        saved = db.set_watchlist(sid, symbols)
    except IntegrityError as exc:
        raise HTTPException(status_code=400,
                            detail=f"watchlist replace rejected: {exc.orig}")
    db.write_log(sid, f"watchlist replaced: {saved}")
    return {"strategy_id": sid, "symbols": saved}


@app.post("/api/watchlists/{strategy_id}")
def add_watchlist_symbol(strategy_id: str, body: SymbolBody) -> Dict[str, Any]:
    sid = _require_strategy(strategy_id)
    _ensure_strategy_or_400(sid)
    # Accept comma-separated bulk input from the UI. Single-symbol callers
    # still work — splitting "AAPL" yields ["AAPL"].
    raw = (body.symbol or "").replace(";", ",").split(",")
    symbols = [_clean_symbol(s) for s in raw if s and s.strip()]
    if not symbols:
        raise HTTPException(status_code=400, detail="no symbols provided")

    added: List[str] = []
    skipped: List[str] = []
    for sym in symbols:
        try:
            if db.add_to_watchlist(sid, sym):
                added.append(sym)
                db.write_log(sid, f"watchlist add: {sym}")
            else:
                skipped.append(sym)
        except IntegrityError as exc:
            raise HTTPException(status_code=400,
                                detail=f"watchlist add rejected for {sym}: {exc.orig}")
        except Exception as exc:                                  # noqa: BLE001
            # Anything else — surface the real reason so the user sees it
            # in the dialog instead of a bare "Internal Server Error".
            logger.exception("add_watchlist_symbol failed for %s/%s", sid, sym)
            raise HTTPException(
                status_code=500,
                detail=f"watchlist add failed for {sym}: {type(exc).__name__}: {exc}",
            )
    return {
        "strategy_id": sid,
        "added": added,
        "skipped_existing": skipped,
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
