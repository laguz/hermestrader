"""LLM overseer configuration.

Routes
------
- ``GET /api/llm`` — current LLM provider config + last_ok / last_error
- ``PUT /api/llm`` — update one or more fields (provider/base_url/model/etc.)

The agent's tick loop snapshots all LLM settings each iteration and
rebuilds the overseer client whenever any field changes. The api_key is
never sent to the browser; only a 4-char hint of the stored value is
surfaced.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from hermes.common import DEFAULT_LLM_TIMEOUT_S, VALID_LLM_PROVIDERS
from hermes.utils import decrypt_value, encrypt_value

from .._app_state import (
    DEFAULT_LLM_BASE_URL,
    SETTING_LLM_API_KEY,
    SETTING_LLM_BASE_URL,
    SETTING_LLM_ERROR,
    SETTING_LLM_MODEL,
    SETTING_LLM_OK_TS,
    SETTING_LLM_PROVIDER,
    SETTING_LLM_TEMPERATURE,
    SETTING_LLM_TIMEOUT,
    SETTING_LLM_VISION,
    db,
    parse_iso,
    seconds_since,
)

router = APIRouter()


def _read_llm_config() -> Dict[str, Any]:
    provider = (db.get_setting(SETTING_LLM_PROVIDER) or "mock").lower()
    if provider not in VALID_LLM_PROVIDERS:
        provider = "mock"
    base_url = (db.get_setting(SETTING_LLM_BASE_URL) or DEFAULT_LLM_BASE_URL).strip()
    model = (db.get_setting(SETTING_LLM_MODEL) or "").strip()
    api_key = decrypt_value((db.get_setting(SETTING_LLM_API_KEY) or "").strip())
    try:
        temperature = float(db.get_setting(SETTING_LLM_TEMPERATURE) or 0.2)
    except ValueError:
        temperature = 0.2
    try:
        timeout_s = max(5.0, float(db.get_setting(SETTING_LLM_TIMEOUT) or DEFAULT_LLM_TIMEOUT_S))
    except ValueError:
        timeout_s = DEFAULT_LLM_TIMEOUT_S
    vision = (db.get_setting(SETTING_LLM_VISION) or "true").lower() != "false"
    last_ok = parse_iso(db.get_setting(SETTING_LLM_OK_TS))
    last_err = (db.get_setting(SETTING_LLM_ERROR) or "").strip() or None
    return {
        "provider": provider,
        "base_url": base_url,
        "model": model,
        "temperature": temperature,
        "timeout_s": timeout_s,
        "vision": vision,
        "last_ok_age_s": seconds_since(last_ok),
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


@router.get("/api/llm")
def get_llm() -> Dict[str, Any]:
    return _read_llm_config()


@router.put("/api/llm")
def set_llm(body: LLMConfigBody) -> Dict[str, Any]:
    if body.provider is not None:
        p = body.provider.lower().strip()
        if p not in VALID_LLM_PROVIDERS:
            raise HTTPException(
                status_code=400,
                detail=f"provider must be one of {list(VALID_LLM_PROVIDERS)}",
            )
        db.set_setting(SETTING_LLM_PROVIDER, p)
        # Pre-fill the canonical cloud URL when switching to ollama_cloud so
        # the agent can connect even if the operator didn't explicitly set
        # base_url.
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
        db.set_setting(SETTING_LLM_API_KEY, encrypt_value(body.api_key.strip()))
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
