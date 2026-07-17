from ._stubs import alias_db_namespaces
import pytest
from pydantic import ValidationError
from hermes.config_schema import RuntimeConfig
from hermes.service1_agent.main import _load_and_validate_runtime_config
from unittest.mock import AsyncMock

def test_runtime_config_valid_defaults():
    config = RuntimeConfig()
    assert config.obp_reserve == 0.0
    assert config.tick_interval == 300

def test_runtime_config_valid_custom():
    config = RuntimeConfig(
        obp_reserve=1500.50,
        tick_interval=60,
    )
    assert config.obp_reserve == 1500.50
    assert config.tick_interval == 60

def test_runtime_config_invalid_reserve():
    with pytest.raises(ValidationError) as exc_info:
        RuntimeConfig(obp_reserve=-100.0)
    assert "obp_reserve must be non-negative" in str(exc_info.value)

def test_runtime_config_invalid_interval():
    with pytest.raises(ValidationError) as exc_info:
        RuntimeConfig(tick_interval=0)
    assert "tick_interval must be at least 1 second" in str(exc_info.value)

    with pytest.raises(ValidationError) as exc_info:
        RuntimeConfig(tick_interval=-5)
    assert "tick_interval must be at least 1 second" in str(exc_info.value)

@pytest.mark.asyncio
async def test_load_and_validate_runtime_config_success(monkeypatch):
    monkeypatch.delenv("HERMES_TICK_INTERVAL", raising=False)
    db = AsyncMock()
    alias_db_namespaces(db)
    # A DB-stored tick_interval must be IGNORED — a stale seeded row
    # silently overriding HERMES_TICK_INTERVAL kept live ticking hourly
    # despite the env file (removed 2026-07-17).
    db.get_setting.side_effect = lambda key: {
        "obp_reserve": "2500",
        "tick_interval": "120",
        "tick_interval_s": "120",
    }.get(key)

    conf = {}
    config = await _load_and_validate_runtime_config(db, conf)
    assert config.obp_reserve == 2500.0
    assert config.tick_interval == 300

@pytest.mark.asyncio
async def test_load_and_validate_runtime_config_db_fallback(monkeypatch):
    monkeypatch.delenv("HERMES_TICK_INTERVAL", raising=False)
    db = AsyncMock()
    alias_db_namespaces(db)
    # No settings in DB
    db.get_setting.return_value = None

    conf = {
        "obp_reserve": 1000.0,
        "tick_interval_s": 45,
    }
    config = await _load_and_validate_runtime_config(db, conf)
    assert config.obp_reserve == 1000.0
    assert config.tick_interval == 45

@pytest.mark.asyncio
async def test_tick_interval_env_beats_conf_and_db(monkeypatch):
    monkeypatch.setenv("HERMES_TICK_INTERVAL", "120")
    db = AsyncMock()
    alias_db_namespaces(db)
    db.get_setting.side_effect = lambda key: {
        "tick_interval": "9999",
    }.get(key)

    conf = {"tick_interval_s": 45}
    config = await _load_and_validate_runtime_config(db, conf)
    assert config.tick_interval == 120

@pytest.mark.asyncio
async def test_load_and_validate_runtime_config_validation_error():
    db = AsyncMock()
    alias_db_namespaces(db)
    # Invalid setting in DB
    db.get_setting.side_effect = lambda key: {
        "obp_reserve": "-500",
        "tick_interval": "120",
        "tick_interval_s": None,
    }.get(key)

    conf = {}
    with pytest.raises(ValidationError):
        await _load_and_validate_runtime_config(db, conf)
