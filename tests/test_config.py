from ._stubs import alias_db_namespaces
import pytest
from unittest.mock import AsyncMock
from hermes.config import HermesSettings


async def test_reconcile_with_db():
    settings = HermesSettings()
    
    db = AsyncMock()
    alias_db_namespaces(db)
    
    settings_dict = {
        "hermes_mode": "live",
        "agent_autonomy": "autonomous",
        "llm_provider": "openai",
        "llm_base_url": "https://api.openai.com/v1",
        "llm_model": "gpt-4",
        "llm_temperature": "0.7",
        "llm_vision": "true",
        "llm_timeout_s": "45.0",
        "llm_api_key": "raw_encrypted_or_plain"
    }
    
    async def get_setting_mock(key, default=None):
        return settings_dict.get(key)
        
    db.get_setting.side_effect = get_setting_mock
    
    # Also mock decrypt_value to return decrypted key
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("hermes.utils.decrypt_value", lambda v: f"decrypted_{v}")
        await settings.reconcile_with_db(db)
        
    assert settings.hermes_mode == "live"
    assert settings.hermes_ai_autonomy == "autonomous"
    assert settings.llm_provider == "openai"
    assert settings.llm_base_url == "https://api.openai.com/v1"
    assert settings.llm_model == "gpt-4"
    assert settings.llm_temperature == 0.7
    assert settings.llm_vision is True
    assert settings.llm_timeout_s == 45.0
    assert settings.llm_api_key == "decrypted_raw_encrypted_or_plain"
