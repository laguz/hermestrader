import sys
import os
import json
import pytest
from unittest.mock import MagicMock, patch

# Manually mock mcp and other missing dependencies before importing server
def tool_decorator(*args, **kwargs):
    def decorator(f):
        return f
    return decorator

mcp_instance = MagicMock()
mcp_instance.tool = tool_decorator

class FastMCP:
    def __new__(cls, *args, **kwargs):
        return mcp_instance

try:
    import mcp
except ImportError:
    sys.modules["mcp"] = MagicMock()

try:
    import mcp.server
except ImportError:
    sys.modules["mcp.server"] = MagicMock()

try:
    import mcp.server.fastmcp
except ImportError:
    sys.modules["mcp.server.fastmcp"] = MagicMock()

if isinstance(sys.modules.get("mcp.server.fastmcp"), MagicMock):
    sys.modules["mcp.server.fastmcp"].FastMCP = FastMCP

from hermes.utils import sync_soul_file_to_db, check_for_updates
from hermes.mcp import server

def test_sync_soul_file_to_db(tmp_path, monkeypatch):
    # Setup a mock database
    mock_db = MagicMock()
    mock_db.get_setting.return_value = "old soul content"
    
    # Setup temp soul.md file
    soul_file = tmp_path / "soul.md"
    soul_file.write_text("new soul content", encoding="utf-8")
    
    monkeypatch.setenv("HERMES_SOUL_PATH", str(soul_file))
    
    sync_soul_file_to_db(mock_db)
    
    mock_db.get_setting.assert_called_with("soul_md")
    mock_db.set_setting.assert_called_with("soul_md", "new soul content")
    mock_db.write_log.assert_called_once()

def test_sync_soul_file_to_db_no_change(tmp_path, monkeypatch):
    mock_db = MagicMock()
    mock_db.get_setting.return_value = "same content"
    
    soul_file = tmp_path / "soul.md"
    soul_file.write_text("same content", encoding="utf-8")
    
    monkeypatch.setenv("HERMES_SOUL_PATH", str(soul_file))
    
    sync_soul_file_to_db(mock_db)
    
    mock_db.set_setting.assert_not_called()

@patch("requests.get")
@patch("hermes.db.models.HermesDB")
def test_check_for_updates_happy_path(mock_db_class, mock_get, monkeypatch):
    mock_db = MagicMock()
    mock_db_class.return_value = mock_db
    
    # Mock VERSION response
    mock_res_ver = MagicMock()
    mock_res_ver.status_code = 200
    mock_res_ver.text = "1.2.3"
    
    # Mock commit response
    mock_res_commit = MagicMock()
    mock_res_commit.status_code = 200
    mock_res_commit.json.return_value = {
        "sha": "abcdef1234567890",
        "commit": {"message": "feat: test message\nwith newlines"}
    }
    
    def side_effect(url, *args, **kwargs):
        if "VERSION" in url:
            return mock_res_ver
        if "commits" in url:
            return mock_res_commit
        return MagicMock(status_code=404)
        
    mock_get.side_effect = side_effect
    
    monkeypatch.setenv("HERMES_MODE", "paper")
    
    with patch("hermes.service2_watcher._app_state.read_version", return_value="1.0.0"):
        check_for_updates()
        
    mock_db.set_setting.assert_called_once()
    args, kwargs = mock_db.set_setting.call_args
    assert args[0] == "update_status"
    payload = json.loads(args[1])
    assert payload["update_available"] is True
    assert payload["remote_version"] == "1.2.3"
    assert payload["latest_commit_sha"] == "abcdef12"
    assert payload["latest_commit_msg"] == "feat: test message"

def test_mcp_server_load_env_file_custom_path(tmp_path, monkeypatch):
    monkeypatch.delenv("TRADIER_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("TRADIER_API_KEY", raising=False)
    monkeypatch.delenv("TRADIER_ACCOUNT_ID", raising=False)
    
    custom_env = tmp_path / ".env.custom"
    custom_env.write_text("TRADIER_API_KEY=custom-key\nTRADIER_ACCOUNT_ID=custom-acct\n")
    
    monkeypatch.setenv("HERMES_ENV_FILE", str(custom_env))
    
    server.load_env_file()
    
    assert os.environ.get("TRADIER_API_KEY") == "custom-key"
    assert os.environ.get("TRADIER_ACCOUNT_ID") == "custom-acct"

def test_mcp_server_broker_mode_aware_paper(monkeypatch):
    monkeypatch.setenv("HERMES_MODE", "paper")
    monkeypatch.setenv("TRADIER_PAPER_TOKEN", "paper-tok")
    monkeypatch.setenv("TRADIER_PAPER_ACCOUNT_ID", "paper-acct")
    monkeypatch.delenv("TRADIER_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("TRADIER_API_KEY", raising=False)
    
    import hermes.config
    hermes.config.settings = hermes.config.HermesSettings()
    
    # We mock AsyncTradierBroker so we don't hit requests inside its init
    with patch("hermes.mcp.server.AsyncTradierBroker") as mock_broker_class:
        # Reset global _BROKERS dict to force instantiation
        if hasattr(server, "_BROKERS"):
            server._BROKERS.clear()
            
        import asyncio
        asyncio.run(server._broker())
        
        mock_broker_class.assert_called_once()
        cfg = mock_broker_class.call_args[0][0]
        assert cfg["tradier_access_token"] == "paper-tok"
        assert cfg["tradier_account_id"] == "paper-acct"
        assert cfg["tradier_base_url"] == "https://sandbox.tradier.com/v1"
        assert cfg["dry_run"] is False

def test_mcp_server_broker_mode_aware_live(monkeypatch):
    monkeypatch.setenv("HERMES_MODE", "live")
    monkeypatch.setenv("TRADIER_LIVE_TOKEN", "live-tok")
    monkeypatch.setenv("TRADIER_LIVE_ACCOUNT_ID", "live-acct")
    monkeypatch.setenv("HERMES_DRY_RUN", "true")
    monkeypatch.delenv("TRADIER_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("TRADIER_API_KEY", raising=False)
    
    import hermes.config
    hermes.config.settings = hermes.config.HermesSettings()
    
    with patch("hermes.mcp.server.AsyncTradierBroker") as mock_broker_class:
        if hasattr(server, "_BROKERS"):
            server._BROKERS.clear()
            
        import asyncio
        asyncio.run(server._broker())
        
        mock_broker_class.assert_called_once()
        cfg = mock_broker_class.call_args[0][0]
        assert cfg["tradier_access_token"] == "live-tok"
        assert cfg["tradier_account_id"] == "live-acct"
        assert cfg["tradier_base_url"] == "https://api.tradier.com/v1"
        assert cfg["dry_run"] is True

