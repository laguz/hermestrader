"""Daily-loss kill switch — auto-pauses the agent when today's realized P&L
breaches the configured limit.

Covers the two testable units extracted from the tick loop:
  * ``resolve_max_daily_loss`` — setting > env > disabled, sign-normalised.
  * ``enforce_daily_loss_limit`` — the pause decision against a mocked DB.
"""
from __future__ import annotations
from ._stubs import alias_db_namespaces

from unittest.mock import AsyncMock
import pytest

from hermes.service1_agent.main import (
    resolve_max_daily_loss,
    enforce_daily_loss_limit,
    _live_armed,
)


# ── resolve_max_daily_loss ────────────────────────────────────────────────
def test_resolve_setting_takes_precedence(monkeypatch):
    monkeypatch.setenv("HERMES_MAX_DAILY_LOSS", "100")
    assert resolve_max_daily_loss("500") == 500.0


def test_resolve_falls_back_to_env(monkeypatch):
    monkeypatch.setenv("HERMES_MAX_DAILY_LOSS", "250")
    assert resolve_max_daily_loss(None) == 250.0
    assert resolve_max_daily_loss("") == 250.0


def test_resolve_disabled_when_unset(monkeypatch):
    monkeypatch.delenv("HERMES_MAX_DAILY_LOSS", raising=False)
    assert resolve_max_daily_loss(None) == 0.0


def test_resolve_normalises_sign_and_bad_input(monkeypatch):
    monkeypatch.delenv("HERMES_MAX_DAILY_LOSS", raising=False)
    assert resolve_max_daily_loss("-500") == 500.0   # sign-normalised
    assert resolve_max_daily_loss("abc") == 0.0      # unparseable → disabled


# ── enforce_daily_loss_limit ──────────────────────────────────────────────
def _db(realized):
    db = AsyncMock()
    alias_db_namespaces(db)
    db.realized_pnl_today.return_value = realized
    return db


@pytest.mark.anyio
async def test_disabled_is_noop():
    db = _db(-9999.0)
    assert await enforce_daily_loss_limit(db, 0.0, currently_paused=False) is False
    db.set_setting.assert_not_awaited()
    db.realized_pnl_today.assert_not_awaited()  # don't even query when disabled


@pytest.mark.anyio
async def test_already_paused_is_noop():
    db = _db(-9999.0)
    assert await enforce_daily_loss_limit(db, 500.0, currently_paused=True) is False
    db.set_setting.assert_not_awaited()


@pytest.mark.anyio
async def test_within_limit_does_not_pause():
    db = _db(-499.99)
    assert await enforce_daily_loss_limit(db, 500.0, currently_paused=False) is False
    db.set_setting.assert_not_awaited()


@pytest.mark.anyio
async def test_profit_does_not_pause():
    db = _db(1200.0)
    assert await enforce_daily_loss_limit(db, 500.0, currently_paused=False) is False
    db.set_setting.assert_not_awaited()


@pytest.mark.anyio
async def test_breach_pauses_and_logs():
    db = _db(-500.0)  # exactly at the limit → trips
    tripped = await enforce_daily_loss_limit(db, 500.0, currently_paused=False)
    assert tripped is True
    db.set_setting.assert_awaited_once_with("agent_paused", "true")
    db.write_log.assert_awaited_once()


@pytest.mark.anyio
async def test_pnl_read_failure_is_safe_noop():
    db = AsyncMock()
    alias_db_namespaces(db)
    db.realized_pnl_today.side_effect = RuntimeError("db down")
    # A failed P&L read must not pause (and must not raise) — fail open so a
    # transient DB hiccup doesn't halt trading, while real breaches still trip.
    assert await enforce_daily_loss_limit(db, 500.0, currently_paused=False) is False
    db.set_setting.assert_not_awaited()


# ── enforce_daily_loss_limit: unrealized (open-position) P&L ───────────────
def _broker(open_pl):
    """Broker stub whose balances report a given open (unrealized) P&L."""
    broker = AsyncMock()
    broker.get_account_balances.return_value = {"raw": {"open_pl": open_pl}}
    return broker


@pytest.mark.anyio
async def test_unrealized_loss_pushes_over_limit():
    # Realized alone (-300) is within the $500 limit, but open positions are
    # down another -250 → combined -550 breaches and must trip.
    db = _db(-300.0)
    broker = _broker(-250.0)
    tripped = await enforce_daily_loss_limit(
        db, 500.0, currently_paused=False, broker=broker
    )
    assert tripped is True
    db.set_setting.assert_awaited_once_with("agent_paused", "true")


@pytest.mark.anyio
async def test_unrealized_profit_offsets_realized_loss():
    # Realized -400 but open positions are up +200 → combined -200, within the
    # $500 limit, so it must not trip.
    db = _db(-400.0)
    broker = _broker(200.0)
    assert await enforce_daily_loss_limit(
        db, 500.0, currently_paused=False, broker=broker
    ) is False
    db.set_setting.assert_not_awaited()


@pytest.mark.anyio
async def test_broker_balance_failure_degrades_to_realized_only():
    # If open P&L can't be read, the check uses realized-only rather than
    # failing open. Realized -300 is within the limit → no trip.
    db = _db(-300.0)
    broker = AsyncMock()
    broker.get_account_balances.side_effect = RuntimeError("tradier 503")
    assert await enforce_daily_loss_limit(
        db, 500.0, currently_paused=False, broker=broker
    ) is False
    db.set_setting.assert_not_awaited()


# ── _live_armed (real-money arming latch) ─────────────────────────────────
@pytest.mark.parametrize("value", ["true", "True", "1", "yes", "on", " TRUE "])
def test_live_armed_truthy(monkeypatch, value):
    monkeypatch.setenv("HERMES_LIVE_ARMED", value)
    assert _live_armed() is True


@pytest.mark.parametrize("value", ["false", "0", "no", "", "off", "garbage"])
def test_live_armed_falsy(monkeypatch, value):
    monkeypatch.setenv("HERMES_LIVE_ARMED", value)
    assert _live_armed() is False


def test_live_armed_unset_is_false(monkeypatch):
    monkeypatch.delenv("HERMES_LIVE_ARMED", raising=False)
    assert _live_armed() is False


# ── _build_broker: arming actually controls real-order routing ────────────
def _prep_live_broker_env(monkeypatch):
    """Wire up just enough env/settings for _build_broker to build a real
    TradierBroker in live mode without touching the network."""
    from hermes.config import settings
    monkeypatch.setattr(settings, "hermes_use_mcp_broker", False, raising=False)
    monkeypatch.setenv("TRADIER_ACCESS_TOKEN", "test-token")
    monkeypatch.setenv("TRADIER_ACCOUNT_ID", "TEST123")


def test_build_broker_live_unarmed_forces_dry_run(monkeypatch):
    from hermes.service1_agent.main import _build_broker
    _prep_live_broker_env(monkeypatch)
    monkeypatch.delenv("HERMES_LIVE_ARMED", raising=False)
    broker = _build_broker({"dry_run": False}, "live")
    # Live but not armed → preview-only regardless of dry_run=False.
    assert broker.dry_run is True


def test_build_broker_live_armed_allows_real_orders(monkeypatch):
    from hermes.service1_agent.main import _build_broker
    _prep_live_broker_env(monkeypatch)
    monkeypatch.setenv("HERMES_LIVE_ARMED", "true")
    broker = _build_broker({"dry_run": False}, "live")
    assert broker.dry_run is False


# ── _build_broker: MCP-broker path honors the same mode-aware dry_run ──────
# Regression: the MCP-broker branch used to return MCPBrokerClient(conf)
# before the dry_run normalization, so paper mode inherited the config
# default (dry_run=True) and every approved trade was marked
# "dry_run=True — no broker order placed" and never reached the broker.
def test_build_broker_mcp_paper_routes_orders(monkeypatch):
    from hermes.service1_agent.main import _build_broker
    from hermes.config import settings
    monkeypatch.setattr(settings, "hermes_use_mcp_broker", True, raising=False)
    # conf carries the config default (dry_run=True); paper must override it.
    broker = _build_broker({"dry_run": True}, "paper")
    assert broker.dry_run is False


def test_build_broker_mcp_live_unarmed_forces_dry_run(monkeypatch):
    from hermes.service1_agent.main import _build_broker
    from hermes.config import settings
    monkeypatch.setattr(settings, "hermes_use_mcp_broker", True, raising=False)
    monkeypatch.delenv("HERMES_LIVE_ARMED", raising=False)
    broker = _build_broker({"dry_run": False}, "live")
    assert broker.dry_run is True


def test_build_broker_mcp_live_armed_allows_real_orders(monkeypatch):
    from hermes.service1_agent.main import _build_broker
    from hermes.config import settings
    monkeypatch.setattr(settings, "hermes_use_mcp_broker", True, raising=False)
    monkeypatch.setenv("HERMES_LIVE_ARMED", "true")
    broker = _build_broker({"dry_run": False}, "live")
    assert broker.dry_run is False
