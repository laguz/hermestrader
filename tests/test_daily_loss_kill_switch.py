"""Daily-loss kill switch — auto-pauses the agent when today's realized P&L
breaches the configured limit.

Covers the two testable units extracted from the tick loop:
  * ``resolve_max_daily_loss`` — setting > env > disabled, sign-normalised.
  * ``enforce_daily_loss_limit`` — the pause decision against a mocked DB.
"""
from __future__ import annotations

from unittest.mock import AsyncMock
import pytest

from hermes.service1_agent.main import (
    resolve_max_daily_loss,
    enforce_daily_loss_limit,
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
    db.realized_pnl_today.side_effect = RuntimeError("db down")
    # A failed P&L read must not pause (and must not raise) — fail open so a
    # transient DB hiccup doesn't halt trading, while real breaches still trip.
    assert await enforce_daily_loss_limit(db, 500.0, currently_paused=False) is False
    db.set_setting.assert_not_awaited()
