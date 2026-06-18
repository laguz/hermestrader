"""
[Service-1: Hermes-Agent-Core] — daily-loss kill switch.

Split out of ``main.py`` so the agent entry point keeps only the run loop and
process wiring. ``main`` re-imports these names, so existing call-sites and
test monkeypatches (``hermes.service1_agent.main.X``) keep working unchanged.
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from .agent_settings import SETTING_PAUSED

log = logging.getLogger("hermes.agent.main")


def resolve_max_daily_loss(setting_value: Optional[str]) -> float:
    """Resolve the daily-loss limit (a positive dollar amount).

    Precedence: the stored ``max_daily_loss`` setting, then the
    ``HERMES_MAX_DAILY_LOSS`` env var. Returns 0.0 (disabled) when neither is
    set or the value can't be parsed. The sign is normalised to positive so a
    limit of "500" and "-500" both mean "halt at $500 of realized loss".
    """
    raw = setting_value
    if raw in (None, ""):
        raw = os.environ.get("HERMES_MAX_DAILY_LOSS", "")
    if raw in (None, ""):
        return 0.0
    try:
        return abs(float(raw))
    except (TypeError, ValueError):
        return 0.0


async def _open_position_pnl(broker) -> Optional[float]:
    """Best-effort unrealized P&L across all open positions, or None.

    Sourced from Tradier's account balances (``open_pl``), which is the live
    mark-to-market of every open position. Returns ``None`` (not 0.0) when no
    broker is available or the read fails, so the caller can tell "flat" apart
    from "unknown" and avoid relaxing the kill switch on a transient error.
    """
    if broker is None:
        return None
    try:
        balances = await broker.get_account_balances() or {}
    except Exception as exc:                                      # noqa: BLE001
        log.warning("daily-loss check: get_account_balances failed: %s", exc)
        return None
    raw = balances.get("raw") or {}
    val = raw.get("open_pl", balances.get("open_pl"))
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


async def enforce_daily_loss_limit(
    db, max_daily_loss: float, *, currently_paused: bool, broker=None
) -> bool:
    """Auto-pause the agent when the day's drawdown breaches the limit.

    The limit is compared against realized P&L for trades closed today **plus**
    the unrealized mark-to-market of open positions (``open_pl`` from the
    broker). Including the open leg closes the gap where a book of losing
    spreads could bleed well past the limit without ever realizing a loss. When
    the broker's open P&L can't be read the check degrades to realized-only
    rather than failing open.

    Returns ``True`` if the limit was hit and the agent was paused on this
    call (caller should skip the rest of the tick). No-op returning ``False``
    when the switch is disabled, the agent is already paused, the P&L read
    fails, or the day is still within the limit. Reuses the ``agent_paused``
    flag so the halt is visible in the dashboard and persists until an
    operator manually re-arms.
    """
    if currently_paused or max_daily_loss <= 0.0:
        return False
    try:
        realized_today = await db.realized_pnl_today()
    except Exception as exc:                                      # noqa: BLE001
        log.warning("daily-loss check: realized_pnl_today failed: %s", exc)
        return False
    unrealized = await _open_position_pnl(broker)
    total_pnl = realized_today + (unrealized or 0.0)
    if total_pnl <= -max_daily_loss:
        await db.set_setting(SETTING_PAUSED, "true")
        unreal_str = "n/a" if unrealized is None else f"${unrealized:,.2f}"
        msg = (
            f"[KILL SWITCH] daily loss limit hit: total P&L "
            f"${total_pnl:,.2f} (realized ${realized_today:,.2f} + "
            f"unrealized {unreal_str}) <= -${max_daily_loss:,.2f} — "
            f"agent auto-paused for the session; operator must resume"
        )
        log.error(msg)
        await db.write_log("ENGINE", msg, level="ERROR")
        return True
    return False
