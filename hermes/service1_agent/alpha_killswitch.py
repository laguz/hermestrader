"""[Service-1: Hermes-Agent-Core] — HermesAlpha weekly kill switch.

Autonomous HermesAlpha is the riskiest path in the system (LLM-originated, no
human in the loop). This guard disables it the moment its trailing 7-day
performance breaches any operator-set bound, mirroring the daily-loss kill
switch in ``agent_risk``: persist the disable through the event-sourced
``set_setting`` path (single-writer-clean) and flip ``control_state`` in-memory
so it takes effect this tick. Re-enabling is an explicit operator action.

Trip conditions (any one fires), per the operator's spec:
  * loss rate >= 60% by closed-trade count (min sample so one early loss can't trip it)
  * realized loss >= 2% of account equity
  * underperforms CS75 over the window while itself negative
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

log = logging.getLogger("hermes.agent.alpha_killswitch")

ALPHA = "HERMESALPHA"
CS75 = "CS75"
ALPHA_ENABLED_KEY = "strategy_hermesalpha_enabled"


def evaluate(
    alpha: Dict[str, Any],
    cs75: Dict[str, Any],
    equity: Optional[float],
    *,
    loss_rate_threshold: float = 0.60,
    min_sample: int = 5,
    capital_loss_pct: float = 0.02,
) -> Optional[str]:
    """Return a human-readable trip reason, or ``None`` to keep Alpha enabled.

    Pure function (no I/O) so the trip logic is unit-testable in isolation.
    ``equity`` may be ``None`` (broker unreadable) — only the capital-loss test
    is skipped then; the other two still apply.
    """
    closed = int(alpha.get("closed", 0) or 0)
    losers = int(alpha.get("losers", 0) or 0)
    pnl = float(alpha.get("realized_pnl", 0.0) or 0.0)

    # (a) loss rate — needs a minimum sample so a single early loser is ignored.
    if closed >= min_sample:
        loss_rate = losers / closed
        if loss_rate >= loss_rate_threshold:
            return f"loss rate {loss_rate:.0%} ({losers}/{closed}) >= {loss_rate_threshold:.0%}"

    # (b) realized loss as a fraction of account equity.
    if equity and equity > 0 and pnl <= -abs(capital_loss_pct) * equity:
        return (f"realized loss ${-pnl:,.2f} >= {capital_loss_pct:.0%} of "
                f"equity ${equity:,.2f}")

    # (c) underperforms CS75 over the window while itself negative.
    cs75_pnl = float(cs75.get("realized_pnl", 0.0) or 0.0)
    if pnl < 0 and pnl < cs75_pnl:
        return f"underperforms CS75 (${pnl:,.2f} < ${cs75_pnl:,.2f}) while negative"
    return None


async def _account_equity(broker) -> Optional[float]:
    """Best-effort account equity, or ``None`` when the broker read fails."""
    if broker is None:
        return None
    try:
        balances = await broker.get_account_balances() or {}
    except Exception as exc:
        log.warning("alpha kill switch: get_account_balances failed: %s", exc)
        return None
    val = balances.get("total_equity")
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


async def enforce_alpha_killswitch(db, broker, control_state, config) -> bool:
    """Disable HermesAlpha if its trailing-week performance breaches a bound.

    No-op (returns ``False``) when Alpha is already disabled or has no resolved
    trades in the window. Returns ``True`` if it tripped and disabled Alpha on
    this call (the caller need not act further — the toggle is persisted).
    """
    if control_state is None:
        return False
    # Skip if already disabled — re-enable is an explicit operator action.
    if not control_state.strategy_enabled.get(ALPHA, True):
        return False

    cfg = config or {}
    days = int(cfg.get("alpha_killswitch_window_days", 7))
    try:
        stats = await db.analytics.strategy_window_stats(days=days)
    except Exception as exc:
        log.warning("alpha kill switch: window stats read failed: %s", exc)
        return False

    alpha = stats.get(ALPHA, {})
    if int(alpha.get("closed", 0) or 0) == 0:
        return False                                  # nothing resolved yet

    equity = await _account_equity(broker)
    reason = evaluate(
        alpha, stats.get(CS75, {}), equity,
        loss_rate_threshold=float(cfg.get("alpha_killswitch_loss_rate", 0.60)),
        min_sample=int(cfg.get("alpha_killswitch_min_sample", 5)),
        capital_loss_pct=float(cfg.get("alpha_killswitch_capital_pct", 0.02)),
    )
    if reason is None:
        return False

    # Trip: persist through the event-sourced path (single-writer) + flip live.
    await db.settings.set_setting(ALPHA_ENABLED_KEY, "false")
    control_state.strategy_enabled[ALPHA] = False
    msg = f"[ALPHA-KILLSWITCH] HermesAlpha disabled — {reason} (window={days}d)"
    log.warning(msg)
    try:
        await db.logs.write_log(ALPHA, msg)
    except Exception:
        log.warning("[ALPHA-KILLSWITCH] audit log write failed")
    return True
