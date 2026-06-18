"""CreditSpreads7 — priority-2 strategy (short-cycle spreads).

Entry contract
--------------
- Mode A (no incomplete IC on this symbol):
    * Find the exact 7 DTE expiry; otherwise skip.
    * Open both put and call spreads (Iron Condor).
- Mode B (already one side open):
    * Reuse the existing expiry only if 4–7 DTE remain.
    * Open the missing side only.

Selection
---------
Same key-level + POP heuristic as CS75, but with a slightly looser delta
window (``0.05 ≤ |Δ| ≤ 0.45``) — short-cycle gamma decay accelerates
fast, so a touch more delta is acceptable in exchange for more premium.

Width is configurable (``cs7_width``, default 1). Required net credit is a
fixed 12% of width — short-cycle entries get less expansion margin so the
threshold is lower than CS75's 25%/20%.

Management contract
-------------------
- TP @ debit ≤ 2% of width (very tight take-profit; collect quickly)
- SL @ debit ≥ 3× entry credit

The entry/management *machinery* is shared with CS75 in
:class:`~hermes.service1_agent.strategies._credit_spread_base.CreditSpreadStrategy`;
this file declares only what makes CS7 a ~7 DTE short-cycle spread.
"""
from __future__ import annotations

from typing import Optional, Tuple

from ._credit_spread_base import CreditSpreadStrategy


class CreditSpreads7(CreditSpreadStrategy):
    PRIORITY = 2
    NAME = "CS7"

    KEY_PREFIX = "cs7_"
    ANALYSIS_PERIOD = "3m"            # short-cycle entries → shorter lookback
    RESCALE_CREDIT_TO_WIDTH = False   # flat min-credit, no width re-scale
    MANAGE_NEEDS_DTE = False          # no time-based exit; pure TP/SL

    def _dte_summary(self, t) -> str:
        return f"{t.cs7_dte}"

    async def _resolve_entry_expiry(self, symbol: str, t) -> Optional[str]:
        # New entry: target the configured DTE (default exact 7).
        expiry = await self.find_expiry_in_dte_range(symbol, t.cs7_dte, t.cs7_dte)
        if not expiry:
            self._log(f"ℹ️ {symbol}: no exact {t.cs7_dte} DTE expiry found for new entry; skip.")
        return expiry

    def _completion_window(self, t) -> Tuple[int, int]:
        # Complete only within the lower half of the DTE window.
        completion_min = max(1, t.cs7_dte - t.cs7_completion_window)
        return completion_min, t.cs7_dte

    def _min_credit(self, dte: int, width: float, t) -> float:
        return round(width * t.cs7_min_credit_pct, 2)

    def _close_reason(self, trade, dte, debit, entry_credit, width, t) -> Optional[str]:
        """TP @ debit ≤ 2% of width; SL @ debit ≥ 3× entry credit."""
        if debit <= width * t.cs7_tp_pct_width:
            return "TP-2pctW"
        if debit >= entry_credit * t.cs7_sl_mult:
            # Stop-loss width safety cap: don't close if already at/above max loss.
            if debit < width:
                return "SL-3x"
            self._log(
                f"ℹ️ {trade['symbol']} {trade.get('side_type')}: debit ${debit:.2f} "
                f"is at/above width ${width:.2f} (max loss); skipping SL close."
            )
            return None
        return None
