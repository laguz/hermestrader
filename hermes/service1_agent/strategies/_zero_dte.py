"""Shared helpers for the 0 DTE strategies (DS0, DS02).

Both strategies anchor their day on the same primitives — ET wall-clock,
the session opening print, and a Wilder ATR over completed daily bars —
so those live here once. Extracted verbatim from ``ds0.py`` (2026-07-17)
when DS02 arrived; the DS0 test suite pins the behaviour.

This is a mixin over :class:`~..core.AbstractStrategy` subclasses: it only
touches ``self.broker`` and ``self.now()``, never strategy state.
"""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from hermes.market_hours import ET as _ET


def _parse_hhmm(raw: Any, default: time) -> time:
    try:
        return datetime.strptime(str(raw).strip(), "%H:%M").time()
    except (TypeError, ValueError):
        return default


class ZeroDTEMixin:
    # ── time helpers ─────────────────────────────────────────────────────────
    def _now_et(self) -> datetime:
        now = self.now()
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        return now.astimezone(_ET)

    # ── quote helpers ────────────────────────────────────────────────────────
    @staticmethod
    def _mid(opt: Optional[Dict[str, Any]]) -> Optional[float]:
        """Bid/ask midpoint, or ``None`` when either side is missing/zero."""
        if not opt:
            return None
        try:
            bid = float(opt.get("bid") or 0)
            ask = float(opt.get("ask") or 0)
        except (TypeError, ValueError):
            return None
        if bid <= 0 or ask <= 0:
            return None
        return (bid + ask) / 2.0

    async def _spot(self, symbol: str) -> Optional[float]:
        quotes = await self.broker.get_quote(symbol) or []
        if not quotes:
            return None
        q = quotes[0]
        raw = q.get("last") if q.get("last") is not None else q.get("close")
        try:
            spot = float(raw) if raw is not None else 0.0
        except (TypeError, ValueError):
            return None
        return spot if spot > 0 else None

    async def _today_open(self, symbol: str) -> Optional[float]:
        """Today's regular-session opening print, from the quote's ``open``.

        Fixed for the whole day — the open ± ATR range must not drift with
        spot intraday. ``None`` (pre-open, halted, stub without the field)
        means the symbol can't qualify and is skipped.
        """
        quotes = await self.broker.get_quote(symbol) or []
        if not quotes:
            return None
        try:
            raw = quotes[0].get("open")
            opn = float(raw) if raw is not None else 0.0
        except (TypeError, ValueError):
            return None
        return opn if opn > 0 else None

    async def _atr(self, symbol: str, period: int) -> Optional[float]:
        """Wilder ATR over the last ``period`` completed daily bars.

        True range includes overnight gaps (max of high−low, |high−prev
        close|, |low−prev close|); the seed is the simple mean of the first
        ``period`` TRs, then Wilder smoothing over the rest. Today's partial
        bar is excluded so the entry range stays anchored. ``None`` when
        history is too short/invalid — the symbol is skipped, never traded
        on a guessed range.
        """
        if period < 1:
            return None
        end = self._now_et().date()
        start = end - timedelta(days=period * 3 + 10)
        bars = await self.broker.get_history(
            symbol, start=start.isoformat(), end=end.isoformat()) or []
        rows: List[Tuple[str, float, float, float]] = []
        for b in bars:
            day = str(b.get("date", ""))[:10]
            if day >= end.isoformat():
                continue
            try:
                rows.append((day, float(b["high"]), float(b["low"]),
                             float(b["close"])))
            except (KeyError, TypeError, ValueError):
                continue
        rows.sort(key=lambda r: r[0])   # mock/history feeds vary in ordering
        if len(rows) < period + 1:
            return None
        trs: List[float] = []
        prev_close = rows[0][3]
        for _, high, low, close in rows[1:]:
            trs.append(max(high - low, abs(high - prev_close),
                           abs(low - prev_close)))
            prev_close = close
        atr = sum(trs[:period]) / period
        for tr in trs[period:]:
            atr = (atr * (period - 1) + tr) / period
        return atr if atr > 0 else None
