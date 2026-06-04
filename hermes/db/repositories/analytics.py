"""Daily P&L rollups and per-strategy performance scoring."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List

from sqlalchemy import select

from hermes.db.orm import Trade, _compute_realized_pnl


class AnalyticsRepositoryMixin:
    async def pnl_daily(self, days: int = 60) -> List[Dict[str, Any]]:
        sql = """
          SELECT day::date, strategy_id, symbol, COALESCE(realized_pnl,0) AS realized_pnl,
                 COALESCE(closed_trades,0) AS closed_trades
          FROM pnl_daily
          WHERE day >= now() - (%s || ' days')::interval
          ORDER BY day
        """
        async with self.async_engine.connect() as conn:
            result = await conn.exec_driver_sql(sql, (days,))
            return [dict(r._mapping) for r in result.fetchall()]

    async def realized_pnl_today(self) -> float:
        """Sum of realized P&L for trades CLOSED today (US/Eastern trading day).

        Used by the daily-loss kill switch. Returns a negative number on a
        losing day. ``trades.pnl`` is ``NUMERIC(12,2)`` so the sum is exact to
        the cent; we convert to ``float`` only at this read boundary.
        """
        sql = """
          SELECT COALESCE(SUM(pnl), 0) AS realized
          FROM trades
          WHERE status = 'CLOSED'
            AND closed_at IS NOT NULL
            AND (closed_at AT TIME ZONE 'America/New_York')::date
                = (now() AT TIME ZONE 'America/New_York')::date
        """
        async with self.async_engine.connect() as conn:
            result = await conn.exec_driver_sql(sql)
            row = result.fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0

    async def get_strategy_performance_metrics(self, days: int = 30) -> Dict[str, Any]:
        """Calculate recent trading performance (PASS/FAIL/NEUTRAL) for each strategy."""
        cutoff = datetime.utcnow() - timedelta(days=days)
        async with self.AsyncSession() as s:
            result = await s.execute(
                select(Trade)
                .filter(Trade.status == "CLOSED", Trade.closed_at >= cutoff)
            )
            closed_trades = result.scalars().all()

        metrics = {
            "CS7": {"closed_trades": 0, "passed": 0, "failed": 0, "total_pnl": 0.0, "details": []},
            "CS75": {"closed_trades": 0, "passed": 0, "failed": 0, "total_pnl": 0.0, "details": []},
            "TT45": {"closed_trades": 0, "passed": 0, "failed": 0, "total_pnl": 0.0, "details": []},
            "WHEEL": {"closed_trades": 0, "passed": 0, "failed": 0, "total_pnl": 0.0, "details": []}
        }

        # 1. Process option spreads: CS7, CS75, TT45
        spread_trades = [t for t in closed_trades if t.strategy_id in ("CS7", "CS75", "TT45")]
        for t in spread_trades:
            strat = t.strategy_id
            pnl_val = float(t.pnl) if t.pnl is not None else None
            if pnl_val is None:
                pnl_val = _compute_realized_pnl(
                    entry_credit=t.entry_credit,
                    entry_debit=t.entry_debit,
                    exit_price=t.exit_price or 0.0,
                    lots=int(t.lots or 0)
                )
            if pnl_val is None:
                continue

            metrics[strat]["total_pnl"] += pnl_val
            metrics[strat]["closed_trades"] += 1

            width_val = float(t.width) if t.width is not None else None
            if width_val is None and t.short_strike is not None and t.long_strike is not None:
                width_val = abs(float(t.short_strike) - float(t.long_strike))

            entry_credit_val = float(t.entry_credit) if t.entry_credit is not None else 0.0
            entry_debit_val = float(t.entry_debit) if t.entry_debit is not None else 0.0
            lots_val = int(t.lots or 1)

            if entry_credit_val > 0 and width_val is not None:
                risk_capital = (width_val - entry_credit_val) * lots_val * 100.0
            elif entry_debit_val > 0:
                risk_capital = entry_debit_val * lots_val * 100.0
            elif width_val is not None:
                risk_capital = width_val * lots_val * 100.0
            else:
                risk_capital = 1.0

            if risk_capital <= 0:
                risk_capital = width_val * lots_val * 100.0 if width_val else 1.0

            return_pct = pnl_val / risk_capital

            outcome = "NEUTRAL"
            if strat == "CS7":
                if return_pct < 0.05:
                    outcome = "FAIL"
                    metrics[strat]["failed"] += 1
                elif return_pct >= 0.10:
                    outcome = "PASS"
                    metrics[strat]["passed"] += 1
            elif strat == "CS75":
                if return_pct <= 0.07:
                    outcome = "FAIL"
                    metrics[strat]["failed"] += 1
                elif return_pct >= 0.22:
                    outcome = "PASS"
                    metrics[strat]["passed"] += 1
            elif strat == "TT45":
                if return_pct <= 0.03:
                    outcome = "FAIL"
                    metrics[strat]["failed"] += 1
                elif return_pct >= 0.05:
                    outcome = "PASS"
                    metrics[strat]["passed"] += 1

            metrics[strat]["details"].append({
                "symbol": t.symbol,
                "closed_at": t.closed_at.isoformat() if t.closed_at else None,
                "pnl": pnl_val,
                "risk_capital": risk_capital,
                "return_pct": return_pct,
                "outcome": outcome
            })

        # 2. Process WHEEL turns by symbol
        wheel_closed = [t for t in closed_trades if t.strategy_id == "WHEEL"]
        if wheel_closed:
            from collections import defaultdict
            symbol_trades = defaultdict(list)
            for t in wheel_closed:
                symbol_trades[t.symbol].append(t)

            for symbol, trades in symbol_trades.items():
                option_pnl_sum = 0.0
                net_shares = 0
                stock_cash_flow = 0.0
                current_spot = await self.last_price(symbol)

                for t in trades:
                    pnl_val = float(t.pnl) if t.pnl is not None else None
                    if pnl_val is None:
                        pnl_val = _compute_realized_pnl(
                            entry_credit=t.entry_credit,
                            entry_debit=t.entry_debit,
                            exit_price=t.exit_price or 0.0,
                            lots=int(t.lots or 0)
                        )
                    if pnl_val is None and t.entry_credit is not None:
                        pnl_val = float(t.entry_credit) * int(t.lots or 1) * 100.0
                    if pnl_val is not None:
                        option_pnl_sum += pnl_val

                    if t.side_type == "put" and (t.close_reason == "RECONCILED_BROKER_FLAT" or (t.closed_at and t.expiry and t.closed_at.date() >= t.expiry)):
                        expiry_price = await self.get_price_on_date(t.symbol, t.expiry)
                        if expiry_price is not None and expiry_price < float(t.short_strike or 0.0):
                            shares_bought = int(t.lots or 1) * 100
                            cost = float(t.short_strike) * shares_bought
                            net_shares += shares_bought
                            stock_cash_flow -= cost

                    elif t.side_type == "call" and (t.close_reason == "RECONCILED_BROKER_FLAT" or (t.closed_at and t.expiry and t.closed_at.date() >= t.expiry)):
                        expiry_price = await self.get_price_on_date(t.symbol, t.expiry)
                        if expiry_price is not None and expiry_price > float(t.short_strike or 0.0):
                            shares_sold = int(t.lots or 1) * 100
                            proceeds = float(t.short_strike) * shares_sold
                            net_shares -= shares_sold
                            stock_cash_flow += proceeds

                if net_shares > 0 and current_spot is not None:
                    stock_value = current_spot * net_shares
                    total_turn_pnl = option_pnl_sum + stock_cash_flow + stock_value
                else:
                    total_turn_pnl = option_pnl_sum + stock_cash_flow

                outcome = "PASS" if total_turn_pnl > 0.0 else ("FAIL" if total_turn_pnl < 0.0 else "NEUTRAL")

                if outcome == "PASS":
                    metrics["WHEEL"]["passed"] += 1
                elif outcome == "FAIL":
                    metrics["WHEEL"]["failed"] += 1

                metrics["WHEEL"]["closed_trades"] += len(trades)
                metrics["WHEEL"]["total_pnl"] += total_turn_pnl
                metrics["WHEEL"]["details"].append({
                    "symbol": symbol,
                    "option_pnl": option_pnl_sum,
                    "stock_cash_flow": stock_cash_flow,
                    "net_shares": net_shares,
                    "current_spot": current_spot,
                    "total_pnl": total_turn_pnl,
                    "outcome": outcome
                })

        for strat in ("CS7", "CS75", "TT45", "WHEEL"):
            m = metrics[strat]
            if m["closed_trades"] == 0 and strat != "WHEEL":
                m["status"] = "NEUTRAL"
            elif strat == "WHEEL" and len(m["details"]) == 0:
                m["status"] = "NEUTRAL"
            else:
                if m["failed"] > m["passed"]:
                    m["status"] = "FAIL"
                elif m["passed"] > m["failed"]:
                    m["status"] = "PASS"
                else:
                    m["status"] = "NEUTRAL"

        return metrics
