#!/usr/bin/env python3
"""
[Agent Self-Learning Loop]
Retrospective closed-trade analyzer. Highlights wins/losses on historical stock charts,
submits them to the LLM (Hermes Agent) for chart analysis, and dynamically updates
the operator's doctrine (soul_md) with lessons learned.

Run via cron / docker-compose schedule, e.g.
    0 4 * * 6  /usr/local/bin/python -m scripts.self_learning_loop
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

logger = logging.getLogger("hermes.scripts.self_learning_loop")


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=14,
                        help="Lookback window for closed trades")
    parser.add_argument("--dsn", type=str, default=None,
                        help="Override HERMES_DSN")
    parser.add_argument("--dry-run", action="store_true",
                        help="Perform analysis but do not save to database")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    try:
        from sqlalchemy import select
        from hermes.db.models import HermesDB, Trade
        from hermes.charts.provider import render_chart
        from hermes.service1_agent.main import _build_llm
    except Exception as exc:                                       # noqa: BLE001
        logger.exception("import failed: %s", exc)
        return 1

    dsn = args.dsn or os.environ.get(
        "HERMES_DSN",
        "postgresql+psycopg://hermes:hermes@localhost:5432/hermes",
    )
    db = HermesDB(dsn)

    # 1. Fetch closed trades
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    async with db.AsyncSession() as session:
        q = (
            select(Trade)
            .filter(Trade.status == "CLOSED", Trade.closed_at >= cutoff)
            .order_by(Trade.closed_at.asc())
        )
        result = await session.execute(q)
        closed_trades = result.scalars().all()
        if not closed_trades:
            logger.info("No closed trades found in the last %d days; nothing to analyze.", args.days)
            return 0

        # Group trades by symbol
        grouped_trades: Dict[str, List[Dict[str, Any]]] = {}
        for t in closed_trades:
            grouped_trades.setdefault(t.symbol.upper(), []).append({
                "strategy_id": t.strategy_id,
                "opened_at": t.opened_at,
                "closed_at": t.closed_at,
                "side_type": t.side_type,
                "short_strike": float(t.short_strike) if t.short_strike else 0.0,
                "long_strike": float(t.long_strike) if t.long_strike else 0.0,
                "entry_credit": float(t.entry_credit) if t.entry_credit else 0.0,
                "exit_price": float(t.exit_price) if t.exit_price is not None else 0.0,
                "pnl": float(t.pnl) if t.pnl is not None else 0.0,
                "close_reason": t.close_reason or "UNKNOWN",
            })

    # 2. Build the LLM client
    llm_client, llm_snap, vision_enabled = await _build_llm(db)
    if llm_snap.get("provider") == "mock":
        logger.warning("LLM client is mocked; self-learning analyses will be synthetic.")

    analyses: List[str] = []

    # 3. For each symbol, render the chart and run the LLM retrospective
    for symbol, trades in grouped_trades.items():
        # Load daily bars covering the trade period
        min_date = min(t["opened_at"] for t in trades) - timedelta(days=20)
        max_date = max(t["closed_at"] for t in trades) + timedelta(days=5)
        
        df_bars = await db.daily_bars(symbol, lookback_days=args.days + 60)
        if df_bars is None or df_bars.empty:
            logger.warning("No daily bar data found for %s; skipping visual analysis", symbol)
            continue
            
        # Filter to the trade window
        df_window = df_bars.loc[(df_bars.index >= min_date) & (df_bars.index <= max_date)]
        if len(df_window) < 5:
            df_window = df_bars.tail(60)

        # Render chart PNG
        try:
            chart_png = render_chart(df_window, symbol, lookback=len(df_window))
        except Exception as chart_exc:
            logger.warning("Failed to render chart for %s: %s", symbol, chart_exc)
            chart_png = None

        # Build prompt
        prompt = (
            f"You are the self-learning retrospective engine of HERMES.\n"
            f"Analyze recently closed options trades for {symbol} to refine our trading doctrine.\n"
            f"Here is the daily candlestick chart for {symbol} covering the trade period (SMA20/50, Bollinger Bands, RSI).\n\n"
            f"Closed Trades for {symbol}:\n"
        )
        for t in trades:
            prompt += (
                f"- Strategy: {t['strategy_id']} | Side: {t['side_type']} | "
                f"Strikes: Short {t['short_strike']:.2f}, Long {t['long_strike']:.2f} | "
                f"Entry: {t['opened_at'].strftime('%Y-%m-%d')} for ${t['entry_credit']:.2f} | "
                f"Exit: {t['closed_at'].strftime('%Y-%m-%d')} at ${t['exit_price']:.2f} | "
                f"PnL: ${t['pnl']:.2f} | Reason: {t['close_reason']}\n"
            )

        prompt += (
            "\nTasks:\n"
            "1. Examine the candlestick chart, support/resistance levels, and RSI relative to these trade entries/exits.\n"
            "2. Provide a brief analysis of why the winning trades succeeded and why the losing trades failed.\n"
            "3. Write 2-3 concrete, actionable rules to improve strike selection and timing for this symbol (e.g. 'Do not enter a CS7 Call Spread on AAPL if the chart outlook is BULLISH and RSI is above 65').\n\n"
            "Output format:\n"
            f"### Lessons for {symbol}\n"
            "- [Actionable Rule / Lesson 1]\n"
            "- [Actionable Rule / Lesson 2]\n"
        )

        try:
            logger.info("Submitting %s trade history to LLM...", symbol)
            images = [chart_png] if (chart_png and vision_enabled) else []
            msg = llm_client.chat(
                [{"role": "system", "content": "You are a senior trading retrospective analyst. Output markdown list of rules only."},
                 {"role": "user",   "content": prompt}],
                images=images,
            )
            analyses.append(msg.strip())
            logger.info("Successfully analyzed %s", symbol)
        except Exception as exc:                                   # noqa: BLE001
            logger.warning("LLM analysis failed for %s: %s", symbol, exc)

    if not analyses:
        logger.info("No analyses produced.")
        return 0

    # 4. Update the soul doctrine
    header = "# Auto-Generated Lessons Learned"
    timestamp_line = f"*(Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} via Self-Learning Loop)*"
    new_section = f"{header}\n{timestamp_line}\n\n" + "\n\n".join(analyses)

    try:
        current_soul = (await db.get_setting("soul_md")) or ""
        if header in current_soul:
            parts = current_soul.split(header, 1)
            updated_soul = parts[0].rstrip() + "\n\n" + new_section
        else:
            updated_soul = current_soul.rstrip() + "\n\n" + new_section

        if args.dry_run:
            logger.info("--- DRY RUN: PROPOSED SOUL_MD UPDATE ---")
            logger.info(updated_soul)
            logger.info("---------------------------------------")
        else:
            await db.set_setting("soul_md", updated_soul)
            await db.write_log("ENGINE", f"[SELF-LEARNING] Updated soul_md with lessons learned for {', '.join(grouped_trades.keys())}")
            logger.info("Successfully updated operator doctrine (soul_md) in system_settings.")
    except Exception as exc:                                       # noqa: BLE001
        logger.exception("Failed to update soul_md: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
