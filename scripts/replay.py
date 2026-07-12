#!/usr/bin/env python3
"""Historical replay (backtest) of the HermesTrader engine.

Replays the real CascadingEngine + strategies over historical bars already
stored in TimescaleDB (read-only) or in local CSV files, with fills simulated
by ReplayBroker and all state kept in memory. Never talks to Tradier, never
writes to any database.

Examples:
    python scripts/replay.py --symbols SPY,QQQ --start 2026-04-01 --end 2026-06-30
    python scripts/replay.py --symbols QQQ --start 2026-06-01 --end 2026-06-30 \\
        --strategies CS75,CS7 --dsn postgresql+psycopg://hermes:hermes@localhost:5432/hermes
    python scripts/replay.py --symbols SPY --start 2026-06-01 --end 2026-06-15 \\
        --csv-dir ./bars --json out.json
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hermes.replay import (          # noqa: E402
    ReplayConfig, ReplayDataSource, ReplayHarness, render_report,
)

log = logging.getLogger("hermes.replay.cli")


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _load_csv_dir(csv_dir: str, symbols: list[str]) -> ReplayDataSource:
    """Per-symbol CSVs (<SYM>.csv with date,open,high,low,close,volume)."""
    import pandas as pd
    daily = {}
    for sym in symbols:
        path = Path(csv_dir) / f"{sym.upper()}.csv"
        if not path.exists():
            raise FileNotFoundError(f"no bar file for {sym}: {path}")
        daily[sym.upper()] = pd.read_csv(path)
    return ReplayDataSource.from_frames(daily)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--symbols", required=True,
                    help="comma-separated underlyings, e.g. SPY,QQQ")
    ap.add_argument("--start", required=True, type=_parse_date,
                    help="replay start date (YYYY-MM-DD)")
    ap.add_argument("--end", required=True, type=_parse_date,
                    help="replay end date (YYYY-MM-DD), inclusive")
    ap.add_argument("--strategies", default=None,
                    help="comma-separated strategy filter "
                         "(CS75,CS7,TT45,WHEEL,HERMESALPHA,DS0); default all")
    ap.add_argument("--dsn", default=os.environ.get(
        "HERMES_DSN", "postgresql+psycopg://hermes:hermes@localhost:5432/hermes"),
        help="source DSN for bars_daily/bars_intraday (read-only SELECTs)")
    ap.add_argument("--csv-dir", default=None,
                    help="load bars from <SYM>.csv files instead of the DB")
    ap.add_argument("--ticks", default="10:35,13:00,15:05",
                    help="comma-separated ET tick times per trading day")
    ap.add_argument("--starting-bp", type=float, default=100_000.0)
    ap.add_argument("--slippage", type=float, default=0.0,
                    help="fill slippage as a fraction of the bid-ask spread")
    ap.add_argument("--lookback-days", type=int, default=300,
                    help="bar warm-up window loaded before --start")
    ap.add_argument("--json", default=None,
                    help="also write the full result (report + trades) to this file")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(name)s %(levelname)s %(message)s")

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    strategies = ([s.strip().upper() for s in args.strategies.split(",") if s.strip()]
                  if args.strategies else None)
    if args.end < args.start:
        ap.error("--end must be on or after --start")

    if args.csv_dir:
        data = _load_csv_dir(args.csv_dir, symbols)
    else:
        log.info("loading bars from %s (read-only)", args.dsn)
        data = ReplayDataSource.from_db(args.dsn, symbols, args.start, args.end,
                                        lookback_days=args.lookback_days)

    cfg = ReplayConfig(
        symbols=symbols, start=args.start, end=args.end, strategies=strategies,
        tick_times_et=[t.strip() for t in args.ticks.split(",") if t.strip()],
        starting_bp=args.starting_bp, slippage_frac=args.slippage,
    )
    harness = ReplayHarness(data, cfg)
    result = asyncio.run(harness.run())

    print(f"\nreplayed {result.ticks} tick(s), "
          f"{len(result.trades)} trade(s), {len(result.fills)} fill(s), "
          f"{len(result.settlements)} settlement(s)\n")
    print(render_report(result.report))

    if args.json:
        def _default(o):
            if isinstance(o, (date, datetime)):
                return o.isoformat()
            return str(o)
        payload = {
            "config": {"symbols": symbols, "start": str(args.start),
                       "end": str(args.end), "strategies": strategies,
                       "ticks": cfg.tick_times_et},
            "report": result.report,
            "trades": result.trades,
            "equity_curve": result.equity_curve,
            "settlements": result.settlements,
        }
        Path(args.json).write_text(json.dumps(payload, indent=2, default=_default))
        print(f"\nfull result written to {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
