#!/usr/bin/env python3
"""Run historical options replay backtests over predefined regime windows.

Supports testing strategies over specific historical regimes (such as the COVID crash,
the 2022 bear market, or the 2017 calm market) using synthetic pricing derived from
the Yang-Zhang daily IV proxy where real chains are unavailable.

Predefined Windows:
- covid_crash: 2020-02-01 to 2020-05-31
- bear_2022:   2022-01-01 to 2022-12-31
- calm_2017:   2017-01-01 to 2017-12-31
- all:         Runs over the entire range available in the corpus manifest

Examples:
    python scripts/replay_backtest.py --window bear_2022
    python scripts/replay_backtest.py --window covid_crash --strategy CS75
    python scripts/replay_backtest.py --window all --symbols SPY
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


import pandas as pd

# Ensure hermes is in path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from hermes.replay import (
    ReplayConfig, ReplayDataSource, ReplayHarness, render_report,
)

logger = logging.getLogger("hermes.replay.backtest_runner")

WINDOWS = {
    "covid_crash": (date(2020, 2, 1), date(2020, 5, 31)),
    "bear_2022": (date(2022, 1, 1), date(2022, 12, 31)),
    "calm_2017": (date(2017, 1, 1), date(2017, 12, 31)),
}


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def load_manifest(corpus_dir: Path) -> Dict[str, Any]:
    manifest_path = corpus_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Corpus manifest not found at {manifest_path}. "
            "Please run scripts/build_replay_corpus.py first to ingest data."
        )
    return json.loads(manifest_path.read_text())


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--window", required=True,
                    help="regime window: covid_crash, bear_2022, calm_2017, or all")
    ap.add_argument("--strategy", default=None,
                    help="strategy filter (CS75, CS7, TT45, WHEEL, HERMESALPHA, DS0); default: all")
    ap.add_argument("--symbols", default=None,
                    help="comma-separated symbol list; default: all from manifest")
    ap.add_argument("--corpus-dir", default="data/replay_corpus",
                    help="path to corpus directory (default: data/replay_corpus)")
    ap.add_argument("--starting-bp", type=float, default=100_000.0)
    ap.add_argument("--slippage", type=float, default=0.0,
                    help="fill slippage as a fraction of the bid-ask spread")
    ap.add_argument("--ticks", default="10:35,13:00,15:05",
                    help="comma-separated ET tick times per trading day")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s: %(message)s"
    )

    corpus_dir = Path(args.corpus_dir)
    try:
        manifest = load_manifest(corpus_dir)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    # Resolve symbols
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = manifest["symbols"]

    # Resolve window dates
    w_name = args.window.lower()
    if w_name in WINDOWS:
        start, end = WINDOWS[w_name]
    elif w_name == "all":
        start = _parse_date(manifest["date_range"][0])
        end = _parse_date(manifest["date_range"][1])
    else:
        print(f"Error: Unknown window {args.window!r}. Supported windows: {list(WINDOWS.keys())} or 'all'", file=sys.stderr)
        return 1

    # Load bars from the corpus files
    daily: Dict[str, pd.DataFrame] = {}
    for sym in symbols:
        file_path = corpus_dir / f"{sym}.csv"
        if not file_path.exists():
            print(f"Warning: Symbol {sym} file not found in corpus at {file_path}. Skipping.", file=sys.stderr)
            continue
        try:
            df = pd.read_csv(file_path)
            daily[sym] = df
        except Exception as e:
            print(f"Error loading {sym} from {file_path}: {e}", file=sys.stderr)
            return 1

    if not daily:
        print("Error: No symbol data loaded for backtest.", file=sys.stderr)
        return 1

    # Print backtest header info
    strategy_str = args.strategy.upper() if args.strategy else "ALL"
    print(f"Replaying Strategy: {strategy_str}")
    print(f"Regime Window:     {args.window} ({start} to {end})")
    print(f"Underlying(s):     {list(daily.keys())}")
    print(f"Starting Capital:  ${args.starting_bp:,.2f}")
    print(f"Slippage Fraction: {args.slippage:.2%}\n")

    # Set up config and data source
    data = ReplayDataSource.from_frames(daily)
    strategies = [args.strategy.strip().upper()] if args.strategy else None
    
    cfg = ReplayConfig(
        symbols=list(daily.keys()),
        start=start,
        end=end,
        strategies=strategies,
        tick_times_et=[t.strip() for t in args.ticks.split(",") if t.strip()],
        starting_bp=args.starting_bp,
        slippage_frac=args.slippage,
    )

    try:
        harness = ReplayHarness(data, cfg)
        result = asyncio.run(harness.run())
    except Exception as e:
        logger.exception("Backtest run encountered an error")
        print(f"Error during backtest run: {e}", file=sys.stderr)
        return 1

    # Render report
    print(f"Replayed {result.ticks} ticks, {len(result.trades)} trades resolved.")
    print(render_report(result.report))

    return 0


if __name__ == "__main__":
    sys.exit(main())
