#!/usr/bin/env python3
"""Build replay corpus by downloading historical bars and computing realized vol IV proxy.

Downloads daily OHLCV bars for a configurable list of symbols and computes an
IV proxy series using the Yang-Zhang realized volatility estimator. Results are saved
under data/replay_corpus/ along with a manifest JSON.

Yang-Zhang Volatility Choice & Limits:
- Yang-Zhang volatility combines overnight variance (close-to-open returns) and
  intraday variance (Rogers-Satchell range estimator).
- Why chosen: It is far more efficient than simple close-to-close volatility as it accounts
  for both overnight gaps and intraday price range, providing a closer proxy to implied volatility.
- Limits:
  1. It is a historical realized volatility measure, which naturally lags changes in option implied
     volatilities (such as before earnings announcements or major macro events).
  2. It assumes continuous trading/pricing, which breaks down during extreme jump regimes.
  3. It does not account for option-specific skew (smile) or term structure; all options for a given
     underlying on a given day are priced using the same proxy volatility.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import numpy as np
import pandas as pd
import yfinance as yf

# Ensure hermes is in path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logger = logging.getLogger("hermes.replay.build_corpus")


def compute_yang_zhang_volatility(df: pd.DataFrame, window: int = 21) -> pd.Series:
    """Compute the Yang-Zhang realized volatility estimator (annualized)."""
    # Map column names to lowercase to be safe
    df_clean = df.copy()
    df_clean.columns = [c.lower() for c in df_clean.columns]
    
    open_val = df_clean["open"]
    high = df_clean["high"]
    low = df_clean["low"]
    close = df_clean["close"]
    
    # Overnight log return (today's open to yesterday's close)
    prev_close = close.shift(1)
    log_ho = np.log(open_val / prev_close)
    
    # Open-to-close log return (intraday return)
    log_co = np.log(close / open_val)
    
    # Rogers-Satchell term
    rs_term = (
        np.log(high / close) * np.log(high / open_val)
        + np.log(low / close) * np.log(low / open_val)
    )
    
    n = window
    k = 0.34 / (1.34 + (n + 1) / (n - 1))
    
    # Rolling variance (unbiased, ddof=1)
    var_ho = log_ho.rolling(window=n).var() * 252
    var_co = log_co.rolling(window=n).var() * 252
    mean_rs = rs_term.rolling(window=n).mean() * 252
    
    yz_var = var_ho + k * var_co + (1.0 - k) * mean_rs
    
    # Stand-in fallback to simple volatility if Yang-Zhang has NaNs/negatives
    simple_vol = np.log(close / prev_close).rolling(window=n).std() * np.sqrt(252)
    
    # Clip variance to be positive, and take sqrt. Fill initial window NaNs with simple vol or first valid
    vol = np.sqrt(yz_var.clip(lower=0.0))
    vol = vol.fillna(simple_vol).bfill().fillna(0.30)

    
    return vol


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--symbols", default="SPY,QQQ",
                    help="comma-separated list of symbols (default: SPY,QQQ)")
    ap.add_argument("--years", type=int, default=8,
                    help="number of years of history to download (default: 8)")
    ap.add_argument("--output-dir", default="data/replay_corpus",
                    help="directory to save corpus data (default: data/replay_corpus)")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    today = date.today()
    start_date = today - timedelta(days=int(args.years * 365.25))

    logger.info("Downloading %d years of history for: %s", args.years, symbols)
    logger.info("Start date: %s, End date: %s", start_date, today)

    manifest_entries = []
    min_date = None
    max_date = None

    for sym in symbols:
        logger.info("Downloading %s...", sym)
        try:
            # We download daily bars (interval='1d')
            df = yf.download(sym, start=start_date.isoformat(), end=today.isoformat(), interval="1d")
            if df.empty:
                logger.error("No data returned for %s", sym)
                continue
                
            # If multi-index columns (often returned by yfinance), flatten them
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
                
            # Keep only the columns we need: Open, High, Low, Close, Volume
            df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
            df = df.dropna(subset=["Close"])
            
            if df.empty:
                logger.error("All rows for %s had NaN close price.", sym)
                continue

            # Compute Yang-Zhang realized volatility as the IV proxy
            logger.info("Computing Yang-Zhang volatility for %s...", sym)
            df["iv_proxy"] = compute_yang_zhang_volatility(df)
            
            # Reset index to make Date a column and rename it to ts
            df = df.copy()
            df["ts"] = pd.to_datetime(df.index).strftime("%Y-%m-%d")


            # Re-order columns
            df = df[["ts", "Open", "High", "Low", "Close", "Volume", "iv_proxy"]]
            df.columns = [c.lower() for c in df.columns]

            # Save as CSV
            out_file = output_dir / f"{sym}.csv"
            df.to_csv(out_file, index=False)
            logger.info("Saved %d rows for %s to %s", len(df), sym, out_file)

            # Update dates
            sym_min = df["ts"].min()
            sym_max = df["ts"].max()
            if min_date is None or sym_min < min_date:
                min_date = sym_min
            if max_date is None or sym_max > max_date:
                max_date = sym_max

            manifest_entries.append({
                "symbol": sym,
                "rows": len(df),
                "start_date": sym_min,
                "end_date": sym_max,
                "file": f"{sym}.csv"
            })
        except Exception as e:
            logger.exception("Error processing symbol %s", sym)

    if not manifest_entries:
        logger.error("Failed to build corpus for any symbol.")
        return 1

    # Write manifest
    manifest = {
        "symbols": symbols,
        "date_range": [min_date, max_date],
        "source": "yfinance",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "entries": manifest_entries
    }
    manifest_file = output_dir / "manifest.json"
    manifest_file.write_text(json.dumps(manifest, indent=2))
    logger.info("Wrote manifest to %s", manifest_file)
    return 0


if __name__ == "__main__":
    sys.exit(main())
