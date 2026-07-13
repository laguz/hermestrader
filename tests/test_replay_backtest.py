"""Tests for corpus ingestion, named windows backtest runner, and extended reporting.

These tests run entirely offline with no network dependencies, mocking the yfinance
download and using deterministic synthetic datasets as fixtures.
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from hermes.replay import (
    ReplayConfig, ReplayDataSource, ReplayHarness, build_report,
)
from hermes.broker.circuit_breaker import CircuitBreaker
from hermes.service1_agent.broker_wrapper import AsyncBrokerWrapper
from scripts.build_replay_corpus import compute_yang_zhang_volatility


# ── Yang-Zhang Volatility tests ──────────────────────────────────────────────
def test_compute_yang_zhang_volatility():
    """Verify that compute_yang_zhang_volatility calculates expected numbers and handles NaNs."""
    # Build 50 days of flat prices to check zero volatility
    dates = pd.bdate_range(end="2026-06-12", periods=50)
    rows = []
    for ts in dates:
        rows.append({
            "Date": ts,
            "Open": 100.0,
            "High": 100.0,
            "Low": 100.0,
            "Close": 100.0,
            "Volume": 10000
        })
    df = pd.DataFrame(rows)
    vol = compute_yang_zhang_volatility(df, window=21)
    
    assert len(vol) == 50
    # Zero price variation should result in zero/low volatility (floored at 0.0)
    assert vol.iloc[-1] == pytest.approx(0.0, abs=1e-5)


# ── Corpus Builder Mocking test ─────────────────────────────────────────────
@patch("yfinance.download")
def test_build_replay_corpus_offline(mock_download, tmp_path):
    """Verify build_replay_corpus.py script runs offline and creates expected files."""
    from scripts.build_replay_corpus import main as corpus_main
    
    # Mock yfinance.download to return a valid DataFrame
    idx = pd.bdate_range(end="2026-06-12", periods=30)
    rows = []
    for ts in idx:
        rows.append({
            "Open": 100.0,
            "High": 101.0,
            "Low": 99.0,
            "Close": 100.5,
            "Volume": 1000000
        })
    mock_df = pd.DataFrame(rows, index=idx)
    mock_download.return_value = mock_df

    output_dir = tmp_path / "replay_corpus"
    test_args = [
        "scripts/build_replay_corpus.py",
        "--symbols", "TEST_SYM",
        "--years", "1",
        "--output-dir", str(output_dir)
    ]
    
    with patch("sys.argv", test_args):
        exit_code = corpus_main()
        assert exit_code == 0
        
    # Check that output files were created
    csv_file = output_dir / "TEST_SYM.csv"
    manifest_file = output_dir / "manifest.json"
    
    assert csv_file.exists()
    assert manifest_file.exists()
    
    # Read manifest and verify contents
    manifest = json.loads(manifest_file.read_text())
    assert manifest["symbols"] == ["TEST_SYM"]
    assert len(manifest["entries"]) == 1
    assert manifest["entries"][0]["file"] == "TEST_SYM.csv"
    
    # Read CSV and verify columns
    df_result = pd.read_csv(csv_file)
    assert "iv_proxy" in df_result.columns
    assert "ts" in df_result.columns
    assert len(df_result) == 30


# ── End-to-end Backtest and Report test ──────────────────────────────────────
def make_synthetic_daily_frame(n: int = 140, base: float = 100.0) -> pd.DataFrame:
    idx = pd.bdate_range(end="2026-06-12", periods=n)
    rows = []
    for i, ts in enumerate(idx):
        px = base + 8.0 * math.sin(i / 9.0) + 0.02 * i
        o = round(px - 0.15, 2)
        c = round(px + 0.15, 2)
        rows.append({
            "ts": ts,
            "open": o,
            "high": round(max(o, c) + 0.4, 2),
            "low": round(min(o, c) - 0.4, 2),
            "close": c,
            "volume": 1_000_000 + (i % 7) * 25_000,
            "iv_proxy": 0.22  # Mock constant IV proxy
        })
    return pd.DataFrame(rows)


@pytest.mark.asyncio
async def test_replay_runner_and_report_calibration(monkeypatch):
    """Verify backtest runner with iv_proxy, report generation, and circuit breaker restore."""
    monkeypatch.setenv("HERMES_ALLOW_OFFHOURS_TRADES", "true")
    
    # Save current circuit breaker to verify restoration
    sentinel_cb = CircuitBreaker(failure_threshold=5)
    AsyncBrokerWrapper._shared_cb = sentinel_cb

    frame = make_synthetic_daily_frame()
    data = ReplayDataSource.from_frames({"SPY": frame})
    
    idx = pd.to_datetime(frame["ts"])
    start = idx.iloc[-10].date()
    end = idx.iloc[-1].date()
    
    cfg = ReplayConfig(
        symbols=["SPY"],
        start=start,
        end=end,
        strategies=["CS75"],
        starting_bp=100_000.0,
        slippage_frac=0.0
    )
    
    harness = ReplayHarness(data, cfg)
    
    # Prep harness simulation time to parse options chains & place entry order
    from hermes.replay.harness import _tick_instants
    days = harness.data.trading_days(harness.cfg.start, harness.cfg.end)
    t0 = _tick_instants(days[0], harness.cfg.tick_times_et)[0]
    harness.clock.set_time(t0)
    harness.broker.set_time(t0)
    
    expiry = idx.iloc[-5].date().strftime("%Y-%m-%d")
    yymmdd = idx.iloc[-5].date().strftime("%y%m%d")
    
    # Construct a multileg TradeAction manually
    from hermes.service1_agent.trade_action import TradeAction

    action = TradeAction(
        strategy_id="CS75", symbol="SPY", order_class="multileg",
        legs=[
            {"option_symbol": f"SPY{yymmdd}P00110000", "side": "sell_to_open", "quantity": 1},
            {"option_symbol": f"SPY{yymmdd}P00105000", "side": "buy_to_open", "quantity": 1},
        ],
        price=0.50, side="sell", quantity=1, order_type="credit",
        tag="HERMES_CS75", expiry=expiry,
        width=5.0,
        strategy_params={"short_leg": f"SPY{yymmdd}P00110000", "long_leg": f"SPY{yymmdd}P00105000",
                         "side_type": "put", "pop": 0.78,
                         "entry_features": {
                             "schema": 2,
                             "pop": 0.78,
                             "short_delta": 0.15,
                             "width": 5.0,
                         }},
    )


    
    # Place the entry trade to open the position with the correct global clock set
    import hermes.utils as _utils
    prev_clock = _utils._GLOBAL_CLOCK
    _utils._GLOBAL_CLOCK = harness.clock
    try:
        await harness.engine._execute_or_queue(action, "entry")
    finally:

        _utils._GLOBAL_CLOCK = prev_clock




    
    # Run the backtest harness to conclusion
    result = await harness.run()
    
    # 1. Verify that the shared circuit breaker is correctly restored
    assert AsyncBrokerWrapper._shared_cb is sentinel_cb
    
    # 2. Verify that options are priced synthetically using the iv_proxy (0.22)
    assert result.report.get("synthetic_pricing") is True
    
    # 3. Verify report structure and calibration table presence
    report = result.report
    assert "strategies" in report
    assert "CS75" in report["strategies"]
    
    cs75_report = report["strategies"]["CS75"]
    assert "avg_win" in cs75_report
    assert "avg_loss" in cs75_report
    assert cs75_report["trades_resolved"] > 0
    assert cs75_report["calibration_table"] is not None
    
    # POP calibration table should have buckets
    found_bucket = False
    for bucket in cs75_report["calibration_table"]:
        if bucket["bucket_range"] == "75% to 80%":
            assert bucket["count"] == 1
            assert bucket["avg_predicted"] == pytest.approx(0.78)
            found_bucket = True
    assert found_bucket
