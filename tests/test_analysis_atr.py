"""ATR fields on the key-levels analysis payload (watcher /api/analysis).

``wilder_atr`` must match DS0's entry-range math (``strategies/ds0.py::_atr``):
gap-inclusive true range, simple-mean seed over the first ``period`` TRs, then
Wilder smoothing. ``analyze_symbol`` exposes it (plus the latest session open)
so the UI can render the open ± ATR band DS0 qualifies levels against.
"""
from __future__ import annotations

import pandas as pd
import pytest

from hermes.ml.pop_engine import wilder_atr
from hermes.service1_agent.mock_broker import MockBroker


def _frame(rows):
    return pd.DataFrame(rows, columns=["high", "low", "close"])


def test_wilder_atr_hand_computed():
    # TRs after the seed row: 2, 2, 4. period=2 → seed (2+2)/2 = 2,
    # then Wilder step (2*1 + 4)/2 = 3.
    df = _frame([
        (10.0, 8.0, 9.0),
        (11.0, 9.0, 10.0),
        (12.0, 10.0, 11.0),
        (15.0, 11.0, 14.0),
    ])
    assert wilder_atr(df, period=2) == pytest.approx(3.0)


def test_wilder_atr_includes_overnight_gap():
    # Second bar gaps far above the prior close: TR must use
    # |high − prev_close| = 10, not high − low = 2.
    df = _frame([
        (10.0, 8.0, 9.0),
        (19.0, 17.0, 18.0),
        (19.0, 17.0, 18.0),
    ])
    assert wilder_atr(df, period=2) == pytest.approx((10.0 + 2.0) / 2)


def test_wilder_atr_short_history_returns_none():
    df = _frame([(10.0, 8.0, 9.0), (11.0, 9.0, 10.0)])
    assert wilder_atr(df, period=14) is None
    assert wilder_atr(df.iloc[:0], period=2) is None
    assert wilder_atr(df.drop(columns=["high"]), period=1) is None


def test_wilder_atr_ignores_unparseable_rows():
    df = pd.DataFrame({
        "high": [10.0, "bad", 11.0, 12.0, 15.0],
        "low": [8.0, None, 9.0, 10.0, 11.0],
        "close": [9.0, None, 10.0, 11.0, 14.0],
    })
    # The corrupt row drops out, leaving the hand-computed series above.
    assert wilder_atr(df, period=2) == pytest.approx(3.0)


@pytest.mark.asyncio
async def test_analyze_symbol_exposes_atr_and_latest_open():
    broker = MockBroker({})
    res = await broker.analyze_symbol("SPY", period="3m")
    assert "error" not in res
    assert res["atr_period"] == 14
    assert res["atr"] is not None and res["atr"] > 0
    assert res["today_open"] is not None and res["today_open"] > 0
    # The mock feed always has ~400 daily bars, far more than period+1.
    assert res["samples"] > 15
