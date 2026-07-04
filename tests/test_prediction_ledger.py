import pytest
from datetime import datetime, timezone, timedelta
import pandas as pd

from hermes.ml.ledger import PredictionLedger, write_record, LedgerRecord, backfill_prediction_outcomes


@pytest.fixture
def test_db(make_db):
    # schema=True provisions the raw bars_* hypertables
    return make_db(schema=True)


async def test_backfill_prediction_outcomes(test_db):
    # Ensure the table is created
    from hermes.ml.ledger import ensure_table
    ensure_table(test_db)

    now = datetime.now(timezone.utc)

    # 1. Insert daily bars for SPY
    # Let's say SPY daily bar close on target_date (which will be row.ts + horizon) is 500
    dates = pd.date_range((now - timedelta(days=15)).strftime("%Y-%m-%d"), periods=20, freq="D")
    df_bars = pd.DataFrame({
        "ts": dates,
        "open": [490.0] * 20,
        "high": [510.0] * 20,
        "low": [480.0] * 20,
        "close": [500.0] * 20,
        "volume": [1000] * 20,
        "vwap_close": [500.0] * 20
    })
    await test_db.save_daily_bars("SPY", df_bars)

    # 2. Insert records into prediction ledger
    # Prediction 1: Target date is in the past (horizon = 5, ts = now - 10d). Spot = 450.
    # realized_close is 500, which is > spot (450), so outcome = 1.0 (profitable/up).
    rec1 = LedgerRecord(
        symbol="SPY",
        model_name="xgb-q50-default",
        horizon_dte=5,
        model_hash="hash1",
        schema_hash="shash1",
        schema_stage="raw",
        predicted_prob=0.75,
        predicted_prob_lo=0.60,
        predicted_prob_hi=0.90,
        predicted_return=0.02,
        spot=450.0,
        ts=now - timedelta(days=10),
    )
    await write_record(test_db, rec1)

    # Prediction 2: Target date is in the past (horizon = 5, ts = now - 10d). Spot = 550.
    # realized_close is 500, which is < spot (550), so outcome = 0.0.
    rec2 = LedgerRecord(
        symbol="SPY",
        model_name="xgb-q50-default",
        horizon_dte=5,
        model_hash="hash1",
        schema_hash="shash1",
        schema_stage="raw",
        predicted_prob=0.25,
        predicted_prob_lo=0.10,
        predicted_prob_hi=0.40,
        predicted_return=-0.02,
        spot=550.0,
        ts=now - timedelta(days=10),
    )
    await write_record(test_db, rec2)

    # Prediction 3: Target date is in the future (horizon = 5, ts = now - 2d). Target = now + 3d.
    # Should not be marked yet.
    rec3 = LedgerRecord(
        symbol="SPY",
        model_name="xgb-q50-default",
        horizon_dte=5,
        model_hash="hash1",
        schema_hash="shash1",
        schema_stage="raw",
        predicted_prob=0.50,
        predicted_prob_lo=0.40,
        predicted_prob_hi=0.60,
        predicted_return=0.0,
        spot=500.0,
        ts=now - timedelta(days=2),
    )
    await write_record(test_db, rec3)

    # Run backfill
    marked = await backfill_prediction_outcomes(test_db, lookback_days=90)
    assert marked == 2

    # Query rows to assert correctness
    async with test_db.AsyncSession() as session:
        from sqlalchemy import select
        q = select(PredictionLedger).order_by(PredictionLedger.spot)
        result = await session.execute(q)
        rows = result.scalars().all()

        assert len(rows) == 3
        # rec1: spot=450, realized_close=500, outcome=1.0
        assert rows[0].spot == 450.0
        assert rows[0].realized_outcome == 1.0
        assert rows[0].realized_close == 500.0
        assert rows[0].realized_at is not None

        # rec3: spot=500, not marked yet
        assert rows[1].spot == 500.0
        assert rows[1].realized_outcome is None
        assert rows[1].realized_close is None

        # rec2: spot=550, realized_close=500, outcome=0.0
        assert rows[2].spot == 550.0
        assert rows[2].realized_outcome == 0.0
        assert rows[2].realized_close == 500.0
        assert rows[2].realized_at is not None
