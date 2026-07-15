from __future__ import annotations

import pytest
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List

from hermes.service1_agent.core import IronCondorBuilder, MoneyManager, TradeAction
from hermes.service1_agent.strategies import CreditSpreads75
from hermes.portfolio.optimizer import PortfolioOptimizer
from hermes.service1_agent.money_manager import resolve_entry_sizing
from tests._stubs import StubBroker, StubDB, _et_today


# ── HELPER FOR BUILDING STRATEGIES ─────────────────────────────────────────

def _build_strat(strategy_cls, today_dt: datetime, *, config=None, db=None):
    broker = StubBroker()
    broker.current_date = today_dt
    
    db = db or StubDB()
    config = config or {}
    for k, v in config.items():
        db.settings[k] = str(v)
        
    mm = MoneyManager(broker, db, config)
    strat = strategy_cls(
        broker=broker, db=db, money_manager=mm,
        ic_builder=IronCondorBuilder(mm),
        config=config, dry_run=False,
    )
    strat.current_date = today_dt
    return strat, broker, db


# ── TESTS ──────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_throttle_default_off():
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    db = StubDB()
    # Mock some underperforming closed predictions in DB
    from hermes.ml.ledger import LedgerRecord, write_record
    for i in range(5):
        # Predicted POP = 0.90, Realized = 0.0 (total underperformance)
        await write_record(db, LedgerRecord(
            symbol="AAPL", model_name="CS75", horizon_dte=7,
            model_hash=None, schema_hash=None, schema_stage="strategy_qualification",
            predicted_prob=0.90, spot=100.0, predicted_prob_lo=None, predicted_prob_hi=None,
            predicted_return=None, ts=today_dt - timedelta(days=i+10)
        ))
    
    # Backfill outcome as 0.0 (loss)
    async with db.AsyncSession() as session:
        from hermes.ml.ledger import PredictionLedger
        res = await session.execute(f"SELECT * FROM prediction_ledger WHERE model_name='CS75'")
        # Update realized outcomes to 0.0
        stmt = "UPDATE prediction_ledger SET realized_outcome = 0.0"
        await session.execute(stmt)
        await session.commit()

    # Strategy defaults to throttle_window = 0 -> multiplier should be 1.0 (OFF)
    strat, _, _ = _build_strat(CreditSpreads75, today_dt, db=db, config={})
    
    mult = await strat.get_throttle_multiplier()
    assert mult == 1.0


@pytest.mark.asyncio
async def test_throttle_insufficient_history():
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    db = StubDB()
    # We only write 3 predictions, but window is set to 5
    from hermes.ml.ledger import LedgerRecord, write_record
    for i in range(3):
        await write_record(db, LedgerRecord(
            symbol="AAPL", model_name="CS75", horizon_dte=7,
            model_hash=None, schema_hash=None, schema_stage="strategy_qualification",
            predicted_prob=0.90, spot=100.0, predicted_prob_lo=None, predicted_prob_hi=None,
            predicted_return=None, ts=today_dt - timedelta(days=i+10)
        ))
    
    async with db.AsyncSession() as session:
        await session.execute("UPDATE prediction_ledger SET realized_outcome = 0.0")
        await session.commit()

    # Strategy has window = 5 -> should return 1.0 due to insufficient history (< 5)
    strat, _, _ = _build_strat(
        CreditSpreads75, today_dt, db=db,
        config={"cs75_throttle_window": 5, "cs75_throttle_drift_threshold": 0.05, "cs75_throttle_floor_mult": 0.5}
    )
    
    mult = await strat.get_throttle_multiplier()
    assert mult == 1.0


@pytest.mark.asyncio
async def test_throttle_engages_on_drift():
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    db = StubDB()
    # Window = 5. Realized win rate is 20% (1/5 wins), predicted is 80% (0.80 avg).
    # Drift = 0.80 - 0.20 = 0.60 > threshold 0.05 -> should throttle to 0.5 multiplier.
    from hermes.ml.ledger import LedgerRecord, write_record
    for i in range(5):
        await write_record(db, LedgerRecord(
            symbol="AAPL", model_name="CS75", horizon_dte=7,
            model_hash=None, schema_hash=None, schema_stage="strategy_qualification",
            predicted_prob=0.80, spot=100.0, predicted_prob_lo=None, predicted_prob_hi=None,
            predicted_return=None, ts=today_dt - timedelta(days=i+10)
        ))
    
    async with db.AsyncSession() as session:
        # Mark one as win (1.0) and four as losses (0.0)
        await session.execute("UPDATE prediction_ledger SET realized_outcome = 0.0")
        await session.execute("UPDATE prediction_ledger SET realized_outcome = 1.0 WHERE id = (SELECT min(id) FROM prediction_ledger)")
        await session.commit()

    strat, _, _ = _build_strat(
        CreditSpreads75, today_dt, db=db,
        config={"cs75_throttle_window": 5, "cs75_throttle_drift_threshold": 0.05, "cs75_throttle_floor_mult": 0.5}
    )
    
    mult = await strat.get_throttle_multiplier()
    assert mult == 0.5


@pytest.mark.asyncio
async def test_throttle_recovery():
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    db = StubDB()
    # Window = 3. Realized win rate is 100% (3/3 wins), predicted is 80%.
    # Drift = 0.80 - 1.00 = -0.20 < threshold 0.05 -> should not throttle (returns 1.0).
    from hermes.ml.ledger import LedgerRecord, write_record
    for i in range(3):
        await write_record(db, LedgerRecord(
            symbol="AAPL", model_name="CS75", horizon_dte=7,
            model_hash=None, schema_hash=None, schema_stage="strategy_qualification",
            predicted_prob=0.80, spot=100.0, predicted_prob_lo=None, predicted_prob_hi=None,
            predicted_return=None, ts=today_dt - timedelta(days=i+10)
        ))
    
    async with db.AsyncSession() as session:
        await session.execute("UPDATE prediction_ledger SET realized_outcome = 1.0")
        await session.commit()

    strat, _, _ = _build_strat(
        CreditSpreads75, today_dt, db=db,
        config={"cs75_throttle_window": 3, "cs75_throttle_drift_threshold": 0.05, "cs75_throttle_floor_mult": 0.5}
    )
    
    mult = await strat.get_throttle_multiplier()
    assert mult == 1.0


def test_resolve_entry_sizing_applies_throttle():
    action = TradeAction(
        strategy_id="CS75", symbol="AAPL", order_class="multileg",
        legs=[{"option_symbol": "AAPL260903C00100000", "quantity": 10}],
        price=1.00, side="sell", quantity=10, width=5.0
    )
    # With throttle multiplier = 0.5, size should be scaled to 5
    action.strategy_params["throttle_mult"] = 0.5
    
    req, max_lots, req_per_lot = resolve_entry_sizing(action, config={})
    assert req == 5
    assert req_per_lot == 500.0


def test_resolve_entry_sizing_honors_max_lots_zero():
    action = TradeAction(
        strategy_id="CS75", symbol="AAPL", order_class="multileg",
        legs=[{"option_symbol": "AAPL260903C00100000", "quantity": 10}],
        price=1.00, side="sell", quantity=10, width=5.0
    )
    action.strategy_params["throttle_mult"] = 0.5
    
    # max_lots=0 config check
    req, max_lots, req_per_lot = resolve_entry_sizing(action, config={"cs75_max_lots": 0})
    assert req == 5
    assert max_lots == 0


def test_resolve_entry_sizing_debit_spread_uses_price_not_width():
    # DS0 pays the debit upfront (~$0.10-0.30/share for a $1-wide spread);
    # that IS the capital requirement, not the full width*100 margin figure
    # that applies to credit spreads (CS75/CS7/TT45/HermesAlpha).
    action = TradeAction(
        strategy_id="DS0", symbol="SPY", order_class="multileg",
        legs=[{"option_symbol": "SPY260715P00500000", "side": "buy_to_open", "quantity": 1},
              {"option_symbol": "SPY260715P00499000", "side": "sell_to_open", "quantity": 1}],
        price=0.20, side="buy", quantity=1, order_type="debit", width=1.0,
    )

    req, max_lots, req_per_lot = resolve_entry_sizing(action, config={"ds0_max_lots": 1})
    assert req == 1
    assert req_per_lot == 20.0


def test_resolve_entry_sizing_debit_spread_falls_back_to_width_without_price():
    action = TradeAction(
        strategy_id="DS0", symbol="SPY", order_class="multileg",
        legs=[{"option_symbol": "SPY260715P00500000", "side": "buy_to_open", "quantity": 1},
              {"option_symbol": "SPY260715P00499000", "side": "sell_to_open", "quantity": 1}],
        price=None, side="buy", quantity=1, order_type="debit", width=1.0,
    )

    req, max_lots, req_per_lot = resolve_entry_sizing(action, config={"ds0_max_lots": 1})
    assert req == 1
    assert req_per_lot == 100.0


@pytest.mark.asyncio
async def test_portfolio_optimizer_applies_throttle():
    action = TradeAction(
        strategy_id="CS75", symbol="AAPL", order_class="multileg",
        legs=[{"option_symbol": "AAPL260903C00100000", "quantity": 10}],
        price=1.00, side="sell", quantity=10, width=2.0
    )
    action.strategy_params["throttle_mult"] = 0.5
    action.strategy_params["pop"] = 0.90

    optimizer = PortfolioOptimizer(config={"portfolio_optimization": True})
    optimized = await optimizer.optimize([action], avail_bp=10000.0, existing_positions=[])
    
    assert len(optimized) == 1
    # 10 lots throttled by 0.5 -> should be 5 lots
    assert optimized[0].quantity == 5
    assert optimized[0].legs[0]["quantity"] == 5


@pytest.mark.asyncio
async def test_strategy_execute_entries_auto_wraps_and_logs():
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    strat, broker, db = _build_strat(
        CreditSpreads75, today_dt,
        config={"cs75_throttle_window": 0} # default off
    )
    
    # Setup broker mock for options
    broker.last_price = lambda sym: 100.0
    broker.get_option_expirations = lambda sym: ["2026-09-03"]
    
    from tests._stubs import make_chain
    # Inject DTE 40 so it matches CS75 target range
    broker.get_option_chains = lambda sym, exp: make_chain(sym, exp, spot=100.0)

    # Calling execute_entries should qualify AAPL entries and write prediction to ledger
    actions = await strat.execute_entries(["AAPL"])
    
    assert len(actions) > 0
    # The returned actions should have been wrapped, adding throttle_mult
    assert actions[0].strategy_params.get("throttle_mult") == 1.0
    
    # A prediction row should have been written to the database prediction_ledger
    async with db.AsyncSession() as session:
        from hermes.ml.ledger import PredictionLedger
        res = await session.execute("SELECT symbol, model_name, predicted_prob FROM prediction_ledger")
        rows = res.fetchall()
        
    assert len(rows) > 0
    assert rows[0][0] == "AAPL"
    assert rows[0][1] == "CS75"  # Strategy NAME is model_name
    assert abs(rows[0][2] - 0.70) < 0.10  # POP around 0.70-0.80

    # The row must carry the spread's win condition so backfill scores it as
    # "short strike unbreached", not as a directional up-move.
    fv = db._prediction_ledger[0].feature_vector
    assert fv.get("win_condition") == "short_otm"
    assert fv.get("option_type") in ("put", "call")
    assert isinstance(fv.get("short_strike"), float)


# ── TRUNCATION SEMANTICS ────────────────────────────────────────────────────

def _one_lot_action(mult):
    action = TradeAction(
        strategy_id="CS75", symbol="AAPL", order_class="multileg",
        legs=[{"option_symbol": "AAPL260903C00100000", "quantity": 1}],
        price=1.00, side="sell", quantity=1, width=5.0
    )
    action.strategy_params["throttle_mult"] = mult
    return action


def test_throttle_fractional_mult_never_zeroes_one_lot():
    # At the typical 1-lot sizing, int(1 * 0.5) == 0 would turn any
    # fractional multiplier into a full entry kill instead of a shrink.
    req, _, _ = resolve_entry_sizing(_one_lot_action(0.5), config={})
    assert req == 1


def test_throttle_zero_mult_is_explicit_full_kill():
    req, _, _ = resolve_entry_sizing(_one_lot_action(0.0), config={})
    assert req == 0


@pytest.mark.asyncio
async def test_optimizer_fractional_mult_never_zeroes_one_lot():
    action = _one_lot_action(0.5)
    action.width = 2.0
    action.strategy_params["pop"] = 0.90
    optimizer = PortfolioOptimizer(config={"portfolio_optimization": True})
    optimized = await optimizer.optimize([action], avail_bp=10000.0, existing_positions=[])
    assert len(optimized) == 1
    assert optimized[0].quantity == 1


# ── LEDGER WRITE GUARDS ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_no_ledger_write_without_spot():
    # An unknown spot must skip the write, never fabricate a $100 stock.
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    strat, broker, db = _build_strat(CreditSpreads75, today_dt)
    broker.last_price = lambda sym: None
    broker.get_quote = lambda symbols: []

    action = TradeAction(
        strategy_id="CS75", symbol="AAPL", order_class="multileg",
        legs=[{"option_symbol": "AAPL260903P00095000", "side": "sell_to_open", "quantity": 1},
              {"option_symbol": "AAPL260903P00090000", "side": "buy_to_open", "quantity": 1}],
        price=1.00, side="sell", quantity=1, width=5.0
    )
    out = await strat._process_and_throttle_actions([action])
    assert out == [action]                            # action itself passes through
    assert out[0].strategy_params["throttle_mult"] == 1.0
    assert len(db._prediction_ledger) == 0            # but nothing fabricated


@pytest.mark.asyncio
async def test_no_ledger_write_without_short_open_leg():
    # Debit spreads / equity actions have no "short strike stays OTM"
    # prediction to score — they must not be logged as one.
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    strat, broker, db = _build_strat(CreditSpreads75, today_dt)
    broker.last_price = lambda sym: 100.0

    action = TradeAction(
        strategy_id="CS75", symbol="AAPL", order_class="multileg",
        legs=[{"option_symbol": "AAPL260903C00105000", "side": "buy_to_open", "quantity": 1},
              {"option_symbol": "AAPL260903C00110000", "side": "sell_to_close", "quantity": 1}],
        price=1.00, side="buy", quantity=1, width=5.0
    )
    await strat._process_and_throttle_actions([action])
    assert len(db._prediction_ledger) == 0


# ── BACKFILL SCORES QUALIFICATION ROWS AGAINST THE SPREAD WIN CONDITION ────

def _mock_ledger_db(rows, closes):
    """DummyDB whose session yields ``rows`` and whose bars close at ``closes``."""
    import pandas as pd
    from unittest.mock import AsyncMock, MagicMock

    db = MagicMock()

    async def mock_execute(*args, **kwargs):
        res = MagicMock()
        res.scalars.return_value.all.return_value = rows
        return res

    session = AsyncMock()
    db.AsyncSession = MagicMock(return_value=session)
    session.__aenter__.return_value.execute = mock_execute

    idx = pd.date_range("2026-06-01", periods=len(closes), freq="D", tz="UTC")
    df = pd.DataFrame({"close": closes}, index=idx)
    db.daily_bars = AsyncMock(return_value=df)
    return db


def _qual_row(option_type, strike, fv_extra=None):
    from unittest.mock import MagicMock
    row = MagicMock()
    row.horizon_dte = 1
    row.ts = datetime(2026, 6, 1, tzinfo=timezone.utc)
    row.symbol = "AAPL"
    row.realized_outcome = None
    row.spot = 100.0
    row.schema_stage = "strategy_qualification"
    row.feature_vector = {"win_condition": "short_otm",
                          "option_type": option_type, "short_strike": strike}
    if fv_extra is not None:
        row.feature_vector = fv_extra
    return row


@pytest.mark.asyncio
async def test_backfill_short_put_scored_otm():
    from hermes.ml.ledger import backfill_prediction_outcomes
    # Short put 95: close 97 stays above the strike → win, even though the
    # move was DOWN from spot 100 (directional scoring would call it a loss).
    row = _qual_row("put", 95.0)
    db = _mock_ledger_db([row], [100.0, 97.0, 97.0])
    await backfill_prediction_outcomes(db, lookback_days=10)
    assert row.realized_outcome == 1.0


@pytest.mark.asyncio
async def test_backfill_short_call_breached_is_loss():
    from hermes.ml.ledger import backfill_prediction_outcomes
    # Short call 105: close 107 breaches → loss, even though close > spot
    # (directional scoring would call it a win).
    row = _qual_row("call", 105.0)
    db = _mock_ledger_db([row], [100.0, 107.0, 107.0])
    await backfill_prediction_outcomes(db, lookback_days=10)
    assert row.realized_outcome == 0.0


@pytest.mark.asyncio
async def test_backfill_legacy_qualification_rows_left_unmarked():
    from hermes.ml.ledger import backfill_prediction_outcomes
    # Rows written before win-condition stamping can't be scored correctly;
    # a directional outcome must NOT be fabricated for them.
    row = _qual_row("put", 95.0, fv_extra={})
    db = _mock_ledger_db([row], [100.0, 107.0, 107.0])
    marked = await backfill_prediction_outcomes(db, lookback_days=10)
    assert marked == 0
    assert row.realized_outcome is None
