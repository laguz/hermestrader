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
