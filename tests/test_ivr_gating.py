from __future__ import annotations

import pytest
from datetime import date, datetime, timedelta
from typing import Dict, Any, List

from hermes.service1_agent.core import IronCondorBuilder, MoneyManager
from hermes.service1_agent.strategies import (
    CreditSpreads75,
    CreditSpreads7,
)
from tests._stubs import StubBroker, StubDB, make_trade, _et_today


# ── HELPER FOR BUILDING STRATEGIES ─────────────────────────────────────────

def _build_strat(strategy_cls, today_dt: datetime, *, expirations=None, config=None, db=None):
    broker = StubBroker(expirations=expirations)
    broker.current_date = today_dt

    # Inject default mid_iv=0.35 into the options Greeks of the StubBroker chain
    orig_get_chains = broker.get_option_chains
    async def mock_get_chains(symbol, expiry):
        import inspect, asyncio
        res = orig_get_chains(symbol, expiry)
        if inspect.iscoroutine(res) or asyncio.iscoroutine(res):
            chain = await res
        else:
            chain = res
        if chain:
            for o in chain:
                if "greeks" in o:
                    o["greeks"]["mid_iv"] = 0.35
        return chain
    broker.get_option_chains = mock_get_chains
    
    db = db or StubDB()
    # Mock settings on the stub db to handle resolving tunables correctly
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


# ── IV GATE TESTS ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fetch_current_atm_iv():
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    strat, broker, db = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-03"]  # ~31 DTE, closest to 30 DTE
    )
    
    # Stub last_price to return 100.0, so strike 100.0 is exactly ATM
    broker.last_price = lambda sym: 100.0
    
    # Fetch ATM IV
    iv = await strat._fetch_current_atm_iv("AAPL")
    # Stub options in make_chain (StubBroker uses it) have default mid_iv=0.35
    assert iv is not None
    assert abs(iv - 0.35) < 1e-4


@pytest.mark.asyncio
async def test_ivr_gating_gated_below_threshold():
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    db = StubDB()
    # Store daily observations for the last 5 days
    # Range of IV is 0.20 to 0.50
    # Today's current IV will be 0.35 (default in make_chain stub)
    # IVR = 100 * (0.35 - 0.20) / (0.50 - 0.20) = 100 * 0.15 / 0.30 = 50.0%
    await db.save_implied_vol("AAPL", 0.20, today_dt - timedelta(days=4))
    await db.save_implied_vol("AAPL", 0.50, today_dt - timedelta(days=3))
    await db.save_implied_vol("AAPL", 0.30, today_dt - timedelta(days=2))
    await db.save_implied_vol("AAPL", 0.40, today_dt - timedelta(days=1))

    # cs75_min_ivr = 60.0, today's IVR is 50.0% -> should be gated/blocked
    strat, broker, _ = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-03"],
        config={"cs75_min_ivr": 60.0},
        db=db
    )
    broker.last_price = lambda sym: 100.0

    actions = await strat.execute_entries(["AAPL"])
    assert len(actions) == 0
    assert any("Entry blocked: AAPL IV rank" in log for log in strat.execution_logs)


@pytest.mark.asyncio
async def test_ivr_gating_passes_above_threshold():
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    db = StubDB()
    # Range of IV is 0.20 to 0.40
    # Today's current IV will be 0.35
    # IVR = 100 * (0.35 - 0.20) / (0.40 - 0.20) = 100 * 0.15 / 0.20 = 75.0%
    await db.save_implied_vol("AAPL", 0.20, today_dt - timedelta(days=4))
    await db.save_implied_vol("AAPL", 0.40, today_dt - timedelta(days=3))
    await db.save_implied_vol("AAPL", 0.30, today_dt - timedelta(days=2))

    # cs75_min_ivr = 60.0, today's IVR is 75.0% -> should pass
    strat, broker, _ = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-03"],
        config={"cs75_min_ivr": 60.0},
        db=db
    )
    broker.last_price = lambda sym: 100.0

    actions = await strat.execute_entries(["AAPL"])
    assert len(actions) > 0


@pytest.mark.asyncio
async def test_ivr_gating_default_off():
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    db = StubDB()
    # IVR will be 50%
    await db.save_implied_vol("AAPL", 0.20, today_dt - timedelta(days=4))
    await db.save_implied_vol("AAPL", 0.50, today_dt - timedelta(days=3))

    # cs75_min_ivr defaults to 0.0 -> should pass and not block
    strat, broker, _ = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-03"],
        config={},  # default-off min_ivr = 0.0
        db=db
    )
    broker.last_price = lambda sym: 100.0

    actions = await strat.execute_entries(["AAPL"])
    assert len(actions) > 0


@pytest.mark.asyncio
async def test_ivr_degraded_path_fails_open_no_history():
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    db = StubDB()
    # No historical observations saved in db

    # cs75_min_ivr = 60.0 but history is empty -> should degrade to no gating (fail-open)
    strat, broker, _ = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-03"],
        config={"cs75_min_ivr": 60.0},
        db=db
    )
    broker.last_price = lambda sym: 100.0

    actions = await strat.execute_entries(["AAPL"])
    assert len(actions) > 0
    # The gate is read-only: history comes from the pipeline's daily IV
    # snapshot, never from inside the gate (selection-bias fix).
    history = await db.get_implied_vol_history("AAPL")
    assert len(history) == 0


@pytest.mark.asyncio
async def test_ivr_degraded_path_fails_open_missing_current_iv():
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    db = StubDB()
    await db.save_implied_vol("AAPL", 0.20, today_dt - timedelta(days=4))
    await db.save_implied_vol("AAPL", 0.50, today_dt - timedelta(days=3))

    strat, broker, _ = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-03"],
        config={"cs75_min_ivr": 60.0},
        db=db
    )
    broker.last_price = lambda sym: 100.0
    
    # Mock broker.get_option_chains to strip mid_iv from Greeks, making current_iv None
    from tests._stubs import make_chain
    broker.get_option_chains = lambda sym, exp: [
        {**o, "greeks": {"delta": o["greeks"]["delta"]}}
        for o in make_chain(sym, exp, spot=100.0)
    ]

    actions = await strat.execute_entries(["AAPL"])
    # Missing current ATM IV -> should degrade and fail-open
    assert len(actions) > 0


@pytest.mark.asyncio
async def test_exits_never_blocked_by_ivr():
    # Today is a low-IVR day which would block entries, but exits must run
    today_dt = datetime(2026, 7, 28, 10, 0, 0)
    db = StubDB()
    
    # IVR is low (e.g. 10.0%)
    await db.save_implied_vol("AAPL", 0.20, today_dt - timedelta(days=4))
    await db.save_implied_vol("AAPL", 0.50, today_dt - timedelta(days=3))
    # today's current IV is 0.35 -> IVR is 50% but we set threshold to 90.0%
    db.settings["cs75_min_ivr"] = "90.0"
    
    expiry_date = date(2026, 8, 27)
    trade = make_trade(
        "CS75", "AAPL",
        side_type="call",
        short_strike=105.0,
        long_strike=110.0,
        entry_credit=2.00,
        expiry=expiry_date
    )
    db.set_open_trades("CS75", [trade])

    strat, broker, _ = _build_strat(
        CreditSpreads75, today_dt,
        db=db
    )
    broker.last_price = lambda sym: 100.0

    broker.get_quote = lambda symbols: [
        {"symbol": s.strip(), "bid": 0.20, "ask": 0.30}
        for s in symbols.split(",")
    ]

    actions = await strat.manage_positions()
    
    # Exits should proceed normally
    assert len(actions) == 1
    assert actions[0].side == "buy"
    assert "HERMES_CS75" in actions[0].tag


@pytest.mark.asyncio
async def test_current_vol_persisted():
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    strat, broker, db = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-03"]
    )
    broker.last_price = lambda sym: 100.0
    
    # Mock analyze_symbol to return current_vol = 0.45
    async def mock_analyze_symbol(symbol, period="6m"):
        return {
            "symbol": symbol,
            "current_price": 100.0,
            "current_vol": 0.45,
            "avg_vol": 0.35,
            "key_levels": []
        }
    broker.analyze_symbol = mock_analyze_symbol

    actions = await strat.execute_entries(["AAPL"])

    # Setting should be saved in DB
    val = await db.settings.get_setting("ml_current_vol__AAPL")
    assert val is not None
    assert float(val) == 0.45


# ── DAILY IV SNAPSHOT (pipeline heartbeat) ─────────────────────────────────

@pytest.mark.asyncio
async def test_daily_iv_snapshot_persists_history():
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    strat, broker, db = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-03"]
    )
    broker.last_price = lambda sym: 100.0

    from hermes.service1_agent.iv_tracker import snapshot_daily_iv
    saved = await snapshot_daily_iv(db, strat.broker, ["AAPL"], today_dt.date())
    assert saved == 1
    history = await db.get_implied_vol_history("AAPL")
    assert len(history) == 1
    assert abs(history[0][1] - 0.35) < 1e-4

    # Re-running the same day upserts, never duplicates.
    saved = await snapshot_daily_iv(db, strat.broker, ["AAPL"], today_dt.date())
    assert saved == 1
    assert len(await db.get_implied_vol_history("AAPL")) == 1


@pytest.mark.asyncio
async def test_daily_iv_snapshot_isolates_symbol_failures():
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    strat, broker, db = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-03"]
    )
    broker.last_price = lambda sym: 100.0

    orig_expirations = broker.get_option_expirations
    def failing_expirations(symbol):
        if symbol == "BAD":
            raise RuntimeError("chain fetch exploded")
        return orig_expirations(symbol)
    broker.get_option_expirations = failing_expirations

    from hermes.service1_agent.iv_tracker import snapshot_daily_iv
    saved = await snapshot_daily_iv(db, strat.broker, ["BAD", "AAPL"], today_dt.date())
    assert saved == 1
    assert len(await db.get_implied_vol_history("AAPL")) == 1
    assert len(await db.get_implied_vol_history("BAD")) == 0


@pytest.mark.asyncio
async def test_ivr_gate_never_writes_history():
    # Selection-bias regression: neither a blocked day nor a passed day may
    # write history from inside the gate — only the daily snapshot writes.
    today_dt = datetime(2026, 7, 25, 10, 0, 0)
    db = StubDB()
    await db.save_implied_vol("AAPL", 0.30, today_dt - timedelta(days=4))
    await db.save_implied_vol("AAPL", 0.90, today_dt - timedelta(days=3))

    strat, broker, _ = _build_strat(
        CreditSpreads75, today_dt,
        expirations=["2026-09-03"],
        config={"cs75_min_ivr": 60.0},
        db=db
    )
    broker.last_price = lambda sym: 100.0

    # current 0.35 vs range [0.30, 0.90] → IVR ~8% < 60 → blocked
    assert await strat.is_ivr_gated("AAPL", 60.0) is True
    assert len(await db.get_implied_vol_history("AAPL")) == 2

    # Passing day (threshold 5%) must not write either.
    assert await strat.is_ivr_gated("AAPL", 5.0) is False
    assert len(await db.get_implied_vol_history("AAPL")) == 2
