"""Replay harness tests — stub-DB pattern, no live DB or broker required.

Covers:
* the MockBroker fill-side regression the harness work surfaced
  (``buy_to_open`` legs were routed through the sell branch),
* the data source's lookahead guard,
* the in-memory ReplayDB trade round-trip,
* first-tick parity: entries recorded through the full engine match what the
  same strategy code proposes when called directly, and
* end-to-end determinism: two runs over the same fixture window produce
  identical trades, fills and reports.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone

import pandas as pd
import pytest

from hermes.replay import (
    ReplayConfig, ReplayDataSource, ReplayHarness, build_report,
)
from hermes.service1_agent.core import TradeAction
from hermes.service1_agent.mock_broker import MockBroker


# ---------------------------------------------------------------------------
# Fixture data — a fully deterministic sine-drift path over business days.
# ---------------------------------------------------------------------------
FIXTURE_END = "2026-06-12"


def make_daily_frame(n: int = 140, base: float = 100.0,
                     end: str = FIXTURE_END) -> pd.DataFrame:
    idx = pd.bdate_range(end=end, periods=n)
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
        })
    return pd.DataFrame(rows)


def make_fixture(symbol: str = "SPY", replay_days: int = 8):
    frame = make_daily_frame()
    data = ReplayDataSource.from_frames({symbol: frame})
    idx = pd.to_datetime(frame["ts"])
    start = idx.iloc[-replay_days].date()
    end = idx.iloc[-1].date()
    return data, start, end


def make_harness(strategies=("CS75",), replay_days: int = 8,
                 symbol: str = "SPY") -> ReplayHarness:
    data, start, end = make_fixture(symbol, replay_days)
    cfg = ReplayConfig(symbols=[symbol], start=start, end=end,
                       strategies=list(strategies))
    return ReplayHarness(data, cfg)


# ---------------------------------------------------------------------------
# MockBroker fill-side regression
# ---------------------------------------------------------------------------
async def test_mock_broker_buy_to_open_leg_is_bought():
    """A buy_to_open leg must reduce the net credit, not add to it."""
    broker = MockBroker({})
    action = TradeAction(
        strategy_id="CS75", symbol="AAPL", order_class="multileg",
        legs=[
            {"option_symbol": "AAPL260620P00150000", "side": "sell_to_open", "quantity": 1},
            {"option_symbol": "AAPL260620P00145000", "side": "buy_to_open", "quantity": 1},
        ],
        price=None, side="sell", quantity=1, order_type="credit",
    )
    result = await broker.place_order_from_action(action)
    net = result["raw_response"]["simulated_net_price"]

    sm, ss = broker._leg_quote("AAPL260620P00150000")
    lm, ls = broker._leg_quote("AAPL260620P00145000")
    s_slip = broker._leg_slippage("AAPL260620P00150000", ss)
    l_slip = broker._leg_slippage("AAPL260620P00145000", ls)
    expected = round(round(sm - s_slip, 2) - round(lm + l_slip, 2), 2)
    assert net == pytest.approx(expected)
    # The buggy both-legs-sold net would have been the sum instead.
    assert net < round(sm, 2) + round(lm, 2)


# ---------------------------------------------------------------------------
# Data source lookahead guard
# ---------------------------------------------------------------------------
def test_datasource_lookahead_guard():
    data, start, end = make_fixture()
    frame = data.daily["SPY"]
    day = frame.index[-1].date()
    bar = frame.iloc[-1]

    morning = datetime(day.year, day.month, day.day, 14, 35)   # 10:35 ET in UTC
    late = datetime(day.year, day.month, day.day, 19, 45)      # 15:45 ET in UTC

    completed = data.completed_daily("SPY", morning)
    assert all(d.date() < day for d in completed.index)

    assert data.spot("SPY", morning) == pytest.approx(float(bar["open"]))
    assert data.spot("SPY", late) == pytest.approx(float(bar["close"]))

    days = data.trading_days(start, end)
    assert days[0] == start and days[-1] == end
    assert all(d.weekday() < 5 for d in days)


# ---------------------------------------------------------------------------
# ReplayDB trade round-trip
# ---------------------------------------------------------------------------
async def test_replaydb_trade_roundtrip():
    from hermes.clock import SimulatedClock
    from hermes.replay import ReplayDB

    clock = SimulatedClock(datetime(2026, 6, 8, 14, 35))
    db = ReplayDB(clock)
    entry = TradeAction(
        strategy_id="CS75", symbol="SPY", order_class="multileg",
        legs=[
            {"option_symbol": "SPY260612P00095000", "side": "sell_to_open", "quantity": 2},
            {"option_symbol": "SPY260612P00090000", "side": "buy_to_open", "quantity": 2},
        ],
        price=1.50, side="sell", quantity=1, order_type="credit",
        expiry="2026-06-12", width=5.0, tag="HERMES_CS75",
        strategy_params={"side_type": "put", "pop": 0.8},
    )
    await db.trades.record_pending_order(entry)
    assert await db.trades.count_pending_orders("CS75", "SPY", "put", "2026-06-12") == 2

    await db.trades.record_order_response(
        entry, {"order": {"id": "SIM-1", "status": "filled"}})
    assert await db.trades.count_pending_orders("CS75", "SPY", "put", "2026-06-12") == 0
    open_rows = await db.trades.open_trades("CS75")
    assert len(open_rows) == 1
    row = open_rows[0]
    assert row["short_leg"] == "SPY260612P00095000"
    assert row["long_leg"] == "SPY260612P00090000"
    assert row["entry_credit"] == 1.50 and row["lots"] == 2
    assert await db.trades.count_open_contracts("CS75", "SPY", "put", "2026-06-12") == 2

    clock.set_time(datetime(2026, 6, 10, 18, 0))
    close = TradeAction(
        strategy_id="CS75", symbol="SPY", order_class="multileg",
        legs=[
            {"option_symbol": "SPY260612P00095000", "side": "buy_to_close", "quantity": 2},
            {"option_symbol": "SPY260612P00090000", "side": "sell_to_close", "quantity": 2},
        ],
        price=0.75, side="buy", quantity=1, order_type="debit",
        tag="HERMES_CS75_CLOSE_TP-50",
        strategy_params={"trade_id": row["id"], "close_reason": "TP-50", "side_type": "put"},
    )
    await db.trades.record_pending_order(close)
    # Pure close flips the row to CLOSING inside the same submission.
    assert (await db.trades.closing_trades("CS75"))[0]["id"] == row["id"]

    await db.trades.close_trade_from_action(
        close, {"order": {"id": "SIM-2", "status": "filled"}})
    closed = db.closed_trade_rows()
    assert len(closed) == 1
    # (1.50 credit − 0.75 debit) × 2 lots × 100
    assert closed[0]["pnl"] == pytest.approx(150.0)
    assert closed[0]["close_reason"] == "TP-50"
    assert await db.trades.latest_closed_trade_time("CS75", "SPY") == clock.utc_now()


# ---------------------------------------------------------------------------
# First-tick parity: engine-recorded entries == direct strategy proposals
# ---------------------------------------------------------------------------
async def test_replay_entries_match_direct_strategy_run(monkeypatch):
    monkeypatch.setenv("HERMES_ALLOW_OFFHOURS_TRADES", "true")

    engine_h = make_harness(("CS75",))
    direct_h = make_harness(("CS75",))

    from hermes.replay.harness import _tick_instants
    t0 = _tick_instants(engine_h.cfg.start, engine_h.cfg.tick_times_et)[0]

    for h in (engine_h, direct_h):
        h.clock.set_time(t0)
        h.broker.set_time(t0)

    await engine_h.engine.tick(["SPY"])
    recorded = {(t["short_leg"], t["long_leg"], t["entry_credit"], t["lots"])
                for t in engine_h.db.all_trade_rows()}

    strategy = direct_h.engine.strategies[0]
    assert strategy.NAME == "CS75"
    proposals = await strategy.execute_entries(["SPY"])
    proposed = set()
    for a in proposals:
        short = next(l["option_symbol"] for l in a.legs if "sell" in l["side"])
        long_ = next(l["option_symbol"] for l in a.legs if "buy" in l["side"])
        lots = max(int(l["quantity"]) for l in a.legs)
        proposed.add((short, long_, float(a.price), lots))

    assert recorded == proposed


# ---------------------------------------------------------------------------
# Full-cycle: injected trade is managed to a close with real P&L
# ---------------------------------------------------------------------------
async def test_injected_trade_full_cycle(monkeypatch):
    monkeypatch.setenv("HERMES_ALLOW_OFFHOURS_TRADES", "true")
    h = make_harness(("CS75",))

    from hermes.replay.harness import _tick_instants
    days = h.data.trading_days(h.cfg.start, h.cfg.end)
    t0 = _tick_instants(days[0], h.cfg.tick_times_et)[0]
    h.clock.set_time(t0)
    h.broker.set_time(t0)

    expiries = await h.broker.get_option_expirations("SPY")
    expiry = next(e for e in expiries
                  if 3 <= (datetime.strptime(e, "%Y-%m-%d").date() - days[0]).days
                  and datetime.strptime(e, "%Y-%m-%d").date() < days[-1])
    chain = await h.broker.get_option_chains("SPY", expiry)
    puts = [o for o in chain if o["option_type"] == "put"]
    short = min(puts, key=lambda o: abs(abs(o["greeks"]["delta"]) - 0.45))
    long_ = min((o for o in puts if o["strike"] < short["strike"]),
                key=lambda o: abs(o["strike"] - (short["strike"] - 5.0)))
    credit = round(((short["bid"] + short["ask"]) / 2)
                   - ((long_["bid"] + long_["ask"]) / 2), 2)
    assert credit > 0

    action = TradeAction(
        strategy_id="CS75", symbol="SPY", order_class="multileg",
        legs=[
            {"option_symbol": short["symbol"], "side": "sell_to_open", "quantity": 1},
            {"option_symbol": long_["symbol"], "side": "buy_to_open", "quantity": 1},
        ],
        price=credit, side="sell", quantity=1, order_type="credit",
        tag="HERMES_CS75", expiry=expiry,
        width=abs(short["strike"] - long_["strike"]),
        strategy_params={"short_leg": short["symbol"], "long_leg": long_["symbol"],
                         "side_type": "put", "pop": 0.80},
    )
    await h.engine._execute_or_queue(action, "entry")
    assert len(await h.db.trades.open_trades("CS75")) == 1

    for day in days:
        for sim_dt in _tick_instants(day, h.cfg.tick_times_et):
            if sim_dt <= t0:
                continue
            h.clock.set_time(sim_dt)
            h.broker.set_time(sim_dt)
            h.broker.settle_expired()
            h.db.settle_expired(h.broker._today_et(), h._settlement_value)
            await h.engine.tick(["SPY"])

    # Final settlement pass one day beyond the window flushes anything
    # still open at its expiry.
    final_dt = _tick_instants(days[-1], h.cfg.tick_times_et)[-1]
    h.clock.set_time(final_dt + pd.Timedelta(days=3).to_pytimedelta())
    h.broker.set_time(h.clock.utc_now())
    h.broker.settle_expired()
    h.db.settle_expired(h.broker._today_et(), h._settlement_value)

    rows = [t for t in h.db.all_trade_rows() if t["tag"] == "HERMES_CS75"]
    injected = rows[0]
    assert injected["status"] == "CLOSED"
    assert injected["pnl"] is not None
    assert injected["close_reason"] in {"TP-50", "TP-75", "SL", "EXPIRED",
                                        "TIME-EXIT", "MANAGED_CLOSE"}

    report = build_report(h.db.all_trade_rows(), [])
    assert report["strategies"]["CS75"]["trades_resolved"] >= 1
    total = sum(float(t["pnl"]) for t in h.db.all_trade_rows()
                if t["status"] == "CLOSED" and t["pnl"] is not None)
    assert report["overall"]["total_pnl"] == pytest.approx(round(total, 2))


# ---------------------------------------------------------------------------
# Determinism: two identical runs → identical trades, fills, report
# ---------------------------------------------------------------------------
async def test_replay_run_is_deterministic():
    def signature(result):
        return {
            "trades": [(t["strategy_id"], t["symbol"], t["short_leg"],
                        t["long_leg"], t["lots"], t["entry_credit"],
                        t["entry_debit"], t["status"], t["exit_price"],
                        t["pnl"], t["close_reason"]) for t in result.trades],
            "fills": [(f["order_id"], f["strategy_id"], f["tag"], f["net"],
                       f["price"]) for f in result.fills],
            "equity": [(pt["total"], tuple(sorted(pt["per_strategy"].items())))
                       for pt in result.equity_curve],
            "report": result.report,
            "ticks": result.ticks,
        }

    r1 = await make_harness(("CS75", "CS7")).run()
    r2 = await make_harness(("CS75", "CS7")).run()

    assert r1.ticks == 24  # 8 fixture days × 3 tick times
    assert signature(r1) == signature(r2)


async def test_replay_offhours_env_restored():
    """run() must not leave the off-hours override behind."""
    import os
    assert os.environ.get("HERMES_ALLOW_OFFHOURS_TRADES") is None or True
    before = os.environ.get("HERMES_ALLOW_OFFHOURS_TRADES")
    await make_harness(("CS75",), replay_days=2).run()
    assert os.environ.get("HERMES_ALLOW_OFFHOURS_TRADES") == before
