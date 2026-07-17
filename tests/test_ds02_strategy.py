"""Regression tests for DS02 (priority-7, 0 DTE implied-move iron condors).

Independent design, not a DS0 derivative — pins docs/ds02_spec.md against
stub broker / stub DB:

- Registration: DS02 in STRATEGY_PRIORITIES at 7 and constructed by
  make_strategies; **default-disabled** via DEFAULT_DISABLED_STRATEGIES —
  with no ``strategy_ds02_enabled`` row the agent must treat DS02 as OFF
  (on live, ``alpha_autonomous_live`` routes enabled strategies' entries
  straight to the broker, so a deploy must never arm a new strategy).
- Entries: the ATM-straddle implied-move level computation (spot ±
  move_mult × straddle arms the put/call side), the shared credit engine's
  delta/POP/credit gates applied on top of it, the 10:00–13:30 ET entry
  window, the one-shot-per-side-per-day gate, and the empty-own-watchlist-
  means-idle contract (no global fallback). Actions must be credit
  multilegs tagged ``HERMES_DS02``.
- Management: TP fires at 50% credit capture, SL fires at 2.5× entry
  credit (both through the base machinery), and the 15:45 ET blanket EOD
  flatten force-closes anything still open regardless of moneyness.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

from hermes.common import DEFAULT_DISABLED_STRATEGIES, STRATEGY_PRIORITIES
from hermes.service1_agent.core import IronCondorBuilder, MoneyManager
from hermes.service1_agent.strategies import CreditSpreads0DTE
from hermes.service1_agent.agent_settings import _read_overseer_settings

from ._stubs import StubBroker, StubDB, alias_db_namespaces, make_trade, _et_today

SYM = "QQQ"
_UTC = timezone.utc


def _utc_at_et(hour: int, minute: int = 0) -> datetime:
    """A UTC datetime whose ET wall-clock time is hour:minute today (EDT)."""
    d = _et_today()
    return datetime(d.year, d.month, d.day, hour + 4, minute, tzinfo=_UTC)


def _opt(expiry: str, opt_type: str, strike: float, bid: float, ask: float,
         delta: float) -> dict:
    yymmdd = datetime.strptime(expiry, "%Y-%m-%d").strftime("%y%m%d")
    pc = "P" if opt_type == "put" else "C"
    occ = f"{SYM}{yymmdd}{pc}{int(round(strike * 1000)):08d}"
    if opt_type == "put":
        delta = -abs(delta)
    return {"symbol": occ, "option_type": opt_type, "strike": float(strike),
            "bid": bid, "ask": ask, "greeks": {"delta": float(delta)}}


def _entry_chain(expiry: str, *, spot=100.0, straddle_half=0.50,
                 short_put_delta=0.10, short_call_delta=0.10,
                 short_bid=0.12, short_ask=0.16, long_bid=0.02, long_ask=0.06):
    """ATM straddle (100 strike, both legs mid ``straddle_half``) → implied
    move ``2 × straddle_half`` (default 1.00).

    move_mult 1.0 → put level 99.0, call level 101.0. Shorts snap to those
    strikes; longs sit one dollar further out; a third strike two dollars
    out on each side lets a widened move_mult still find a valid,
    non-colliding short/long pair. Default mids: short 0.14, long 0.04 →
    credit 0.10, exactly the 10%-of-width floor on a 1-wide.
    """
    return [
        _opt(expiry, "call", spot, straddle_half - 0.02, straddle_half + 0.02, 0.50),
        _opt(expiry, "put", spot, straddle_half - 0.02, straddle_half + 0.02, 0.50),
        _opt(expiry, "put", spot - 1.0, short_bid, short_ask, short_put_delta),
        _opt(expiry, "put", spot - 2.0, long_bid, long_ask, 0.05),
        _opt(expiry, "put", spot - 3.0, 0.005, 0.015, 0.03),
        _opt(expiry, "call", spot + 1.0, short_bid, short_ask, short_call_delta),
        _opt(expiry, "call", spot + 2.0, long_bid, long_ask, 0.05),
        _opt(expiry, "call", spot + 3.0, 0.005, 0.015, 0.03),
    ]


def _analysis(price: float = 100.0):
    # No key_levels needed — DS02 replaces them with the synthetic
    # implied-move level before delegating to the shared engine.
    return {
        "symbol": SYM, "current_price": price,
        "current_vol": 0.20, "avg_vol": 0.20,
        "key_levels": [],
        "samples": 100, "period": "3m",
    }


def _build_ds02(*, now_utc: datetime, expiry: str | None = None,
                analysis: dict | None = None, chain: list | None = None,
                db: StubDB | None = None, config: dict | None = None,
                spot: float = 100.0, leg_quotes: dict | None = None):
    expiry = expiry or _et_today().isoformat()
    broker = StubBroker(expirations=[expiry])
    broker.current_date = now_utc
    the_analysis = _analysis(spot) if analysis is None else analysis
    broker.analyze_symbol = lambda symbol, period="3m": dict(the_analysis)
    the_chain = _entry_chain(expiry, spot=spot) if chain is None else chain
    broker.get_option_chains = lambda symbol, exp: list(the_chain)

    base_quote: dict = {"last": spot, "bid": spot - 0.05, "ask": spot + 0.05}
    lq = leg_quotes or {}

    def _get_quote(symbols):
        out = []
        for s_ in symbols.split(","):
            s_ = s_.strip()
            out.append({"symbol": s_, **lq[s_]} if s_ in lq
                       else {"symbol": s_, **base_quote})
        return out
    broker.get_quote = _get_quote

    db = db or StubDB()
    db.set_watchlist("DS02", [SYM])
    cfg = {"ds02_max_lots": 1, "ds02_target_lots": 1}
    cfg.update(config or {})
    mm = MoneyManager(broker, db, cfg)
    s = CreditSpreads0DTE(broker=broker, db=db, money_manager=mm,
                          ic_builder=IronCondorBuilder(mm), config=cfg,
                          dry_run=False, overseer=None)
    return s, broker, db


async def _quiet_macro(db: StubDB) -> None:
    """Pin the macro gate off — the real FOMC/CPI calendar must not decide
    whether an entry test passes depending on the day it runs."""
    await db.settings.set_setting("ds02_macro_blackout_days", "0")


# ── registration & default-disabled ──────────────────────────────────────────
def test_ds02_registered_at_priority_7():
    assert STRATEGY_PRIORITIES["DS02"] == 7
    assert CreditSpreads0DTE.PRIORITY == 7
    assert CreditSpreads0DTE.NAME == "DS02"
    from hermes.service1_agent.agent_construction import make_strategies
    common = dict(broker=None, db=None, money_manager=None, ic_builder=None,
                  config={}, overseer=None, dry_run=True)
    names = {type(s).__name__ for s in make_strategies(common)}
    assert "CreditSpreads0DTE" in names


def test_ds02_in_default_disabled_set():
    assert "DS02" in DEFAULT_DISABLED_STRATEGIES


async def test_ds02_defaults_to_disabled_when_setting_absent():
    db = AsyncMock()
    alias_db_namespaces(db)          # get_setting → None for every key
    out = await _read_overseer_settings(db, {})
    assert out["strategy_enabled"]["DS02"] is False
    assert out["strategy_enabled"]["DS0"] is True   # everyone else stays on


async def test_ds02_enable_setting_arms_it():
    db = AsyncMock()
    alias_db_namespaces(db)
    db.get_setting.side_effect = (
        lambda key: "true" if key == "strategy_ds02_enabled" else None)
    out = await _read_overseer_settings(db, {})
    assert out["strategy_enabled"]["DS02"] is True


# ── implied move ─────────────────────────────────────────────────────────────
def test_implied_move_is_atm_straddle_price():
    expiry = _et_today().isoformat()
    chain = _entry_chain(expiry, spot=100.0, straddle_half=0.60)
    move = CreditSpreads0DTE._implied_move(chain, 100.0)
    assert move == 1.2   # 0.60 call mid + 0.60 put mid


def test_implied_move_none_on_missing_leg():
    expiry = _et_today().isoformat()
    chain = [o for o in _entry_chain(expiry) if o["option_type"] != "put"]
    assert CreditSpreads0DTE._implied_move(chain, 100.0) is None


# ── entries ──────────────────────────────────────────────────────────────────
async def test_both_sides_arm_at_the_implied_move_boundary():
    s, _, db = _build_ds02(now_utc=_utc_at_et(11, 0))
    await _quiet_macro(db)
    actions = await s.execute_entries([SYM])
    assert len(actions) == 2
    by_side = {a.strategy_params["side_type"]: a for a in actions}
    put, call = by_side["put"], by_side["call"]
    for a in (put, call):
        assert a.order_type == "credit"
        assert a.tag == "HERMES_DS02"
        assert a.dte == 0
        assert {leg["side"] for leg in a.legs} == {"sell_to_open", "buy_to_open"}
    # Implied move = 1.00 (0.50 call mid + 0.50 put mid); move_mult 1.0 →
    # put level 99, call level 101 → shorts snap to the 99/101 strikes.
    assert "00099000" in put.strategy_params["short_leg"]
    assert "00101000" in call.strategy_params["short_leg"]


async def test_move_mult_widens_the_boundary():
    # move_mult 2.0 → levels at 98/102, one strike further from the money
    # than the default 99/101 — the strike selection must follow the wider
    # boundary (and find a non-colliding long leg one further out at 97/103)
    # rather than collapsing back to the nearest-the-money pair.
    s, _, db = _build_ds02(now_utc=_utc_at_et(11, 0),
                           config={"ds02_move_mult": 2.0,
                                   "ds02_min_credit_pct": 0.02})
    await _quiet_macro(db)
    actions = await s.execute_entries([SYM])
    assert len(actions) == 2
    by_side = {a.strategy_params["side_type"]: a for a in actions}
    assert "00098000" in by_side["put"].strategy_params["short_leg"]
    assert "00102000" in by_side["call"].strategy_params["short_leg"]


async def test_entry_window_gates():
    for hour, minute in ((9, 30), (13, 30), (15, 0)):
        s, _, db = _build_ds02(now_utc=_utc_at_et(hour, minute))
        await _quiet_macro(db)
        assert await s.execute_entries([SYM]) == [], f"armed at {hour}:{minute:02d} ET"


async def test_one_shot_per_side_per_day():
    s, _, db = _build_ds02(now_utc=_utc_at_et(11, 0))
    await _quiet_macro(db)
    db.set_closed_trades("DS02", [make_trade(
        "DS02", SYM, side_type="put", short_strike=99.0, long_strike=98.0,
        width=1.0, entry_credit=0.10, days_to_expiry=0)])
    actions = await s.execute_entries([SYM])
    assert {a.strategy_params["side_type"] for a in actions} == {"call"}


async def test_min_credit_floor_rejects_cheap_spreads():
    expiry = _et_today().isoformat()
    chain = _entry_chain(expiry, short_bid=0.08, short_ask=0.12,  # mid 0.10
                         long_bid=0.02, long_ask=0.06)            # mid 0.04
    # credit 0.06 < 0.10 floor on a 1-wide
    s, _, db = _build_ds02(now_utc=_utc_at_et(11, 0), chain=chain)
    await _quiet_macro(db)
    assert await s.execute_entries([SYM]) == []


async def test_short_delta_cap_rejects_near_money():
    expiry = _et_today().isoformat()
    chain = _entry_chain(expiry, short_put_delta=0.35, short_call_delta=0.35)
    s, _, db = _build_ds02(now_utc=_utc_at_et(11, 0), chain=chain)
    await _quiet_macro(db)
    assert await s.execute_entries([SYM]) == []


async def test_no_implied_move_skips_symbol():
    expiry = _et_today().isoformat()
    chain = [o for o in _entry_chain(expiry) if o["option_type"] != "call"]
    s, _, db = _build_ds02(now_utc=_utc_at_et(11, 0), chain=chain)
    await _quiet_macro(db)
    assert await s.execute_entries([SYM]) == []


async def test_empty_own_watchlist_means_idle():
    s, _, db = _build_ds02(now_utc=_utc_at_et(11, 0))
    await _quiet_macro(db)
    db.set_watchlist("DS02", [])
    # The engine would pass the global default list here — DS02 must not
    # trade it.
    assert await s.execute_entries([SYM, "SPY", "IWM"]) == []


# ── management ────────────────────────────────────────────────────────────────
def _open_ds02_trade(**kw):
    defaults = dict(side_type="put", short_strike=99.0, long_strike=98.0,
                    width=1.0, entry_credit=0.10, days_to_expiry=0, lots=1)
    defaults.update(kw)
    return make_trade("DS02", SYM, **defaults)


async def test_tp_fires_at_half_credit_captured():
    trade = _open_ds02_trade()
    leg_quotes = {
        trade["short_leg"]: {"bid": 0.03, "ask": 0.05},   # mid 0.04
        trade["long_leg"]:  {"bid": 0.005, "ask": 0.015},  # mid 0.01 → debit 0.03
    }
    s, _, db = _build_ds02(now_utc=_utc_at_et(13, 0), leg_quotes=leg_quotes)
    db.set_open_trades("DS02", [trade])
    actions = await s.manage_positions()
    assert len(actions) == 1
    assert actions[0].tag == "HERMES_DS02_CLOSE_TP"


async def test_sl_fires_at_multiple_of_credit():
    trade = _open_ds02_trade()
    leg_quotes = {
        trade["short_leg"]: {"bid": 0.32, "ask": 0.36},   # mid 0.34
        trade["long_leg"]:  {"bid": 0.04, "ask": 0.06},   # mid 0.05 → debit 0.29
    }
    s, _, db = _build_ds02(now_utc=_utc_at_et(13, 0), leg_quotes=leg_quotes)
    db.set_open_trades("DS02", [trade])
    actions = await s.manage_positions()
    assert len(actions) == 1
    assert actions[0].tag == "HERMES_DS02_CLOSE_SL"


async def test_holds_between_tp_and_sl_bands():
    trade = _open_ds02_trade()
    leg_quotes = {
        trade["short_leg"]: {"bid": 0.10, "ask": 0.12},   # mid 0.11
        trade["long_leg"]:  {"bid": 0.02, "ask": 0.04},   # mid 0.03 → debit 0.08
    }
    s, _, db = _build_ds02(now_utc=_utc_at_et(13, 0), leg_quotes=leg_quotes)
    db.set_open_trades("DS02", [trade])
    assert await s.manage_positions() == []


async def test_eod_flatten_closes_whatever_survives():
    trade = _open_ds02_trade()
    leg_quotes = {
        trade["short_leg"]: {"bid": 0.10, "ask": 0.12},
        trade["long_leg"]:  {"bid": 0.02, "ask": 0.04},   # debit 0.08 — between TP/SL
    }
    s, _, db = _build_ds02(now_utc=_utc_at_et(15, 46), leg_quotes=leg_quotes)
    db.set_open_trades("DS02", [trade])
    actions = await s.manage_positions()
    assert len(actions) == 1
    assert actions[0].tag == "HERMES_DS02_CLOSE_EOD-FLATTEN"


async def test_eod_flatten_does_not_double_close_a_tp_trade():
    trade = _open_ds02_trade()
    leg_quotes = {
        trade["short_leg"]: {"bid": 0.03, "ask": 0.05},
        trade["long_leg"]:  {"bid": 0.005, "ask": 0.015},  # debit 0.03 → TP
    }
    s, _, db = _build_ds02(now_utc=_utc_at_et(15, 46), leg_quotes=leg_quotes)
    db.set_open_trades("DS02", [trade])
    actions = await s.manage_positions()
    assert len(actions) == 1
    assert actions[0].tag == "HERMES_DS02_CLOSE_TP"


async def test_no_flatten_before_close_time():
    trade = _open_ds02_trade()
    leg_quotes = {
        trade["short_leg"]: {"bid": 0.10, "ask": 0.12},
        trade["long_leg"]:  {"bid": 0.02, "ask": 0.04},
    }
    s, _, db = _build_ds02(now_utc=_utc_at_et(14, 0), leg_quotes=leg_quotes)
    db.set_open_trades("DS02", [trade])
    assert await s.manage_positions() == []
