import pytest
import math
from datetime import datetime, date
from unittest.mock import MagicMock
from hermes.service1_agent.trade_action import TradeAction
from hermes.service1_agent.risk_engine import PortfolioRiskEngine
from ._stubs import StubDB, StubBroker


def _action(strategy_id: str, symbol: str, quantity: int, width: float, price: float, side: str = "sell") -> TradeAction:
    return TradeAction(
        strategy_id=strategy_id,
        symbol=symbol,
        order_class="multileg",
        legs=[
            {"option_symbol": f"{symbol}250620P00090000", "side": "sell_to_open", "quantity": quantity},
            {"option_symbol": f"{symbol}250620P00085000", "side": "buy_to_open",  "quantity": quantity},
        ],
        price=price,
        side=side,
        quantity=quantity,
        order_type="credit",
        tag=f"HERMES_{strategy_id}",
        strategy_params={"side_type": "put", "pop": 0.85},
        expiry="2025-06-20",
        width=width
    )


@pytest.mark.asyncio
async def test_greeks_aggregation_and_persistence():
    db = StubDB()
    # Seed 2 positions in StubBroker
    # Short premium put spreads (short P00090000, long P00085000)
    pos = [
        {"symbol": "AAPL250620P00090000", "quantity": -2.0},  # short 2 contracts
        {"symbol": "AAPL250620P00085000", "quantity": 2.0},   # long 2 contracts
    ]
    broker = StubBroker(option_buying_power=100000.0, positions=pos)

    # Let's mock broker.get_quote to return specific quotes and greeks for these options
    original_get_quote = broker.get_quote
    def mock_get_quote(symbols: str):
        parts = symbols.split(",")
        res = []
        for s in parts:
            s_clean = s.strip()
            if s_clean == "AAPL250620P00090000":
                res.append({
                    "symbol": s_clean, "last": 5.0, "bid": 4.90, "ask": 5.10,
                    "greeks": {"delta": -0.45, "vega": 0.25, "theta": -0.05}
                })
            elif s_clean == "AAPL250620P00085000":
                res.append({
                    "symbol": s_clean, "last": 2.0, "bid": 1.90, "ask": 2.10,
                    "greeks": {"delta": -0.20, "vega": 0.15, "theta": -0.02}
                })
            else:
                # Underlying quote
                res.append({"symbol": s_clean, "last": 100.0, "bid": 99.95, "ask": 100.05})
        return res

    broker.get_quote = mock_get_quote

    risk_engine = PortfolioRiskEngine(broker, db, {})
    await risk_engine.record_portfolio_greeks()

    # Net Delta: -2 * -0.45 * 100 + 2 * -0.20 * 100 = 90 - 40 = 50.0
    # Net Vega: -2 * 0.25 * 100 + 2 * 0.15 * 100 = -50 + 30 = -20.0
    # Net Theta: -2 * -0.05 * 100 + 2 * -0.02 * 100 = 10 - 4 = 6.0
    latest = await db.get_latest_greeks_snapshot()
    assert latest is not None
    assert latest["net_delta"] == pytest.approx(50.0)
    assert latest["net_vega"] == pytest.approx(-20.0)
    assert latest["net_theta"] == pytest.approx(6.0)


@pytest.mark.asyncio
async def test_ceiling_scaling_down_to_fit():
    db = StubDB()
    # Set custom ceilings (Vega max 10.0, Short Delta max 200.0)
    # Note: max vega limit is scaled by total_equity / 100000.0
    # With total_equity = 100,000, max_vega = 10.0
    await db.settings.set_setting("portfolio_max_net_vega", "10.0")
    await db.settings.set_setting("portfolio_max_short_delta", "200.0")

    # Initial positions: None (so running vega=0, running delta=0)
    broker = StubBroker(option_buying_power=100000.0, positions=[])

    # Candidate action: CS75 on AAPL, quantity 5.
    action = _action("CS75", "AAPL", 5, 5.0, 1.0)

    # Let's mock broker.get_quote to define AAPL legs greeks
    # Short leg: delta = -0.30, vega = 0.15
    # Long leg: delta = -0.10, vega = 0.05
    # Per lot Greeks:
    #   Vega: -1 * 0.15 * 100 + 1 * 0.05 * 100 = -15.0 + 5.0 = -10.0
    #   Delta: -1 * -0.30 * 100 + 1 * -0.10 * 100 = 30.0 - 10.0 = 20.0
    # For quantity = 5 lots, Vega would be -50.0 (absolute value 50.0 > 10.0 limit).
    # To fit within Vega limit of 10.0, it must scale down to 1 lot (Vega = -10.0).
    def mock_get_quote(symbols: str):
        parts = symbols.split(",")
        res = []
        for s in parts:
            s_clean = s.strip()
            if "AAPL" in s_clean and "P" in s_clean:
                if "90000" in s_clean:
                    res.append({
                        "symbol": s_clean, "last": 5.0, "bid": 4.90, "ask": 5.10,
                        "greeks": {"delta": -0.30, "vega": 0.15, "theta": -0.05}
                    })
                else:
                    res.append({
                        "symbol": s_clean, "last": 2.0, "bid": 1.90, "ask": 2.10,
                        "greeks": {"delta": -0.10, "vega": 0.05, "theta": -0.02}
                    })
            else:
                res.append({"symbol": s_clean, "last": 100.0, "bid": 99.95, "ask": 100.05})
        return res

    broker.get_quote = mock_get_quote

    risk_engine = PortfolioRiskEngine(broker, db, {"cs75_max_lots": 10})
    validated = await risk_engine.evaluate_and_scale([action])

    assert len(validated) == 1
    assert validated[0].quantity == 1
    # Check that a log is written
    assert any("[RISK CONTROL] Scaled AAPL 5->1 lots to fit portfolio ceiling." in log for log in db.logs)


@pytest.mark.asyncio
async def test_ceiling_dropping_at_1_lot():
    db = StubDB()
    # Set Vega ceiling to 5.0
    await db.settings.set_setting("portfolio_max_net_vega", "5.0")

    # Initial positions: None
    broker = StubBroker(option_buying_power=100000.0, positions=[])

    # Candidate action: quantity 2
    action = _action("CS75", "AAPL", 2, 5.0, 1.0)

    # Per lot Vega = -10.0 (absolute value 10.0 > 5.0 limit).
    # Even 1 lot breaches the ceiling, so the entry must be dropped.
    def mock_get_quote(symbols: str):
        parts = symbols.split(",")
        res = []
        for s in parts:
            s_clean = s.strip()
            if "AAPL" in s_clean and "P" in s_clean:
                if "90000" in s_clean:
                    res.append({
                        "symbol": s_clean, "greeks": {"delta": -0.30, "vega": 0.15, "theta": -0.05}
                    })
                else:
                    res.append({
                        "symbol": s_clean, "greeks": {"delta": -0.10, "vega": 0.05, "theta": -0.02}
                    })
            else:
                res.append({"symbol": s_clean, "last": 100.0})
        return res

    broker.get_quote = mock_get_quote

    risk_engine = PortfolioRiskEngine(broker, db, {"cs75_max_lots": 10})
    validated = await risk_engine.evaluate_and_scale([action])

    assert len(validated) == 0
    # Check that a warning log is written
    assert any("[RISK VIOLATION] AAPL dropped: even 1 lot breaches portfolio ceiling." in log for log in db.logs)


@pytest.mark.asyncio
async def test_regime_based_gross_scaling():
    db = StubDB()
    # Configure regime settings
    await db.settings.set_setting("regime_scale_iv_pct", "50.0")
    await db.settings.set_setting("regime_gross_mult", "0.5")

    # Seed IV history so that current IV is high (e.g. 90th percentile)
    # Let's mock db.timeseries.get_implied_vol_history to return past values:
    # 0.20, 0.21, 0.22, 0.23 (current will be 0.30)
    db._implied_vols["AAPL"] = [
        (date(2026, 7, 1), 0.20),
        (date(2026, 7, 2), 0.21),
        (date(2026, 7, 3), 0.22),
        (date(2026, 7, 4), 0.23),
    ]
    
    # We also mock the fetch_current_atm_iv function to return 0.30
    # To mock fetch_current_atm_iv, we can patch it or mock it.
    # Since fetch_current_atm_iv is imported from iv_tracker:
    # we can mock it directly. Let's patch the import or use a side effect.
    import hermes.service1_agent.iv_tracker as iv_tracker
    original_fetch = iv_tracker.fetch_current_atm_iv
    async def mock_fetch(broker, symbol, dt):
        return 0.30
    iv_tracker.fetch_current_atm_iv = mock_fetch

    try:
        broker = StubBroker(option_buying_power=100000.0, positions=[])
        action = _action("CS75", "AAPL", 4, 5.0, 1.0)

        risk_engine = PortfolioRiskEngine(broker, db, {"cs75_max_lots": 10})
        validated = await risk_engine.evaluate_and_scale([action])

        # 4 lots * 0.5 = 2 lots
        assert len(validated) == 1
        assert validated[0].quantity == 2
    finally:
        iv_tracker.fetch_current_atm_iv = original_fetch


@pytest.mark.asyncio
async def test_missing_greeks_conservative_fallback():
    db = StubDB()
    # Seed short position with NO greeks
    pos = [
        {"symbol": "AAPL250620P00090000", "quantity": -1.0},
    ]
    broker = StubBroker(option_buying_power=100000.0, positions=pos)

    # Mock get_quote to return quote with NO greeks, bid/ask/mid_iv missing or fails BS
    def mock_get_quote(symbols: str):
        return [{"symbol": s.strip(), "last": 100.0} for s in symbols.split(",")]

    broker.get_quote = mock_get_quote

    risk_engine = PortfolioRiskEngine(broker, db, {})
    net_delta, net_vega, net_theta = await risk_engine._calculate_current_greeks(pos)

    # Since it's a short put with no Greeks/IV, conservative fallback is:
    # max_vega = spot * 0.3989 * sqrt(T)
    # T = dte / 365. dte = (2025-06-20 - today).
    # Since dte will be calculated dynamically, let's check:
    # vega should be qty * max_vega * 100.0 (< 0 since qty < 0)
    # delta should be 0.0 (short put delta is assumed to be 0.0 / no offset under conservative fallback)
    assert net_delta == 0.0
    assert net_vega < 0.0


@pytest.mark.asyncio
async def test_default_tunables_are_inert():
    db = StubDB()
    # Do not set any custom settings (so defaults: max vega = 999999.0, max short delta = 999999.0, regime gross mult = 1.0)
    broker = StubBroker(option_buying_power=100000.0, positions=[])

    action = _action("CS75", "AAPL", 4, 5.0, 1.0)

    risk_engine = PortfolioRiskEngine(broker, db, {"cs75_max_lots": 10})
    validated = await risk_engine.evaluate_and_scale([action])

    # Should NOT scale or drop — matches baseline behavior (4 lots)
    assert len(validated) == 1
    assert validated[0].quantity == 4
