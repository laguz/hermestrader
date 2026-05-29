from datetime import datetime
from hermes.service1_agent.core import TradeAction
from hermes.broker.mock_engine import MockAsyncTradierBroker

async def test_mock_engine_equity_orders():
    # 1. Setup Mock Broker
    broker = MockAsyncTradierBroker()
    
    # Verify initial state
    balances = await broker.get_account_balances()
    assert balances["cash"] == 100000.0
    assert len(await broker.get_positions()) == 0

    # 2. Place an equity buy market order (10 shares of AAPL at spot 150)
    broker.quotes["AAPL"] = {"symbol": "AAPL", "last": 150.0}
    action_buy = TradeAction(
        strategy_id="TEST",
        symbol="AAPL",
        order_class="equity",
        legs=[],
        price=150.0,
        side="buy",
        quantity=10,
        order_type="market"
    )
    
    res = await broker.place_order_from_action(action_buy)
    assert res["status"] == "ok"
    
    # Verify balance and position (no commissions/slippage defaults since we just test logic)
    # Default config has commission_per_contract=0.35, slippage_pct=0.05
    # For equity, commissions = 0.35 * 1 * 10 * 2 = 7.0, slippage = 150 * 0.05 * 10 = 75.0
    # Cash change = 1500 + 7.0 + 75.0 = 1582.0
    balances = await broker.get_account_balances()
    assert balances["cash"] == 100000.0 - 1582.0
    
    positions = await broker.get_positions()
    assert len(positions) == 1
    assert positions[0]["symbol"] == "AAPL"
    assert positions[0]["quantity"] == 10

    # 3. Sell 5 shares
    action_sell = TradeAction(
        strategy_id="TEST",
        symbol="AAPL",
        order_class="equity",
        legs=[],
        price=160.0,
        side="sell",
        quantity=5,
        order_type="market"
    )
    # Update spot price to 160
    broker.quotes["AAPL"] = {"symbol": "AAPL", "last": 160.0}
    await broker.place_order_from_action(action_sell)
    
    positions = await broker.get_positions()
    assert len(positions) == 1
    assert positions[0]["quantity"] == 5


async def test_mock_engine_option_spread_fill_and_touch():
    broker = MockAsyncTradierBroker(config={
        "commission_per_contract": 0.0,
        "slippage_pct": 0.0
    })
    
    # 1. Place credit spread
    # Sell AAPL 150 Put, Buy AAPL 145 Put. Spot is 155. Credit is 1.50.
    spot = 155.0
    broker.tick_underlying("AAPL", spot, spot, spot, datetime(2025, 4, 1))
    
    action = TradeAction(
        strategy_id="TEST",
        symbol="AAPL",
        order_class="multileg",
        legs=[
            {"option_symbol": "AAPL250418P00150000", "side": "sell_to_open", "quantity": 1},
            {"option_symbol": "AAPL250418P00145000", "side": "buy_to_open", "quantity": 1}
        ],
        price=1.50,
        side="sell",
        quantity=1,
        order_type="credit"
    )
    
    await broker.place_order_from_action(action)
    
    # Cash should increase by credit * 100 = 150.0
    balances = await broker.get_account_balances()
    assert balances["cash"] == 100000.0 + 150.0
    
    # Verify open positions
    positions = await broker.get_positions()
    assert len(positions) == 2
    
    # 2. Tick underlying price below short strike (low = 148.0)
    broker.tick_underlying("AAPL", 148.0, 149.0, 147.0, datetime(2025, 4, 2))
    
    # Verify positions are removed (mock engine executes touch closure)
    positions = await broker.get_positions()
    assert len(positions) == 0
    
    # Cash should reflect loss of width (5.0 * 100 = 500.0) from the balance before touch
    balances = await broker.get_account_balances()
    assert balances["cash"] == 100000.0 + 150.0 - 500.0


async def test_mock_engine_chains_and_expirations():
    broker = MockAsyncTradierBroker()
    broker.current_date = datetime(2026, 5, 23)
    
    expirations = await broker.get_option_expirations("AAPL")
    assert len(expirations) > 0
    assert expirations[0] == "2026-05-29"  # Next Friday after May 23, 2026
    
    chain = await broker.get_option_chains("AAPL", expirations[0])
    assert len(chain) > 0
    assert chain[0]["underlying"] == "AAPL"
    assert chain[0]["expiration"] == expirations[0]
