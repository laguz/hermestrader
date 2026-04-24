
from unittest.mock import MagicMock
from datetime import datetime, date
from bot.strategies.wheel import WheelStrategy

class MockTradier:
    def __init__(self, current_date):
        self.current_date = current_date
        self.account_id = "mock_account"
        self.get_quote = MagicMock()
        self.get_quotes = MagicMock(return_value={})
        self.get_option_chains = MagicMock()
        self.get_option_expirations = MagicMock()
        self.get_orders = MagicMock(return_value=[])
        self.place_order = MagicMock()

def test_wheel_roll_conditions():
    # Setup
    today = datetime(2026, 1, 1, 12, 0)
    mock_tradier = MockTradier(today)
    mock_db = MagicMock()
    strategy = WheelStrategy(mock_tradier, mock_db, dry_run=False)
    
    # 1. RIOT 13 Put, ITM, DTE 1 (Expires Jan 2, 2026)
    # Today is Jan 1. Jan 2 is DTE 1.
    expiry_date = date(2026, 1, 2)
    position = {
        'symbol': 'RIOT260102P00013000',
        'underlying': 'RIOT',
        'quantity': -1,
        'strike': 13.0,
        'option_type': 'put'
    }
    
    # Mock data for roll
    mock_tradier.get_quotes.return_value = {'RIOT': {'last': 12.50}} # ITM
    mock_tradier.get_option_chains.side_effect = [
        # Chain for current expiry closure
        [{'strike': 13.0, 'option_type': 'put', 'bid': 0.59, 'ask': 0.60, 'symbol': 'RIOT260102P00013000'}],
        # Chain for new expiry opening (at strike 12)
        [{'strike': 12.0, 'option_type': 'put', 'bid': 1.50, 'symbol': 'RIOT260213P00012000'}]
    ]
    mock_tradier.get_option_expirations.return_value = ['2026-02-14', '2026-02-20']
    mock_tradier.place_order.return_value = {'id': 'order_id', 'status': 'ok'}

    # Execute
    strategy._manage_positions([position], watchlist=['RIOT'])

    # Check triggers
    # BTC should be called
    mock_tradier.place_order.assert_any_call(
        account_id='mock_account',
        symbol='RIOT',
        side='buy_to_close',
        quantity=1,
        order_type='limit',
        duration='day',
        price=0.60,
        option_symbol='RIOT260102P00013000',
        order_class='option',
        tag="WHEEL"
    )
    # STO should be called (at strike 12)
    mock_tradier.place_order.assert_any_call(
        account_id='mock_account',
        symbol='RIOT',
        side='sell_to_open',
        quantity=1,
        order_type='limit',
        duration='day',
        price=1.49, # 1.50 - 0.01
        option_symbol='RIOT260213P00012000',
        order_class='option',
        tag="WHEEL"
    )

def test_wheel_no_roll_if_otm():
    # Setup
    today = datetime(2026, 1, 1, 12, 0)
    mock_tradier = MockTradier(today)
    mock_db = MagicMock()
    strategy = WheelStrategy(mock_tradier, mock_db, dry_run=False)
    
    expiry_date = date(2026, 1, 2)
    position = {
        'symbol': 'RIOT260102P00013000',
        'underlying': 'RIOT',
        'quantity': -1,
        'strike': 13.0,
        'option_type': 'put'
    }
    
    mock_tradier.get_quotes.return_value = {'RIOT': {'last': 14.00}} # OTM
    
    # Execute
    strategy._manage_positions([position], watchlist=['RIOT'])

    # BTC should NOT be called
    mock_tradier.place_order.assert_not_called()

def test_wheel_no_roll_if_dte_high():
    # Setup
    today = datetime(2026, 1, 1, 12, 0)
    mock_tradier = MockTradier(today)
    mock_db = MagicMock()
    strategy = WheelStrategy(mock_tradier, mock_db, dry_run=False)
    
    # DTE 10 (Expires Jan 11)
    expiry_date = date(2026, 1, 11)
    # Symbol format: RIOT260111P00013000 (yyMMdd)
    position = {
        'symbol': 'RIOT260111P00013000',
        'underlying': 'RIOT',
        'quantity': -1,
        'strike': 13.0,
        'option_type': 'put'
    }
    
    # Execute
    strategy._manage_positions([position], watchlist=['RIOT'])

    # Quote should not even be fetched if DTE > 7
    mock_tradier.place_order.assert_not_called()

def test_wheel_no_roll_if_not_in_watchlist():
    # Setup
    today = datetime(2026, 1, 1, 12, 0)
    mock_tradier = MockTradier(today)
    mock_db = MagicMock()
    strategy = WheelStrategy(mock_tradier, mock_db, dry_run=False)
    
    # TSLA Put, ITM, DTE 1
    # This should be ignored because TSLA is NOT in the watchlist passed to _manage_positions
    position = {
        'symbol': 'TSLA260102P00046500',
        'underlying': 'TSLA',
        'quantity': -1,
        'strike': 465.0,
        'option_type': 'put'
    }
    
    mock_tradier.get_quotes.return_value = {'RIOT': {'last': 450.00}} # ITM
    
    # Execute with watchlist NOT containing TSLA
    strategy._manage_positions([position], watchlist=['RIOT', 'NFLX'])

    # BTC should NOT be called even if ITM
    mock_tradier.place_order.assert_not_called()
    # Quote should not even be fetched

def test_no_new_put_when_max_lots_reached():
    """Verify _process_symbol does NOT open a new put when max_lots is already met."""
    today = datetime(2026, 1, 15, 12, 0)
    mock_tradier = MockTradier(today)
    mock_db = MagicMock()
    strategy = WheelStrategy(mock_tradier, mock_db, dry_run=True)

    # Existing short put position (1 contract = max_lots already reached)
    positions = [
        {
            'symbol': 'RIOT260227P00012000',
            'underlying': 'RIOT',
            'quantity': -1,
            'option_type': 'put'
        }
    ]

    mock_analysis = MagicMock()
    mock_tradier.get_positions = MagicMock(return_value=positions)
    mock_analysis.analyze_symbol.return_value = {
        'current_price': 13.00,
        'put_entry_points': [{'price': 12.0, 'pop': 60}],
    }

    # Execute with max_lots=1
    strategy._process_symbol('RIOT', positions, mock_analysis, max_lots=1)

    # Should NOT have tried to fetch chains or expirations for a new put
    mock_tradier.get_option_chains.assert_not_called()

    # Verify the log confirms skipping
    assert any("No suitable expiry found" in log for log in strategy.execution_logs)

def test_manage_positions_only_rolls_up_to_max_lots():
    """With 2 ITM puts near expiry and max_lots=1, only 1 should be rolled; the other just closed."""
    today = datetime(2026, 1, 1, 12, 0)
    mock_tradier = MockTradier(today)
    mock_db = MagicMock()
    strategy = WheelStrategy(mock_tradier, mock_db, dry_run=True)

    # Two short puts, both DTE 1, both will be ITM
    positions = [
        {'symbol': 'RIOT260102P00013000', 'underlying': 'RIOT', 'quantity': -1, 'option_type': 'put'},
        {'symbol': 'RIOT260102P00012000', 'underlying': 'RIOT', 'quantity': -1, 'option_type': 'put'},
    ]

    # Price is below both strikes → both ITM
    mock_tradier.get_quotes.return_value = {'RIOT': {'last': 11.00}}
    mock_tradier.get_option_expirations.return_value = ['2026-02-14', '2026-02-20']
    mock_tradier.get_option_chains.side_effect = [
        # Chain for first position's current expiry
        [{'strike': 13.0, 'option_type': 'put', 'bid': 2.09, 'ask': 2.10, 'symbol': 'RIOT260102P00013000'},
         {'strike': 12.0, 'option_type': 'put', 'bid': 1.09, 'ask': 1.10, 'symbol': 'RIOT260102P00012000'}],
        # Chain for first position's new expiry
        [{'strike': 12.0, 'option_type': 'put', 'bid': 2.50, 'ask': 2.60, 'symbol': 'RIOT260213P00012000'}],
        # Chain for second position's current expiry
        [{'strike': 13.0, 'option_type': 'put', 'bid': 2.09, 'ask': 2.10, 'symbol': 'RIOT260102P00013000'},
         {'strike': 12.0, 'option_type': 'put', 'bid': 1.09, 'ask': 1.10, 'symbol': 'RIOT260102P00012000'}],
        # Chain for second position's new expiry
        [{'strike': 11.0, 'option_type': 'put', 'bid': 1.80, 'ask': 1.90, 'symbol': 'RIOT260213P00011000'}],
    ]

    config = {'max_wheel_contracts_per_symbol': 1}
    strategy._manage_positions(positions, watchlist=['RIOT'], config=config)

    logs = "\n".join(strategy.execution_logs)

    # First put should be rolled (BTC + STO)
    assert "Triggering ROLL" in logs
    # Second put should be closed without replacement
    assert "Rollover: BTC RIOT260102P00012000" in logs

    # Verify dry run recorded: 1 roll (BTC+STO) + 1 excess close (BTC only) = 3 trades
    roll_btc_count = sum(1 for log in strategy.execution_logs if "[DRY RUN] Rollover" in log)
    excess_btc_count = sum(1 for log in strategy.execution_logs if "[DRY RUN] Close excess" in log)
    assert roll_btc_count == 2
    assert excess_btc_count == 0

def test_no_duplicate_roll_if_pending_btc():
    """If a BTC order is already pending for a position, skip it entirely."""
    today = datetime(2026, 1, 1, 12, 0)
    mock_tradier = MockTradier(today)
    mock_db = MagicMock()
    strategy = WheelStrategy(mock_tradier, mock_db, dry_run=False)

    # 1 short put, DTE 1, ITM
    positions = [
        {'symbol': 'RIOT260102P00013000', 'underlying': 'RIOT', 'quantity': -1, 'option_type': 'put'},
    ]

    # Simulate a pending BTC order already exists for this exact option symbol
    mock_tradier.get_orders.return_value = [
        {
            'status': 'open',
            'side': 'buy_to_close',
            'option_symbol': 'RIOT260102P00013000',
            'symbol': 'RIOT',
            'quantity': 1,
        }
    ]

    mock_tradier.get_quotes.return_value = {'RIOT': {'last': 12.00}}  # ITM

    config = {'max_wheel_contracts_per_symbol': 1}
    strategy._manage_positions(positions, watchlist=['RIOT'], config=config)

    # Should NOT place any new orders (no BTC, no STO)
    mock_tradier.place_order.assert_not_called()
    # Should log the skip
    assert any("Pending BTC order exists" in log for log in strategy.execution_logs)
