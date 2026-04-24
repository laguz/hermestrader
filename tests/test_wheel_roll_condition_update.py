
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, date, timedelta
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

def test_wheel_no_roll_if_dte_7():
    """Verify that no roll occurs when DTE is exactly 7."""
    # Setup: Jan 1st. Jan 8th is DTE 7.
    today = datetime(2026, 1, 1, 12, 0)
    mock_tradier = MockTradier(today)
    mock_db = MagicMock()
    strategy = WheelStrategy(mock_tradier, mock_db, dry_run=False)
    
    expiry_date = date(2026, 1, 8)
    position = {
        'symbol': 'RIOT260108P00013000',
        'underlying': 'RIOT',
        'quantity': -1,
        'strike': 13.0,
        'option_type': 'put'
    }
    
    # Even if ITM, it should NOT roll at DTE 7
    mock_tradier.get_quotes.return_value = {'RIOT': {'last': 12.00}}
    
    # Execute
    strategy._manage_positions([position], watchlist=['RIOT'])

    # BTC should NOT be called
    mock_tradier.place_order.assert_not_called()

def test_wheel_roll_if_dte_6():
    """Verify that roll occurs when DTE is 6 (which is < 7)."""
    # Setup: Jan 1st. Jan 7th is DTE 6.
    today = datetime(2026, 1, 1, 12, 0)
    mock_tradier = MockTradier(today)
    mock_db = MagicMock()
    strategy = WheelStrategy(mock_tradier, mock_db, dry_run=False)
    
    expiry_date = date(2026, 1, 7)
    position = {
        'symbol': 'RIOT260107P00013000',
        'underlying': 'RIOT',
        'quantity': -1,
        'strike': 13.0,
        'option_type': 'put'
    }
    
    # Mock data for roll
    mock_tradier.get_quotes.return_value = {'RIOT': {'last': 12.00}} # ITM
    mock_tradier.get_option_chains.side_effect = [
        # Chain for current expiry closure
        [{'strike': 13.0, 'option_type': 'put', 'bid': 0.99, 'ask': 1.00, 'symbol': 'RIOT260107P00013000'}],
        # Chain for new expiry opening (at strike 12)
        [{'strike': 12.0, 'option_type': 'put', 'bid': 1.50, 'symbol': 'RIOT260213P00012000'}]
    ]
    mock_tradier.get_option_expirations.return_value = ['2026-02-14', '2026-02-20']
    mock_tradier.place_order.return_value = {'id': 'order_id', 'status': 'ok'}

    # Execute
    strategy._manage_positions([position], watchlist=['RIOT'])

    # BTC should be called
    assert mock_tradier.place_order.called
