import unittest
from unittest.mock import MagicMock
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.strategies.credit_spreads import CreditSpreadStrategy

class TestDeltaFallback(unittest.TestCase):
    def setUp(self):
        self.mock_tradier = MagicMock()
        self.mock_db = MagicMock()
        self.strategy = CreditSpreadStrategy(self.mock_tradier, self.mock_db, dry_run=True)

    def test_find_delta_strike(self):
        # Mock Option Chain
        chain = [
            {'strike': 100, 'option_type': 'put', 'greeks': {'delta': -0.20}},
            {'strike': 105, 'option_type': 'put', 'greeks': {'delta': -0.32}}, # Target (0.32 is close to 0.30)
            {'strike': 110, 'option_type': 'put', 'greeks': {'delta': -0.45}},
            {'strike': 120, 'option_type': 'call', 'greeks': {'delta': 0.25}},
            {'strike': 125, 'option_type': 'call', 'greeks': {'delta': 0.35}}, # Target
            {'strike': 130, 'option_type': 'call', 'greeks': {'delta': 0.40}},
        ]
        
        # Test Put
        strike = self.strategy._find_delta_strike(chain, 'put', 0.30, 0.37)
        self.assertEqual(strike, 105)
        
        # Test Call
        strike = self.strategy._find_delta_strike(chain, 'call', 0.30, 0.37)
        self.assertEqual(strike, 125)

    def test_fallback_logic_execution(self):
        # Test that _place_credit_put_spread calls _find_delta_strike when valid_points is empty
        
        symbol = "TEST"
        current_price = 115
        analysis = {
            'put_entry_points': [], # Empty S/R points
            'call_entry_points': []
        }
        
        # Mock Expiry
        self.strategy._find_expiry = MagicMock(return_value="2023-01-20")
        
        # Mock Chain for Fallback
        chain = [
            {'symbol': 'TEST230120P00105000', 'strike': 105, 'option_type': 'put', 'bid': 1.0, 'ask': 1.2, 'greeks': {'delta': -0.32}},
             # Long leg needs to exist too (strike - 5) -> 100
            {'symbol': 'TEST230120P00100000', 'strike': 100, 'option_type': 'put', 'bid': 0.5, 'ask': 0.6, 'greeks': {'delta': -0.15}},
        ]
        self.mock_tradier.get_option_chains.return_value = chain
        
        # Run Put Spread Logic
        self.strategy._place_credit_put_spread(symbol, current_price, analysis)
        
        # Verify it logged the fallback
        logs = "\n".join(self.strategy.execution_logs)
        self.assertIn("No valid support levels found", logs)
        self.assertIn("Checking Delta 0.30-0.37", logs)
        self.assertIn("Found Delta Strike for Put: 105", logs)
        self.assertIn("Placing Bull Put Spread", logs)

if __name__ == '__main__':
    unittest.main()
