import sys
import os
sys.path.append(os.getcwd())

from unittest.mock import MagicMock
from bot.strategies.credit_spreads import CreditSpreadStrategy
from datetime import datetime

def test_limits():
    # 1. Mock Tradier Service
    mock_tradier = MagicMock()
    
    # Mock Positions: 5 Short Puts for TSLA expiring 2025-01-17
    # Symbol format: TSLA250117P00200000
    fake_positions = []
    for i in range(5):
        fake_positions.append({
            'symbol': f'TSLA250117P00200{i}000', # Unique fake symbols
            'underlying': 'TSLA',
            'option_type': 'put',
            'quantity': -1, # Short
            'date_acquired': '2024-12-01'
        })
    
    mock_tradier.get_positions.return_value = fake_positions
    mock_tradier.get_option_expirations.return_value = ['2025-01-17', '2025-01-24']
    
    # 2. Mock Analysis Service (via Container? No, strict unit test better, but Strategy uses Container)
    # Strategy imports container inside execute/execute_spread... 
    # But current logic calls `check_expiry_constraints` directly.
    
    strategy = CreditSpreadStrategy(mock_tradier, db=None, dry_run=True)
    
    print("Testing Limit Logic for TSLA (Put)...")
    
    # Check constraints directly
    exclusions = strategy._check_expiry_constraints('TSLA', is_put=True)
    
    print(f"Exclusions found: {exclusions}")
    
    if '2025-01-17' in exclusions:
        print("✅ SUCCESS: 2025-01-17 is excluded (Limit of 5 met).")
    else:
        print("❌ FAILURE: 2025-01-17 should be excluded.")

    # Test Call side (should have 0)
    exclusions_call = strategy._check_expiry_constraints('TSLA', is_put=False)
    print(f"Call Exclusions: {exclusions_call}")
    if not exclusions_call:
        print("✅ SUCCESS: No call exclusions (0 positions).")
        
if __name__ == "__main__":
    test_limits()
