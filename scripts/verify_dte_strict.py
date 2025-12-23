import sys
import os
sys.path.append(os.getcwd())
from datetime import date, timedelta
from unittest.mock import MagicMock
from bot.strategies.credit_spreads import CreditSpreadStrategy
from bot.strategies.wheel import WheelStrategy

def test_strict_dte():
    mock_tradier = MagicMock()
    mock_db = MagicMock()
    
    # helper to mock date return
    def set_mock_chains(dates):
        mock_tradier.get_option_expirations.return_value = dates

    print("--- Testing Credit Spreads (Strict 16-22 Days) ---")
    cs = CreditSpreadStrategy(mock_tradier, mock_db, dry_run=True)
    today = date.today()
    
    # Case 1: Dates at 18 and 25 days.
    # 18 days (Jan 9 equivalent) -> OK
    # 25 days (Jan 16 equivalent) -> REJECT
    d18 = (today + timedelta(days=18)).strftime("%Y-%m-%d")
    d25 = (today + timedelta(days=25)).strftime("%Y-%m-%d")
    
    set_mock_chains([d18, d25])
    
    # Default call (uses strict defaults)
    res = cs._find_expiry("TEST")
    print(f"Available: [{d18} (18d), {d25} (25d)] -> Selected: {res}")
    assert res == d18, f"Should accept 18d. Got {res}"
    
    # Case 2: Exclude 18d. Should reject 25d.
    res_ex = cs._find_expiry("TEST", exclude_dates=[d18])
    print(f"Available: [{d18}, {d25}] Exclude: {d18} -> Selected: {res_ex}")
    assert res_ex is None, f"Should reject 25d as outside 16-22 range. Got {res_ex}"

    print("\n--- Testing Wheel Strategy (Strict Ranges) ---")
    ws = WheelStrategy(mock_tradier, mock_db, dry_run=True)
    
    d35 = (today + timedelta(days=35)).strftime("%Y-%m-%d")
    d42 = (today + timedelta(days=42)).strftime("%Y-%m-%d")
    d49 = (today + timedelta(days=49)).strftime("%Y-%m-%d")
    d63 = (today + timedelta(days=63)).strftime("%Y-%m-%d")
    
    set_mock_chains([d35, d42, d49, d63])
    
    # Case 3: Wheel Opening (41-48 Days)
    # Available: 35, 42, 49, 63
    # 35 < 41 -> Reject
    # 42 in [41, 48] -> Accept
    # 49 > 48 -> Reject
    
    res_open = ws._find_expiry("TEST", target_dte=42, min_dte=41, max_dte=48, method='closest')
    print(f"Wheel Open [41-48]. Available: [35, 42, 49] -> Selected: {res_open}")
    assert res_open == d42, f"Should select 42d. Got {res_open}"
    
    # Case 4: Wheel Roll (42-63 Days, Min)
    # Available: 35, 42, 49, 63
    # 35 < 42 -> Reject
    # 42, 49, 63 in range.
    # Min -> 42.
    
    res_roll = ws._find_expiry("TEST", target_dte=42, min_dte=42, max_dte=63, method='min')
    print(f"Wheel Roll [42-63, Min]. Available: [35, 42, 49, 63] -> Selected: {res_roll}")
    assert res_roll == d42, f"Should select 42d (Lowest in range). Got {res_roll}"
    
    # Case 5: Wheel Roll without d42
    set_mock_chains([d49, d63])
    res_roll2 = ws._find_expiry("TEST", target_dte=42, min_dte=42, max_dte=63, method='min')
    print(f"Wheel Roll [42-63, Min]. Available: [49, 63] -> Selected: {res_roll2}")
    assert res_roll2 == d49, f"Should select 49d (Lowest valid). Got {res_roll2}"

    print("\n✅ All Strict DTE Tests Passed!")

if __name__ == "__main__":
    test_strict_dte()
