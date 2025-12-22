# scripts/dry_run_bot.py
import sys
import os
import time
from unittest.mock import MagicMock
from datetime import datetime, date, timedelta

# Adjust path to include project root
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.container import Container
from bot.strategies.credit_spreads import CreditSpreadStrategy, Colors
from bot.strategies.wheel import WheelStrategy
from bot.money_manager import MoneyManager
import logging
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('pymongo').setLevel(logging.WARNING)
logging.getLogger('matplotlib').setLevel(logging.WARNING)

# Initialize Services
# We need real Tradier for quotes/chains but MOCKED Tradier for positions to test limits.
container = Container()
real_tradier = container.get_tradier_service()
db = container.get_db()

# --- MOCKING TRADIER POSITIONS FOR LIMIT TESTING ---
# Create a wrapper or mock that delegates to real tradier for everything EXCEPT positions.
class MockedTradier:
    def __init__(self, real_service):
        self.real = real_service
        self.account_id = real_service.account_id
        # Define Mock Positions Here to verify limits
        # Let's verify:
        # 1. Wheel Limit: Simulate 1 existing put for next week -> Should skip next week.
        # 2. Spread Limit: Simulate 5 existing call spreads for 2 weeks out -> Should skip.
        
        target_date_1 = (date.today() + timedelta(weeks=1)).strftime("%Y-%m-%d") # Next Week
        target_date_2 = (date.today() + timedelta(weeks=2)).strftime("%Y-%m-%d") # 2 Weeks out (Spread Limit)
        
        # Convert to Symbol Format: RIOTyyMMdd...
        d1 = datetime.strptime(target_date_1, "%Y-%m-%d")
        d2 = datetime.strptime(target_date_2, "%Y-%m-%d")
        
        # Wheel Position (1 Short Put Expiring Week 1)
        sym1 = f"RIOT{d1.strftime('%y%m%d')}P00010000" 
        
        # Spread Positions (5 Short Calls Expiring Week 2)
        sym2 = f"RIOT{d2.strftime('%y%m%d')}C00020000"
        
        self.mock_positions = [
             # Wheel Limit Tester (1 Existing Put)
            {'symbol': sym1, 'underlying': 'RIOT', 'quantity': -1, 'option_type': 'put', 'strike': 10.0},
            
             # Spread Limit Tester (5 Existing Short Calls)
            {'symbol': sym2, 'underlying': 'RIOT', 'quantity': -5, 'option_type': 'call', 'strike': 20.0},
             # (Assume long legs exist for spread but logic counts shorts)
        ]
        print(f"\n{Colors.WARNING}[MOCK] Injected Mock Positions for Limit Verification:{Colors.ENDC}")
        print(f"   • Wheel: 1 Short Put Expiring {target_date_1} (Should BLOCK new Wheel orders for this date)")
        print(f"   • Spread: 5 Short Calls Expiring {target_date_2} (Should BLOCK new Call Spreads for this date)")

    def get_positions(self):
        return self.mock_positions
        
    def get_account_balances(self):
        return {'option_buying_power': 100000, 'total_equity': 100000, 'cash': 100000}

    # Delegate everything else to real tradier
    def __getattr__(self, name):
        return getattr(self.real, name)

# Instantiate Mock
mock_tradier = MockedTradier(real_tradier)

def run_dry_run():
    watchlist = ['RIOT'] # Focused debugging
    
    print(f"\n{Colors.HEADER}=================================================={Colors.ENDC}")
    print(f"{Colors.HEADER}    LAGUZ REFLEX BOT - DRY RUN (MONEY MANAGER)    {Colors.ENDC}")
    print(f"{Colors.HEADER}=================================================={Colors.ENDC}")
    
    # Initialize Strategies with MOCKED Tradier
    cs_strategy = CreditSpreadStrategy(mock_tradier, db, dry_run=True)
    wheel_strategy = WheelStrategy(mock_tradier, db, dry_run=True)
    
    # Initialize Money Manager
    mm = MoneyManager(mock_tradier, db, wheel_strategy, cs_strategy)
    
    for symbol in watchlist:
        print(f"\n{Colors.BOLD}🤖 Processing {symbol} via Money Manager...{Colors.ENDC}")
        
        # Execute Money Manager Logic
        # Target: 1 Wheel Unit, 1 Spread Unit (per side effectively)
        # The Manager will check inventory (using mocked positions) and fire orders.
        # The Strategies will receive orders and check Expiry Constraints (using mocked positions).
        
        mm.process_symbol(symbol, target_wheel_qty=2, target_spread_qty=1)

    print(f"\n{Colors.HEADER}=================================================={Colors.ENDC}")
    print(f"{Colors.HEADER}             DRY RUN COMPLETE                     {Colors.ENDC}")
    print(f"{Colors.HEADER}=================================================={Colors.ENDC}")

if __name__ == "__main__":
    run_dry_run()
