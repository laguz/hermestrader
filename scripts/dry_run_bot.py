import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.container import Container
from bot.strategies.credit_spreads import CreditSpreadStrategy
from bot.strategies.wheel import WheelStrategy
import logging
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('pymongo').setLevel(logging.WARNING)
logging.getLogger('matplotlib').setLevel(logging.WARNING)

def run_dry_run():
    print("Starting Dry Run Bot...")
    
    # Initialize Services
    tradier = Container.get_tradier_service()
    db = Container.get_db()
    
    # Initialize Strategy with Dry Run = True
    strategy = CreditSpreadStrategy(tradier, db, dry_run=True)
    
    # Test watchlist
    watchlist = ['SPY', 'IWM', 'QQQ', 'DIA', 'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA']
    
    print(f"Executing strategy on: {watchlist}")
    print(f"Executing strategy on: {watchlist}")
    
    print("\n--- Running Credit Spreads ---")
    strategy.execute(watchlist)

    print("\n--- Running Wheel Strategy ---")
    wheel_strategy = WheelStrategy(tradier, db, dry_run=True)
    wheel_strategy.execute(watchlist)
    
    print("Dry Run Complete.")

if __name__ == "__main__":
    run_dry_run()
