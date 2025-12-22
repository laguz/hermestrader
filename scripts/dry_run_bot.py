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

class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def run_dry_run():
    print(f"{Colors.HEADER}=================================================={Colors.ENDC}")
    print(f"{Colors.HEADER}           LAGUZ TRADING BOT - DRY RUN           {Colors.ENDC}")
    print(f"{Colors.HEADER}=================================================={Colors.ENDC}")
    
    # Initialize Services
    tradier = Container.get_tradier_service()
    db = Container.get_db()
    
    # Test watchlist
    watchlist = ['RIOT']
    
    print(f"Executing strategy on: {watchlist}")
    
    # --- CREDIT SPREADS ---
    print(f"\n{Colors.OKBLUE}--------------------------------------------------{Colors.ENDC}")
    print(f"{Colors.OKBLUE}   STRATEGY 1: Credit Spreads (Bull Put / Bear Call)   {Colors.ENDC}")
    print(f"{Colors.OKBLUE}--------------------------------------------------{Colors.ENDC}")
    
    cs_strategy = CreditSpreadStrategy(tradier, db, dry_run=True)
    cs_strategy.execute(watchlist)

    # --- WHEEL STRATEGY ---
    print(f"\n{Colors.OKCYAN}--------------------------------------------------{Colors.ENDC}")
    print(f"{Colors.OKCYAN}   STRATEGY 2: The Wheel (Cash Secured Puts)         {Colors.ENDC}")
    print(f"{Colors.OKCYAN}--------------------------------------------------{Colors.ENDC}")
    
    wheel_strategy = WheelStrategy(tradier, db, dry_run=True)
    wheel_strategy.execute(watchlist)
    
    print(f"\n{Colors.HEADER}=================================================={Colors.ENDC}")
    print(f"{Colors.HEADER}           DRY RUN COMPLETE                       {Colors.ENDC}")
    print(f"{Colors.HEADER}=================================================={Colors.ENDC}")

if __name__ == "__main__":
    run_dry_run()
