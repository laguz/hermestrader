import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.container import Container
from bot.strategies.credit_spreads_7 import CreditSpreads7Strategy

def main():
    tradier = Container.get_tradier_service()
    db = Container.get_db()
    
    strategy = CreditSpreads7Strategy(tradier, db, dry_run=True)
    logs = strategy.execute(['SPY'])
    
    for log in logs:
        print(log)

if __name__ == "__main__":
    main()
