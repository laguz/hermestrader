import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.tradier_service import TradierService
from dotenv import load_dotenv
from datetime import datetime, timedelta

def main():
    load_dotenv()
    service = TradierService()
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    print(f"Attempting to fetch history for SPY from {start_date} to {end_date}")
    history = service.get_historical_pricing('SPY', start_date, end_date)
    
    if history:
        print(f"✅ Success! Found {len(history)} candles.")
        print(f"Sample: {history[0]}")
    else:
        print("❌ Failed to fetch history.")

if __name__ == "__main__":
    main()
