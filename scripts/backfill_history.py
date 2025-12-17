import sys
import os
import argparse
from datetime import datetime, timedelta

# Add project root
sys.path.append(os.getcwd())

from dotenv import load_dotenv
load_dotenv()

from services.container import Container

def backfill_symbol(symbol):
    print(f"Backfilling history for {symbol}...")
    
    tradier = Container.get_tradier_service()
    db = Container.get_db()
    
    if db is None:
        print("ERROR: MongoDB not configured.")
        return

    # Calculate dates: 3 years history
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=365 * 3)).strftime('%Y-%m-%d')
    
    print(f"Fetching data from {start_date} to {end_date}...")
    
    history = tradier.get_historical_pricing(symbol, start_date, end_date)
    
    if not history:
        print("No data returned from Tradier.")
        return

    print(f"Retrieved {len(history)} records. Saving to MongoDB...")
    
    collection = db['market_data']
    count = 0
    
    for record in history:
        # Schema: {symbol, date, open, high, low, close, volume}
        # record from tradier: {'date': '2023-01-01', 'open': 100, ...}
        
        doc = {
            "symbol": symbol,
            "date": record['date'],
            "open": float(record['open']),
            "high": float(record['high']),
            "low": float(record['low']),
            "close": float(record['close']),
            "volume": float(record['volume'])
        }
        
        # Upsert based on symbol + date
        result = collection.update_one(
            {"symbol": symbol, "date": record['date']},
            {"$set": doc},
            upsert=True
        )
        if result.upserted_id or result.modified_count > 0:
            count += 1
            
    print(f"Backfill Complete! Processed {count} records (upserted/modified).")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Backfill historical data')
    parser.add_argument('--symbol', type=str, default='SPY', help='Symbol to backfill (default: SPY)')
    
    args = parser.parse_args()
    backfill_symbol(args.symbol)
