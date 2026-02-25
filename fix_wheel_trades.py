import os
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv('.env')
uri = os.getenv("MONGODB_URI")
if uri: uri = uri.strip('"').strip("'")
db = MongoClient(uri)['investment_db']

trades = list(db.auto_trades.find({"status": "OPEN"}).sort("entry_date", 1))

# Split into STO and BTC
st_trades = []
bt_trades = []

for t in trades:
    strat = t.get('strategy', '').lower()
    if 'sto' in strat or 'sell_to_open' in strat:
        st_trades.append(t)
    elif 'btc' in strat or 'take profit' in strat or 'excess' in strat:
        bt_trades.append(t)

print(f"Found {len(st_trades)} STO and {len(bt_trades)} BTC trades.")

# Match each BTC with the oldest available STO of the same symbol
matched_count = 0
for btc in bt_trades:
    sym = btc.get('symbol')
    exit_price = btc.get('price', 0)
    btc_id = btc['_id']
    
    # Find matching STO
    matching_sto = None
    for sto in st_trades:
        if sto.get('symbol') == sym and sto.get('status') == 'OPEN':
            matching_sto = sto
            break
            
    if matching_sto:
        entry_price = matching_sto.get('price', 0)
        qty = matching_sto.get('quantity', 1)
        # PNL = Entry Credit - Exit Debit
        pnl = (entry_price - exit_price) * 100 * qty
        
        # Update STO
        db.auto_trades.update_one(
            {"_id": matching_sto['_id']},
            {"$set": {
                "status": "CLOSED",
                "close_date": btc.get('entry_date', datetime.now()),
                "exit_price": exit_price,
                "pnl": round(pnl, 2)
            }}
        )
        # Mark sto as used in local memory
        matching_sto['status'] = 'CLOSED'
        
        # Delete or mark the BTC record as a duplicate/filler since it's merged
        db.auto_trades.delete_one({"_id": btc_id})
        
        matched_count += 1

print(f"Successfully matched and closed {matched_count} trades. Updated database.")
