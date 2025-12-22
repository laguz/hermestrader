from services.container import Container
from dotenv import load_dotenv
import pprint
import time
from datetime import datetime

load_dotenv()

def debug_lifecycle():
    print("--- Initializing ---")
    bot = Container.get_bot_service()
    db = Container.get_db()
    
    # 1. Sync Real Positions
    print("\n--- 1. Initial Sync ---")
    bot.sync_open_positions()
    
    # 2. Insert FAKE position (Simulating a position that was open but is now closed)
    print("\n--- 2. Injecting Fake 'Old' Position ---")
    fake_id = "FAKE_999"
    fake_pos = {
        "_id": fake_id,
        "id": 999123,
        "symbol": "FAKE_STOCK",
        "quantity": 100,
        "cost_basis": 1000.0,
        "date_acquired": "2023-01-01T12:00:00.000Z",
        "status": "OPEN",
        "last_updated": datetime.now()
    }
    db['open_positions'].insert_one(fake_pos)
    print("Inserted FAKE_STOCK as OPEN.")
    
    # 3. Sync Again (Should detect FAKE_STOCK is missing from Tradier and close it)
    print("\n--- 3. Re-Syncing to Trigger Close ---")
    # Note: It will try to fetch gainloss for FAKE_STOCK and fail (gracefully), leaving defaults.
    bot.sync_open_positions()
    
    # 4. Check DB Status
    print("\n--- 4. Verifying DB Status ---")
    closed_doc = db['open_positions'].find_one({"_id": fake_id})
    print(f"FAKE_STOCK Status: {closed_doc.get('status')}")
    print(f"FAKE_STOCK Exit P&L: {closed_doc.get('realized_pnl')}")
    
    # 5. Check P&L Output
    print("\n--- 5. Checking P&L Data ---")
    pnl_data = bot.get_open_positions_pnl()
    print("Open Positions:", len(pnl_data['open']))
    print("Closed Positions:", len(pnl_data['closed']))
    
    # 6. Cleanup
    db['open_positions'].delete_one({"_id": fake_id})
    print("\n--- Cleanup Done ---")

if __name__ == "__main__":
    debug_lifecycle()
