from services.container import Container
from dotenv import load_dotenv
import time
import pprint

load_dotenv()

def debug_backfill():
    print("--- Debugging Backfill ---")
    bot = Container.get_bot_service()
    db = Container.get_db()
    
    # 1. Check Pre-Sync State
    pre_count = db['open_positions'].count_documents({})
    pre_closed = db['open_positions'].count_documents({"status": "CLOSED"})
    print(f"Pre-Sync: Total={pre_count}, Closed={pre_closed}")
    
    # 2. Run Sync
    print("\nRunning sync_open_positions()...")
    
    # We want to see logs normally, but they go to DB/stdout if configured.
    # We'll just run it.
    try:
        count = bot.sync_open_positions()
        print(f"Sync returned count (updated/synced): {count}")
    except Exception as e:
        print(f"Sync CRASHED: {e}")
        import traceback
        traceback.print_exc()

    # 3. Check Post-Sync State
    post_count = db['open_positions'].count_documents({})
    post_closed = db['open_positions'].count_documents({"status": "CLOSED"})
    print(f"\nPost-Sync: Total={post_count}, Closed={post_closed}")
    print(f"Delta: {post_count - pre_count} new documents.")
    
    # 4. Inspect a few CLOSED positions
    print("\nSample CLOSED positions:")
    cursor = db['open_positions'].find({"status": "CLOSED"}).limit(3)
    for doc in cursor:
        print(f" - {doc.get('symbol')} | PnL: {doc.get('realized_pnl')} | Exit: {doc.get('exit_date')}")

    print("\n--- Done ---")

if __name__ == "__main__":
    debug_backfill()
