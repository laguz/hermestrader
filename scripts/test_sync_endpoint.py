from services.container import Container
from dotenv import load_dotenv
import time

load_dotenv()

def test_sync():
    print("--- Testing Sync ---")
    bot = Container.get_bot_service()
    
    print("Calling sync_open_positions()...")
    start = time.time()
    count = bot.sync_open_positions()
    duration = time.time() - start
    
    print(f"Sync complete in {duration:.2f}s.")
    print(f"Synced {count} positions.")
    
    print("\nGetting P&L Data...")
    pnl = bot.get_open_positions_pnl()
    print(f"Open: {len(pnl['open'])}, Closed: {len(pnl['closed'])}")
    print("--- Done ---")

if __name__ == "__main__":
    test_sync()
