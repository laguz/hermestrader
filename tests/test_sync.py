from services.container import Container
from dotenv import load_dotenv

load_dotenv()

def test_sync():
    print("Initializing Bot Service...")
    bot_service = Container.get_bot_service()
    
    print("Calling sync_open_positions()...")
    count = bot_service.sync_open_positions()
    print(f"Synced {count} positions.")
    
    print("Verifying DB Content...")
    db = Container.get_db()
    positions = list(db['open_positions'].find())
    print(f"Found {len(positions)} documents in 'open_positions'.")
    for p in positions:
        print(f"- {p.get('symbol')} ({p.get('quantity')}) Last Updated: {p.get('last_updated')}")

if __name__ == "__main__":
    test_sync()
