from services.container import Container
from dotenv import load_dotenv
import pprint
import traceback

load_dotenv()

def debug_pnl():
    print("Initializing Bot Service...")
    try:
        service = Container.get_bot_service()
        print("Calling get_open_positions_pnl()...")
        data = service.get_open_positions_pnl()
        print(f"Returned {len(data)} records.")
        pprint.pprint(data)
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    debug_pnl()
