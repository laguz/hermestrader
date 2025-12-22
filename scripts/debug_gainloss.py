from services.container import Container
from dotenv import load_dotenv
import json

load_dotenv()

def debug_gainloss():
    print("--- Inspecting Gain/Loss Data ---")
    bot = Container.get_bot_service()
    # Fetch recent closed positions
    try:
        data = bot.tradier.get_gainloss(limit=5)
        if data:
            print(json.dumps(data[0], indent=2, default=str))
        else:
            print("No gain/loss data found.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_gainloss()
