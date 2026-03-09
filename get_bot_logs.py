import sys
import os
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.getenv('MONGODB_URI') or os.getenv('MONGODB_URI_LOCAL', 'mongodb://localhost:27017/')

kwargs = {'serverSelectionTimeoutMS': 2000}
if 'localhost' not in mongo_uri and '127.0.0.1' not in mongo_uri and 'mongodb' not in mongo_uri:
    kwargs['tlsCAFile'] = certifi.where()

client = MongoClient(mongo_uri, **kwargs)
db = client['investment_db']

bot_config = db['bot_config'].find_one({"_id": "main_bot"})
if not bot_config:
    print("Bot config not found!")
    sys.exit(1)

settings = bot_config.get('settings', {})
print("--- Bot Settings ---")
print(f"Wheel Watchlist: {settings.get('watchlist_wheel')}")
print(f"Wheel Max Lots: {settings.get('max_wheel_contracts_per_symbol')}")
print("\n--- Recent Logs focusing on RIOT ---")
logs = bot_config.get('logs', [])
riot_logs = [log for log in logs if 'riot' in log.get('message', '').lower() or 'wheel' in log.get('message', '').lower()]
for log in riot_logs[-20:]:  # Last 20 relevant logs
    print(f"[{log.get('timestamp')}] {log.get('message')}")

