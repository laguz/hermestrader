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
    sys.exit(1)

logs = bot_config.get('logs', [])
# Look at the most recent 30 logs
recent_logs = logs[-30:]
for log in recent_logs:
    msg = log.get('message', '')
    if 'WHEEL' in msg or 'RIOT' in msg or 'Roll' in msg or 'ITM' in msg:
        print(f"[{log.get('timestamp')}] {msg}")

