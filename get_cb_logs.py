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
cb_logs = [log for log in logs if 'CIRCUIT BREAKER' in log.get('message', '')]
print(f"Total Circuit Breaker Logs found: {len(cb_logs)}")
for log in cb_logs[-5:]:
    print(f"[{log.get('timestamp')}] {log.get('message')}")

