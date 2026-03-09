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
if bot_config:
    settings = bot_config.get('settings', {})
    print(f"Current min_obp_reserve: {settings.get('min_obp_reserve', 1000)}")
    
    # Let's update it to 0 temporarily to see if Wheel runs
    db['bot_config'].update_one(
        {"_id": "main_bot"},
        {"$set": {"settings.min_obp_reserve": 0}}
    )
    print("Updated min_obp_reserve to 0 in DB.")
