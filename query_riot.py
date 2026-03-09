import sys
import os
import certifi
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.getenv('MONGODB_URI') or os.getenv('MONGODB_URI_LOCAL', 'mongodb://localhost:27017/')
kwargs = {'serverSelectionTimeoutMS': 2000}
if 'localhost' not in mongo_uri and '127.0.0.1' not in mongo_uri and 'mongodb' not in mongo_uri:
    kwargs['tlsCAFile'] = certifi.where()

client = MongoClient(mongo_uri, **kwargs)
db = client['investment_db']

riot_positions = list(db['open_positions'].find({"symbol": {"$regex": "RIOT"}}))
if not riot_positions:
    print("No RIOT positions in DB")
else:
    for p in riot_positions:
        print(p.get('symbol', ''), p.get('status', ''), p.get('quantity', ''))
