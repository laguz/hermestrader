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

# We need the api key and account id
user = db['users'].find_one({"username": "laguz"}) or db['users'].find_one() # grab first if necessary
# Actually the bot uses session logic in TradierService `_get_headers`. So we have to inject them.
# The `fix_obp.py` worked without credentials because it just connected to Mongo.
# For Tradier, let's get keys from the user object if possible, but they are encrypted.
# Hmm, actually, there is a `bot/strategies/wheel.py` dry run feature!
import requests

# The easiest way: hit the app's dry run API using requests
# We need to be authenticated. We can bypass auth by making a small script that instantiates the classes correctly.
# But wait... we can just set TRADIER_ACCESS_TOKEN and TRADIER_ACCOUNT_ID env variables if we know them.
# I don't know them. 
