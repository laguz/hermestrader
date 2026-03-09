import sys
import os
import json
from pymongo import MongoClient
import certifi
from dotenv import load_dotenv

load_dotenv()
from services.tradier_service import TradierService
from bot.strategies.wheel import WheelStrategy

class DummyAnalysisService:
    def analyze_symbol(self, symbol, period):
        return {}

tradier = TradierService()
# Need auth to get positions. We don't have flask session. The service might fail if environment variables are not set.
# Let's get them from DB.
mongo_uri = os.getenv('MONGODB_URI') or os.getenv('MONGODB_URI_LOCAL', 'mongodb://localhost:27017/')

kwargs = {'serverSelectionTimeoutMS': 2000}
if 'localhost' not in mongo_uri and '127.0.0.1' not in mongo_uri and 'mongodb' not in mongo_uri:
    kwargs['tlsCAFile'] = certifi.where()
client = MongoClient(mongo_uri, **kwargs)
db = client['investment_db']

# Let's do a quick hack to run the execute method of WheelStrategy, it will try to get positions.
# Wait, TradierService uses `_get_headers()` which looks at environment or Flask session or AuthService DB directly!
# The `_get_headers` calls Container.get_auth_service().get_api_key() which uses session.
# We need to manually set `access_token` and `account_id` on the `TradierService` object.

from cryptography.fernet import Fernet
import base64
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

def derive_key(password, salt):
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=480000,
    )
    return base64.urlsafe_b64encode(kdf.derive(password.encode()))

# I don't have the user's password. 
# BUT we can just print the exact options being evaluated if I can fetch from tradier.
# Wait, `get_wheel_logs_2.py` in my previous step did not output anything because I didn't wait long enough maybe?
# It's been a minute since I updated the circuit breaker min reserve.
