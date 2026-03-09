import sys
import os
import certifi
from pymongo import MongoClient

mongo_uri = os.getenv('MONGODB_URI') or os.getenv('MONGODB_URI_LOCAL', 'mongodb://localhost:27017/')
kwargs = {'serverSelectionTimeoutMS': 2000}
if 'localhost' not in mongo_uri and '127.0.0.1' not in mongo_uri and 'mongodb' not in mongo_uri:
    kwargs['tlsCAFile'] = certifi.where()

client = MongoClient(mongo_uri, **kwargs)
db = client['investment_db']

from services.tradier_service import TradierService
class MockAnalysisService:
    def analyze_symbol(self, *args, **kwargs): return {}

tradier = TradierService()
tradier.account_id = "VA60129978"
tradier.access_token = "7xVW1wTgL4DwxD4dGFDZieCIl1ta"

def mock_get_headers():
    return {
        'Authorization': f'Bearer {tradier.access_token}',
        'Accept': 'application/json'
    }
tradier._get_headers = mock_get_headers
def mock_get_account_id():
    return tradier.account_id
tradier._get_account_id = mock_get_account_id

from bot.strategies.wheel import WheelStrategy

class DebugWheel(WheelStrategy):
    def _log(self, message):
        print(f"[WHEEL LOG] {message}")

wheel = DebugWheel(tradier, db, dry_run=False, analysis_service=MockAnalysisService())

print("Fetching positions...")
positions = tradier.get_positions()
riot_positions = [p for p in (positions or []) if 'RIOT' in p.get('symbol', '')]

if not riot_positions:
    print("No RIOT positions found!")
    sys.exit(0)

print(f"Executing _manage_positions on {len(riot_positions)} RIOT positions...")
wheel._manage_positions(riot_positions, watchlist=['RIOT'], config={'max_wheel_contracts_per_symbol': 20})
print("Done.")

