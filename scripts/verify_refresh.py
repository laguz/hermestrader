import sys
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
sys.path.append(os.getcwd())

from services.container import Container
from app import app

def verify_refresh():
    print("Verifying Refresh API...")
    
    db = Container.get_db()
    if db is None:
        print("DB Connection failed")
        return

    # 1. Setup: Insert dummy prediction with missing actual
    symbol = "TEST_REFRESH"
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Ensure market data exists (so refresh can find it)
    # We will simulate that Tradier would return this. 
    # But wait, the refresh logic calls Tradier. 
    # If I don't mock Tradier, it will fail or try to hit real API.
    # I should probably just insert market data into DB manually and ensure refresh logic uses it?
    # NO, refresh logic calls: history = self.tradier.get_historical_pricing(...)
    # I need to mock Tradier service or ensure it returns something valid.
    # For now, I'll trust that Tradier isn't mocked in this env and I might not want to make real API calls for a TEST symbol.
    # 
    # Workaround: I will mock the TradierService in the container for this test.
    
    print("Mocking TradierService...")
    original_tradier = Container._tradier_service
    
    class MockTradier:
        def get_historical_pricing(self, sym, start, end):
            if sym == "TEST_REFRESH":
                return [{
                    "date": (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'),
                    "open": 100, "high": 105, "low": 95, "close": 102.5, "volume": 1000
                }]
            return []
            
    Container._tradier_service = MockTradier()
    
    try:
        # Create pending prediction
        db.predictions.update_one(
            {"symbol": symbol, "prediction_date": yesterday_str, "model_type": "test"},
            {"$set": {
                "predicted_price": 100.0,
                "actual_close_price": None,
                "created_at": datetime.now()
            }},
            upsert=True
        )
        
        # 2. Call API
        with app.test_client() as client:
            res = client.post('/api/history/refresh')
            print(f"API Response Status: {res.status_code}")
            print(f"API Response Body: {res.json}")
            
            if res.status_code == 200 and res.json.get('updated_count', 0) >= 1:
                print("SUCCESS: API returned success and updated count >= 1")
            else:
                 print("FAILURE: API did not return expected success.")

        # 3. Verify DB
        rec = db.predictions.find_one({"symbol": symbol, "prediction_date": yesterday_str, "model_type": "test"})
        if rec and rec.get('actual_close_price') == 102.5:
             print("SUCCESS: DB record updated with correct price (102.5).")
        else:
             print(f"FAILURE: DB record not updated. Actual: {rec.get('actual_close_price')}")
             
    except Exception as e:
        print(f"Test Error: {e}")
    finally:
        # Cleanup
        Container._tradier_service = original_tradier
        db.predictions.delete_one({"symbol": symbol, "prediction_date": yesterday_str, "model_type": "test"})
        db.market_data.delete_one({"symbol": symbol, "date": yesterday_str})

if __name__ == "__main__":
    verify_refresh()
