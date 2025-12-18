import sys
import os
sys.path.append(os.getcwd())

from app import app
from services.container import Container

def verify_persistence():
    with app.app_context():
        client = app.test_client()
        db = Container.get_db()
        collection = db['predictions']
        
        # 1. Clear existing predictions for SPY/rf/future_date (optional, for clean test)
        # Actually better to just check upsert.
        
        print("1. Triggering Prediction for SPY (RF)...")
        resp = client.post('/api/predict', json={"symbol": "SPY", "model_type": "rf"})
        if resp.status_code != 200:
            print(f"Prediction Failed: {resp.get_data(as_text=True)}")
            return
            
        data = resp.get_json()
        pred_date = data['prediction_date']
        pred_price = data['predicted_price']
        print(f"Predicted Price: {pred_price} for Date: {pred_date}")
        
        # 2. Check DB
        doc = collection.find_one({"symbol": "SPY", "model_type": "rf", "prediction_date": pred_date})
        if doc:
            print(f"SUCCESS: Found document in DB! ID: {doc['_id']}")
            print(f" stored predicted price: {doc['predicted_price']}")
            
            if 'actual_close_price' in doc:
                 print(f" actual_close_price field exists: {doc['actual_close_price']}")
            else:
                 print(" FAILURE: actual_close_price field missing!")

            if abs(doc['predicted_price'] - pred_price) < 0.01:
                print(" Price Matches!")
            else:
                 print(" Price Mismatch!")
        else:
            print("FAILURE: Document not found in DB.")
            return

        # 3. Trigger Again (Test Upsert)
        print("\n3. Triggering Again (Testing Upsert)...")
        # Let's verify count doesn't increase
        count_before = collection.count_documents({"symbol": "SPY", "model_type": "rf", "prediction_date": pred_date})
        
        resp = client.post('/api/predict', json={"symbol": "SPY", "model_type": "rf"})
        
        count_after = collection.count_documents({"symbol": "SPY", "model_type": "rf", "prediction_date": pred_date})
        
        if count_before == count_after == 1:
            print(f"SUCCESS: Count remained {count_after}. Upsert worked.")
        else:
             print(f"FAILURE: Count changed from {count_before} to {count_after}. Duplicate created?")

if __name__ == "__main__":
    verify_persistence()
