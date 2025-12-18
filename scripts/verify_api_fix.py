import sys
import os
sys.path.append(os.getcwd())

from flask import Flask
from routes.ml_routes import ml_bp
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
app.register_blueprint(ml_bp)

def test_routes():
    client = app.test_client()
    
    print("1. Testing /api/predict with VALID payload (Call 1)...")
    resp = client.post('/api/predict', json={"symbol": "SPY", "model_type": "rf"})
    print(f"Status: {resp.status_code}")
    print(f"Body: {resp.get_data(as_text=True)}")

    print("1b. Testing /api/predict with VALID payload (Call 2 - Test DB Caching)...")
    resp = client.post('/api/predict', json={"symbol": "SPY", "model_type": "rf"})
    print(f"Status: {resp.status_code}")
    print(f"Body: {resp.get_data(as_text=True)}")

    print("1c. Testing /api/predict for TSLA (Post-Backfill)...")
    resp = client.post('/api/predict', json={"symbol": "TSLA", "model_type": "rf"})
    print(f"Status: {resp.status_code}")
    print(f"Body: {resp.get_data(as_text=True)}")

    print("1d. Testing /api/predict for MSFT (Auto-Backfill Trigger)...")
    resp = client.post('/api/predict', json={"symbol": "MSFT", "model_type": "rf"})
    print(f"Status: {resp.status_code}")
    print(f"Body: {resp.get_data(as_text=True)}")

    print("1e. Testing /api/train for SPY with LSTM (Improved Architecture)...")
    # This might take a while, but confirms no crash
    resp = client.post('/api/train', json={"symbol": "SPY", "model_type": "lstm"})
    print(f"Status: {resp.status_code}")
    # Don't print full body if huge, but here it's small JSON
    print(f"Body: {resp.get_data(as_text=True)}")
    
    print("\n2. Testing /api/predict with INVALID JSON...")
    # Send empty body (content-type json but no data? or just invalid?)
    resp = client.post('/api/predict', data="INVALID", content_type='application/json')
    print(f"Status: {resp.status_code}")
    print(f"Body: {resp.get_data(as_text=True)}")

    print("\n3. Testing /api/predict with Missing Body...")
    resp = client.post('/api/predict') # No content type
    print(f"Status: {resp.status_code}")
    print(f"Body: {resp.get_data(as_text=True)}")

if __name__ == "__main__":
    test_routes()
