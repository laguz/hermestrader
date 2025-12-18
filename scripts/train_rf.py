import sys
import os
sys.path.append(os.getcwd())

from app import app
from services.container import Container

def train_rf():
    # Initialize app to get DB connection
    with app.app_context():
        client = app.test_client()
        
        print("Training RF Model for SPY (Return-based)...")
        resp = client.post('/api/train', json={"symbol": "SPY", "model_type": "rf"})
        
        print(f"Status: {resp.status_code}")
        print(f"Body: {resp.get_data(as_text=True)}")

if __name__ == "__main__":
    train_rf()
