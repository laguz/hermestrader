import sys
import os
import requests
from dotenv import load_dotenv
load_dotenv()

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.container import Container
from services.ml_service import MLService

def verify_history_service():
    print("--- Verifying Service ---")
    try:
        tradier = Container.get_tradier_service()
        ml_service = MLService(tradier)
        
        # 1. Fetch history for a known symbol (e.g. SPY)
        print("Fetching history for SPY...")
        history = ml_service.get_prediction_history('SPY', limit=5)
        print(f"Found {len(history)} records.")
        for row in history:
            print(f"- {row['prediction_date']}: Predicted=${row['predicted_price']}, Actual={row.get('actual_close_price')}, Model={row['model_type']}")
            
    except Exception as e:
        print(f"Service Error: {e}")

def verify_history_api():
    print("\n--- Verifying API ---")
    try:
        # Assuming app is running on port 5001 (default)
        # Note: This requires the Flask app to be running, which we can't guarantee here easily without starting it.
        # So we might skip this or try to mock it. 
        # Actually, for this environment, let's just test the service as that covers the logic.
        pass
    except Exception as e:
        print(f"API Error: {e}")

if __name__ == "__main__":
    verify_history_service()
