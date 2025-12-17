import requests
import os
import sys

# Add project root to sys.path
sys.path.append(os.getcwd())

from dotenv import load_dotenv
load_dotenv()

from services.tradier_service import TradierService

def test_api():
    # 1. Test Service Method
    print("Testing TradierService.get_positions()...")
    service = TradierService()
    try:
        positions = service.get_positions()
        print(f"Service returned: {positions}")
    except Exception as e:
        print(f"Service failed: {e}")

    # 2. Test Endpoint (requires app running, skipping for script standalone)
    # But we can verify imports and setup
    print("\nVerifying Flask app imports...")
    try:
        from app import app
        print("Flask app imported successfully.")
    except Exception as e:
        print(f"Flask app import failed: {e}")

if __name__ == "__main__":
    test_api()
