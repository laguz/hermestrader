
import os
import sys
from unittest.mock import MagicMock

# Mock missing dependencies
sys.modules["numpy"] = MagicMock()
sys.modules["pandas"] = MagicMock()
sys.modules["hermes.ml.pop_engine"] = MagicMock()

import requests

def test_tradier():
    # Load env vars from .env
    env = {}
    with open(".env", "r") as f:
        for line in f:
            if "=" in line and not line.startswith("#"):
                key, val = line.strip().split("=", 1)
                env[key] = val.strip('"')

    token = env.get("TRADIER_ACCESS_TOKEN")
    account_id = env.get("TRADIER_ACCOUNT_ID")
    base_url = env.get("TRADIER_ENDPOINT", "https://api.tradier.com/v1")

    print(f"Testing Tradier connection to {base_url} for account {account_id}...")

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    
    url = f"{base_url}/accounts/{account_id}/balances"
    try:
        r = requests.get(url, headers=headers, timeout=10)
        print(f"Status Code: {r.status_code}")
        if r.ok:
            print("Response:", r.json())
        else:
            print("Error Response:", r.text)
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    test_tradier()
