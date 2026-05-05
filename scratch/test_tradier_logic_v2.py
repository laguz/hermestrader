
import os
import sys
import json
import urllib.request
import urllib.error

def test_tradier():
    # Load env vars from .env
    env = {}
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            for line in f:
                if "=" in line and not line.startswith("#"):
                    key, val = line.strip().split("=", 1)
                    env[key] = val.strip('"')

    token = env.get("TRADIER_ACCESS_TOKEN")
    account_id = env.get("TRADIER_ACCOUNT_ID")
    base_url = env.get("TRADIER_ENDPOINT", "https://api.tradier.com/v1")

    print(f"Testing Tradier connection to {base_url} for account {account_id}...")

    url = f"{base_url}/accounts/{account_id}/balances"
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")
    
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            status = response.getcode()
            body = response.read().decode("utf-8")
            print(f"Status Code: {status}")
            print("Response:", json.loads(body))
    except urllib.error.HTTPError as e:
        print(f"HTTP Error {e.code}: {e.read().decode('utf-8')}")
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    test_tradier()
