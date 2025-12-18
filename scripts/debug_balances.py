from services.container import Container
from dotenv import load_dotenv
import json

load_dotenv()

def debug_balances():
    tradier = Container.get_tradier_service()
    print(f"Fetching balances for account: {tradier.account_id}")
    
    # We want to inspect the raw response if possible, but the service method returns a processed dict.
    # Let's call the private request method or just invoke the public method and infer.
    # Actually, let's copy the logic to see the raw response.
    import requests
    url = f"{tradier.endpoint}/accounts/{tradier.account_id}/balances"
    headers = tradier._get_headers()
    
    try:
        res = requests.get(url, headers=headers)
        print(f"Status: {res.status_code}")
        data = res.json()
        print("RAW RESPONSE:")
        print(json.dumps(data, indent=2))
        
        # Test the service method
        processed = tradier.get_account_balances()
        print("\nPROCESSED SERVICE OUTPUT:")
        print(json.dumps(processed, indent=2))
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_balances()
