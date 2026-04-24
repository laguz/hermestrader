
import requests

def test_live_api():
    url = "http://localhost:8080/api/bot/dry_run"
    
    payload = {
        "credit_spreads_watchlist": ["SPY"],
        "wheel_watchlist": ["SPY"]
    }
    
    print(f"Sending POST to {url}...")
    try:
        response = requests.post(url, json=payload, timeout=60)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            logs = data.get('logs', [])
            print(f"Logs Received: {len(logs)} lines")
            
            # Check for Wheel Strategy marker
            has_wheel = any("Wheel Strategy" in line for line in logs)
            has_cs = any("Credit Spread Strategy" in line for line in logs)
            
            print(f"Contains 'Credit Spread Strategy': {has_cs}")
            print(f"Contains 'Wheel Strategy': {has_wheel}")
            
            if has_wheel:
                print("✅ Server is running UPDATED code.")
            else:
                print("❌ Server is running OLD code (Wheel Strategy missing).")
                
            print("\nFull Logs:")
            for line in logs:
                print(line)
        else:
            print(f"Error: {response.text}")
            
    except Exception as e:
        print(f"Request Failed: {e}")

if __name__ == "__main__":
    test_live_api()
