import requests
import time

def test_http_sync():
    url = "http://127.0.0.1:8080/api/bot/sync_positions"
    print(f"POST {url}...")
    try:
        start = time.time()
        res = requests.post(url)
        duration = time.time() - start
        print(f"Status: {res.status_code}")
        print(f"Response: {res.text}")
        print(f"Duration: {duration:.2f}s")
    except Exception as e:
        print(f"Request failed: {e}")

    print("\nGET http://127.0.0.1:8080/api/pnl...")
    try:
        res = requests.get("http://127.0.0.1:8080/api/pnl")
        data = res.json()
        pnl = data.get('pnl_data', {})
        open_len = len(pnl.get('open', []))
        closed_len = len(pnl.get('closed', []))
        print(f"Status: {res.status_code}")
        print(f"Open: {open_len}")
        print(f"Closed: {closed_len}")
    except Exception as e:
        print(f"P&L Fetch Failed: {e}")
        
    if closed_len == 0:
        print("WARNING: No closed positions returned!")

if __name__ == "__main__":
    test_http_sync()
