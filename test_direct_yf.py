import requests
import json
try:
    url = "https://query1.finance.yahoo.com/v8/finance/chart/TSLA?interval=1d&range=1d"
    headers = {'User-Agent': 'Mozilla/5.0'}
    resp = requests.get(url, headers=headers)
    data = resp.json()
    price = data['chart']['result'][0]['meta']['regularMarketPrice']
    print("Price via raw requests:", price)
except Exception as e:
    print("Error:", e)
