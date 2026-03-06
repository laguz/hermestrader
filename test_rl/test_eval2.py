import requests
import json
import logging

logging.basicConfig(level=logging.INFO)

url = "http://127.0.0.1:8080/api/evaluate"
payload = {"symbol": "RIOT", "days": "30", "model_type": "rl"}
headers = {'Content-Type': 'application/json'}

try:
    response = requests.post(url, json=payload, headers=headers)
    print(json.dumps(response.json(), indent=2))
except Exception as e:
    print(f"Error: {e}")
