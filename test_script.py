import requests
import json
print(json.dumps(requests.get('http://127.0.0.1:5000/api/analysis/tsla').json()).split('atr')[1][:100])
