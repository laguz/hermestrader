import requests
import time

def test_routes():
    routes = [
        '/', '/login', '/register', '/dashboard', '/pnl', '/manual_orders',
        '/automated_trading', '/bot_performance', '/rule1', '/ai_prediction', '/evaluation'
    ]
    for r in routes:
        try:
            res = requests.get(f'http://localhost:5001{r}')
            print(f"{r}: {res.status_code}")
        except Exception as e:
            print(f"{r}: {e}")

test_routes()
