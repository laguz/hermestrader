import unittest
from app import app
from services.container import Container

class TestAuthProtection(unittest.TestCase):
    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    def test_positions_protected(self):
        response = self.app.get('/api/positions')
        self.assertEqual(response.status_code, 302)
        self.assertIn('/login', response.location)

    def test_account_protected(self):
        response = self.app.get('/api/account')
        self.assertEqual(response.status_code, 302)
        self.assertIn('/login', response.location)

    def test_trading_protected(self):
        routes = [
            ('/manual_orders', 'GET'),
            ('/automated_trading', 'GET'),
            ('/bot_performance', 'GET'),
            ('/pnl', 'GET'),
            ('/api/orders', 'POST'),
            ('/api/bot/start', 'POST'),
            ('/api/bot/stop', 'POST'),
            ('/api/bot/watchlist', 'POST'),
            ('/api/bot/settings', 'POST'),
            ('/api/bot/dry_run', 'POST'),
            ('/api/bot/sync_positions', 'POST'),
            ('/api/pnl', 'GET')
        ]
        for route, method in routes:
            with self.subTest(route=route):
                if method == 'POST':
                    response = self.app.post(route)
                else:
                    response = self.app.get(route)
                self.assertEqual(response.status_code, 302, f"Route {route} failed with {response.status_code}")
                self.assertIn('/login', response.location)

    def test_ml_protected(self):
        routes = [
            ('/api/train', 'POST'),
            ('/api/predict', 'POST'),
            ('/api/evaluate', 'POST'),
            ('/api/history', 'GET'),
            ('/api/history/refresh', 'POST')
        ]
        for route, method in routes:
            with self.subTest(route=route):
                if method == 'POST':
                    response = self.app.post(route)
                else:
                    response = self.app.get(route)
                self.assertEqual(response.status_code, 302, f"Route {route} failed with {response.status_code}")
                self.assertIn('/login', response.location)

    def test_tradier_service_no_credentials(self):
        # Reset tradier service to ensure it uses default env (which we know are empty in .env)
        from services.tradier_service import TradierService
        ts = TradierService(access_token="", account_id="")
        
        # Should return [] or None instead of crashing or making bad requests
        result = ts.get_positions()
        self.assertEqual(result, [])
        
        result = ts.get_account_balances()
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()
