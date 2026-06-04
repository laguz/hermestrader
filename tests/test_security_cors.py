import unittest
import sys
import os
from unittest.mock import MagicMock, patch

# Note: We must mock BEFORE the app is imported in the test methods
# to handle the lack of dependencies in the test environment.

class TestSecurityCORS(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Mock dependencies
        sys.modules["fastapi"] = MagicMock()
        sys.modules["fastapi.staticfiles"] = MagicMock()
        cls.mock_cors = MagicMock()
        sys.modules["fastapi.middleware.cors"] = MagicMock()
        sys.modules["fastapi.middleware.cors"].CORSMiddleware = cls.mock_cors

        # Mock internal components
        mock_app_state = MagicMock()
        mock_app_state.STATIC_DIR = "/tmp/static"
        mock_app_state.db = MagicMock()
        sys.modules["hermes.service2_watcher._app_state"] = mock_app_state

        sys.modules["hermes.service2_watcher.routes"] = MagicMock()
        sys.modules["hermes.service2_watcher.routes.agent"] = MagicMock()
        sys.modules["hermes.service2_watcher.routes.analytics"] = MagicMock()
        sys.modules["hermes.service2_watcher.routes.approvals"] = MagicMock()
        sys.modules["hermes.service2_watcher.routes.charts"] = MagicMock()
        sys.modules["hermes.service2_watcher.routes.llm"] = MagicMock()
        sys.modules["hermes.service2_watcher.routes.soul"] = MagicMock()
        sys.modules["hermes.service2_watcher.routes.status"] = MagicMock()
        sys.modules["hermes.service2_watcher.routes.strategies"] = MagicMock()
        sys.modules["hermes.service2_watcher.routes.watchlist"] = MagicMock()

        mock_common = MagicMock()
        mock_common.STRATEGY_PRIORITIES = {}
        sys.modules["hermes.common"] = mock_common

    def test_cors_middleware_applied(self):
        # Clear any existing import to ensure fresh app creation
        if "hermes.service2_watcher.api" in sys.modules:
            del sys.modules["hermes.service2_watcher.api"]

        from hermes.service2_watcher.api import app

        # Verify CORSMiddleware was added
        found_cors = False
        for call in app.add_middleware.call_args_list:
            args, kwargs = call
            if args[0] == self.mock_cors:
                found_cors = True
                self.assertIn("http://localhost", kwargs.get('allow_origins'))
                self.assertTrue(kwargs.get('allow_credentials'))
                self.assertIn("GET", kwargs.get('allow_methods'))
                self.assertIn("POST", kwargs.get('allow_methods'))

        self.assertTrue(found_cors, "CORSMiddleware should be added to the app")

    def test_cors_custom_origins(self):
        # Clear any existing import
        if "hermes.service2_watcher.api" in sys.modules:
            del sys.modules["hermes.service2_watcher.api"]

        custom_origin = "https://dashboard.hermes.trading"
        with patch.dict(os.environ, {"HERMES_CORS_ORIGINS": custom_origin}):
            from hermes.service2_watcher.api import app

            for call in app.add_middleware.call_args_list:
                args, kwargs = call
                if args[0] == self.mock_cors:
                    self.assertIn(custom_origin, kwargs.get('allow_origins'))

if __name__ == "__main__":
    unittest.main()
