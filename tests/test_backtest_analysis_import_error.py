import unittest
from unittest.mock import patch, MagicMock
import sys

class TestBacktestAnalysisImportError(unittest.TestCase):
    def test_analysis_service_import_error(self):
        # We need to ensure we can mock successfully
        # so let's mock all third party dependencies to be safe.
        required_mocks = {
            'pandas': MagicMock(),
            'numpy': MagicMock(),
            'scipy': MagicMock(),
            'scipy.stats': MagicMock(),
            'scipy.signal': MagicMock(),
            'certifi': MagicMock(),
            'pymongo': MagicMock(),
            'flask': MagicMock(),
            'flask_login': MagicMock(),
            'dotenv': MagicMock(),
            'sklearn': MagicMock(),
            'sklearn.cluster': MagicMock(),
            'tensorflow': MagicMock(),
            'keras': MagicMock(),
            'plotly': MagicMock(),
            'yfinance': MagicMock(),
            'nostr_sdk': MagicMock(),
            'requests': MagicMock(),
        }

        if 'services.backtest_service' in sys.modules:
            del sys.modules['services.backtest_service']

        original_import = __import__
        def mock_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == 'services.analysis_service':
                raise ImportError("Mocked ImportError for services.analysis_service")
            return original_import(name, globals, locals, fromlist, level)

        with patch.dict(sys.modules, required_mocks):
            with patch('builtins.__import__', side_effect=mock_import):
                # Import the module so the try block gets executed and catches our forced ImportError
                import services.backtest_service

                # Verify that it caught the error and assigned None
                self.assertIsNone(services.backtest_service.AnalysisService,
                                 "AnalysisService should be None after import failure")

if __name__ == '__main__':
    unittest.main()
