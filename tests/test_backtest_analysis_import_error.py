import unittest
from unittest.mock import patch, MagicMock
import sys

class TestBacktestAnalysisImportError(unittest.TestCase):
    def test_analysis_service_import_error(self):
        # These are modules that the backtest_service and its transitive dependencies
        # might try to import. We mock them to ensure the test can run in environments
        # missing these libraries.
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
            # Specifically mock the one we want to fail
            'services.analysis_service': None
        }

        # We need to remove the target from sys.modules to ensure import triggers the code
        if 'services.backtest_service' in sys.modules:
            del sys.modules['services.backtest_service']

        with patch.dict(sys.modules, required_mocks):
            # Now import backtest_service which should trigger the try/except
            import services.backtest_service

            # Check if AnalysisService is None as expected
            self.assertIsNone(services.backtest_service.AnalysisService,
                             "AnalysisService should be None after import failure")

if __name__ == '__main__':
    unittest.main()
