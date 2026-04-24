import unittest
from unittest.mock import patch, MagicMock
import sys

class TestImportError(unittest.TestCase):
    def test_import_error(self):
        if 'services.backtest_service' in sys.modules:
            del sys.modules['services.backtest_service']

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
            'bot': MagicMock(),
            'bot.strategies': MagicMock(),
            'bot.strategies.wheel': MagicMock(),
            'utils': MagicMock(),
            'utils.indicators': MagicMock(),
            'utils.data_generator': MagicMock(),
            'services.analysis_service': None
        }

        with patch.dict(sys.modules, required_mocks):
            import services.backtest_service
            self.assertIsNone(services.backtest_service.AnalysisService)

if __name__ == '__main__':
    unittest.main()
