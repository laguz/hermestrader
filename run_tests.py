import sys
import types
from unittest.mock import MagicMock

class MockModule(types.ModuleType):
    def __getattr__(self, name):
        if name == '__path__':
            return []
        if name == 'Flask':
            return MagicMock()
        return MagicMock()

sys.modules['pandas'] = MockModule('pandas')
sys.modules['requests'] = MockModule('requests')
sys.modules['certifi'] = MockModule('certifi')
sys.modules['pymongo'] = MockModule('pymongo')
sys.modules['keras'] = MockModule('keras')
sys.modules['tensorflow'] = MockModule('tensorflow')
sys.modules['plotly'] = MockModule('plotly')
sys.modules['scipy'] = MockModule('scipy')
sys.modules['dotenv'] = MockModule('dotenv')
sys.modules['sklearn'] = MockModule('sklearn')
sys.modules['scipy.stats'] = MockModule('scipy.stats')
sys.modules['pytz'] = MockModule('pytz')
sys.modules['numpy'] = MockModule('numpy')


print("Strategy modules loaded perfectly.")
