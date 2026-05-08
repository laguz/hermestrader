import sys
import unittest
from unittest.mock import MagicMock

# 1. Handle missing dependencies safely for the environment
try:
    import pandas as pd
    import numpy as np
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    mock_pd = MagicMock()
    mock_np = MagicMock()
    sys.modules["pandas"] = mock_pd
    sys.modules["numpy"] = mock_np
    import pandas as pd
    import numpy as np

from hermes.charts.provider import _bollinger

class TestChartsProviderBollinger(unittest.TestCase):
    def test_bollinger_formula_logic(self):
        """Verify the internal arithmetic and parameter propagation using mocks."""
        series = MagicMock()
        rolling_obj = MagicMock()
        series.rolling.return_value = rolling_obj

        # Mock mean=100, std=5
        rolling_obj.mean.return_value = 100.0
        rolling_obj.std.return_value = 5.0

        # Test default n=20, k=2.0
        upper, mid, lower = _bollinger(series)
        series.rolling.assert_called_with(20)
        self.assertEqual(mid, 100.0)
        self.assertEqual(upper, 110.0) # 100 + 2*5
        self.assertEqual(lower, 90.0)  # 100 - 2*5

        # Test custom n=10, k=3.0
        series.rolling.reset_mock()
        upper, mid, lower = _bollinger(series, n=10, k=3.0)
        series.rolling.assert_called_with(10)
        self.assertEqual(upper, 115.0) # 100 + 3*5
        self.assertEqual(lower, 85.0)  # 100 - 3*5

    @unittest.skipUnless(PANDAS_AVAILABLE, "Pandas/Numpy not available in this environment")
    def test_bollinger_with_real_data(self):
        """Verify mathematical correctness with real pandas/numpy data."""
        # Create a predictable sequence
        # n=5, so the first 4 will be NaN
        data = [10.0, 20.0, 30.0, 40.0, 50.0]
        s = pd.Series(data)

        upper, mid, lower = _bollinger(s, n=5, k=2.0)

        # Last element: mean of (10,20,30,40,50) = 30.0
        # std of (10,20,30,40,50) ≈ 15.811388
        expected_mid = 30.0
        expected_std = np.std(data, ddof=1) # pandas uses ddof=1 by default

        self.assertAlmostEqual(mid.iloc[-1], expected_mid)
        self.assertAlmostEqual(upper.iloc[-1], expected_mid + 2.0 * expected_std)
        self.assertAlmostEqual(lower.iloc[-1], expected_mid - 2.0 * expected_std)

        # Verify first 4 are NaN
        self.assertTrue(np.isnan(mid.iloc[0:4]).all())

    @unittest.skipUnless(PANDAS_AVAILABLE, "Pandas/Numpy not available in this environment")
    def test_bollinger_edge_cases(self):
        """Verify behavior with empty or short series."""
        # Empty
        s_empty = pd.Series([], dtype=float)
        upper, mid, lower = _bollinger(s_empty, n=20)
        self.assertEqual(len(mid), 0)

        # Shorter than window
        s_short = pd.Series([1, 2, 3])
        upper, mid, lower = _bollinger(s_short, n=5)
        self.assertTrue(np.isnan(mid).all())

if __name__ == "__main__":
    unittest.main()
