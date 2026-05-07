import numpy as np
import pandas as pd
import pytest
from hermes.ml.pop_engine import find_key_levels

def test_find_key_levels_empty_series():
    """
    Test that find_key_levels returns an empty list when provided with empty series.
    This verifies the early return logic:
    if n == 0:
        return []
    """
    close_series = pd.Series([], dtype=float)
    volume_series = pd.Series([], dtype=float)

    # Execute
    result = find_key_levels(close_series, volume_series)

    # Verify
    assert result == [], "Expected empty list for empty input series"

def test_find_key_levels_no_pivots():
    """
    Test that find_key_levels returns an empty list when no pivots are found.
    This verifies the second early return logic:
    if len(all_pivots_idx) == 0:
        return []
    """
    # A short series without enough data to form pivots with window=5
    close_series = pd.Series([100.0, 101.0, 102.0])
    volume_series = pd.Series([1000, 1100, 1200])

    # Execute
    result = find_key_levels(close_series, volume_series, window=5)

    # Verify
    assert result == [], "Expected empty list when no pivots are found"

def test_find_key_levels_with_data():
    """
    Smoke test to ensure it works with some data that should produce pivots.
    """
    np.random.seed(42)
    # 20 points, should be enough for window=5 to find something if there's a peak
    prices = [100, 101, 102, 103, 104, 105, 104, 103, 102, 101, 100, 99, 98, 97, 96, 95, 96, 97, 98, 99]
    volumes = np.random.randint(100, 1000, size=len(prices))

    close_series = pd.Series(prices)
    volume_series = pd.Series(volumes)

    result = find_key_levels(close_series, volume_series, window=2, n_clusters=2)

    assert isinstance(result, list)
    if len(result) > 0:
        for level in result:
            assert "price" in level
            assert "type" in level
            assert "strength" in level
