import pytest
from bot.utils import get_expiry_str

def test_get_expiry_str_happy_path():
    """Test get_expiry_str with a valid option symbol."""
    assert get_expiry_str("AAPL230120C00150000") == "2023-01-20"

def test_get_expiry_str_invalid_format():
    """Test get_expiry_str with an invalid symbol that doesn't match the regex."""
    assert get_expiry_str("AAPL") is None
    assert get_expiry_str("INVALID") is None

def test_get_expiry_str_value_error():
    """Test get_expiry_str with a symbol that matches regex but has an invalid date."""
    # 99 is year, 13 is month (invalid), 32 is day (invalid)
    assert get_expiry_str("AAPL991332C00150000") is None
    # Leap year issue, 2023-02-29 does not exist
    assert get_expiry_str("AAPL230229C00150000") is None
