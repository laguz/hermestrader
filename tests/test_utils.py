from bot.utils import get_expiry_str

def test_get_expiry_str_valid_call():
    """Test valid call option symbol parsing."""
    assert get_expiry_str("AAPL230616C00150000") == "2023-06-16"

def test_get_expiry_str_valid_put():
    """Test valid put option symbol parsing."""
    assert get_expiry_str("TSLA240119P00200000") == "2024-01-19"

def test_get_expiry_str_invalid_date_format():
    """Test when regex matches but datetime parsing fails (e.g. invalid month/day)."""
    # 99 is invalid month, 99 is invalid day
    assert get_expiry_str("AAPL999999C00150000") is None
    # 2019 was not a leap year, so Feb 29 is invalid
    assert get_expiry_str("AAPL190229C00150000") is None

def test_get_expiry_str_missing_date_or_type():
    """Test symbols that do not match the expected pattern."""
    assert get_expiry_str("AAPL") is None
    # Matches length but uses X instead of C/P
    assert get_expiry_str("AAPL230616X00150000") is None
    # Missing date numbers
    assert get_expiry_str("AAPLC00150000") is None
