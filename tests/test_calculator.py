import pytest
from logic.calculator import calculate_sticker_price

def test_calculate_sticker_price_happy_path():
    """Test sticker price calculation with valid positive inputs."""
    # current_eps = 2.0
    # growth_rate = 0.10 (10%)
    # future_pe = 20
    # min_rate_of_return = 0.15 (15%) - default

    # 1. future_eps = 2.0 * (1 + 0.10)^10 = 2.0 * 2.5937424601 = 5.1874849202
    # 2. future_price = 5.1874849202 * 20 = 103.749698404
    # 3. sticker_price = 103.749698404 / (1 + 0.15)^10 = 103.749698404 / 4.0455577357 = 25.6453412586
    # 4. buy_price = 25.6453412586 * 0.5 = 12.8226706293

    result = calculate_sticker_price(2.0, 0.10, 20)

    assert result is not None
    assert result['Current_EPS'] == 2.0
    assert result['Estimated_Growth_Rate'] == 0.10
    assert result['Future_PE'] == 20
    assert result['Future_EPS_10y'] == pytest.approx(5.18748, rel=1e-4)
    assert result['Future_Stock_Price_10y'] == pytest.approx(103.7497, rel=1e-4)
    assert result['Sticker_Price'] == pytest.approx(25.6453, rel=1e-4)
    assert result['Buy_Price'] == pytest.approx(12.8226, rel=1e-4)

def test_calculate_sticker_price_custom_min_return():
    """Test sticker price calculation with a custom minimum rate of return."""
    # current_eps = 2.0
    # growth_rate = 0.10
    # future_pe = 20
    # min_rate_of_return = 0.20 (20%)

    # future_price = 103.749698404
    # sticker_price = 103.749698404 / (1 + 0.20)^10 = 103.749698404 / 6.1917364224 = 16.7561494918
    # buy_price = 16.7561494918 * 0.5 = 8.3780747459

    result = calculate_sticker_price(2.0, 0.10, 20, min_rate_of_return=0.20)

    assert result is not None
    assert result['Sticker_Price'] == pytest.approx(16.7561, rel=1e-4)
    assert result['Buy_Price'] == pytest.approx(8.3780, rel=1e-4)

def test_calculate_sticker_price_missing_eps():
    """Test that missing current_eps returns None."""
    assert calculate_sticker_price(0, 0.10, 20) is None
    assert calculate_sticker_price(None, 0.10, 20) is None

def test_calculate_sticker_price_missing_growth_rate():
    """Test that missing growth_rate returns None."""
    assert calculate_sticker_price(2.0, 0, 20) is None
    assert calculate_sticker_price(2.0, None, 20) is None

def test_calculate_sticker_price_missing_future_pe():
    """Test that missing future_pe returns None."""
    assert calculate_sticker_price(2.0, 0.10, 0) is None
    assert calculate_sticker_price(2.0, 0.10, None) is None
