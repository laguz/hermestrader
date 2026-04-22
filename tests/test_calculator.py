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

import pandas as pd
from logic.calculator import analyze_stock

def create_financial_df(periods=11, growth_rate=0.10, base_eps=1.0, base_equity=100.0, base_shares=100.0, base_revenue=500.0, base_ocf=50.0):
    """Helper to create a predictable financial DataFrame."""
    data = []

    eps = base_eps
    equity = base_equity
    shares = base_shares
    revenue = base_revenue
    ocf = base_ocf

    for i in range(periods):
        net_income = eps * shares
        debt = 20.0
        cash = 10.0

        row = {
            'Year': 2010 + i,
            'EPS': eps,
            'Equity': equity,
            'Shares': shares,
            'Revenue': revenue,
            'OCF': ocf,
            'NetIncome': net_income,
            'LongTermDebt': debt,
            'Cash': cash
        }
        data.append(row)

        # Grow the values for the next period
        eps *= (1 + growth_rate)
        equity *= (1 + growth_rate)
        revenue *= (1 + growth_rate)
        ocf *= (1 + growth_rate)

    return pd.DataFrame(data)

def test_analyze_stock_happy_path():
    """Test analyze_stock with a valid 10-year growth DataFrame."""
    df = create_financial_df(periods=11, growth_rate=0.10, base_eps=2.0)
    metrics, valuation = analyze_stock('AAPL', df)

    assert metrics is not None
    assert valuation is not None

    assert 'Error' not in valuation

    # Check valuation keys
    assert 'Current_EPS' in valuation
    assert 'Estimated_Growth_Rate' in valuation
    assert 'Future_PE' in valuation
    assert 'Future_EPS_10y' in valuation
    assert 'Future_Stock_Price_10y' in valuation
    assert 'Sticker_Price' in valuation
    assert 'Buy_Price' in valuation

    # Growth rate should be approximately 0.10 (lowest of 10y/5y average EPS/Equity growth)
    assert valuation['Estimated_Growth_Rate'] == pytest.approx(0.10, rel=1e-4)

    # Future PE = 2 * (0.10 * 100) = 20
    assert valuation['Future_PE'] == pytest.approx(20.0, rel=1e-4)


def test_analyze_stock_insufficient_data():
    """Test analyze_stock with an empty or None DataFrame."""
    metrics, valuation = analyze_stock('AAPL', pd.DataFrame())
    assert metrics is None
    assert valuation == {'Error': 'Not enough data'}

    metrics, valuation = analyze_stock('AAPL', None)
    assert metrics is None
    assert valuation == {'Error': 'Not enough data'}

def test_analyze_stock_negative_growth_fallback():
    """Test analyze_stock fallback to 10y ROIC when growth is negative."""
    # Negative growth rate
    df = create_financial_df(periods=11, growth_rate=-0.05, base_eps=2.0)

    # We need ROIC to be positive to trigger the fallback logic.
    # Current ROIC in create_financial_df:
    # NetIncome = EPS * Shares
    # Equity = grows with eps
    # Debt = 20, Cash = 10
    # ROIC = NetIncome / (Equity + Debt - Cash)
    # Since EPS and Equity shrink, ROIC will fluctuate but should be positive

    metrics, valuation = analyze_stock('AAPL', df)

    assert metrics is not None
    assert valuation is not None
    assert 'Error' not in valuation

    # Fallback to mean ROIC when estimated growth is < 0.
    # The mean ROIC will be > 0 in this case, so it should be used instead of -0.05
    assert valuation['Estimated_Growth_Rate'] > 0
    # The estimated growth rate should equal the mean 10y ROIC
    assert valuation['Estimated_Growth_Rate'] == pytest.approx(min(metrics['Mean_ROIC_10y'], 0.20), rel=1e-4)


def test_analyze_stock_capped_growth():
    """Test analyze_stock with growth > 20% caps at 20%."""
    df = create_financial_df(periods=11, growth_rate=0.30, base_eps=2.0)

    metrics, valuation = analyze_stock('AAPL', df)

    assert metrics is not None
    assert valuation is not None
    assert 'Error' not in valuation

    # The actual growth is 30%, but it should be capped at 20%
    assert valuation['Estimated_Growth_Rate'] == pytest.approx(0.20, rel=1e-4)
