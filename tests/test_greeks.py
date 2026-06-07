import pytest
import math
from hermes.greeks import (
    norm_cdf,
    norm_pdf,
    black_scholes_price,
    black_scholes_greeks,
    implied_volatility,
)

def test_norm_distribution():
    # CDF values
    assert pytest.approx(norm_cdf(0.0), abs=1e-7) == 0.5
    assert norm_cdf(-5.0) < 1e-5
    assert norm_cdf(5.0) > 0.9999
    
    # PDF values
    assert pytest.approx(norm_pdf(0.0), abs=1e-7) == 1.0 / math.sqrt(2.0 * math.pi)
    assert norm_pdf(3.0) < norm_pdf(0.0)

def test_black_scholes_pricing_and_greeks():
    # S=100, K=100, T=1 year (365 DTE), r=0.05 (5%), sigma=0.20 (20%)
    S, K, T, r, sigma = 100.0, 100.0, 1.0, 0.05, 0.20
    
    # Call option pricing
    call_price = black_scholes_price(S, K, T, r, sigma, "call")
    # Call price should be around 10.45
    assert pytest.approx(call_price, abs=1e-2) == 10.45058
    
    # Put option pricing
    put_price = black_scholes_price(S, K, T, r, sigma, "put")
    # Put price should satisfy Call-Put Parity: C - P = S - K * exp(-r * T)
    # 10.45058 - P = 100 - 100 * exp(-0.05) = 100 - 95.12294 = 4.87706
    # P = 10.45058 - 4.87706 = 5.57352
    assert pytest.approx(put_price, abs=1e-2) == 5.57352
    
    # Greeks
    call_greeks = black_scholes_greeks(S, K, T, r, sigma, "call")
    put_greeks = black_scholes_greeks(S, K, T, r, sigma, "put")
    
    # Call delta should be around 0.6368
    assert pytest.approx(call_greeks["delta"], abs=1e-2) == 0.6368
    # Put delta should be delta_call - 1 (around -0.3632)
    assert pytest.approx(put_greeks["delta"], abs=1e-2) == -0.3632
    
    # Gamma should be positive and identical for call/put
    assert call_greeks["gamma"] > 0
    assert pytest.approx(call_greeks["gamma"], abs=1e-7) == put_greeks["gamma"]
    
    # Vega should be positive and identical
    assert call_greeks["vega"] > 0
    assert pytest.approx(call_greeks["vega"], abs=1e-7) == put_greeks["vega"]
    
    # Theta should be negative (time decay)
    assert call_greeks["theta"] < 0
    assert put_greeks["theta"] < 0

def test_implied_volatility():
    S, K, T, r = 100.0, 100.0, 1.0, 0.05
    target_vol = 0.25
    
    # Generate Call and Put target prices
    call_mkt_price = black_scholes_price(S, K, T, r, target_vol, "call")
    put_mkt_price = black_scholes_price(S, K, T, r, target_vol, "put")
    
    # Solve for IV
    solved_call_iv = implied_volatility(call_mkt_price, S, K, T, r, "call")
    solved_put_iv = implied_volatility(put_mkt_price, S, K, T, r, "put")
    
    assert pytest.approx(solved_call_iv, abs=1e-4) == target_vol
    assert pytest.approx(solved_put_iv, abs=1e-4) == target_vol

def test_edge_cases():
    # T <= 0 (expired)
    assert black_scholes_price(100, 100, 0.0, 0.05, 0.20, "call") == 0.0
    assert black_scholes_price(105, 100, 0.0, 0.05, 0.20, "call") == 5.0
    assert black_scholes_price(95, 100, -0.1, 0.05, 0.20, "put") == 5.0
    
    # T <= 0 Greeks
    g_exp = black_scholes_greeks(105, 100, 0.0, 0.05, 0.20, "call")
    assert g_exp["delta"] == 1.0
    assert g_exp["gamma"] == 0.0
    assert g_exp["vega"] == 0.0
    assert g_exp["theta"] == 0.0
    
    # sigma <= 0
    assert pytest.approx(black_scholes_price(100, 100, 1.0, 0.05, 0.0, "call"), abs=1e-7) == 100.0 - 100.0 * math.exp(-0.05)
    
    # Invalid market price (below intrinsic)
    assert implied_volatility(1.0, 100, 100, 1.0, 0.05, "call") == 0.0
