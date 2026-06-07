"""Local Black-Scholes Greeks and Implied Volatility calculations.

Does not require scipy. Uses standard math library and error function erf.
"""
from __future__ import annotations

import math

def norm_cdf(x: float) -> float:
    """Cumulative distribution function of standard normal distribution."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def norm_pdf(x: float) -> float:
    """Probability density function of standard normal distribution."""
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)

def black_scholes_price(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    option_type: str,
) -> float:
    """Calculate the theoretical Black-Scholes price of a European option.

    S: Spot price of underlying
    K: Strike price
    T: Time to expiration in years (DTE / 365.0)
    r: Risk-free interest rate (annualized, e.g. 0.05 for 5%)
    sigma: Implied volatility (annualized, e.g. 0.30 for 30%)
    option_type: 'call' or 'put'
    """
    option_type = option_type.lower()
    if S <= 0 or K <= 0:
        return 0.0
    if T <= 0:
        if option_type == "call":
            return max(0.0, S - K)
        else:
            return max(0.0, K - S)
    if sigma <= 0:
        if option_type == "call":
            return max(0.0, S - K * math.exp(-r * T))
        else:
            return max(0.0, K * math.exp(-r * T) - S)

    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)

    if option_type == "call":
        price = S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
    else:
        price = K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)

    return max(0.0, price)

def black_scholes_greeks(
    S: float,
    K: float,
    T: float,
    r: float,
    sigma: float,
    option_type: str,
) -> dict[str, float]:
    """Calculate theoretical Black-Scholes greeks of a European option.

    S: Spot price of underlying
    K: Strike price
    T: Time to expiration in years (DTE / 365.0)
    r: Risk-free interest rate (annualized, e.g. 0.05 for 5%)
    sigma: Implied volatility (annualized, e.g. 0.30 for 30%)
    option_type: 'call' or 'put'

    Returns a dict with delta, gamma, vega (annual), theta (annual), and theta_daily.
    """
    option_type = option_type.lower()
    greeks = {
        "delta": 0.0,
        "gamma": 0.0,
        "vega": 0.0,
        "theta": 0.0,
        "theta_daily": 0.0,
    }
    if S <= 0 or K <= 0:
        return greeks
    if T <= 0:
        if option_type == "call":
            greeks["delta"] = 1.0 if S > K else 0.0
        else:
            greeks["delta"] = -1.0 if S < K else 0.0
        return greeks
    if sigma <= 0:
        discounted_strike = K * math.exp(-r * T)
        if option_type == "call":
            greeks["delta"] = 1.0 if S > discounted_strike else 0.0
        else:
            greeks["delta"] = -1.0 if S < discounted_strike else 0.0
        return greeks

    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T

    # Delta
    if option_type == "call":
        greeks["delta"] = norm_cdf(d1)
    else:
        greeks["delta"] = norm_cdf(d1) - 1.0

    # Gamma
    greeks["gamma"] = norm_pdf(d1) / (S * sigma * sqrt_T)

    # Vega (annual derivative)
    greeks["vega"] = S * norm_pdf(d1) * sqrt_T

    # Theta (annual)
    term1 = -(S * norm_pdf(d1) * sigma) / (2.0 * sqrt_T)
    term2 = r * K * math.exp(-r * T)
    if option_type == "call":
        greeks["theta"] = term1 - term2 * norm_cdf(d2)
    else:
        greeks["theta"] = term1 + term2 * norm_cdf(-d2)

    # Theta per calendar day
    greeks["theta_daily"] = greeks["theta"] / 365.0

    return greeks

def implied_volatility(
    market_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    option_type: str,
    max_iter: int = 100,
    tolerance: float = 1e-5,
) -> float:
    """Find implied volatility given the market price of an option.

    Uses Newton-Raphson method with Bisection method fallback.
    """
    option_type = option_type.lower()
    if T <= 0:
        return 0.0

    discounted_strike = K * math.exp(-r * T)
    if option_type == "call":
        intrinsic = max(0.0, S - discounted_strike)
        max_price = S
    else:
        intrinsic = max(0.0, discounted_strike - S)
        max_price = discounted_strike

    if market_price <= intrinsic:
        return 0.0
    if market_price >= max_price:
        return 5.0  # limit upper bound

    # Initial guess
    sigma = 0.5

    # Newton-Raphson iteration
    for _ in range(max_iter):
        p = black_scholes_price(S, K, T, r, sigma, option_type)
        diff = p - market_price
        if abs(diff) < tolerance:
            return sigma

        g = black_scholes_greeks(S, K, T, r, sigma, option_type)
        vega = g.get("vega", 0.0)

        # Fallback to Bisection if Vega gets too small or Newton-Raphson overflows
        if vega < 1e-4:
            break

        step = diff / vega
        sigma -= step

        # If guess wanders out of bound, fall back to bisection
        if sigma <= 0.001 or sigma > 5.0:
            break
    else:
        if 0.001 < sigma <= 5.0:
            return sigma

    # Bisection search fallback
    low = 0.0001
    high = 10.0
    for _ in range(100):
        mid = (low + high) / 2.0
        p = black_scholes_price(S, K, T, r, mid, option_type)
        diff = p - market_price
        if abs(diff) < tolerance:
            return mid
        if diff > 0:
            high = mid
        else:
            low = mid

    return (low + high) / 2.0
