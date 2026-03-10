import logging
import numpy as np
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def generate_synthetic_history(symbol, start_date, end_date):
    """Generate synthetic price history using Geometric Brownian Motion (GBM).
    Used as fallback when Tradier API is unavailable."""
    # Approximate starting prices for common symbols
    default_prices = {
        'SPY': 450.0, 'QQQ': 380.0, 'IWM': 200.0,
        'AAPL': 190.0, 'MSFT': 370.0, 'TSLA': 250.0,
        'AMZN': 180.0, 'NVDA': 120.0, 'RIOT': 12.0,
    }
    start_price = default_prices.get(symbol, 100.0)
    
    # GBM parameters
    annual_return = 0.10   # 10% annualized drift
    annual_vol = 0.20      # 20% annualized volatility
    dt = 1.0 / 252.0       # Daily time step
    
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    history = []
    price = start_price
    current_dt = start_dt
    
    np.random.seed(42)  # Reproducible results
    
    while current_dt <= end_dt:
        # Skip weekends
        if current_dt.weekday() >= 5:
            current_dt += timedelta(days=1)
            continue
        
        # GBM step
        drift = (annual_return - 0.5 * annual_vol**2) * dt
        shock = annual_vol * np.sqrt(dt) * np.random.randn()
        price = price * np.exp(drift + shock)
        
        # Generate OHLCV
        daily_range = price * 0.015  # ~1.5% intraday range
        open_price = price + np.random.uniform(-daily_range/2, daily_range/2)
        high = max(price, open_price) + abs(np.random.normal(0, daily_range/3))
        low = min(price, open_price) - abs(np.random.normal(0, daily_range/3))
        volume = int(np.random.uniform(50_000_000, 150_000_000))
        
        history.append({
            'date': current_dt.strftime('%Y-%m-%d'),
            'open': round(open_price, 2),
            'high': round(high, 2),
            'low': round(low, 2),
            'close': round(price, 2),
            'volume': volume
        })
        
        current_dt += timedelta(days=1)
    
    logger.info(f"Generated {len(history)} synthetic data points for {symbol} "
                 f"(${start_price:.0f} → ${price:.2f})")
    return history
