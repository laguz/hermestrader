import pandas as pd
import numpy as np
import logging

def calculate_growth_rate(series, periods):
    """
    Calculates the CAGR over a specific number of periods.
    Formula: (End / Start)^(1/n) - 1
    """
    if len(series) < periods + 1:
        return None
    
    # Get the value 'periods' years ago (index -periods)
    # Series is sorted ascending by Year
    
    current_val = series.iloc[-1]
    past_val = series.iloc[-(periods + 1)]
    
    if past_val <= 0 or current_val <= 0:
        return None # Growth rate irrelevant if negative or zero base/end
        
    rate = (current_val / past_val)**(1/periods) - 1
    return rate

def calculate_metrics(df, splits=None):
    """
    Calculates ROIC and Growth Rates (1, 3, 5, 10 years).
    Adjusts per-share metrics (EPS) and Shares for splits.
    Derived metrics (BVPS) are calculated after adjustment.
    """
    if df is None or df.empty:
        return None
    
    # --- Split Normalization ---
    # We need to normalize historical EPS to be comparable to current price.
    # We effectively adjust historicals "down" if there was a split (e.g. 2:1 split -> Hist EPS / 2).
    # Splits from yfinance are ratios: e.g. 2.0 (2:1).
    # If Split Date > Row Date: Row Value = Row Value / Ratio.
    
    # Create copies to avoid setting warnings
    df = df.copy()
    
    logger = logging.getLogger(__name__)

    if splits is not None and not splits.empty:
        # Splits is a Series: Date -> Ratio. E.g. 2020-08-31: 4.0
        # Iterate over splits
        for date, ratio in splits.items():
            # Find rows where FilingDate < Split Date
            if 'FilingDate' in df.columns:
                split_date = pd.to_datetime(date)
                if split_date.tzinfo is not None:
                    split_date = split_date.tz_localize(None)
                mask = pd.to_datetime(df['FilingDate']) < split_date
                
                if mask.any():
                    logger.info(f"Adjusting historical data for {ratio}:1 split on {date}")
                
                # Adjust EPS (Per Share)
                if 'EPS' in df.columns:
                    df.loc[mask, 'EPS'] = df.loc[mask, 'EPS'] / ratio
                
                # Adjust Shares (Total Count) - Inverse
                if 'Shares' in df.columns:
                    df.loc[mask, 'Shares'] = df.loc[mask, 'Shares'] * ratio
                    
                # Note: Totals (Net Income, Revenue, Equity) are NOT affected by splits.

    # --- Derived Metrics (Post-Split) ---
    
    # Book Value Per Share (BVPS) = Equity / Shares
    # Use Shares if available, otherwise fallback to Equity (assuming const shares, which is wrong but fallback)
    if 'Shares' in df.columns and 'Equity' in df.columns:
         df['BVPS'] = df['Equity'] / df['Shares']
    else:
         df['BVPS'] = df['Equity'] # Fallback, though growth rate will be same as Equity growth if shares constant
    
    # 1. ROIC Calculation
    # ROIC = NOPAT / Invested Capital (or simplified as Net Income / Equity + Debt - Cash)
    df['InvestedCapital'] = df['Equity'] + df['LongTermDebt'].fillna(0) - df['Cash'].fillna(0)
    df['ROIC'] = df['NetIncome'] / df['InvestedCapital']
    
    # We want historical ROIC (Need to check if it's consistently >= 10%)
    results = {}
    results['ROIC_History'] = df['ROIC'].tail(10).to_dict()
    results['Mean_ROIC_10y'] = df['ROIC'].tail(10).mean()
    results['Mean_ROIC_5y'] = df['ROIC'].tail(5).mean()
    results['Mean_ROIC_1y'] = df['ROIC'].tail(1).mean()
    
    # 2. Growth Rates
    # Use BVPS for Equity Growth
    years = [1, 3, 5, 10]
    metrics = {
        'Revenue_Growth': 'Revenue', 
        'EPS_Growth': 'EPS', 
        'Equity_Growth': 'BVPS', # Rule #1 uses Book Value Per Share
        'OCF_Growth': 'OCF'
    }
    
    for name, col in metrics.items():
        results[name] = {}
        if col not in df.columns:
            continue
            
        for y in years:
            rate = calculate_growth_rate(df[col], y)
            results[name][f'{y}y'] = rate

    results['Financials'] = df # Store raw data for debugging/display
    
    return results

def calculate_sticker_price(current_eps, growth_rate, future_pe, min_rate_of_return=0.15):
    """
    Calculates the Rule #1 Sticker Price.
    
    1. Future EPS = Current EPS * (1 + growth_rate)^10
    2. Future Price = Future EPS * Future PE
    3. Sticker Price = Future Price / (1 + min_rate_of_return)^10
    4. Buy Price = Sticker Price * 0.5 (Margin of Safety) - Optional return
    """
    
    if current_eps <= 0:
        return {'Error': "Current EPS is negative, cannot calculate projection."}
    
    future_eps = current_eps * ((1 + growth_rate) ** 10)
    future_price = future_eps * future_pe
    sticker_price = future_price / ((1 + min_rate_of_return) ** 10)
    
    return {
        'Current_EPS': current_eps,
        'Estimated_Growth_Rate': growth_rate,
        'Future_PE': future_pe,
        'Future_EPS_10y': future_eps,
         'Future_Stock_Price_10y': future_price,
        'Sticker_Price': sticker_price,
        'Buy_Price': sticker_price * 0.5
    }

def analyze_stock(ticker, df, splits=None):
    """
    Main analysis function calling metrics and valuation.
    """
    metrics = calculate_metrics(df, splits)
    if not metrics:
        return None, {'Error': "Not enough data"}
    
    # Determine Growth Rate to use:
    # Rule #1 says use the LOWER of historical Equity growth (BVPS) or EPS growth, 
    # or Analyst estimates. We'll stick to historicals for now.
    # We look at 10y, 5y, 3y averages and be conservative.
    
    # Let's extract valid growth rates
    def get_valid_rate(metric_dict, period):
        val = metric_dict.get(period)
        return val if val is not None else -999
    
    # Simple logic: Take the minimum of the 10y/5y averages of Equity and EPS.
    # If 10y unavailable, use 5y.
    
    # Example logic:
    # 1. Get Equity Growth (5y and 10y)
    eq_10 = get_valid_rate(metrics['Equity_Growth'], '10y')
    eq_5 = get_valid_rate(metrics['Equity_Growth'], '5y')
    
    # 2. Get EPS Growth
    eps_10 = get_valid_rate(metrics['EPS_Growth'], '10y')
    eps_5 = get_valid_rate(metrics['EPS_Growth'], '5y')
    
    # Basic selection:
    rates = [r for r in [eq_10, eq_5, eps_10, eps_5] if r > -0.5] # Filter garbage
    
    if not rates:
        return metrics, {'Error': "Could not determine a positive growth rate."}
    
    estimated_growth = min(rates) # Conservative: take the lowest reasonable historical rate
    
    # Fallback: If growth negative, use 10y ROIC (proxy for internal compounding)
    if estimated_growth < 0:
        mean_roic = metrics.get('Mean_ROIC_10y', 0)
        if mean_roic > 0:
             estimated_growth = mean_roic

    # Cap growth rate? Rule #1 often caps at 15% or 20% to be conservative.
    estimated_growth = min(estimated_growth, 0.20)
    
    # Future PE: Rule #1 says defaults to lower of (2 * GrowthRate * 100) or (Historical High/Avg PE).
    # Since we don't have historical PE easily without price history, we can approximate.
    # Often PE is capped at 40 or 50.
    # Standard Rule #1: Future PE = 2 * (Growth Rate * 100). E.g. 10% growth -> PE 20.
    future_pe = 2 * (estimated_growth * 100)
    
    # However, Future PE should not exceed historical PE averages generally. 
    # Without price history, we can't check historical PE. 
    # NOTE: The user prompt asked to use EDGAR. Does EDGAR have price? No.
    # We might lack Price data to calculate historical PE or current PE.
    # We need CURRENT PRICE to compare Sticker Price to.
    # We need CURRENT PRICE to calculate Current PE.
    # Wait, the user didn't ask us to scrape Yahoo Finance/Polygon. 
    # Challenge: Where to get Current Price?
    # EDGAR doesn't provide real-time stock prices.
    # If the user only wants "Sticker Price" output, we can calculate that without current price.
    # But "Please don't invest" implies comparing Sticker to Current Price.
    
    # I will add a placeholder for fetching current price, maybe using a free simple API or scraping?
    # Or I'll ask for it in the UI? 
    # Prompt says: "Input: stock symbol." "Output: Sticker Price or '...'"
    
    # I will stick to calculating Sticker Price. Comparison might be done by user or if I can fetch price.
    # I'll try to find a free source for current price in `edgar_client` or just output the Sticker Price.
    # Actually, often these apps use `yfinance` or similar. The user didn't ban it.
    # But for now, let's just calculate the Sticker Price.
    
    # Use adjusted EPS if available
    adjusted_df = metrics.get('Financials', df)
    current_eps = adjusted_df['EPS'].iloc[-1]
    
    valuation = calculate_sticker_price(current_eps, estimated_growth, future_pe)
    
    return metrics, valuation
