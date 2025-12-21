import pandas as pd
from datetime import datetime
# Placeholder for strategy logic
# In a real engine, we would have classes for Strategy, Position, etc.

class BacktestService:
    def __init__(self, tradier_service):
        self.tradier = tradier_service

from utils.indicators import calculate_rsi, calculate_bollinger_bands, calculate_support_resistance, find_key_levels
import numpy as np

from datetime import datetime, timedelta

class BacktestService:
    def __init__(self, tradier_service):
        self.tradier = tradier_service

    def run_backtest(self, symbol, strategy_type, start_date, end_date):
        print(f"DEBUG: Running backtest for {symbol}, {strategy_type}, {start_date} to {end_date}")
        # Fetch underlying history with 60 days buffer warm-up for indicators
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        warmup_start_date = (start_dt - timedelta(days=60)).strftime('%Y-%m-%d')
        
        history = self.tradier.get_historical_pricing(symbol, warmup_start_date, end_date)
        if not history:
            print("DEBUG: No history returned from Tradier.")
            return {"error": "No historical data found"}
        
        print(f"DEBUG: Fetched {len(history)} candles. Processing...")

        df = pd.DataFrame(history)
        df['date'] = pd.to_datetime(df['date'])
        
        # Calculate Indicators on the full dataset (including warm-up)
        try:
            df['rsi'] = calculate_rsi(df['close'])
            df['upper_bb'], df['mid_bb'], df['lower_bb'] = calculate_bollinger_bands(df['close'])
            df['support'], df['resistance'] = calculate_support_resistance(df['close'])
        except Exception as e:
            print(f"DEBUG: Error calculating indicators: {e}")
            return {"error": f"Indicator error: {e}"}
        
        # Filter down to the requested date range
        # We use a copy to avoid SettingWithCopy warnings if applicable
        df = df[df['date'] >= start_dt].copy()
        print(f"DEBUG: {len(df)} candles remaining after date filtering.")
        
        if df.empty:
             print("DEBUG: No data left after filtering.")
             return {"error": "No data found for requested range after warmup"}

        results = {
            "dates": df['date'].dt.strftime('%Y-%m-%d').tolist(),
            "portfolio_value": [],
            "trades": []
        }
        
        current_cash = 10000.0
        active_position = None # { 'type':, 'entry_price':, 'short_strike':, 'long_strike':, 'credit':, 'opened_at':, 'days_held':, 'days_itm': }
        portfolio_history = []
        
        # Simulation Loop
        import math
        
        for index, row in df.iterrows():
            if index < 90: continue # Need history for improved algos
            
            date = row['date']
            price = row['close']
            rsi = row['rsi']
            
            # Rolling Window for Analysis (Last 90 days)
            # Use iloc to slice (index is label based if not reset, but iterrows yields label)
            # We need positional slicing.
            # Let's trust df is sorted by date.
            # Convert index label to integer location? df.index is usually RangeIndex if reset.
            # Let's assume we are iterating linear.
            
            # Optimization: We already have indicators pre-calculated for simple things.
            # But for Key Levels (KMeans), we need the slice.
            
            # Get integer location for slicing
            i = df.index.get_loc(index)
            start_i = max(0, i - 90)
            window_df = df.iloc[start_i:i+1] # Include current day as "latest known"
            
            key_levels = []
            volatility = 0.5
            
            if strategy_type == "credit_spread":
                # efficient calculation on window
                key_levels = find_key_levels(
                    window_df['close'], 
                    window_df['volume'], 
                    high_series=window_df['high'], 
                    low_series=window_df['low']
                )
                
                # Calculate Volatility for Delta Proxy
                # Daily Returns std dev * sqrt(252)
                if len(window_df) > 30:
                    returns = window_df['close'].pct_change().dropna()
                    volatility = returns.std() * math.sqrt(252)
            
            # --- EXIT LOGIC ---
            if active_position:
                active_position['days_held'] += 1
                
                # Check ITM condition
                is_itm = False
                if active_position['type'] == 'put_credit_spread' and price < active_position['short_strike']:
                    is_itm = True
                elif active_position['type'] == 'call_credit_spread' and price > active_position['short_strike']:
                    is_itm = True
                elif active_position['type'] == 'long_call' and price > active_position['long_strike']:
                    is_itm = True
                elif active_position['type'] == 'long_put' and price < active_position['long_strike']:
                    is_itm = True
                
                if is_itm:
                    active_position['days_itm'] += 1
                else:
                    active_position['days_itm'] = 0
                
                # --- CREDIT SPREAD EXIT ---
                if 'credit_spread' in active_position['type']:
                    # Close if 2 consecutive days ITM (Loss)
                    if active_position['days_itm'] >= 2:
                        loss = (active_position['width'] - active_position['credit']) * 100
                        current_cash -= loss
                        results['trades'].append({
                            "date": date.strftime('%Y-%m-%d'),
                            "action": "CLOSE_LOSS_ITM",
                            "pnl": -loss
                        })
                        active_position = None
                    
                    # Close if 50% Profit (Time decay)
                    elif active_position['days_held'] > 5: 
                         profit = active_position['credit'] * 0.5 * 100
                         current_cash += profit
                         results['trades'].append({
                             "date": date.strftime('%Y-%m-%d'),
                             "action": "CLOSE_PROFIT_50",
                             "pnl": profit
                         })
                         active_position = None
                
                # --- LONG OPTION EXIT ---
                else:
                    # Simple Mock Exit for Long positions
                    # Close if Held > 10 days or significant move
                    if active_position['days_held'] > 10:
                        # Mock PnL based on price move
                        if is_itm:
                            # Assume 50% profit
                            profit = active_position['debit'] * 0.5 * 100
                            current_cash += profit
                            results['trades'].append({
                                "date": date.strftime('%Y-%m-%d'),
                                "action": "CLOSE_PROFIT",
                                "pnl": profit
                            })
                        else:
                            # Assume 50% loss (decay)
                            loss = active_position['debit'] * 0.5 * 100
                            current_cash -= loss
                            results['trades'].append({
                                "date": date.strftime('%Y-%m-%d'),
                                "action": "CLOSE_LOSS",
                                "pnl": -loss
                            })
                        active_position = None

            # --- ENTRY LOGIC ---
            # Only enter if no position
            if not active_position:
                dte = 30 # Default 30 DTE for Algo
                
                entry_signal = None
                trade_params = {}
                
                if strategy_type == "credit_spread": 
                    # 1. Try S/R Algo
                    put_signal = False
                    call_signal = False
                    
                    # Check Put Entry (Bullish) - Price near Support
                    # Closest support below price
                    supports = [k for k in key_levels if k['type'] == 'support' and k['price'] < price]
                    supports.sort(key=lambda x: x['price'])
                    
                    if supports:
                         closest_support = supports[-1]
                         dist = (price - closest_support['price']) / price
                         if dist < 0.05 and rsi < 50: # Trigger condition similar to bot
                             entry_signal = "put_credit_spread"
                             trade_params['short_strike'] = closest_support['price']
                             trade_params['method'] = 'Algo S/R'
                    
                    if not entry_signal:
                        # Check Call Entry (Bearish)
                        resistances = [k for k in key_levels if k['type'] == 'resistance' and k['price'] > price]
                        resistances.sort(key=lambda x: x['price'])
                        
                        if resistances:
                            closest_res = resistances[0]
                            dist = (closest_res['price'] - price) / price
                            if dist < 0.05 and rsi > 50:
                                entry_signal = "call_credit_spread"
                                trade_params['short_strike'] = closest_res['price']
                                trade_params['method'] = 'Algo S/R'
                                
                    # 2. Fallback to Pseudo-Delta (Volatility) if no S/R triggered
                    if not entry_signal:
                         # Proxy for 30 Delta is approx 0.5 std dev move for 1 month?
                         # Expected 1 SD move over 30 days = Price * Vol * sqrt(30/365)
                         move_1sd = price * volatility * math.sqrt(30/365)
                         
                         # 30 Delta is roughly 0.52 SD OTM? (Norm inv(0.3) ~= -0.52)
                         dist_30_delta = 0.52 * move_1sd
                         
                         # We can enter random-ish or trend following? 
                         # Let's say we follow RSI: < 40 Bullish, > 60 Bearish
                         if rsi < 40:
                             entry_signal = "put_credit_spread"
                             trade_params['short_strike'] = price - dist_30_delta
                             trade_params['method'] = 'Algo Delta'
                         elif rsi > 60:
                             entry_signal = "call_credit_spread"
                             trade_params['short_strike'] = price + dist_30_delta
                             trade_params['method'] = 'Algo Delta'
                
                elif strategy_type == "long_call": # Legacy logic
                    if rsi < 30: entry_signal = "long_call"
                elif strategy_type == "long_put":
                    if rsi > 70: entry_signal = "long_put"
                
                if entry_signal:
                    width = 0
                    credit = 0
                    debit = 0
                    short_strike = 0
                    long_strike = 0
                    
                    if "credit_spread" in entry_signal:
                        width = 5.0
                        credit = 1.0 # Mock credit
                        short_strike = trade_params.get('short_strike')
                        # Round strike
                        if short_strike > 100: short_strike = 5 * round(short_strike/5)
                        else: short_strike = round(short_strike)
                        
                        if entry_signal == 'put_credit_spread':
                             long_strike = short_strike - width
                        else:
                             long_strike = short_strike + width
                             
                    elif entry_signal in ["long_call", "long_put"]:
                        debit = 2.0
                        short_strike = 0
                        long_strike = price
                    
                    active_position = {
                        'type': entry_signal,
                        'entry_price': price,
                        'short_strike': short_strike,
                        'long_strike': long_strike,
                        'width': width,
                        'credit': credit,
                        'debit': debit,
                        'opened_at': date,
                        'days_held': 0,
                        'days_itm': 0,
                        'target_dte': dte
                    }
                    
                    results['trades'].append({
                        "date": date.strftime('%Y-%m-%d'),
                        "action": f"OPEN_{entry_signal.upper()} ({trade_params.get('method', 'Legacy')})",
                        "price": price,
                        "credit": credit if credit > 0 else None,
                        "debit": debit if debit > 0 else None
                    })

            portfolio_history.append(current_cash)

        results['portfolio_value'] = portfolio_history
        
        # Calculate summary metrics
        if not portfolio_history:
             print("DEBUG: Portfolio history is empty. No days processed.")
             return {
                "dates": [],
                "values": [],
                "trades": [],
                "metrics": {
                    "total_return": "0.00%",
                    "final_value": "$10000.00",
                    "trade_count": 0
                }
             }

        total_return = (portfolio_history[-1] - 10000.0) / 10000.0
        
        return {
            "dates": results['dates'],
            "values": results['portfolio_value'],
            "trades": results['trades'],
            "metrics": {
                "total_return": f"{total_return*100:.2f}%",
                "final_value": f"${portfolio_history[-1]:.2f}",
                "trade_count": len(results['trades'])
            }
        }
