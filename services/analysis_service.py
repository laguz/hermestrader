import logging
import pandas as pd

logger = logging.getLogger(__name__)
import numpy as np
from datetime import datetime, timedelta
from utils.indicators import (
    calculate_rsi, calculate_ema,
    calculate_macd, 
    calculate_support_resistance, 
    calculate_sma, 
    calculate_bollinger_bands,
    calculate_adx,
    calculate_hv_rank,
    find_key_levels,
    calculate_historical_volatility,
    calculate_prob_it_expires_otm,
    calculate_prob_of_touch,
    calculate_atr
)

class AnalysisService:
    def __init__(self, tradier_service, ml_service, db=None):
        self.tradier_service = tradier_service
        self.ml_service = ml_service
        self.db = db


    def analyze_symbol(self, symbol, period='6m'):
        end_date = datetime.now()
        
        days_map = {
            '3m': 90,
            '6m': 180,
            '1y': 365
        }
        days = days_map.get(period, 365)
        
        # Fetch extra data for rolling calculations (e.g. 50 days buffer)
        # Ensure at least 300 days for SMA 200
        days_needed = max(days + 60, 300)
        start_date = end_date - timedelta(days=days_needed)
        
        quotes = self.tradier_service.get_historical_pricing(
            symbol,
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d'),
            interval='daily'
        )
        if not quotes:
            return {"error": "No data found for symbol"}

        df = pd.DataFrame(quotes)
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['volume'] = df['volume'].astype(float)
        df['date'] = pd.to_datetime(df['date'])
        
        # Forward-fill any NaN in OHLCV columns to prevent cascading NaN in indicators
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = df[col].ffill()
        
        # Filter to requested period for display/levels, but keep history for indicators
        # Actually indicators need history.
        # Let's calculate indicators on full DF then slice for "Key Levels" calculation if we want levels ONLY from that period.
        # Yes, user said "use ... data to create a list ... for the selected period".
        
        df, hv_rank = self._calculate_indicators(df)
        
        # Slice for the specific period analysis (visualization and key levels)
        # However, ML prediction might rely on latest? 
        # Logic below relies on df.iloc[-1]. That should be "Latest available data" regardless of "view period".
        # BUT the chart data should respect the period.
        # And Key Levels should respect the period.
        
        # Date cutoff
        cutoff_date = end_date - timedelta(days=days)
        period_df = df[df['date'] >= cutoff_date].copy()
        
        if period_df.empty:
            period_df = df.tail(days) # Fallback
            
        # Recalculate Key Levels specifically on this period data
        # Using new KMeans algo which expects Volume
        # from utils.indicators import find_key_levels
        
        # Pass high/low series to automatically get min/max levels
        key_levels = find_key_levels(
            period_df['close'], 
            period_df['volume'], 
            high_series=period_df['high'], 
            low_series=period_df['low']
        )
        
        # Note: Period Min/Max are now included in key_levels by find_key_levels

        # Calculate Volatility
        volatility = calculate_historical_volatility(df['close'], window=30)
        if len(df) > 30:
            volatility = calculate_historical_volatility(df['close'], window=min(len(df)-1, 252))
        else:
            volatility = 0.5

        current_price = df.iloc[-1]['close']

        # --- NEW: Retrieve Implied Volatility (IV) Proxy ---
        implied_vol = self._get_implied_volatility(symbol, current_price)

        # Use IV if available, otherwise fallback to HV
        calc_vol = implied_vol if implied_vol else volatility

        # Calculate Entry Points (Rounded Key Levels)
        put_entry_points, call_entry_points = self._calculate_entry_points(key_levels, current_price, calc_vol)

        # Get latest data point
        latest = df.iloc[-1]
        prev = df.iloc[-2]

        current_price = latest['close']

        # 3. Get AI Prediction
        prediction_result = {}
        try:
            prediction_result = self.ml_service.predict_next_day(symbol)
        except Exception as e:
            logger.error(f"Error getting prediction: {e}")
        
        predicted_price = prediction_result.get('predicted_price')
        pred_change_pct = 0
        if predicted_price:
            pred_change_pct = (predicted_price - current_price) / current_price

        # 4. Logic & Scoring
        
        # --- Sell Put Entry (Bullish) ---
        sp_score, sp_confidence, sp_reasons = self._evaluate_sell_put(
            current_price, latest, prev, hv_rank, pred_change_pct
        )


        # --- Sell Call Entry (Bearish) ---
        sc_score, sc_confidence, sc_reasons = self._evaluate_sell_call(
            current_price, latest, prev, hv_rank, pred_change_pct
        )


        # Prepare Chart Data
        # Use period_df to reflect the requested timeframe
        chart_df = period_df

        chart_data = {
            "dates": chart_df['date'].dt.strftime('%Y-%m-%d').tolist(),
            "close": chart_df['close'].tolist(),
            "support": chart_df['support'].fillna(0).tolist(),
            "resistance": chart_df['resistance'].fillna(0).tolist(),
            "rsi": chart_df['rsi'].fillna(50).tolist(),
            "macd": chart_df['macd'].fillna(0).tolist(),
            "signal": chart_df['signal'].fillna(0).tolist(),
            "bb_upper": chart_df['bb_upper'].fillna(0).tolist(),
            "bb_middle": chart_df['bb_mid'].fillna(0).tolist(),
            "bb_lower": chart_df['bb_lower'].fillna(0).tolist(),
            "sma_200": chart_df['sma_200'].fillna(0).tolist(),
            "adx": chart_df['adx'].fillna(0).tolist()
        }

        result = {
            "symbol": symbol.upper(),
            "current_price": current_price,
            "key_levels": key_levels,
            "put_entry_points": put_entry_points,
            "call_entry_points": call_entry_points,
            "prediction": {
                "price": predicted_price,
                "change_pct": round(pred_change_pct * 100, 2)
            },
            "sell_put_entry": {
                "score": sp_score,
                "confidence": sp_confidence,
                "reasons": sp_reasons
            },
            "sell_call_entry": {
                "score": sc_score,
                "confidence": sc_confidence,
                "reasons": sc_reasons
            },
            "indicators": {
                "ema_50": round(latest['ema_50'], 2),
                "macd": round(latest['macd'], 2),
                "support": round(latest['support'], 2),
                "resistance": round(latest['resistance'], 2),
                "resistance": round(latest['resistance'], 2),
                "bb_upper": round(latest['bb_upper'], 2),
                "bb_lower": round(latest['bb_lower'], 2),
                "sma_200": round(latest['sma_200'], 2) if not np.isnan(latest['sma_200']) else None,
                "adx": round(latest['adx'], 2),
                "atr": round(latest['atr'], 2) if not np.isnan(latest['atr']) else None,
                "hv_rank": round(hv_rank, 1),
                "volume_rel": round(latest['volume'] / latest['vol_sma'], 2),
                "volatility": round(volatility * 100, 1),
                "implied_volatility": round(implied_vol * 100, 1) if implied_vol else None
            },
            "chart_data": chart_data
        }

        # Upsert entry to DB
        if self.db is not None:
            try:
                # Add timestamp
                result['updated_at'] = datetime.now()
                self.db.entries.update_one(
                    {'symbol': symbol.upper()},
                    {'$set': result},
                    upsert=True
                )
                logger.info(f"Saved analysis for {symbol} to entries collection.")
            except Exception as e:
                logger.error(f"Error saving entry to DB: {e}")

        return result

    def _calculate_indicators(self, df):
        # 2. Calculate Indicators
        # RSI
        df['rsi'] = calculate_rsi(df['close'], period=14)

        # EMA 50
        df['ema_50'] = calculate_ema(df['close'], span=50)

        # MACD
        df['macd'], df['signal'], df['hist'] = calculate_macd(df['close'])

        # Support & Resistance (Rolling 20 - Dynamic)
        df['support'], df['resistance'] = calculate_support_resistance(df['close'], window=20)

        # Volume SMA
        df['vol_sma'] = calculate_sma(df['volume'], window=20)

        # ADX (14)
        df['adx'] = calculate_adx(df['high'], df['low'], df['close'], period=14)

        # ATR (14)
        df['atr'] = calculate_atr(df['high'], df['low'], df['close'], window=14)

        # HV Rank (Percentile of 30-day Vol over last year)
        hv_rank = calculate_hv_rank(df['close'], window=30, lookback=252)

        # Bollinger Bands (20, 2)
        df['bb_upper'], df['bb_mid'], df['bb_lower'] = calculate_bollinger_bands(df['close'], window=20, num_std=2)

        # SMA 200
        df['sma_200'] = calculate_sma(df['close'], window=200)

        return df, hv_rank

    def _get_implied_volatility(self, symbol, current_price):
        implied_vol = None
        try:
            expirations = self.tradier_service.get_option_expirations(symbol)
            if expirations:
                # Find expiry closest to 30 days
                today = datetime.now().date()
                target_date = today + timedelta(days=30)
                best_exp = min(expirations, key=lambda x: abs((datetime.strptime(x, "%Y-%m-%d").date() - target_date).days))

                chain = self.tradier_service.get_option_chains(symbol, best_exp)
                if chain:
                    # Filter for ATM options
                    atm_options = [o for o in chain if abs(o['strike'] - current_price) / current_price < 0.05]
                    ivs = [o.get('greeks', {}).get('mid_iv', 0) for o in atm_options if o.get('greeks', {}).get('mid_iv', 0) > 0]
                    if ivs:
                        implied_vol = sum(ivs) / len(ivs)
                        logger.debug(f"Calculated blended IV for {symbol} at {best_exp}: {implied_vol:.4f}")
        except Exception as e:
            logger.warning(f"Error fetching IV for {symbol}: {e}")
        return implied_vol

    def _calculate_entry_points(self, key_levels, current_price, calc_vol):
        put_entry_points = []
        call_entry_points = []

        seen_put_prices = set()
        seen_call_prices = set()

        for level in key_levels:
            price = level['price']
            level_type = level['type']

            if price > 100:
                rounded_price = 5 * round(price / 5)
            else:
                rounded_price = round(price)

            rounded_price = int(rounded_price)

            if level_type == 'support':
                 if rounded_price not in seen_put_prices:
                    seen_put_prices.add(rounded_price)
                    put_entry_points.append({
                        'price': rounded_price,
                        'type': 'support',
                        'strength': level.get('strength', 1)
                    })
            elif level_type == 'resistance':
                 if rounded_price not in seen_call_prices:
                    seen_call_prices.add(rounded_price)
                    call_entry_points.append({
                        'price': rounded_price,
                        'type': 'resistance',
                        'strength': level.get('strength', 1)
                    })

        # Calculate PoP and POT for all entry points (assuming 30 DTE)
        # Use calc_vol (IV or HV fallback)

        for ep in put_entry_points:
            # Shift break-even by estimated credit (approx 1/3 of width)
            # Entry points are key levels, so we assume we sell at/near them.
            # Simplified: assuming $5 wide spread, credit approx $1.50
            est_credit = 1.0 if ep['price'] < 100 else 2.5

            pop = calculate_prob_it_expires_otm(current_price, ep['price'], calc_vol, days_to_expiry=30, credit=est_credit)
            pot = calculate_prob_of_touch(current_price, ep['price'], calc_vol, days_to_expiry=30)

            ep['pop'] = round(pop * 100, 1)
            ep['pot'] = round(pot * 100, 1)

        for ep in call_entry_points:
            est_credit = 1.0 if ep['price'] < 100 else 2.5
            pop = calculate_prob_it_expires_otm(current_price, ep['price'], calc_vol, days_to_expiry=30, credit=est_credit)
            pot = calculate_prob_of_touch(current_price, ep['price'], calc_vol, days_to_expiry=30)

            ep['pop'] = round(pop * 100, 1)
            ep['pot'] = round(pot * 100, 1)

        # Sort entry points by price
        put_entry_points.sort(key=lambda x: x['price'])
        call_entry_points.sort(key=lambda x: x['price'])

        return put_entry_points, call_entry_points

    def _evaluate_sell_put(self, current_price, latest, prev, hv_rank, pred_change_pct):
        sp_score = 0
        sp_reasons = []
        
        # Support
        dist_to_support = (current_price - latest['support']) / current_price
        if abs(dist_to_support) < 0.015:
            sp_score += 3
            sp_reasons.append("Price at Support")
        elif dist_to_support < 0.05 and dist_to_support > 0:
             sp_score += 1
             sp_reasons.append("Price approaching Support")

        # RSI (Oversold is good for bullish entry)
        if latest['rsi'] < 30:
            sp_score += 3
            sp_reasons.append("RSI Oversold (<30)")
        elif latest['rsi'] < 45:
            sp_score += 1
            sp_reasons.append("RSI Neutral/Low")
            
        # MACD (Bullish Momentum)
        if latest['macd'] > latest['signal']:
            sp_score += 2
            sp_reasons.append("MACD Bullish")
        elif latest['hist'] > prev['hist']:
             sp_score += 1
             sp_reasons.append("MACD Momentum Improving")

        # Bollinger Bands (Oversold - Price near or below lower band)
        # 1% buffer
        if latest['close'] <= latest['bb_lower'] * 1.01:
            sp_score += 2
            sp_reasons.append("Price near/below Lower Bollinger Band")
            
        # SMA 200 (Long Term Trend)
        if latest['sma_200'] > 0 and latest['close'] > latest['sma_200']:
            sp_score += 1
            sp_reasons.append("Price > SMA 200 (Bullish Trend)")

        # ADX Logic
        if latest['adx'] > 25:
             # Strong Trend
             if latest['close'] > latest['sma_200']:
                 sp_score += 1
                 sp_reasons.append("Strong Trend (ADX > 25) supporting Bullish bias")
        elif latest['adx'] < 20: 
             # Weak Trend (Range Bound) - Good for selling puts at support
             if abs(dist_to_support) < 0.05:
                 sp_score += 1
                 sp_reasons.append("Low ADX (Range Bound) at Support")

        # HV Rank Logic (Selling Premium)
        if hv_rank > 80:
            sp_score += 2
            sp_reasons.append(f"High IV Percentile ({hv_rank:.0f}%) - Premium Rich")
        elif hv_rank > 50:
            sp_score += 1
            sp_reasons.append("Good IV Percentile")
        elif hv_rank < 20:
            sp_score -= 1
            sp_reasons.append("Low IV Percentile - Premium Cheap")

        # Prediction
        if pred_change_pct > 0.005:
            sp_score += 3
            sp_reasons.append(f"AI Predicts Bullish (+{pred_change_pct*100:.1f}%)")
        elif pred_change_pct > 0:
            sp_score += 1
            sp_reasons.append("AI Predicts Slight Up")
            
        sp_confidence = "Low"
        if sp_score >= 7: sp_confidence = "High"
        elif sp_score >= 4: sp_confidence = "Medium"

        return sp_score, sp_confidence, sp_reasons

    def _evaluate_sell_call(self, current_price, latest, prev, hv_rank, pred_change_pct):
        sc_score = 0
        sc_reasons = []

        # Resistance
        dist_to_resistance = (latest['resistance'] - current_price) / current_price
        if abs(dist_to_resistance) < 0.015:
            sc_score += 3
            sc_reasons.append("Price at Resistance")
        elif abs(dist_to_resistance) < 0.05:
            sc_score += 1
            sc_reasons.append("Price approaching Resistance")

        # RSI (Overbought is good for bearish entry)
        if latest['rsi'] > 70:
            sc_score += 3
            sc_reasons.append("RSI Overbought (>70)")
        elif latest['rsi'] > 55:
            sc_score += 1
            sc_reasons.append("RSI Neutral/High")

        # MACD (Bearish Momentum)
        if latest['macd'] < latest['signal']:
            sc_score += 2
            sc_reasons.append("MACD Bearish")
        elif latest['hist'] < prev['hist']:
            sc_score += 1
            sc_reasons.append("MACD Momentum Weakening")

        # Bollinger Bands (Overbought - Price near or above upper band)
        # 1% buffer
        if latest['close'] >= latest['bb_upper'] * 0.99:
            sc_score += 2
            sc_reasons.append("Price near/above Upper Bollinger Band")

        # SMA 200 (Long Term Trend)
        if latest['sma_200'] > 0 and latest['close'] < latest['sma_200']:
             sc_score += 1
             sc_reasons.append("Price < SMA 200 (Bearish Trend)")

        # ADX Logic
        if latest['adx'] > 25:
             # Strong Trend
             if latest['close'] < latest['sma_200']:
                 sc_score += 1
                 sc_reasons.append("Strong Trend (ADX > 25) supporting Bearish bias")
        elif latest['adx'] < 20: 
             # Weak Trend (Range Bound) - Good for selling calls at resistance
             if abs(dist_to_resistance) < 0.05:
                 sc_score += 1
                 sc_reasons.append("Low ADX (Range Bound) at Resistance")

        # HV Rank Logic (Selling Premium)
        if hv_rank > 80:
            sc_score += 2
            sc_reasons.append(f"High IV Percentile ({hv_rank:.0f}%) - Premium Rich")
        elif hv_rank > 50:
            sc_score += 1
            sc_reasons.append("Good IV Percentile")
        elif hv_rank < 20:
            sc_score -= 1
            sc_reasons.append("Low IV Percentile - Premium Cheap")

        # Prediction
        if pred_change_pct < -0.005:
            sc_score += 3
            sc_reasons.append(f"AI Predicts Bearish ({pred_change_pct*100:.1f}%)")
        elif pred_change_pct < 0:
            sc_score += 1
            sc_reasons.append("AI Predicts Slight Down")
            
        sc_confidence = "Low"
        if sc_score >= 7: sc_confidence = "High"
        elif sc_score >= 4: sc_confidence = "Medium"
        
        return sc_score, sc_confidence, sc_reasons
