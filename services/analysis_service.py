import pandas as pd
import numpy as np
from utils.indicators import calculate_rsi, calculate_macd, calculate_support_resistance, calculate_sma, calculate_bollinger_bands

class AnalysisService:
    def __init__(self, tradier_service, ml_service):
        self.tradier_service = tradier_service
        self.ml_service = ml_service

    def analyze_symbol(self, symbol):
        from datetime import datetime, timedelta
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
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
        
        # 2. Calculate Indicators
        # RSI
        df['rsi'] = calculate_rsi(df['close'], period=14)
        
        # MACD
        df['macd'], df['signal'], df['hist'] = calculate_macd(df['close'])
        
        # Support & Resistance (Rolling 20)
        df['support'], df['resistance'] = calculate_support_resistance(df['close'], window=20)
        
        # Volume SMA
        df['vol_sma'] = calculate_sma(df['volume'], window=20)

        # Get latest data point
        latest = df.iloc[-1]
        prev = df.iloc[-2]

        current_price = latest['close']

        # 3. Get AI Prediction
        prediction_result = {}
        try:
            prediction_result = self.ml_service.predict_next_day(symbol)
        except Exception as e:
            print(f"Error getting prediction: {e}")
        
        predicted_price = prediction_result.get('predicted_price')
        pred_change_pct = 0
        if predicted_price:
            pred_change_pct = (predicted_price - current_price) / current_price

        # 4. Logic & Scoring
        
        # --- Sell Put Entry (Bullish) ---
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


        # --- Sell Call Entry (Bearish) ---
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


        # Prepare Chart Data
        # Truncate to last 100 days for display
        chart_df = df.tail(100)
        
        chart_data = {
            "dates": chart_df['date'].dt.strftime('%Y-%m-%d').tolist(),
            "close": chart_df['close'].tolist(),
            "support": chart_df['support'].fillna(0).tolist(),
            "resistance": chart_df['resistance'].fillna(0).tolist(),
            "rsi": chart_df['rsi'].fillna(50).tolist(),
            "macd": chart_df['macd'].fillna(0).tolist(),
            "signal": chart_df['signal'].fillna(0).tolist()
        }

        return {
            "symbol": symbol.upper(),
            "current_price": current_price,
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
                "rsi": round(latest['rsi'], 2),
                "macd": round(latest['macd'], 2),
                "support": round(latest['support'], 2),
                "resistance": round(latest['resistance'], 2),
                "volume_rel": round(latest['volume'] / latest['vol_sma'], 2)
            },
            "chart_data": chart_data
        }
