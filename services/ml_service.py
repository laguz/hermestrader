import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import os
import joblib
from datetime import datetime, timedelta

from utils.indicators import calculate_rsi, calculate_bollinger_bands, calculate_macd, calculate_atr, calculate_sma

class MLService:
    def __init__(self, tradier_service):
        self.tradier = tradier_service
        self.model_dir = "models"
        if not os.path.exists(self.model_dir):
            os.makedirs(self.model_dir)
        
        # Features to use for prediction
        self.features = ['close', 'volume', 'rsi', 'upper_bb', 'lower_bb', 'mid_bb', 'macd', 'macd_signal', 'atr', 'sma_50']

    def prepare_data(self, df):
        df = df.copy()
        df['close'] = df['close'].astype(float)
        # Ensure high/low/volume are floats
        if 'high' in df.columns: df['high'] = df['high'].astype(float)
        if 'low' in df.columns: df['low'] = df['low'].astype(float)
        if 'volume' in df.columns: df['volume'] = df['volume'].astype(float)
        
        # Calculate Indicators
        df['rsi'] = calculate_rsi(df['close'])
        df['upper_bb'], df['mid_bb'], df['lower_bb'] = calculate_bollinger_bands(df['close'])
        
        # New Indicators
        df['macd'], df['macd_signal'], _ = calculate_macd(df['close'])
        df['sma_50'] = calculate_sma(df['close'], window=50)
        
        if 'high' in df.columns and 'low' in df.columns:
            df['atr'] = calculate_atr(df['high'], df['low'], df['close'])
        else:
            # Fallback if high/low missing (shouldn't happen with Tradier history)
            df['atr'] = 0.0

        # Drop NaN
        df.dropna(inplace=True)
        
        # Target: Next day's close
        df['target'] = df['close'].shift(-1)
        df.dropna(inplace=True)
        
        return df

    def train_model(self, symbol):
        print(f"Starting Random Forest training for {symbol}...")
        
        # 1. Fetch Data (2 years)
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')
        
        history = self.tradier.get_historical_pricing(symbol, start_date, end_date)
        if not history:
            return {"error": "No data for training"}
            
        df = pd.DataFrame(history)
        df['date'] = pd.to_datetime(df['date'])
        
        # 2. Prepare Data
        df = self.prepare_data(df)
        
        if len(df) < 100:
            return {"error": "Not enough data for training"}

        X = df[self.features]
        y = df['target']
        
        # 3. Train Test Split
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
        
        # 4. Train Model
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        
        # Evaluate
        predictions = model.predict(X_test)
        mse = mean_squared_error(y_test, predictions)
        print(f"Model MSE: {mse}")
        
        # 5. Save Model
        joblib.dump(model, f"{self.model_dir}/{symbol}_rf.pkl")
        
        return {
            "status": "trained", 
            "symbol": symbol, 
            "data_points": len(df),
            "mse": round(mse, 4)
        }

    def predict_next_day(self, symbol):
        model_path = f"{self.model_dir}/{symbol}_rf.pkl"
        
        if not os.path.exists(model_path):
            return {"error": "Model not found. Please train first."}

        # Load Model
        model = joblib.load(model_path)
        
        # Fetch recent data
        # We need enough data to calculate indicators (~60 days buffer is safe)
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
        
        history = self.tradier.get_historical_pricing(symbol, start_date, end_date)
        if not history:
             return {"error": "No recent data found"}

        df = pd.DataFrame(history)
        df['date'] = pd.to_datetime(df['date'])
        
        # Prepare latest features
        df['close'] = df['close'].astype(float)
        if 'high' in df.columns: df['high'] = df['high'].astype(float)
        if 'low' in df.columns: df['low'] = df['low'].astype(float)
        if 'volume' in df.columns: df['volume'] = df['volume'].astype(float)

        df['rsi'] = calculate_rsi(df['close'])
        df['upper_bb'], df['mid_bb'], df['lower_bb'] = calculate_bollinger_bands(df['close'])
        
        df['macd'], df['macd_signal'], _ = calculate_macd(df['close'])
        df['sma_50'] = calculate_sma(df['close'], window=50)
        
        if 'high' in df.columns and 'low' in df.columns:
            df['atr'] = calculate_atr(df['high'], df['low'], df['close'])
        else:
            df['atr'] = 0.0
        
        # We need the very last row which corresponds to "today" to predict "tomorrow"
        # Since prepare_data shifts target, we just want the features of the last available day
        last_row = df.iloc[-1]
        
        # Check if indicators are valid
        if pd.isna(last_row['rsi']) or pd.isna(last_row['sma_50']):
             return {"error": "Not enough data to calculate recent indicators (need > 50 days)"}
        
        # Predict
        features_df = pd.DataFrame([last_row[self.features]])
        prediction = model.predict(features_df)[0]
        
        last_close = last_row['close']
        change = prediction - last_close
        percent_change = (change / last_close) * 100
        
        return {
            "symbol": symbol,
            "predicted_price": round(float(prediction), 2),
            "last_close": round(float(last_close), 2),
            "change": round(float(change), 2),
            "percent_change_str": f"{percent_change:.2f}%",
            "prediction_date": (last_row['date'] + timedelta(days=1)).strftime('%Y-%m-%d')
        }
