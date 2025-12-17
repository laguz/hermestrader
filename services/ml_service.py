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

    def evaluate_model(self, symbol, days=60):
        """
        Evaluate the saved model against recent historical data (backtest).
        """
        model_path = f"{self.model_dir}/{symbol}_rf.pkl"
        if not os.path.exists(model_path):
            return {"error": "Model not found. Please train first."}

        model = joblib.load(model_path)

        # Fetch enough data: days to evaluate + buffer for indicators (100 days)
        # We want to test 'days' amount of predictions.
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days + 150)).strftime('%Y-%m-%d')

        history = self.tradier.get_historical_pricing(symbol, start_date, end_date)
        if not history:
            return {"error": "Not enough data for evaluation"}

        df = pd.DataFrame(history)
        df['date'] = pd.to_datetime(df['date'])

        # Prepare features for the whole set
        df = self.prepare_data(df)
        
        # We can only evaluate on rows where we have targets (next day price known)
        # prepare_data drops NaNs, so df should be clean.
        # We take the last 'days' rows
        eval_df = df.tail(days).copy()
        
        if len(eval_df) < 10:
             return {"error": "Not enough clean data after indicator calc for evaluation"}

        X = eval_df[self.features]
        y_actual = eval_df['target']
        dates = eval_df['date'] # this is T (feature date), prediction is for T+1 target

        predictions = model.predict(X)
        
        results = []
        squared_errors = []
        absolute_errors = []
        correct_direction = 0
        total_predictions = len(predictions)

        # Iterate to build result list
        # Align indices
        y_actual_list = y_actual.values
        dates_list = dates.values
        close_list = eval_df['close'].values # Current day close, to check direction

        for i in range(total_predictions):
            actual = y_actual_list[i]
            predicted = predictions[i]
            current_close = close_list[i]
            date_t = pd.to_datetime(dates_list[i])
            date_target = date_t + timedelta(days=1) # Approx T+1 (ignoring weekends logic for display)
            # Actually, date_target is just the date of the target. 
            # Note: prepare_data shifts target = shift(-1). So target for date T is price at T+1.
            
            err = actual - predicted
            squared_errors.append(err ** 2)
            absolute_errors.append(abs(err))
            
            # Directional accuracy: Did we correctly predict up/down relative to current close?
            actual_move = actual - current_close
            predicted_move = predicted - current_close
            
            if (actual_move > 0 and predicted_move > 0) or (actual_move < 0 and predicted_move < 0):
                correct_direction += 1
            elif abs(actual_move) < 0.01 and abs(predicted_move) < 0.01: # Flat
                correct_direction += 1
                
            results.append({
                "date": date_t.strftime('%Y-%m-%d'),
                "actual": round(actual, 2),
                "predicted": round(predicted, 2),
                "error": round(err, 2)
            })

        mse = np.mean(squared_errors)
        mae = np.mean(absolute_errors)
        direction_accuracy = (correct_direction / total_predictions) * 100

        return {
            "symbol": symbol,
            "mse": round(mse, 4),
            "mae": round(mae, 4),
            "accuracy": round(direction_accuracy, 2),
            "predictions": results
        }
