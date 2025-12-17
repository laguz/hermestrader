import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler
import os
import joblib
from datetime import datetime, timedelta
import tensorflow as tf
from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import LSTM, Dense, Dropout

from utils.indicators import calculate_rsi, calculate_bollinger_bands, calculate_macd, calculate_atr, calculate_sma

class MLService:
    def __init__(self, tradier_service):
        self.tradier = tradier_service
        self.model_dir = "models"
        if not os.path.exists(self.model_dir):
            os.makedirs(self.model_dir)
        
        # Features to use for prediction
        self.features = ['close', 'volume', 'rsi', 'upper_bb', 'lower_bb', 'mid_bb', 'macd', 'macd_signal', 'atr', 'sma_50']
        self.sequence_length = 60 # Lookback for LSTM

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

    def _prepare_lstm_data(self, df, fit_scaler=False, scaler=None):
        """Prepare sequences for LSTM"""
        data = df[self.features].values
        target = df['target'].values
        
        if fit_scaler:
            scaler = MinMaxScaler(feature_range=(0, 1))
            scaled_data = scaler.fit_transform(data)
        else:
            if not scaler:
                 raise ValueError("Scaler required for transforming data")
            scaled_data = scaler.transform(data)
            
        X, y = [], []
        # Create sequences
        for i in range(self.sequence_length, len(scaled_data)):
            X.append(scaled_data[i-self.sequence_length:i])
            y.append(target[i])
            
        return np.array(X), np.array(y), scaler

    def _build_lstm_model(self, input_shape):
        model = Sequential()
        model.add(LSTM(50, return_sequences=True, input_shape=input_shape))
        model.add(Dropout(0.2))
        model.add(LSTM(50, return_sequences=False))
        model.add(Dropout(0.2))
        model.add(Dense(25))
        model.add(Dense(1)) # Predict price directly
        
        model.compile(optimizer='adam', loss='mean_squared_error')
        return model

    def train_model(self, symbol, model_type='rf'):
        print(f"Starting {model_type.upper()} training for {symbol}...")
        
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
        
        mse = 0
        
        if model_type == 'lstm':
            X, y, scaler = self._prepare_lstm_data(df, fit_scaler=True)
            
            # Split
            split = int(len(X) * 0.8)
            X_train, X_test = X[:split], X[split:]
            y_train, y_test = y[:split], y[split:]
            
            model = self._build_lstm_model(input_shape=(X_train.shape[1], X_train.shape[2]))
            model.fit(X_train, y_train, batch_size=32, epochs=20, validation_data=(X_test, y_test), verbose=1)
            
            predictions = model.predict(X_test)
            mse = mean_squared_error(y_test, predictions)
            
            # Save
            model.save(f"{self.model_dir}/{symbol}_lstm.keras")
            joblib.dump(scaler, f"{self.model_dir}/{symbol}_lstm_scaler.pkl")
            
        else: # Random Forest
            X = df[self.features]
            y = df['target']
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
            
            model = RandomForestRegressor(n_estimators=100, random_state=42)
            model.fit(X_train, y_train)
            
            predictions = model.predict(X_test)
            mse = mean_squared_error(y_test, predictions)
            joblib.dump(model, f"{self.model_dir}/{symbol}_rf.pkl")
        
        print(f"Model ({model_type}) MSE: {mse}")
        
        return {
            "status": "trained", 
            "symbol": symbol, 
            "type": model_type,
            "data_points": len(df),
            "mse": round(mse, 4)
        }

    def predict_next_day(self, symbol, model_type='rf'):
        
        # Fetch recent data
        end_date = datetime.now().strftime('%Y-%m-%d')
        # Increase lookback for LSTM (need 60 sequence + 50 warmup + buffer)
        # 365 calendar days ~ 250 trading days, plenty.
        start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        history = self.tradier.get_historical_pricing(symbol, start_date, end_date)
        if not history:
             return {"error": "No recent data found"}

        df = pd.DataFrame(history)
        df['date'] = pd.to_datetime(df['date'])
        
        # Prepare features (calculates indicators)
        # Note: prepare_data shifts target, but we need features for the LATEST day(s)
        # So we run prepare_data but ignore the target dropping at the end for the very last row?
        # Actually prepare_data drops NaNs.
        # We need to leverage the prepare_data logic but be careful about the last row.
        
        # Let's copy prepare_data logic partially or use it and retrieve the last rows.
        # If we use prepare_data, it drops the last row because it doesn't have a target (tomorrow).
        # We need that last row (today) to predict tomorrow.
        
        # HACK: Duplicate the last row with dummy target so prepare_data keeps it?
        # Or just re-calc indicators manually here?
        # Better: Refactor prepare_data to optionally not drop last row.
        # For minimal disruption, let's just calc indicators on 'df' directly here like prepare_data does but without drops.
        
        df['close'] = df['close'].astype(float)
        # ... standard conversions ...
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
            
        # Check sufficient data
        if pd.isna(df.iloc[-1]['rsi']) or pd.isna(df.iloc[-1]['sma_50']):
             return {"error": "Not enough data for indicators"}

        prediction = 0
        last_row = df.iloc[-1]
        
        if model_type == 'lstm':
            model_path = f"{self.model_dir}/{symbol}_lstm.keras"
            scaler_path = f"{self.model_dir}/{symbol}_lstm_scaler.pkl"
            
            if not os.path.exists(model_path):
                return {"error": f"LSTM model for {symbol} not found."}
                
            model = load_model(model_path)
            scaler = joblib.load(scaler_path)
            
            # Get last 60 days of features
            # We need the FEATURES, not just close price.
            # features list: ['close', 'volume', 'rsi', ...]
            
            # Ensure we have clean data (indicators might be NaN at start)
            clean_df = df.dropna()
            
            last_sequence_df = clean_df.tail(self.sequence_length)
            if len(last_sequence_df) < self.sequence_length:
                return {"error": f"Not enough valid data for LSTM sequence. Has {len(last_sequence_df)}, need {self.sequence_length}"}
                
            data = last_sequence_df[self.features].values
            scaled_data = scaler.transform(data) # (60, n_features)
            
            # Reshape for LSTM (1, 60, n_features)
            X_input = np.array([scaled_data])
            pred_scaled = model.predict(X_input)
            
            # Prediction is price directly (if we trained on raw targets)
            prediction = float(pred_scaled[0][0])
            
        else: # RF
            model_path = f"{self.model_dir}/{symbol}_rf.pkl"
            if not os.path.exists(model_path):
                return {"error": "RF Model not found."}
            model = joblib.load(model_path)
            
            features_df = pd.DataFrame([last_row[self.features]])
            prediction = model.predict(features_df)[0]
            
        last_close = last_row['close']
        change = prediction - last_close
        percent_change = (change / last_close) * 100
        
        return {
            "symbol": symbol,
            "model": model_type,
            "predicted_price": round(float(prediction), 2),
            "last_close": round(float(last_close), 2),
            "change": round(float(change), 2),
            "percent_change_str": f"{percent_change:.2f}%",
            "prediction_date": (last_row['date'] + timedelta(days=1)).strftime('%Y-%m-%d')
        }

    def evaluate_model(self, symbol, days=60, model_type='rf'):
        """
        Evaluate the saved model.
        """
        if model_type == 'lstm':
            model_path = f"{self.model_dir}/{symbol}_lstm.keras"
            if not os.path.exists(model_path): return {"error": "LSTM Model not found."}
            model = load_model(model_path)
            scaler = joblib.load(f"{self.model_dir}/{symbol}_lstm_scaler.pkl")
        else:
            model_path = f"{self.model_dir}/{symbol}_rf.pkl"
            if not os.path.exists(model_path): return {"error": "RF Model not found."}
            model = joblib.load(model_path)

        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days + 200)).strftime('%Y-%m-%d')

        history = self.tradier.get_historical_pricing(symbol, start_date, end_date)
        if not history: return {"error": "Not enough data"}

        df = pd.DataFrame(history)
        df['date'] = pd.to_datetime(df['date'])
        
        # Calc indicators on full set (don't drop yet)
        df['close'] = df['close'].astype(float)
        # ... (indicators same as predict)
        # Use prepare_data to get clean dataset with indicators + target
        clean_df = self.prepare_data(df) # This drops last row (target unknown) + NaNs
        
        # Logic: we need sequences for LSTM
        # We want to evaluate roughly 'days' targets.
        
        eval_df = clean_df.tail(days + (60 if model_type=='lstm' else 0)) # Grab extra for LSTM sequences
        
        results = []
        squared_errors = []
        absolute_errors = []
        correct_direction = 0
        
        if model_type == 'lstm':
             # Need to regenerate sequences from the eval slice?
             # actually _prepare_lstm will generate sequences from the provided DF.
             # We want sequences corresponding to the last 'days' targets.
             # X, y, _ = self._prepare_lstm_data(clean_df, fit_scaler=False, scaler=scaler)
             # X will be (N, 60, feat).
             # We just take the last 'days' items from X and y.
             
             X_all, y_all, _ = self._prepare_lstm_data(clean_df, fit_scaler=False, scaler=scaler)
             if len(X_all) < days:
                 return {"error": "Not enough data for evaluation constraints"}
                 
             X = X_all[-days:]
             y_actual = y_all[-days:]
             # Dates? corresponding to these targets.
             # clean_df has 'date'. The target for row i is T+1. 
             # sequence i ends at row i.
             # So indices align.
             dates = clean_df['date'].values[self.sequence_length:][-days:]
             close_list = clean_df['close'].values[self.sequence_length:][-days:] # Close at T
             
             predictions = model.predict(X) 
             # predictions shape (days, 1)
             predictions = [float(p[0]) for p in predictions]
             
        else:
            X = eval_df[self.features].tail(days)
            y_actual = eval_df['target'].tail(days).values
            dates = eval_df['date'].tail(days).values
            close_list = eval_df['close'].tail(days).values
            predictions = model.predict(X)

        # Loop metrics
        for i in range(len(predictions)):
            actual = y_actual[i]
            predicted = predictions[i]
            current_close = close_list[i]
            
            err = actual - predicted
            squared_errors.append(err ** 2)
            absolute_errors.append(abs(err))
            
            actual_move = actual - current_close
            predicted_move = predicted - current_close
            
            if (actual_move * predicted_move) > 0:
                correct_direction += 1
            elif abs(actual_move) < 0.01 and abs(predicted_move) < 0.01:
                correct_direction += 1
                
            results.append({
                "date": pd.to_datetime(dates[i]).strftime('%Y-%m-%d'),
                "actual": round(actual, 2),
                "predicted": round(predicted, 2),
                "error": round(err, 2)
            })

        mse = np.mean(squared_errors) if squared_errors else 0
        mae = np.mean(absolute_errors) if absolute_errors else 0
        acc = (correct_direction / len(predictions) * 100) if predictions else 0

        return {
            "symbol": symbol,
            "mse": round(mse, 4),
            "mae": round(mae, 4),
            "accuracy": round(acc, 2),
            "predictions": results
        }
