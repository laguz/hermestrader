import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler
import os
import joblib
import json
from datetime import datetime, timedelta
try:
    import tensorflow as tf
    from tensorflow.keras.models import Sequential, load_model
    from tensorflow.keras.layers import LSTM, Dense, Dropout, Bidirectional
    from tensorflow.keras.callbacks import EarlyStopping
    HAS_TENSORFLOW = True
except ImportError:
    print("Warning: TensorFlow not found. LSTM model will be disabled.")
    HAS_TENSORFLOW = False

from utils.indicators import calculate_rsi, calculate_bollinger_bands, calculate_macd, calculate_atr, calculate_sma
from services.container import Container

class MLService:
    def __init__(self, tradier_service):
        self.tradier = tradier_service # Keep for compatibility/fallback if needed
        self.db = Container.get_db()
        self.model_dir = "models"
        if not os.path.exists(self.model_dir):
            os.makedirs(self.model_dir)
        
        self.sequence_length = 60 # Lookback for LSTM
        self.default_features = ['close', 'volume', 'rsi', 'upper_bb', 'lower_bb', 'mid_bb', 'macd', 'macd_signal', 'atr', 'sma_50']

    def _get_feature_file_path(self, symbol):
        return os.path.join(self.model_dir, f"{symbol}_features.json")

    def backfill_symbol(self, symbol, years=5):
        """
        Backfill historical data for a symbol.
        """
        if self.db is None:
            print("DB unavailable for backfill.")
            return False

        print(f"Backfilling history for {symbol} ({years} years)...")
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365 * years)).strftime('%Y-%m-%d')
        
        history = self.tradier.get_historical_pricing(symbol, start_date, end_date)
        
        if not history:
            print("No data returned from Tradier.")
            return False

        print(f"Retrieved {len(history)} records. Saving to MongoDB...")
        
        collection = self.db['market_data']
        count = 0
        
        for record in history:
            doc = {
                "symbol": symbol,
                "date": record['date'],
                "open": float(record['open']),
                "high": float(record['high']),
                "low": float(record['low']),
                "close": float(record['close']),
                "volume": float(record['volume'])
            }
            
            result = collection.update_one(
                {"symbol": symbol, "date": record['date']},
                {"$set": doc},
                upsert=True
            )
            if result.upserted_id or result.modified_count > 0:
                count += 1
                
        print(f"Backfill Complete! Processed {count} records.")
        return True

    def prepare_features(self, df):
        """
        Calculate indicators and prepare features.
        Returns a DataFrame with features added.
        Note: Does NOT drop NaNs here to allow for inspection, 
        caller must handle dropna for training.
        """
        df = df.copy()
        df['close'] = df['close'].astype(float)
        # Ensure high/low/volume are floats
        if 'high' in df.columns: df['high'] = df['high'].astype(float)
        if 'low' in df.columns: df['low'] = df['low'].astype(float)
        if 'volume' in df.columns: df['volume'] = df['volume'].astype(float)
        
        # Base Indicators
        df['rsi'] = calculate_rsi(df['close'])
        df['upper_bb'], df['mid_bb'], df['lower_bb'] = calculate_bollinger_bands(df['close'])
        df['macd'], df['macd_signal'], _ = calculate_macd(df['close'])
        df['sma_50'] = calculate_sma(df['close'], window=50)
        
        if 'high' in df.columns and 'low' in df.columns:
            df['atr'] = calculate_atr(df['high'], df['low'], df['close'])
        else:
            df['atr'] = 0.0

        # Additional Features for Selection
        # Lags
        for lag in [1, 2, 3, 5]:
            df[f'close_lag_{lag}'] = df['close'].shift(lag)
            
        # Returns
        df['daily_return'] = df['close'].pct_change()
        df['daily_return_lag_1'] = df['daily_return'].shift(1)

        return df

    def select_top_features(self, df, target_col='target', n_top=10):
        """
        Select top N features using Random Forest.
        """
        # Exclude non-feature columns
        exclude = ['date', 'symbol', 'target']
        potential_features = [c for c in df.columns if c not in exclude]
        
        X = df[potential_features]
        y = df[target_col]
        
        # Train a quick RF to get importances
        model = RandomForestRegressor(n_estimators=50, max_depth=10, random_state=42, n_jobs=-1)
        model.fit(X, y)
        
        importances = model.feature_importances_
        indices = np.argsort(importances)[::-1]
        
        top_indices = indices[:n_top]
        top_features = [potential_features[i] for i in top_indices]
        
        print(f"Top {n_top} Features selected: {top_features}")
        return top_features

    def _prepare_lstm_data(self, df, features, fit_scaler=False, scaler=None):
        """Prepare sequences for LSTM using specific features"""
        data = df[features].values
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
        # Bidirectional with more units
        model.add(Bidirectional(LSTM(128, return_sequences=True), input_shape=input_shape))
        model.add(Dropout(0.2))
        model.add(Bidirectional(LSTM(128, return_sequences=False)))
        model.add(Dropout(0.2))
        model.add(Dense(25))
        model.add(Dense(1)) # Predict price directly
        
        model.compile(optimizer='adam', loss='mean_squared_error')
        return model

    def train_model(self, symbol, model_type='rf'):
        symbol = symbol.upper()
        print(f"Starting {model_type.upper()} training for {symbol} using local DB...")
        
        if self.db is None:
            return {"error": "Database not available"}

        # 1. Fetch Data from MongoDB
        collection = self.db['market_data']
        # Fetch last 5 years to be safe
        cutoff_date = (datetime.now() - timedelta(days=365*5)).strftime('%Y-%m-%d')
        cursor = collection.find({"symbol": symbol, "date": {"$gte": cutoff_date}}).sort("date", 1)
        
        data = list(cursor)
        if not data:
            print(f"No data found for {symbol}. Attempting backfill...")
            success = self.backfill_symbol(symbol)
            if success:
                # Re-fetch
                cursor = collection.find({"symbol": symbol, "date": {"$gte": cutoff_date}}).sort("date", 1)
                data = list(cursor)
            else:
                return {"error": f"No data found for {symbol} and backfill failed."}
            
            if not data:
                 return {"error": f"No data found in DB for {symbol} after backfill"}
            
        df = pd.DataFrame(data)
        if '_id' in df.columns: df.drop(columns=['_id'], inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        
        # 2. Prepare Data (Features + Target)
        df = self.prepare_features(df)
        
        # Target: Next day's close
        df['target'] = df['close'].shift(-1)
        
        # Drop NaNs created by lags/indicators/shifting
        df.dropna(inplace=True)
        
        if len(df) < 100:
            return {"error": "Not enough data for training after processing"}
        
        # 3. Feature Selection
        top_features = self.select_top_features(df)
        
        # Save selected features
        with open(self._get_feature_file_path(symbol), 'w') as f:
            json.dump(top_features, f)
            
        mse = 0
        
        if model_type == 'lstm':
            X, y, scaler = self._prepare_lstm_data(df, top_features, fit_scaler=True)
            
            # Scale Target
            y = y.reshape(-1, 1)
            target_scaler = MinMaxScaler(feature_range=(0, 1))
            y_scaled = target_scaler.fit_transform(y)
            
            # Split
            split = int(len(X) * 0.8)
            X_train, X_test = X[:split], X[split:]
            y_train, y_test = y_scaled[:split], y_scaled[split:]
            
            model = self._build_lstm_model(input_shape=(X_train.shape[1], X_train.shape[2]))
            
            # Early Stopping
            early_stopping = EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True)
            
            model.fit(X_train, y_train, batch_size=32, epochs=15, validation_data=(X_test, y_test), 
                      callbacks=[early_stopping], verbose=1)
            
            # Eval on test set (inverse transform)
            pred_scaled = model.predict(X_test)
            predictions = target_scaler.inverse_transform(pred_scaled)
            y_test_inv = target_scaler.inverse_transform(y_test)
            
            mse = mean_squared_error(y_test_inv, predictions)
            
            # Save
            model.save(f"{self.model_dir}/{symbol}_lstm.keras")
            joblib.dump(scaler, f"{self.model_dir}/{symbol}_lstm_scaler.pkl")
            joblib.dump(target_scaler, f"{self.model_dir}/{symbol}_lstm_target_scaler.pkl")
            
        else: # Random Forest
            # For RF, we predict the Daily Return (change) rather than raw price
            # because RF cannot extrapolate beyond training range.
            
            # Target = Return for NEXT day
            # current close -> next close
            # return = (next_close - close) / close
            
            df['target_return'] = df['close'].shift(-1) / df['close'] - 1
            
            # We must drop the last row which has NaN target
            train_df = df.dropna(subset=['target_return'])
            
            X = train_df[top_features]
            y = train_df['target_return']
            
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)
            
            model = RandomForestRegressor(n_estimators=100, random_state=42)
            model.fit(X_train, y_train)
            
            predictions_return = model.predict(X_test)
            
            # Calculate MSE on Price (reconstructed) to be comparable
            # actual_price = current_close * (1 + actual_return)
            # pred_price = current_close * (1 + pred_return)
            # getting current close from X_test? 
            # X_test has 'close' feature
            
            test_closes = X_test['close'].values
            pred_prices = test_closes * (1 + predictions_return)
            actual_prices = test_closes * (1 + y_test.values)
            
            mse = mean_squared_error(actual_prices, pred_prices)
            joblib.dump(model, f"{self.model_dir}/{symbol}_rf.pkl")
        
        print(f"Model ({model_type}) MSE: {mse}")
        
        return {
            "status": "trained", 
            "symbol": symbol, 
            "type": model_type,
            "data_points": len(df),
            "mse": round(mse, 4),
            "features": top_features
        }

    def predict_next_day(self, symbol, model_type='rf'):
        symbol = symbol.upper()
        if self.db is None:
            return {"error": "Database not available"}

        # Fetch recent data from DB
        # Lookback needs to cover enough for lags (max lag 5) + indicators (e.g. sma 50, rsi 14) + LSTM sequence (60)
        # 300 days is safe
        cutoff_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        collection = self.db['market_data']
        cursor = collection.find({"symbol": symbol, "date": {"$gte": cutoff_date}}).sort("date", 1)
        data = list(cursor)
        
        if not data:
             print(f"No recent data for {symbol}. Attempting backfill...")
             success = self.backfill_symbol(symbol)
             if success:
                 cursor = collection.find({"symbol": symbol, "date": {"$gte": cutoff_date}}).sort("date", 1)
                 data = list(cursor)
             else:
                 return {"error": f"No recent data found in DB and backfill failed"}

        if not data:
             return {"error": "No recent data found in DB after backfill"}

        df = pd.DataFrame(data)
        if '_id' in df.columns: df.drop(columns=['_id'], inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        
        # Prepare features
        df = self.prepare_features(df)
        
        # Load Selected Features
        feature_file = self._get_feature_file_path(symbol)
        if not os.path.exists(feature_file):
            print(f"Warning: Feature file for {symbol} not found. using defaults.")
            features = [f for f in self.default_features if f in df.columns]
        else:
            with open(feature_file, 'r') as f:
                features = json.load(f)

        if df.iloc[-1].isna().any():
             df_clean = df.dropna()
             if df_clean.empty: return {"error": "Not enough data for indicators"}
             pass

        last_row = df.iloc[-1]
        
        if df[features].iloc[-1].isna().any():
             return {"error": "Latest data has NaNs in required features."}

        prediction = 0
        
        if model_type == 'lstm':
            model_path = f"{self.model_dir}/{symbol}_lstm.keras"
            scaler_path = f"{self.model_dir}/{symbol}_lstm_scaler.pkl"
            target_scaler_path = f"{self.model_dir}/{symbol}_lstm_target_scaler.pkl"
            
            if not os.path.exists(model_path):
                return {"error": f"LSTM model for {symbol} not found."}
                
            model = load_model(model_path)
            scaler = joblib.load(scaler_path)
            
            df_clean = df.dropna(subset=features)
            last_sequence_df = df_clean.tail(self.sequence_length)
            
            if len(last_sequence_df) < self.sequence_length:
                return {"error": f"Not enough valid data for LSTM sequence."}
                
            data = last_sequence_df[features].values
            scaled_data = scaler.transform(data) 
            
            X_input = np.array([scaled_data])
            pred_scaled = model.predict(X_input)
            
            if os.path.exists(target_scaler_path):
                target_scaler = joblib.load(target_scaler_path)
                prediction = float(target_scaler.inverse_transform(pred_scaled)[0][0])
            else:
                prediction = float(pred_scaled[0][0])
            
        else: # RF
            model_path = f"{self.model_dir}/{symbol}_rf.pkl"
            if not os.path.exists(model_path):
                return {"error": "RF Model not found."}
            model = joblib.load(model_path)
            
            features_df = pd.DataFrame([last_row[features]])
            pred_return = model.predict(features_df)[0]
            
            # Reconstruct Price: Close * (1 + return)
            last_close_val = last_row['close']
            prediction = last_close_val * (1 + pred_return)
            
        last_close = last_row['close']
        change = prediction - last_close
        percent_change = (change / last_close) * 100
        
        prediction_date = (last_row['date'] + timedelta(days=1)).strftime('%Y-%m-%d')
        
        # PERSISTENCE: Save to MongoDB
        # Check if we have the ACTUAL close price for the target date (unlikely for future, possible for backtest)
        actual_close_doc = self.db['market_data'].find_one({"symbol": symbol, "date": prediction_date})
        actual_close_price = actual_close_doc['close'] if actual_close_doc else None
        
        try:
            pred_doc = {
                "symbol": symbol,
                "model_type": model_type,
                "prediction_date": prediction_date,
                "predicted_price": float(prediction),
                "actual_close_price": float(actual_close_price) if actual_close_price else None,
                "created_at": datetime.now(),
                "used_features": features
            }
            
            self.db['predictions'].update_one(
                {
                    "symbol": symbol, 
                    "model_type": model_type, 
                    "prediction_date": prediction_date
                },
                {"$set": pred_doc},
                upsert=True
            )
            print(f"Saved prediction for {symbol} ({model_type}) on {prediction_date} (Actual: {actual_close_price})")
        except Exception as e:
            print(f"Error saving prediction to DB: {e}")

        return {
            "symbol": symbol,
            "model": model_type,
            "predicted_price": round(float(prediction), 2),
            "last_close": round(float(last_close), 2),
            "change": round(float(change), 2),
            "percent_change_str": f"{percent_change:.2f}%",
            "prediction_date": prediction_date,
            "used_features": features
        }

    def get_prediction_history(self, symbol=None, limit=100):
        """
        Retrieves recent predictions. If symbol is provided, filters by symbol.
        """
        query = {}
        if symbol:
            symbol = symbol.upper().strip()
            query["symbol"] = symbol

        try:
            cursor = self.db['predictions'].find(
                query,
                {"_id": 0}
            ).sort("prediction_date", -1).limit(limit)
            
            history = list(cursor)
            return history
        except Exception as e:
            print(f"Error fetching history: {e}")
            return []

    def evaluate_model(self, symbol, days=60, model_type='rf'):
        symbol = symbol.upper()
        if self.db is None: return {"error": "DB Unavailable"}

        cutoff_date = (datetime.now() - timedelta(days=days + 400)).strftime('%Y-%m-%d')
        collection = self.db['market_data']
        cursor = collection.find({"symbol": symbol, "date": {"$gte": cutoff_date}}).sort("date", 1)
        data = list(cursor)
        
        if not data: return {"error": "Not enough data"}

        df = pd.DataFrame(data)
        if '_id' in df.columns: df.drop(columns=['_id'], inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        
        df = self.prepare_features(df)
        df['target'] = df['close'].shift(-1)
        # For RF evaluation, we need target return validation?
        # No, evaluate returns actual vs predicted PRICE.
        
        df.dropna(inplace=True)
        
        feature_file = self._get_feature_file_path(symbol)
        if os.path.exists(feature_file):
            with open(feature_file, 'r') as f:
                features = json.load(f)
        else:
            features = [f for f in self.default_features if f in df.columns]

        if model_type == 'lstm':
            model_path = f"{self.model_dir}/{symbol}_lstm.keras"
            if not os.path.exists(model_path): return {"error": "Model not found"}
            model = load_model(model_path)
            scaler = joblib.load(f"{self.model_dir}/{symbol}_lstm_scaler.pkl")
            
            X_all, y_all, _ = self._prepare_lstm_data(df, features, fit_scaler=False, scaler=scaler)
            if len(X_all) < days: return {"error": "Not enough eval data"}
            
            X = X_all[-days:]
            # y_actual = y_all[-days:] # This is scaled
            
            # Get actual prices correct aligned
            # LSTM aligns X[t] (seq) to y[t] (target). 
            # We want validation on same dates
            
            # y_all from prepare_lstm_data matches X_all. 
            # We just need to reconstruct.
            
            pred_scaled = model.predict(X)
            
            target_scaler_path = f"{self.model_dir}/{symbol}_lstm_target_scaler.pkl"
            if os.path.exists(target_scaler_path):
                 target_scaler = joblib.load(target_scaler_path)
                 predictions = [float(p[0]) for p in target_scaler.inverse_transform(pred_scaled)]
            else:
                 predictions = [float(p[0]) for p in pred_scaled]
                 
            # Align dates and actuals
            # prepare_lstm_data trims first sequence_length rows
            # so indices match df[sequence_length:]
            
            eval_indices = df.index[self.sequence_length:][-days:]
            dates = df.loc[eval_indices, 'date'].dt.strftime('%Y-%m-%d').tolist()
            actuals = df.loc[eval_indices, 'target'].tolist()
            close_list = df.loc[eval_indices, 'close'].tolist()
            
        else:
            model_path = f"{self.model_dir}/{symbol}_rf.pkl"
            if not os.path.exists(model_path): return {"error": "Model not found"}
            model = joblib.load(model_path)
            
            eval_df = df.tail(days)
            X = eval_df[features]
            
            # Predict Returns
            pred_returns = model.predict(X)
            
            # Reconstruct Price
            # close * (1 + pred_return)
            closes = eval_df['close'].values
            predictions = closes * (1 + pred_returns)
            predictions = predictions.tolist()
            
            dates = eval_df['date'].dt.strftime('%Y-%m-%d').tolist()
            actuals = eval_df['target'].tolist()
            close_list = eval_df['close'].tolist()

        results = []
        squared_errors = []
        correct_direction = 0
        
        for i in range(len(predictions)):
            actual = actuals[i]
            predicted = predictions[i]
            current_close = close_list[i]
            
            err = actual - predicted
            squared_errors.append(err ** 2)
            
            actual_move = actual - current_close
            predicted_move = predicted - current_close
            
            if (actual_move * predicted_move) > 0:
                correct_direction += 1
                
            results.append({
                "date": pd.to_datetime(dates[i]).strftime('%Y-%m-%d'),
                "actual": round(actual, 2),
                "predicted": round(predicted, 2),
                "error": round(err, 2)
            })

        mse = np.mean(squared_errors)
        mae = np.mean(np.abs([p['actual'] - p['predicted'] for p in results]))
        acc = (correct_direction / len(predictions) * 100)
        
        return {
            "symbol": symbol,
            "mse": round(mse, 4),
            "mae": round(mae, 4),
            "accuracy": round(acc, 2),
            "predictions": results
        }
