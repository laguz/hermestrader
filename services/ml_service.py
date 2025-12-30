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
import pandas as pd
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday, \
    USMartinLutherKingJr, USPresidentsDay, USMemorialDay, USLaborDay, USThanksgivingDay, GoodFriday
from pandas.tseries.offsets import CustomBusinessDay

try:
    import tensorflow as tf
    from tensorflow.keras.models import Sequential, load_model
    from tensorflow.keras.layers import LSTM, Dense, Dropout, Bidirectional, Input
    from tensorflow.keras.callbacks import EarlyStopping
    HAS_TENSORFLOW = True
except ImportError:
    print("Warning: TensorFlow not found. LSTM model will be disabled.")
    HAS_TENSORFLOW = False

class NYSEHolidayCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        GoodFriday,
        USMemorialDay,
        Holiday('Juneteenth', month=6, day=19, observance=nearest_workday),
        Holiday('USIndependenceDay', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]

from utils.indicators import calculate_rsi, calculate_bollinger_bands, calculate_macd, calculate_atr, calculate_sma, calculate_obv, calculate_vwap
from services.container import Container
from exceptions import ValidationError, ExternalServiceError, ResourceNotFoundError, AppError

class MLService:
    def __init__(self, tradier_service):
        self.tradier = tradier_service # Keep for compatibility/fallback if needed
        self.db = Container.get_db()
        self.model_dir = "models"
        if not os.path.exists(self.model_dir):
            os.makedirs(self.model_dir)
        
        self.sequence_length = 60 # Lookback for LSTM
        self.default_features = ['close', 'volume', 'rsi', 'upper_bb', 'lower_bb', 'mid_bb', 'macd', 'macd_signal', 'atr', 'sma_50', 'obv', 'vwap']

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

        # Volume Momentum
        df['obv'] = calculate_obv(df['close'], df['volume'])
        df['vwap'] = calculate_vwap(df['high'], df['low'], df['close'], df['volume'], window=14)
        
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
        # Also exclude calculated targets (leakage)
        exclude = ['date', 'symbol', 'target', 'target_return', 'log_return']
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
        # Fix warning: Do not pass input_shape to layer, use Input(shape) first
        model.add(Input(shape=input_shape))
        model.add(Bidirectional(LSTM(128, return_sequences=True)))
        model.add(Dropout(0.2))
        model.add(Bidirectional(LSTM(128, return_sequences=False)))
        model.add(Dropout(0.2))
        model.add(Dense(25))
        model.add(Dense(1)) # Predict price directly
        
        model.compile(optimizer='adam', loss='mean_squared_error')
        return model

    def perform_walk_forward_validation(self, df, top_features, model_type='rf', min_train_size=200, test_size=20):
        """
        Perform Walk-Forward Validation:
        Train on expanding window [0..t], predict on [t..t+test_size].
        Returns average MSE and other metrics.
        """
        print(f"Starting Walk-Forward Validation for {model_type.upper()}...")
        
        n_samples = len(df)
        if n_samples < min_train_size + test_size:
            print("Not enough data for Walk-Forward Validation. Skipping.")
            return {}

        errors = []
        accuracies = []
        
        # Expanding window loop
        # Start at min_train_size, step by test_size
        for t in range(min_train_size, n_samples - test_size, test_size):
            train_df = df.iloc[:t].copy()
            test_df = df.iloc[t:t+test_size].copy()
            
            # Prepare Data & Train
            if model_type == 'lstm':
                if not HAS_TENSORFLOW: continue
                # LSTM Training
                df_lstm_train = train_df.copy()
                df_lstm_train['target'] = df_lstm_train['log_return'] # Ensure target is log return
                X_train, y_train, scaler = self._prepare_lstm_data(df_lstm_train, top_features, fit_scaler=True)
                
                # LSTM Testing
                df_lstm_test = pd.concat([train_df.tail(self.sequence_length), test_df]) # Need lookback
                df_lstm_test['target'] = df_lstm_test['log_return']
                # Use trained scaler
                X_test, y_test, _ = self._prepare_lstm_data(df_lstm_test, top_features, fit_scaler=False, scaler=scaler)
                
                # Filter X_test to only include the new test period rows
                # _prepare_lstm_data returns sequences for the whole DF usually
                # Here we constructed df_lstm_test to start exactly `sequence_length` before test_df
                # So X_test should match test_df length roughly
                # Adjust if needed
                
                # Train Model (Quick epoch for validation)
                y_train = y_train.reshape(-1, 1)
                target_scaler = MinMaxScaler(feature_range=(0, 1))
                y_train_scaled = target_scaler.fit_transform(y_train)
                
                model = self._build_lstm_model(input_shape=(X_train.shape[1], X_train.shape[2]))
                early_stop = EarlyStopping(monitor='loss', patience=2)
                model.fit(X_train, y_train_scaled, epochs=5, batch_size=32, verbose=0, callbacks=[early_stop])
                
                # Predict
                # Use model(X, training=False) to avoid TF function retracing warnings in loops
                pred_scaled = model(X_test, training=False).numpy()
                pred_log_ret = target_scaler.inverse_transform(pred_scaled).flatten()
                actual_log_ret = y_test # These are raw log returns from df 'target' because _prepare_lstm_data returns y as target values (unscaled if we didnt scale y inside?) 
                # Wait, _prepare returns y as target column values. 
                # In training we scaled y. In validation we inversed pred. 
                # y_test from _prepare is raw values from `target` column.
                # So actual_log_ret is correct.
                
                # Calculate Error (MSE on Log Returns)
                mse_fold = mean_squared_error(actual_log_ret, pred_log_ret)
                errors.append(mse_fold)
                
                # Accuracy (Direction)
                # We need actual returns to check sign
                correct = np.sum(np.sign(pred_log_ret) == np.sign(actual_log_ret))
                acc_fold = correct / len(actual_log_ret)
                accuracies.append(acc_fold)
                
            else: # RF
                # RF Training
                X_train = train_df[top_features]
                y_train = train_df['target_return']
                
                model = RandomForestRegressor(n_estimators=50, max_depth=10, random_state=42, n_jobs=-1)
                model.fit(X_train, y_train)
                
                # RF Prediction
                X_test = test_df[top_features]
                y_test = test_df['target_return']
                
                pred_ret = model.predict(X_test)
                
                mse_fold = mean_squared_error(y_test, pred_ret)
                errors.append(mse_fold)
                
                correct = np.sum(np.sign(pred_ret) == np.sign(y_test))
                acc_fold = correct / len(y_test)
                accuracies.append(acc_fold)
                
        avg_mse = np.mean(errors) if errors else 0
        avg_acc = np.mean(accuracies) if accuracies else 0
        
        print(f"Validation Complete. Avg MSE: {avg_mse:.6f}, Avg Accuracy: {avg_acc:.2%}")
        return {"val_mse": avg_mse, "val_accuracy": avg_acc}

    def train_model(self, symbol, model_type='rf'):
        symbol = symbol.upper()
        if model_type == 'ensemble':
            print(f"Starting ENSEMBLE training for {symbol} (RF + LSTM)...")
            res_rf = self.train_model(symbol, model_type='rf')
            res_lstm = self.train_model(symbol, model_type='lstm')
            return {
                "status": "trained",
                "symbol": symbol,
                "type": "ensemble",
                "rf_mse": res_rf['mse'],
                "lstm_mse": res_lstm['mse']
            }

        print(f"Starting {model_type.upper()} training for {symbol} using local DB...")
        
        if self.db is None:
            raise ExternalServiceError("Database not available")

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
                raise ExternalServiceError(f"No data found for {symbol} and backfill failed.")
            
            if not data:
                 raise ExternalServiceError(f"No data found in DB for {symbol} after backfill")
            
        df = pd.DataFrame(data)
        if '_id' in df.columns: df.drop(columns=['_id'], inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        
        # 2. Prepare Data (Features + Target)
        df = self.prepare_features(df)
        
        # Target: Log Return for Next Day (Shifted -1)
        # log_return = ln(close_t+1 / close_t)
        df['log_return'] = np.log(df['close'].shift(-1) / df['close'])
        
        # Keep 'target' column for legacy/reference or specific model usage
        # For LSTM, we will use 'log_return' as target
        # For RF, we use 'target_return' (pct_change)
        
        df['target'] = df['close'].shift(-1) # Actual price for later comparison/RF fallback
        
        # RF Target (Simple Return)
        df['target_return'] = df['close'].shift(-1) / df['close'] - 1
        
        # Drop NaNs created by lags/indicators/shifting
        df.dropna(inplace=True)
        
        if len(df) < 100:
            raise ValidationError("Not enough data for training after processing")
        
        # 3. Feature Selection
        top_features = self.select_top_features(df)
        
        # Save selected features
        with open(self._get_feature_file_path(symbol), 'w') as f:
            json.dump(top_features, f)
            
        # 4. Walk-Forward Validation (Before final training)
        validation_results = self.perform_walk_forward_validation(df, top_features, model_type=model_type)
        
        mse = 0
        final_model_stats = {}
        
        if model_type == 'lstm':
            if not HAS_TENSORFLOW:
                raise AppError("LSTM training requires TensorFlow, but it is not installed.", 501)
                
            # Use 'log_return' as target for LSTM
            df_lstm = df.copy()
            df_lstm['target'] = df_lstm['log_return']
            
            X, y, scaler = self._prepare_lstm_data(df_lstm, top_features, fit_scaler=True)
            
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
            pred_log_returns = target_scaler.inverse_transform(pred_scaled)
            actual_log_returns = target_scaler.inverse_transform(y_test)
            
            # Reconstruct Prices for MSE Calculation
            # We need the 'close' price at the step BEFORE prediction.
            # X_test indices start at split. 
            # The corresponding original DF index is split + sequence_length.
            # But the 'close' price needed for reconstruction is at index (split + sequence_length + i) 
            # where i is the test sample index.
            
            # It's safer to calculate MSE on the log returns themselves for model separation,
            # OR roughly approximate.
            # Let's stick to MSE on Log Returns for training metric to avoid complex reconstruction here,
            # but rely on 'evaluate_model' for full price verification.
            
            mse = mean_squared_error(actual_log_returns, pred_log_returns)
            
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
            
            # RF Target is already set up in 'target_return'
            # df['target_return'] = df['close'].shift(-1) / df['close'] - 1
            
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
            "data_points": len(df),
            "mse": round(mse, 6), # Train/Test Split MSE (Last Fold)
            "val_mse": round(validation_results.get('val_mse', 0), 6), # Walk-Forward MSE
            "val_accuracy": round(validation_results.get('val_accuracy', 0), 4),
            "features": top_features
        }

    def predict_next_day(self, symbol, model_type='rf'):
        symbol = symbol.upper()
        if self.db is None:
            raise ExternalServiceError("Database not available")

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
                 raise ExternalServiceError(f"No recent data found in DB and backfill failed")

        if not data:
             raise ExternalServiceError("No recent data found in DB after backfill")

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
                
        # Safety: Ensure no target leakage columns are in features list
        # This handles legacy feature files that might have included them
        forbidden = ['target', 'target_return', 'log_return', 'date', 'symbol']
        features = [f for f in features if f not in forbidden]

        if df.iloc[-1].isna().any():
             df_clean = df.dropna()
             if df_clean.empty: raise ValidationError("Not enough data for indicators")
             pass

        last_row = df.iloc[-1]
        
        if df[features].iloc[-1].isna().any():
             raise ValidationError("Latest data has NaNs in required features.")

        prediction = 0
        
        if model_type == 'ensemble':
            # recursive calls
            pred_rf = self.predict_next_day(symbol, model_type='rf')
            pred_lstm = self.predict_next_day(symbol, model_type='lstm')
            
            p_rf = pred_rf['predicted_price']
            p_lstm = pred_lstm['predicted_price']
            
            # Simple Average
            prediction = (p_rf + p_lstm) / 2
            
            # Use strict features from one (doesn't matter much for display)
            features = pred_rf['used_features']
            
            change = prediction - pred_rf['last_close']
            percent_change = (change / pred_rf['last_close']) * 100
            
            prediction_date = pred_rf['prediction_date']
            
             # Save Ensemble Prediction
            try:
                pred_doc = {
                    "symbol": symbol,
                    "model_type": 'ensemble',
                    "prediction_date": prediction_date,
                    "predicted_price": float(prediction),
                    "raw_prediction": float(prediction),
                    "bias_correction": 0.0,
                    "actual_close_price": None,
                    "created_at": datetime.now(),
                    "components": {"rf": float(p_rf), "lstm": float(p_lstm)}
                }
                self.db['predictions'].update_one(
                    {"symbol": symbol, "model_type": 'ensemble', "prediction_date": prediction_date},
                    {"$set": pred_doc},
                    upsert=True
                )
            except Exception as e:
                print(f"Error saving ensemble prediction: {e}")
                
            return {
                "symbol": symbol,
                "model": "ensemble",
                "predicted_price": round(float(prediction), 2),
                "last_close": round(float(pred_rf['last_close']), 2),
                "change": round(float(change), 2),
                "percent_change_str": f"{percent_change:.2f}%",
                "prediction_date": prediction_date,
                "components": {"rf": round(p_rf, 2), "lstm": round(p_lstm, 2)}
            }

        if model_type == 'lstm':
            if not HAS_TENSORFLOW:
                raise AppError("LSTM prediction requires TensorFlow, but it is not installed.", 501)
            model_path = f"{self.model_dir}/{symbol}_lstm.keras"
            scaler_path = f"{self.model_dir}/{symbol}_lstm_scaler.pkl"
            target_scaler_path = f"{self.model_dir}/{symbol}_lstm_target_scaler.pkl"
            
            if not os.path.exists(model_path):
                raise ResourceNotFoundError(f"LSTM model for {symbol} not found.")
                
            model = load_model(model_path)
            scaler = joblib.load(scaler_path)
            
            df_clean = df.dropna(subset=features)
            last_sequence_df = df_clean.tail(self.sequence_length)
            
            if len(last_sequence_df) < self.sequence_length:
                raise ValidationError(f"Not enough valid data for LSTM sequence.")
                
            data = last_sequence_df[features].values
            scaled_data = scaler.transform(data) 
            
            X_input = np.array([scaled_data])
            pred_scaled = model.predict(X_input)
            
            if os.path.exists(target_scaler_path):
                target_scaler = joblib.load(target_scaler_path)
                pred_log_return = float(target_scaler.inverse_transform(pred_scaled)[0][0])
            else:
                pred_log_return = float(pred_scaled[0][0])
                
            # Reconstruct Price
            # Price = Last Close * exp(Log Return)
            prediction = last_row['close'] * np.exp(pred_log_return)
            
        else: # RF
            model_path = f"{self.model_dir}/{symbol}_rf.pkl"
            if not os.path.exists(model_path):
                raise ResourceNotFoundError("RF Model not found.")
            model = joblib.load(model_path)
            
            features_df = pd.DataFrame([last_row[features]])
            pred_return = model.predict(features_df)[0]
            
            # Reconstruct Price: Close * (1 + return)
            last_close_val = last_row['close']
            prediction = last_close_val * (1 + pred_return)
            
        last_close = last_row['close']
        
        # Calculate next trading day (skip weekends and holidays)
        nyse_trading_day = CustomBusinessDay(calendar=NYSEHolidayCalendar())
        
        # DEBUG
        print(f"DEBUG: Last Date={last_row['date']}, Offset={nyse_trading_day}, Next={last_row['date'] + nyse_trading_day}")
        
        prediction_date = (last_row['date'] + nyse_trading_day).strftime('%Y-%m-%d')

        # ---------------------------------------------------------
        # Bias Correction Mechanism: Self-Correction using Evaluation Data
        # ---------------------------------------------------------
        raw_prediction = prediction
        bias_correction = 0.0
        
        try:
            # Fetch last 30 completed predictions
            recent_evals = list(self.db['predictions'].find({
                "symbol": symbol,
                "model_type": model_type,
                "actual_close_price": {"$ne": None},
                "prediction_date": {"$lt": prediction_date} 
            }).sort("prediction_date", -1).limit(30))
            
            if recent_evals and len(recent_evals) > 5:
                # bias = predicted - actual
                # If bias > 0, we over-predict. We should SUBTRACT it.
                # If bias < 0, we under-predict. We should SUBTRACT it (add).
                biases = [(p['predicted_price'] - p['actual_close_price']) for p in recent_evals]
                mean_bias = np.mean(biases)
                
                # Apply correction
                bias_correction = float(mean_bias)
                prediction = raw_prediction - bias_correction
                
                # Sanity check: Don't flip to negative (unlikely but safe)
                if prediction < 0:
                    prediction = raw_prediction
                    bias_correction = 0.0
                    
                print(f"Applying Bias Correction for {symbol}: Raw={raw_prediction:.2f}, Bias={bias_correction:.2f}, Final={prediction:.2f}")

        except Exception as e:
            print(f"Error calculating bias correction: {e}")

        
        last_close = last_row['close']
        change = prediction - last_close
        percent_change = (change / last_close) * 100
        
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
                "raw_prediction": float(raw_prediction),
                "bias_correction": float(bias_correction),
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

    def get_prediction_history(self, symbol=None, limit=100, days=None):
        """
        Retrieves recent predictions. If symbol is provided, filters by symbol.
        Checks for missing actual_close_price and backfills if available.
        If days is provided, limits to predictions made in the last 'days' days.
        """
        query = {}
        if symbol:
            symbol = symbol.upper().strip()
            query["symbol"] = symbol
            
        if days:
            try:
                days_int = int(days)
                cutoff = (datetime.now() - timedelta(days=days_int)).strftime('%Y-%m-%d')
                query["prediction_date"] = {"$gte": cutoff}
            except ValueError:
                pass # Ignore invalid days


        try:
            cursor = self.db['predictions'].find(
                query,
                {"_id": 0}
            ).sort("prediction_date", -1).limit(limit)
            
            history = list(cursor)
            
            # Lazy update of actuals
            for record in history:
                if record.get('actual_close_price') is None:
                    pred_date = record.get('prediction_date')
                    rec_symbol = record.get('symbol')
                    
                    if pred_date and rec_symbol:
                        # Check market data
                        market_doc = self.db['market_data'].find_one({"symbol": rec_symbol, "date": pred_date})
                        if market_doc and 'close' in market_doc:
                            actual_price = float(market_doc['close'])
                            
                            # Update DB
                            self.db['predictions'].update_one(
                                {
                                    "symbol": rec_symbol, 
                                    "prediction_date": pred_date, 
                                    "model_type": record.get('model_type')
                                },
                                {"$set": {"actual_close_price": actual_price}}
                            )
                            
                            # Update memory
                            record['actual_close_price'] = actual_price
            
            return history
        except Exception as e:
            print(f"Error fetching history: {e}")
            return []

    def refresh_prediction_actuals(self):
        """
        Scans for predictions with missing actuals, fetches recent market data,
        and updates the database.
        """
        if self.db is None: raise ExternalServiceError("DB unavailable")
        
        # Find pending predictions from last 30 days
        cutoff = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        pending = list(self.db['predictions'].find({
            "actual_close_price": None,
            "prediction_date": {"$gte": cutoff}
        }))
        
        if not pending:
            return {"message": "No pending predictions to refresh."}
            
        symbols = list(set([p['symbol'] for p in pending]))
        updated_count = 0
        
        print(f"Refreshing actuals for {len(symbols)} symbols: {symbols}")
        
        # Refresh market data for these symbols (last 14 days is enough to cover recent gaps)
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
        
        updated_symbols = []

        for symbol in symbols:
            try:
                # Fetch fresh data
                history = self.tradier.get_historical_pricing(symbol, start_date, end_date)
                if history:
                    # Update Market Data
                    collection = self.db['market_data']
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
                         collection.update_one(
                            {"symbol": symbol, "date": record['date']},
                            {"$set": doc},
                            upsert=True
                        )
                    updated_symbols.append(symbol)
            except Exception as e:
                print(f"Error refreshing {symbol}: {e}")
                
        # Now update the predictions
        # We can just re-run the lazy update logic essentially, but explicitly
        # Re-fetch pending to be safe or iterate pending list
        
        for p in pending:
            symbol = p['symbol']
            p_date = p['prediction_date']
            
            market_doc = self.db['market_data'].find_one({"symbol": symbol, "date": p_date})
            if market_doc and 'close' in market_doc:
                actual = float(market_doc['close'])
                self.db['predictions'].update_one(
                    {"_id": p['_id']}, 
                    {"$set": {"actual_close_price": actual}}
                )
                updated_count += 1
                
        return {
            "message": f"Refreshed {len(updated_symbols)} symbols. Updated {updated_count} prediction records.",
            "updated_count": updated_count
        }

    def evaluate_model(self, symbol, days=60, model_type='rf'):
        symbol = symbol.upper()
        if self.db is None: raise ExternalServiceError("DB Unavailable")

        cutoff_date = (datetime.now() - timedelta(days=days + 400)).strftime('%Y-%m-%d')
        collection = self.db['market_data']
        cursor = collection.find({"symbol": symbol, "date": {"$gte": cutoff_date}}).sort("date", 1)
        data = list(cursor)
        
        if not data: raise ValidationError("Not enough data")

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
            if not HAS_TENSORFLOW:
                raise AppError("LSTM evaluation requires TensorFlow, but it is not installed.", 501)
            model_path = f"{self.model_dir}/{symbol}_lstm.keras"
            if not os.path.exists(model_path): raise ResourceNotFoundError("Model not found")
            model = load_model(model_path)
            scaler = joblib.load(f"{self.model_dir}/{symbol}_lstm_scaler.pkl")
            
            X_all, y_all, _ = self._prepare_lstm_data(df, features, fit_scaler=False, scaler=scaler)
            if len(X_all) < days: raise ValidationError("Not enough eval data")
            
            X = X_all[-days:]
            # Align dates and actuals
            # prepare_lstm_data trims first sequence_length rows
            # inputs ended at index: (len(df) - days + i) ?
            # Let's use indices logic
            
            # X[0] is the sequence ending at (len(df) - days).
            # The target for X[0] is price at (len(df) - days + 1)? No, usually target is next day.
            # If we trained on target = shifted(-1), then input[t] predicts target[t] (which is close[t+1]).
            
            # We need the Close price corresponding to the END of the sequence X[i].
            # Sequence X[i] uses data up to index T. We need Close[T].
            
            # Indices in X_all correspond to df[sequence_length:]
            # So X_all[0] ends at df.iloc[sequence_length-1]? No.
            # prepare_lstm_data loop: range(self.sequence_length, len(scaled_data))
            # i traverses from seq_len to end.
            # X.append(scaled_data[i-self.sequence_length:i])
            # The sequence includes indices [i-seq_len : i]. 
            # So the last data point in the sequence is at index i-1.
            # The target is at index i. (which corresponds to df.iloc[i])
            
            # So for prediction X_all[k], the "last close" is at df.iloc[sequence_length + k - 1].
            # And the target is df.iloc[sequence_length + k]['target'] (which we set to log_return/close shifted).
            
            # Indices of validation set:
            total_samples = len(X_all)
            val_start_idx = total_samples - days
            
            # Reconstruct predictions
            reconstructed_preds = []
            
            # We need to iterate
            for k in range(days):
                # Global index in df corresponding to the target row
                # X_all[0] target corresponds to df row `sequence_length`.
                # X_all[k] target corresponds to df row `sequence_length + k`.
                
                # We are looking at the last `days` samples.
                # sample_idx among X_all = val_start_idx + k
                
                df_idx = self.sequence_length + val_start_idx + k
                
                # Last Close comes from previous row
                last_close = df.iloc[df_idx - 1]['close']
                
                pred_log_ret = predictions[k] # This is log return
                pred_price = last_close * np.exp(pred_log_ret)
                reconstructed_preds.append(pred_price)
            
            predictions = reconstructed_preds
            
            eval_indices = df.index[self.sequence_length:][-days:]
            dates = df.loc[eval_indices, 'date'].dt.strftime('%Y-%m-%d').tolist()
            
            # Actual prices (we need clean close prices, not log returns)
            # targets in df['target'] might be log returns now for LSTM logic!
            # So we should fetch 'close'.shift(-1) from df, or just take 'close' at the target index.
            
            # df['target'] was set to log_return earlier if we modified it?
            # In evaluate_model, we set df['target'] = df['close'].shift(-1) at line 671.
            # Wait, line 671 sets target to Close Price.
            # But prepare_lstm_data takes 'target' column.
            # Does prepare_lstm_data re-calculate log returns? No, it takes raw column.
            
            # CRITICAL: We need scaling to match training.
            # If training used Log Returns, we must provide Log Returns to prepare_lstm_data check?
            # Or reliance on scaler?
            # The scaler was trained on Log Returns.
            # So the column 'target' passed to prepare_lstm_data MUST be log returns.
            
            # Recalculate 'target' as log returns for the preparation step
            df['log_return'] = np.log(df['close'].shift(-1) / df['close'])
            df['target_for_lstm'] = df['log_return']
            
            # Temporarily swap target for prep
            df['actual_price_target'] = df['target'] # Save price
            df['target'] = df['target_for_lstm']
            
            X_eval_all, _, _ = self._prepare_lstm_data(df, features, fit_scaler=False, scaler=scaler)
            X = X_eval_all[-days:]
            
            # Restore target for actuals (prices)
            df['target'] = df['actual_price_target']
            
            # Now predict
            pred_scaled = model.predict(X)
             
            if os.path.exists(target_scaler_path):
                 target_scaler = joblib.load(target_scaler_path)
                 pred_log_returns = [float(p[0]) for p in target_scaler.inverse_transform(pred_scaled)]
            else:
                 pred_log_returns = [float(p[0]) for p in pred_scaled]
                 
            # Reconstruct
            predictions = []
            actuals = [] # Price
            
            for k in range(days):
                df_idx = self.sequence_length + val_start_idx + k
                last_close = df.iloc[df_idx - 1]['close']
                
                # Reconstruct
                pred_price = last_close * np.exp(pred_log_returns[k])
                predictions.append(pred_price)
                
                # Actual
                actual_price = df.iloc[df_idx]['close'] # Current close? 
                # Wait, df['target'] was close.shift(-1).
                # prepare_lstm_data aligns:
                # X[i] (0..T) -> y[i] (target at T+1 implied by shift)
                # target column is user defined.
                # If we used shift(-1), then row `i` has target `i+1`.
                # `prepare_lstm_data` usually just maps X[i] -> col['target'].iloc[i].
                
                # In training: df['target'] = log_return.shift(-1).
                # No, I did df['log_return'] = shift(-1)/close. 
                # So row T has the return for T -> T+1.
                
                # So X[T] (data ending at T) predicts row T's target (Return T->T+1).
                
                # So we need Last Close = Close[T] (from row df_idx)
                # And Actual Price = Close[T+1] (from row df_idx + 1?)
                # OR if 'target' was just aligned to row T.
                
                # Let's check training logic:
                # df['log_return'] = np.log(df['close'].shift(-1) / df['close'])
                # df['target'] = df['log_return'] (implicit in my change above for training)
                # prepare_lstm_data: y.append(target[i])
                
                # So y[i] is the log return at index i.
                # Index i contains features for day i.
                # Return at i is (Close_i+1 / Close_i).
                
                # So Last Close = Close[i].
                # Actual Price = Close_i+1.
                
                # In Eval loop:
                # df_idx is the index i.
                last_close = df.iloc[df_idx]['close']
                
                # Reconstruct
                pred_price = last_close * np.exp(pred_log_returns[k])
                predictions.append(pred_price)
                
                actuals.append(df.iloc[df_idx]['actual_price_target']) # This is close.shift(-1)
                
            close_list = [df.iloc[self.sequence_length + val_start_idx + k]['close'] for k in range(days)]
            
        else:
            model_path = f"{self.model_dir}/{symbol}_rf.pkl"
            if not os.path.exists(model_path): raise ResourceNotFoundError("Model not found")
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
