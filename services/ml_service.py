import logging
import numpy as np
import pandas as pd
logger = logging.getLogger(__name__)
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
import tempfile

try:
    import tensorflow as tf
    from tensorflow.keras.models import Sequential, load_model
    from tensorflow.keras.layers import LSTM, Dense, Dropout, Bidirectional, Input
    from tensorflow.keras.callbacks import EarlyStopping
    HAS_TENSORFLOW = True
except ImportError:
    logger.warning("TensorFlow not found. LSTM model will be disabled.")
    HAS_TENSORFLOW = False
    
# Global setting for Mixed Precision to speed up training
if HAS_TENSORFLOW:
    try:
        from tensorflow.keras import mixed_precision
        # Enable mixed precision if a GPU is detected to speed up training
        # Note: In sandbox/CPU environments this might just be a no-op or slightly faster
        policy = mixed_precision.Policy('mixed_float16')
        # Only set if GPU available to avoid issues on some CPU-only setups
        if tf.config.list_physical_devices('GPU'):
            mixed_precision.set_global_policy(policy)
            logger.info("ML: Mixed precision enabled (float16)")
    except Exception as e:
        logger.warning(f"ML: Could not enable mixed precision: {e}")

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
from concurrent.futures import ThreadPoolExecutor

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
            logger.error("DB unavailable for backfill.")
            return False

        logger.info(f"Backfilling history for {symbol} ({years} years)...")
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365 * years)).strftime('%Y-%m-%d')
        
        history = self.tradier.get_historical_pricing(symbol, start_date, end_date)
        
        if not history:
            logger.warning("No data returned from Tradier.")
            return False

        logger.info(f"Retrieved {len(history)} records. Saving to MongoDB...")
        
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
                
        logger.info(f"Backfill Complete! Processed {count} records.")
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
        Select top N features using correlation with the target.
        """
        # Exclude non-feature columns
        exclude = ['date', 'symbol', 'target', 'target_return', 'log_return']
        potential_features = [c for c in df.columns if c not in exclude]
        
        # Calculate correlation with target
        # Use target_col if it exists in DF, else fallback to 'close'
        actual_target = target_col if target_col in df.columns else 'close'
        correlations = df[potential_features].corrwith(df[actual_target]).abs()
        
        # Sort and take top N
        top_features = correlations.sort_values(ascending=False).head(n_top).index.tolist()
        
        logger.info(f"Top {n_top} Features selected via correlation: {top_features}")
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
        # Reduced units from 128 to 64 for speed and to prevent overfitting
        model.add(Input(shape=input_shape))
        model.add(LSTM(64, return_sequences=True)) # Unidirectional is faster than Bidirectional
        model.add(Dropout(0.2))
        model.add(LSTM(64, return_sequences=False))
        model.add(Dropout(0.2))
        model.add(Dense(32, activation='relu'))
        model.add(Dense(1)) # Predict price/return directly
        
        model.compile(optimizer='adam', loss='mean_squared_error')
        return model

    def perform_walk_forward_validation(self, df, top_features, model_type='lstm', min_train_size=200, test_size=20):
        """
        Perform Walk-Forward Validation:
        Train on expanding window [0..t], predict on [t..t+test_size].
        Returns average MSE and other metrics.
        """
        logger.info(f"Starting Walk-Forward Validation for {model_type.upper()}...")
        
        n_samples = len(df)
        if n_samples < min_train_size + test_size:
            logger.warning("Not enough data for Walk-Forward Validation. Skipping.")
            return {}

        errors = []
        accuracies = []
        
        # Expanding window loop
        # Start at min_train_size, step by test_size
        # For LSTM, we use a larger step size to avoid too many retraining cycles
        actual_step = test_size if model_type != 'lstm' else test_size * 5
        
        for t in range(min_train_size, n_samples - test_size, actual_step):
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
                # Increased batch_size to 64 for faster training
                model.fit(X_train, y_train_scaled, epochs=5, batch_size=64, verbose=0, callbacks=[early_stop])
                
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
                
            else:
                raise ValueError(f"Walk-forward validation not supported for {model_type}")
                
        avg_mse = np.mean(errors) if errors else 0
        avg_acc = np.mean(accuracies) if accuracies else 0
        
        logger.info(f"Validation Complete. Avg MSE: {avg_mse:.6f}, Avg Accuracy: {avg_acc:.2%}")
        return {"val_mse": avg_mse, "val_accuracy": avg_acc}

    def _fetch_and_prepare_training_data(self, symbol):
        """Helper to fetch and prepare data from DB."""
        if self.db is None:
            raise ExternalServiceError("Database not available")

        # 1. Fetch Data from MongoDB
        collection = self.db['market_data']
        # Fetch last 5 years to be safe
        cutoff_date = (datetime.now() - timedelta(days=365*5)).strftime('%Y-%m-%d')
        cursor = collection.find({"symbol": symbol, "date": {"$gte": cutoff_date}}).sort("date", 1)
        
        data = list(cursor)
        if not data:
            logger.warning(f"No data found for {symbol}. Attempting backfill...")
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
        df['target'] = df['close'].shift(-1) # Actual price for later comparison/RF fallback
        
        # RF Target (Simple Return)
        df['target_return'] = df['close'].shift(-1) / df['close'] - 1
        
        # Drop NaNs created by lags/indicators/shifting
        df.dropna(inplace=True)
        return df

    def train_model(self, symbol, model_type='lstm', express=False, pre_prepared_df=None):
        symbol = symbol.upper()
        
        logger.info(f"Starting {model_type.upper()} training for {symbol}...")
        
        # Use pre-prepared data if available
        if pre_prepared_df is not None:
            df = pre_prepared_df
        else:
            df = self._fetch_and_prepare_training_data(symbol)
        
        if len(df) < 100:
            raise ValidationError("Not enough data for training after processing")
        
        # 3. Feature Selection
        top_features = self.select_top_features(df)
        
        # Save selected features
        with open(self._get_feature_file_path(symbol), 'w') as f:
            json.dump(top_features, f)
            
        # 4. Walk-Forward Validation (Before final training)
        if not express and model_type != 'rl':
            validation_results = self.perform_walk_forward_validation(df, top_features, model_type=model_type)
        else:
            logger.info(f"Skipping Walk-Forward Validation for {model_type} (express={express}).")
            validation_results = {}
        
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
            early_stopping = EarlyStopping(monitor='val_loss', patience=3, restore_best_weights=True)
            
            # Increased batch_size to 64 and reduced epochs to 10 for speed
            model.fit(X_train, y_train, batch_size=64, epochs=10, validation_data=(X_test, y_test), 
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
            
        elif model_type == 'rl':
            # RL training - Run in a separate process to avoid memory/library conflicts (SIGSEGV)
            import subprocess
            import sys
            import tempfile
            
            # Prepare a small script to run the training
            tmp_dir = tempfile.gettempdir()
            df_path = os.path.join(tmp_dir, f'ml_tmp_df_{symbol}.pkl')
            features_path = os.path.join(tmp_dir, f'ml_tmp_features_{symbol}.pkl')
            worker_path = os.path.join(tmp_dir, f'rl_train_worker_{symbol}.py')

            train_script = f"""
import pandas as pd
import joblib
import os
from services.rl_price_predictor import RLPricePredictor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('rl_train_subprocess')

try:
    df = joblib.load(r'{df_path}')
    top_features = joblib.load(r'{features_path}')
    
    predictor = RLPricePredictor('{symbol}', df, top_features, '{self.model_dir}')
    predictor.train(timesteps=10_000)
    logger.info("RL model training subprocess completed successfully")
except Exception as e:
    logger.error(f"RL training subprocess failed: {{e}}")
    exit(1)
"""
            # Save temporary data for the subprocess
            joblib.dump(df.dropna(subset=top_features + ['close']), df_path)
            joblib.dump(top_features, features_path)
            
            with open(worker_path, 'w') as f:
                f.write(train_script)
            
            try:
                # Run the subprocess
                result = subprocess.run([sys.executable, worker_path], capture_output=True, text=True)
                if result.returncode != 0:
                    logger.error(f"RL Subprocess failed: {result.stderr}")
                    raise AppError(f"RL Training failed in subprocess: {result.stderr}", 500)
                
                logger.info("RL model trained via subprocess")
                mse = 0.0
            finally:
                # Cleanup
                for f in [df_path, features_path, worker_path]:
                    if os.path.exists(f): os.remove(f)
            
        else:
            raise ValueError(f"Unknown model_type: {model_type}")
        
        logger.info(f"Model ({model_type}) MSE: {mse}")
        
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

    def predict_next_day(self, symbol, model_type='lstm'):
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
             logger.warning(f"No recent data for {symbol}. Attempting backfill...")
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
            logger.warning(f"Feature file for {symbol} not found. Using defaults.")
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
            
        elif model_type == 'rl':
            import subprocess
            import sys
            import tempfile
            
            # Prepare a small script to run the prediction
            tmp_dir = tempfile.gettempdir()
            df_path = os.path.join(tmp_dir, f'predict_tmp_df_{symbol}.pkl')
            features_path = os.path.join(tmp_dir, f'predict_tmp_features_{symbol}.pkl')
            worker_path = os.path.join(tmp_dir, f'rl_predict_worker_{symbol}.py')

            predict_script = f"""
import pandas as pd
import joblib
import os
import json
from services.rl_price_predictor import RLPricePredictor
import logging

logging.basicConfig(level=logging.ERROR)

try:
    df = joblib.load(r'{df_path}')
    features = joblib.load(r'{features_path}')
    
    predictor = RLPricePredictor('{symbol}', df, features, '{self.model_dir}')
    prediction = predictor.predict(df)
    # Output only the prediction value
    print(prediction)
except Exception as e:
    # Print error but only to stderr
    import sys
    print(str(e), file=sys.stderr)
    exit(1)
"""
            # Save temporary data for the subprocess
            joblib.dump(df.dropna(subset=features + ['close']), df_path)
            joblib.dump(features, features_path)
            
            with open(worker_path, 'w') as f:
                f.write(predict_script)
            
            try:
                # Run the subprocess
                result = subprocess.run([sys.executable, worker_path], capture_output=True, text=True)
                if result.returncode != 0:
                    error_msg = result.stderr.strip()
                    if "not found" in error_msg.lower():
                        raise ResourceNotFoundError(f"RL model for {symbol} not found.")
                    logger.error(f"RL Predict Subprocess failed: {error_msg}")
                    raise AppError(f"RL Prediction failed in subprocess: {error_msg}", 500)
                
                prediction = float(result.stdout.strip())
            finally:
                # Cleanup
                for f in [df_path, features_path, worker_path]:
                    if os.path.exists(f): os.remove(f)
            
        else:
            raise ValueError(f"Unknown model_type: {model_type}")
            
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
                    
                logger.info(f"Applying Bias Correction for {symbol}: Raw={raw_prediction:.2f}, Bias={bias_correction:.2f}, Final={prediction:.2f}")

        except Exception as e:
            logger.error(f"Error calculating bias correction: {e}")

        
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
                "predicted_price": round(float(prediction), 2),
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
            logger.info(f"Saved prediction for {symbol} ({model_type}) on {prediction_date} (Actual: {actual_close_price})")
        except Exception as e:
            logger.error(f"Error saving prediction to DB: {e}")

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

    def evaluate_model(self, symbol, days=60, model_type='lstm'):
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
            
            # Prepare data (Need Log Return logic similar to training)
            # Ensure target is set correctly for prepare_lstm_data
            df['log_return'] = np.log(df['close'].shift(-1) / df['close'])
            df['target_for_lstm'] = df['log_return']
            
            # Save original target price for validation
            # We want actual future price (Close T+1) to compare with predicted price
            df['actual_future_price'] = df['close'].shift(-1)
            
            # Swap target for prep
            df['target'] = df['target_for_lstm']
            
            X_all, _, _ = self._prepare_lstm_data(df, features, fit_scaler=False, scaler=scaler)
            
            if len(X_all) < days: raise ValidationError("Not enough eval data")
            
            # Select last 'days' sequences
            X = X_all[-days:]
            
            # Load Target Scaler
            target_scaler_path = f"{self.model_dir}/{symbol}_lstm_target_scaler.pkl"
            target_scaler = joblib.load(target_scaler_path) if os.path.exists(target_scaler_path) else None

            # Predict
            pred_scaled = model(X, training=False).numpy()
            
            if target_scaler:
                 pred_log_returns = target_scaler.inverse_transform(pred_scaled).flatten()
            else:
                 pred_log_returns = pred_scaled.flatten()
            
            # Reconstruct and Align
            predictions = []
            actuals = [] 
            dates = []
            close_list = [] # Current close (basis for prediction)
            
            # Indices logic:
            # X_all is aligned with df[sequence_length:].
            # X_all[0] corresponds to sequence ending at index `sequence_length-1`?
            # No, `prepare_lstm_data` loop:
            # for i in range(seq_len, len(scaled_data)):
            #    X.append(scaled_data[i-seq_len:i])
            #    y.append(target[i])
            
            # So X[k] contains data up to index `seq_len + k - 1`.
            # And predicts target at index `seq_len + k`.
            
            # If we take `X_all[-days:]`, let N = len(X_all).
            # We take indices [N-days, N-days+1, ... N-1].
            
            total_samples = len(X_all)
            start_idx_in_x = total_samples - days
            
            for k in range(days):
                # Map to DataFrame index
                # X_index = start_idx_in_x + k
                # Corresponding DF index = sequence_length + X_index
                df_idx = self.sequence_length + start_idx_in_x + k
                
                # Check bounds
                if df_idx >= len(df): continue
                
                # Input Ends at previous row (basis)
                row_basis = df.iloc[df_idx - 1]
                row_target = df.iloc[df_idx]
                
                basis_close = row_basis['close']
                
                # Reconstruct Prediction
                pred_price = basis_close * np.exp(pred_log_returns[k])
                
                # Actual Future Price
                actual_price = row_basis['actual_future_price'] # = row_target['close']?
                # df['actual_future_price'][i] = close[i+1].
                # row_basis is at T. actual_future_price is Close[T+1].
                # row_target is at T+1? No.
                # df_idx corresponds to `i` in loop.
                # X[i] -> target[i].
                # target[i] depends on construction.
                # If target was 'log_return' = shift(-1)/close.
                # Then target[i] is return T->T+1.
                # But X[i] contains data [i-seq:i]. Last point is i-1.
                # So X[i] predicts for period starting at i-1?
                # No.
                
                # Let's trust the dates alignment:
                # date[i] in loop?
                
                current_date = row_target['date'].strftime('%Y-%m-%d')
                # Wait, if target[i] is at row i.
                # And X[i] ends at i-1.
                # Then we are predicting for time `i`.
                
                predictions.append(pred_price)
                actuals.append(row_basis['actual_future_price']) # Verify this exists
                dates.append(current_date)
                close_list.append(basis_close)
            
        elif model_type == 'rl':
            from services.rl_price_predictor import RLPricePredictor
            recent_df = df.dropna(subset=features + ['close']).copy()
            if recent_df.empty: raise ValidationError("Not enough clean data for RL evaluation.")
            
            predictor = RLPricePredictor(symbol, recent_df, features, self.model_dir)
            predictions = []
            actuals = []
            dates = []
            close_list = []
            
            # Predict each day in the test set
            eval_start_idx = len(recent_df) - days
            for i in range(max(0, eval_start_idx), len(recent_df)):
                basis_df = recent_df.iloc[:i+1] # up to current day T
                try:
                    pred_price = predictor.predict(basis_df)
                except Exception:
                    continue
                
                predictions.append(pred_price)
                # target is the close of the day after
                # if row is T, actual target is df['actual_future_price']
                actuals.append(recent_df.iloc[i].get('actual_future_price', recent_df.iloc[i]['close']))
                dates.append(recent_df.iloc[i]['date'].strftime('%Y-%m-%d'))
                close_list.append(recent_df.iloc[i]['close'])
                
        else:
            raise ValueError(f"Unknown model_type for evaluation: {model_type}")

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

    # ---------------------------------------------------------------
    # Batch Operations (called by BotService scheduler)
    # ---------------------------------------------------------------

    def run_batch_predictions(self, symbols):
        """
        Run predict_next_day for each symbol across all model types.
        Skips models that aren't trained yet. Returns summary dict.
        """
        model_types = ['lstm', 'rl']
        
        success_count = 0
        skipped_count = 0
        error_count = 0
        details = []

        # Refresh actuals first (backfill any missing close prices)
        try:
            self.refresh_prediction_actuals()
        except Exception as e:
            logger.error(f"Batch: Error refreshing actuals: {e}")

        for symbol in symbols:
            for mt in model_types:
                try:
                    result = self.predict_next_day(symbol, model_type=mt)
                    success_count += 1
                    logger.info(f"✅ Prediction {mt.upper()} for {symbol}: ${result['predicted_price']}")
                except ResourceNotFoundError:
                    skipped_count += 1
                except Exception as e:
                    error_count += 1
                    logger.error(f"❌ Prediction {mt.upper()} for {symbol} failed: {e}")
                    details.append(f"{symbol}/{mt}: {str(e)[:80]}")

        batch_results = {
            "success": success_count,
            "skipped": skipped_count,
            "errors": error_count,
            "details": details
        }
        logger.info(f"📊 Batch Predictions Complete: {success_count} OK, {skipped_count} skipped, {error_count} errors")
        return batch_results

    def run_batch_training(self, symbols, express=True):
        """
        Train all model types (RF, LSTM, Ensemble) for each symbol.
        Uses express mode by default for speed (skips walk-forward validation).
        Returns summary dict.
        """
        model_types = ['lstm', 'rl']
        
        success_count = 0
        error_count = 0
        details = []

        for symbol in symbols:
            for mt in model_types:
                try:
                    logger.info(f"🔄 Training {mt.upper()} for {symbol}...")
                    result = self.train_model(symbol, model_type=mt, express=express)
                    success_count += 1
                    logger.info(f"✅ {symbol} {mt.upper()}: MSE={result.get('mse')}")
                except Exception as e:
                    error_count += 1
                    logger.error(f"❌ Training {mt.upper()} failed for {symbol}: {e}")
                    details.append(f"{symbol}/{mt}: {str(e)[:100]}")

        batch_results = {
            "success": success_count,
            "errors": error_count,
            "details": details
        }
        logger.info(f"🎓 Batch Training Complete: {success_count} OK, {error_count} errors")
        return batch_results
