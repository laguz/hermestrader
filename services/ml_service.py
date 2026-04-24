import logging
import re
import numpy as np
import pandas as pd
from pymongo import UpdateOne
from sklearn.metrics import mean_squared_error
from sklearn.preprocessing import MinMaxScaler
import os
import joblib
import json
from datetime import datetime, timedelta
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday, \
    USMartinLutherKingJr, USPresidentsDay, USMemorialDay, USLaborDay, USThanksgivingDay, GoodFriday
from pandas.tseries.offsets import CustomBusinessDay

logger = logging.getLogger(__name__)

try:
    import tensorflow as tf
    from tensorflow.keras.models import Sequential, load_model
    from tensorflow.keras.layers import LSTM, Dense, Dropout, Input
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

class MLService:
    def __init__(self, tradier_service):
        self.tradier = tradier_service # Keep for compatibility/fallback if needed
        self.db = Container.get_db()
        self.model_dir = "models"
        if not os.path.exists(self.model_dir):
            os.makedirs(self.model_dir)
        
        self.sequence_length = 60 # Lookback for LSTM
        self.default_features = ['close', 'volume', 'rsi', 'upper_bb', 'lower_bb', 'mid_bb', 'macd', 'macd_signal', 'atr', 'sma_50', 'obv', 'vwap']

    def _validate_symbol(self, symbol):
        """
        Validates and sanitizes a ticker symbol.
        Ensures symbol is a string and matches expected format to prevent NoSQL injection.
        """
        if not isinstance(symbol, str):
            raise ValidationError(f"Symbol must be a string, got {type(symbol).__name__}")

        symbol = symbol.strip().upper()
        if not re.match(r'^[A-Z0-9\-\.]+$', symbol):
            raise ValidationError(f"Invalid symbol format: {symbol}")

        return symbol

    def _get_feature_file_path(self, symbol):
        return os.path.join(self.model_dir, f"{symbol}_features.json")

    def backfill_symbol(self, symbol, years=5):
        """
        Backfill historical data for a symbol.
        """
        symbol = self._validate_symbol(symbol)
        if self.db is None:
            logger.error("DB unavailable for backfill.")
            return False

        logger.info(f"Backfilling history for {symbol} ({years} years)...")
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=365 * years)).strftime('%Y-%m-%d')
        
        try:
            history = self.tradier.get_historical_pricing(symbol, start_date, end_date)
        except Exception as e:
            print(f"Error fetching history for {symbol}: {e}")
            return False
        
        if not history:
            logger.warning("No data returned from Tradier.")
            return False

        logger.info(f"Retrieved {len(history)} records. Saving to MongoDB...")
        
        collection = self.db['market_data']
        
        operations = []
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
            operations.append(
                UpdateOne(
                    {"symbol": symbol, "date": record['date']},
                    {"$set": doc},
                    upsert=True
                )
            )

        count = 0
        if operations:
            result = collection.bulk_write(operations)
            count = result.upserted_count + result.modified_count

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
        # Ensure high/low/volume/open are floats
        if 'open' in df.columns: df['open'] = df['open'].astype(float)
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
        
        if 'high' in df.columns and 'low' in df.columns:
            df['vwap'] = calculate_vwap(df['high'], df['low'], df['close'], df['volume'], window=14)
            df['atr'] = calculate_atr(df['high'], df['low'], df['close'])
        else:
            df['vwap'] = 0.0
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
        symbol = self._validate_symbol(symbol)
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
        symbol = self._validate_symbol(symbol)
        
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
        if model_type in ['lstm', 'rl']:
            mse = self._run_training_worker(symbol, model_type, df, top_features)
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


    def _get_lstm_worker_script(self):
        return """
import os
import sys
import pandas as pd
import numpy as np
import joblib
import logging
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import mean_squared_error

# Setup simple logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LSTM_Worker")

# Add project root to path for imports if needed
sys.path.append(os.getcwd())

try:
    symbol = sys.argv[1]
    model_dir = sys.argv[2]
    df_path = sys.argv[3]
    features_path = sys.argv[4]
    
    # Load data
    df = joblib.load(df_path)
    top_features = joblib.load(features_path)
    
    # LSTM Prep
    df_lstm = df.copy()
    df_lstm['target'] = df_lstm['log_return']
    
    # Minimal version of _prepare_lstm_data logic
    seq_len = 60
    scaled_data = MinMaxScaler(feature_range=(0,1)).fit_transform(df_lstm[top_features])
    scaler = MinMaxScaler(feature_range=(0,1))
    scaled_data = scaler.fit_transform(df_lstm[top_features])
    
    X, y = [], []
    target = df_lstm['target'].values
    for i in range(seq_len, len(scaled_data)):
        X.append(scaled_data[i-seq_len:i])
        y.append(target[i])
    X, y = np.array(X), np.array(y)
    
    # Scale Target
    y = y.reshape(-1, 1)
    target_scaler = MinMaxScaler(feature_range=(0, 1))
    y_scaled = target_scaler.fit_transform(y)
    
    # Split
    split = int(len(X) * 0.8)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y_scaled[:split], y_scaled[split:]
    
    # Build
    model = Sequential([
        LSTM(units=50, return_sequences=True, input_shape=(X_train.shape[1], X_train.shape[2])),
        Dropout(0.2),
        LSTM(units=50, return_sequences=False),
        Dropout(0.2),
        Dense(units=1)
    ])
    model.compile(optimizer='adam', loss='mean_squared_error')
    
    early_stopping = EarlyStopping(monitor='val_loss', patience=3, restore_best_weights=True)
    model.fit(X_train, y_train, batch_size=64, epochs=10, validation_data=(X_test, y_test), 
              callbacks=[early_stopping], verbose=0)
    
    # Save
    os.makedirs(model_dir, exist_ok=True)
    model.save(os.path.join(model_dir, f"{symbol}_lstm.h5"))
    joblib.dump(scaler, os.path.join(model_dir, f"{symbol}_lstm_scaler.pkl"))
    joblib.dump(target_scaler, os.path.join(model_dir, f"{symbol}_lstm_target_scaler.pkl"))
    
    logger.info(f"LSTM training for {symbol} completed successfully")
except Exception as e:
    logger.error(f"LSTM training failed: {e}")
    sys.exit(1)
"""

    def _get_rl_worker_script(self):
        return """
import os
import sys
import pandas as pd
import joblib
import logging

# Setup simple logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RL_Worker")

# Add project root to path
sys.path.append(os.getcwd())
from services.rl_price_predictor import RLPricePredictor

try:
    symbol = sys.argv[1]
    model_dir = sys.argv[2]
    df_path = sys.argv[3]
    features_path = sys.argv[4]
    
    # Load data
    df = joblib.load(df_path)
    top_features = joblib.load(features_path)
    
    predictor = RLPricePredictor(symbol, df, top_features, model_dir)
    predictor.train(timesteps=10_000)
    logger.info(f"RL model training for {symbol} completed successfully")
except Exception as e:
    logger.error(f"RL training subprocess failed: {e}")
    sys.exit(1)
"""


    def _run_training_worker(self, symbol, model_type, df, top_features):
        import subprocess
        import sys
        import tempfile
        import joblib
        import os
        
        tmp_dir = tempfile.gettempdir()
        
        df_path = os.path.join(tmp_dir, f'ml_tmp_df_{symbol}_{model_type}.pkl')
        features_path = os.path.join(tmp_dir, f'ml_tmp_features_{symbol}_{model_type}.pkl')
        worker_path = os.path.join(tmp_dir, f'ml_train_worker_{symbol}_{model_type}.py')

        try:
            if model_type == 'lstm':
                train_script = self._get_lstm_worker_script()
            elif model_type == 'rl':
                train_script = self._get_rl_worker_script()
            else:
                raise ValueError(f"Unknown model_type for subprocess: {model_type}")

            # Save temporary data and script
            joblib.dump(df.dropna(subset=top_features + ['close']), df_path)
            joblib.dump(top_features, features_path)

            with open(worker_path, 'w') as f:
                f.write(train_script)

            try:
                # Run the subprocess with a 30-minute timeout for safety
                result = subprocess.run(
                    [sys.executable, worker_path, symbol, self.model_dir, df_path, features_path],
                    capture_output=True, text=True, timeout=1800
                )
                if result.returncode != 0:
                    logger.error(f"{model_type.upper()} Subprocess failed: {result.stderr}")
                    raise AppError(f"{model_type.upper()} Training failed in subprocess: {result.stderr}", 500)

                logger.info(f"{model_type.upper()} model trained via subprocess")
                return 0.0 # mse
            except subprocess.TimeoutExpired:
                logger.error(f"{model_type.upper()} Training subprocess for {symbol} timed out after 30 minutes.")
                raise AppError(f"{model_type.upper()} Training for {symbol} timed out", 504)
            finally:
                # Cleanup
                for f in [df_path, features_path, worker_path]:
                    if os.path.exists(f): os.remove(f)

        except Exception as e:
            if not isinstance(e, AppError):
                logger.error(f"Error orchestrating {model_type} training: {e}", exc_info=True)
                raise AppError(f"Training orchestration error: {e}", 500)
            raise e

    def predict_next_day(self, symbol, model_type='lstm'):
        symbol = self._validate_symbol(symbol)
            
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

        # Handle NaN values in feature columns for the last row.
        # Forward-fill first (uses most recent valid value), then check again.
        if df[features].iloc[-1].isna().any():
            nan_cols = [f for f in features if pd.isna(df[f].iloc[-1])]
            logger.warning(f"NaN detected in last row for {symbol}, columns: {nan_cols}. Attempting forward-fill...")
            df[features] = df[features].ffill()

            # If still NaN after ffill (e.g. column was entirely NaN), fall back to clean rows
            if df[features].iloc[-1].isna().any():
                still_nan = [f for f in features if pd.isna(df[f].iloc[-1])]
                logger.warning(f"Forward-fill insufficient for {symbol}, still NaN: {still_nan}. Falling back to last clean row.")
                df_clean = df.dropna(subset=features)
                if df_clean.empty or len(df_clean) < self.sequence_length:
                    raise ValidationError(f"Latest data has NaNs in required features ({still_nan}) and not enough clean data to proceed.")
                # Replace the tail of df with clean data for prediction
                df = df_clean

        last_row = df.iloc[-1]

        prediction = 0
        
        if model_type == 'lstm':
            if not HAS_TENSORFLOW:
                raise AppError("LSTM prediction requires TensorFlow, but it is not installed.", 501)
            model_path = f"{self.model_dir}/{symbol}_lstm.h5"
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
            
            # Use explicit local tmp directory
            tmp_dir = tempfile.gettempdir()
            
            df_path = os.path.join(tmp_dir, f'predict_tmp_df_{symbol}.pkl')
            features_path = os.path.join(tmp_dir, f'predict_tmp_features_{symbol}.pkl')
            worker_path = os.path.join(tmp_dir, f'rl_predict_worker_{symbol}.py')
            result_path = os.path.join(tmp_dir, f'rl_predict_result_{symbol}.json')

            predict_script = """
import pandas as pd
import joblib
import os
import sys
import json
import logging

sys.path.append(os.getcwd())
from services.rl_price_predictor import RLPricePredictor

logging.basicConfig(level=logging.ERROR)

try:
    symbol = sys.argv[1]
    model_dir = sys.argv[2]
    df_path = sys.argv[3]
    features_path = sys.argv[4]
    result_path = sys.argv[5]

    df = joblib.load(df_path)
    features = joblib.load(features_path)
    
    predictor = RLPricePredictor(symbol, df, features, model_dir)
    prediction = predictor.predict(df)
    
    with open(result_path, 'w') as fh:
        json.dump({"prediction": prediction}, fh)
except Exception as e:
    import sys
    print(str(e), file=sys.stderr)
    exit(1)
"""
            # Need to actually dump the data into the pkl files for the subprocess BEFORE running it!
            joblib.dump(df.dropna(subset=features + ['close']), df_path)
            joblib.dump(features, features_path)
            with open(worker_path, 'w') as f:
                f.write(predict_script)
            
            try:
                result = subprocess.run([sys.executable, worker_path, symbol, self.model_dir, df_path, features_path, result_path], capture_output=True, text=True)
                if result.returncode != 0:
                    error_msg = result.stderr.strip()
                    if "not found" in error_msg.lower():
                        raise ResourceNotFoundError(f"RL model for {symbol} not found.")
                    logger.error(f"RL Predict Subprocess failed: {error_msg}")
                    raise AppError(f"RL Prediction failed in subprocess: {error_msg}", 500)
                
                with open(result_path, 'r') as fh:
                    output = json.load(fh)
                prediction = float(output['prediction'])
            finally:
                for f in [df_path, features_path, worker_path, result_path]:
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
            symbol = self._validate_symbol(symbol)
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
            
            # Identify missing actuals
            missing_actuals = set()
            for record in history:
                if record.get('actual_close_price') is None:
                    pred_date = record.get('prediction_date')
                    rec_symbol = record.get('symbol')
                    if pred_date and rec_symbol:
                        missing_actuals.add((rec_symbol, pred_date))

            if missing_actuals:
                # Query all missing market data in one go
                query_conds = [{"symbol": sym, "date": d} for sym, d in missing_actuals]
                market_docs = list(self.db['market_data'].find({"$or": query_conds}))

                # Build lookup map
                market_map = { (doc["symbol"], doc["date"]): float(doc["close"])
                               for doc in market_docs if "close" in doc }

                # Update memory and prepare batch update
                bulk_ops = []
                for record in history:
                    if record.get('actual_close_price') is None:
                        pred_date = record.get('prediction_date')
                        rec_symbol = record.get('symbol')
                        if pred_date and rec_symbol and (rec_symbol, pred_date) in market_map:
                            actual_price = market_map[(rec_symbol, pred_date)]
                            record['actual_close_price'] = actual_price
                            
                            bulk_ops.append(
                                UpdateOne(
                                    {
                                        "symbol": rec_symbol,
                                        "prediction_date": pred_date,
                                        "model_type": record.get('model_type')
                                    },
                                    {"$set": {"actual_close_price": actual_price}}
                                )
                            )
                            
                # Execute bulk update
                if bulk_ops:
                    self.db['predictions'].bulk_write(bulk_ops)
            
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
                    operations = []
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
                         operations.append(
                            UpdateOne(
                                {"symbol": symbol, "date": record['date']},
                                {"$set": doc},
                                upsert=True
                            )
                        )
                    if operations:
                        collection.bulk_write(operations)
                    updated_symbols.append(symbol)
            except Exception as e:
                print(f"Error refreshing {symbol}: {e}")
                
        # Now update the predictions
        # We can just re-run the lazy update logic essentially, but explicitly
        # Re-fetch pending to be safe or iterate pending list
        
        prediction_updates = []

        # Optimize with bulk fetch instead of N+1 find_one
        query_conds = []
        for p in pending:
            query_conds.append({"symbol": p['symbol'], "date": p['prediction_date']})

        market_map = {}
        if query_conds:
            market_docs = list(self.db['market_data'].find({"$or": query_conds}))
            for doc in market_docs:
                if 'close' in doc:
                    market_map[(doc['symbol'], doc['date'])] = float(doc['close'])

        for p in pending:
            symbol = p['symbol']
            p_date = p['prediction_date']
            
            if (symbol, p_date) in market_map:
                actual = market_map[(symbol, p_date)]
                prediction_updates.append(
                    UpdateOne(
                        {"_id": p['_id']},
                        {"$set": {"actual_close_price": actual}}
                    )
                )
                updated_count += 1
                
        if prediction_updates:
            self.db['predictions'].bulk_write(prediction_updates, ordered=False)

        return {
            "message": f"Refreshed {len(updated_symbols)} symbols. Updated {updated_count} prediction records.",
            "updated_count": updated_count
        }

    def evaluate_model(self, symbol, days=60, model_type='lstm'):
        try:
            symbol = self._validate_symbol(symbol)
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

            # Safety: Ensure no target leakage columns are in features list
            forbidden = ['target', 'target_return', 'log_return', 'date', 'symbol']
            features = [f for f in features if f not in forbidden]

            if model_type == 'lstm':
                if not HAS_TENSORFLOW:
                    raise AppError("LSTM evaluation requires TensorFlow, but it is not installed.", 501)
                model_path = f"{self.model_dir}/{symbol}_lstm.h5"
                if not os.path.exists(model_path): raise ResourceNotFoundError("Model not found")
                model = load_model(model_path)
                scaler = joblib.load(f"{self.model_dir}/{symbol}_lstm_scaler.pkl")
            
                # Prepare data (Need Log Return logic similar to training)
                # Ensure target is set correctly for prepare_lstm_data
                df['log_return'] = np.log(df['target'] / df['close'])
                df['target_for_lstm'] = df['log_return']
            
                # Save original target price for validation
                # We want actual future price (Close T+1) to compare with predicted price
                df['actual_future_price'] = df['target']
            
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
                import subprocess
                import sys
                import tempfile
            
                recent_df = df.dropna(subset=features + ['close']).copy()
                if recent_df.empty: raise ValidationError("Not enough clean data for RL evaluation.")
            
                if 'actual_future_price' not in recent_df.columns:
                    recent_df['actual_future_price'] = recent_df['target']
                
                tmp_dir = tempfile.gettempdir()
                df_path = os.path.join(tmp_dir, f'eval_tmp_df_{symbol}.pkl')
                features_path = os.path.join(tmp_dir, f'eval_tmp_features_{symbol}.pkl')
                worker_path = os.path.join(tmp_dir, f'rl_eval_worker_{symbol}.py')
                result_path = os.path.join(tmp_dir, f'rl_eval_result_{symbol}.json')

                eval_script = """
import pandas as pd
import joblib
import os
import sys
import json
import logging

sys.path.append(os.getcwd())
from services.rl_price_predictor import RLPricePredictor

try:
        df_path = sys.argv[1]
        features_path = sys.argv[2]
        symbol = sys.argv[3]
        model_dir = sys.argv[4]
        days = int(sys.argv[5])
        result_path = sys.argv[6]

        recent_df = joblib.load(df_path)
        features = joblib.load(features_path)
    
        predictor = RLPricePredictor(symbol, recent_df, features, model_dir)
    
        predictions = []
        actuals = []
        dates = []
        close_list = []
    
        eval_start_idx = len(recent_df) - days
        for i in range(max(0, eval_start_idx), len(recent_df)):
            basis_df = recent_df.iloc[:i+1]
            try:
                pred_price = predictor.predict(basis_df)
            except Exception as pred_e:
                import logging
                logging.error("Prediction failed for index " + str(i) + ": " + str(pred_e))
                continue
            
            predictions.append(pred_price)
            actual_price = recent_df.iloc[i]['actual_future_price']
            if pd.isna(actual_price):
                 actual_price = recent_df.iloc[i]['close']
            actuals.append(float(actual_price))
            dates.append(recent_df.iloc[i]['date'].strftime('%Y-%m-%d'))
            close_list.append(float(recent_df.iloc[i]['close']))
        
        if not predictions:
            raise ValueError("RL Predictor failed to generate any predictions. Check observation space shapes.")
        
        with open(result_path, 'w') as fh:
            json.dump({"predictions": predictions, "actuals": actuals, "dates": dates, "close_list": close_list}, fh)
except Exception as e:
        print(str(e), file=sys.stderr)
        exit(1)
"""
                joblib.dump(recent_df, df_path)
                joblib.dump(features, features_path)
            
                with open(worker_path, 'w') as f:
                    f.write(eval_script)
                
                try:
                    result = subprocess.run(
                        [sys.executable, worker_path, df_path, features_path, symbol, self.model_dir, str(days), result_path],
                        capture_output=True,
                        text=True
                    )
                    if result.returncode != 0:
                        error_msg = result.stderr.strip()
                        raise AppError(f"RL Evaluation failed in subprocess: {error_msg}", 500)
                
                    with open(result_path, 'r') as fh:
                        output = json.load(fh)
                    predictions = output['predictions']
                    actuals = output['actuals']
                    dates = output['dates']
                    close_list = output['close_list']
                finally:
                    for f in [df_path, features_path, worker_path, result_path]:
                        if os.path.exists(f): os.remove(f)
                    
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

            mse = np.mean(squared_errors) if squared_errors else 0.0
            mae = np.mean(np.abs([p['actual'] - p['predicted'] for p in results])) if results else 0.0
            acc = (correct_direction / len(predictions) * 100) if predictions else 0.0
        
            return {
                "symbol": symbol,
                "mse": round(mse, 4),
                "mae": round(mae, 4),
                "accuracy": round(acc, 2),
                "predictions": results
            }

        except Exception as e:
            return {"error": str(e)}

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
