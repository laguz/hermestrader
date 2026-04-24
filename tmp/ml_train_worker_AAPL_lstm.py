
import os
import sys
import numpy as np
import joblib
import logging
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping
from sklearn.preprocessing import MinMaxScaler

# Setup simple logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("LSTM_Worker")

# Add project root to path for imports if needed
sys.path.append(os.getcwd())

try:
    symbol = 'AAPL'
    model_dir = 'models'
    
    # Load data
    df = joblib.load(r'/Users/laguz/Git/LaguzTechInvestment/tmp/ml_tmp_df_AAPL_lstm.pkl')
    top_features = joblib.load(r'/Users/laguz/Git/LaguzTechInvestment/tmp/ml_tmp_features_AAPL_lstm.pkl')
    
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
    model.save(f"{model_dir}/{symbol}_lstm.h5")
    joblib.dump(scaler, f"{model_dir}/{symbol}_lstm_scaler.pkl")
    joblib.dump(target_scaler, f"{model_dir}/{symbol}_lstm_target_scaler.pkl")
    
    logger.info(f"LSTM training for {symbol} completed successfully")
except Exception as e:
    logger.error(f"LSTM training failed: {e}")
    sys.exit(1)
