import sys
import os
sys.path.append(os.getcwd())

from services.container import Container
from services.ml_service import MLService
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from tensorflow.keras.models import load_model
import joblib

load_dotenv()

def debug_lstm_predictions(symbol='SPY'):
    service = MLService(Container.get_tradier_service())
    
    print(f"Loading data for {symbol}...")
    df = pd.DataFrame(list(service.db['market_data'].find({"symbol": symbol}).sort("date", 1)))
    
    if df.empty:
        print("No data found.")
        return
        
    df['date'] = pd.to_datetime(df['date'])
    df = service.prepare_features(df)
    
    # Load model and scaler
    model_path = f"models/{symbol}_lstm.keras"
    scaler_path = f"models/{symbol}_lstm_scaler.pkl"
    
    if not os.path.exists(model_path):
        print("Model not found.")
        return
        
    model = load_model(model_path)
    scaler = joblib.load(scaler_path)
    
    # Load features
    import json
    with open(f"models/{symbol}_features.json", 'r') as f:
        features = json.load(f)
        
    print(f"Features: {features}")
    
    # Try load target scaler
    target_scaler = None
    target_scaler_path = f"models/{symbol}_lstm_target_scaler.pkl"
    if os.path.exists(target_scaler_path):
        target_scaler = joblib.load(target_scaler_path)
        print("Target scaler loaded.")
    
    # Prepare last 10 sequences
    sequences = []
    
    # We need sequence_length + 10 days
    df_slice = df.dropna().tail(service.sequence_length + 10)
    
    print(f"Inspecting last 5 predictions...")
    
    data = df_slice[features].values
    scaled_data = scaler.transform(data)
    
    for i in range(1, 6):
        # Taking a window
        seq = scaled_data[-service.sequence_length-i : -i]
        if len(seq) != service.sequence_length:
            continue
            
        pred_scaled = model.predict(np.array([seq]), verbose=0)
        raw_val = pred_scaled[0][0]
        
        if target_scaler:
            real_val = target_scaler.inverse_transform(pred_scaled)[0][0]
            print(f"Time -{i}: Prediction: {real_val:.2f} (Scaled: {raw_val:.4f})")
        else:
            print(f"Time -{i}: Prediction (Raw): {raw_val}")

if __name__ == "__main__":
    debug_lstm_predictions()
