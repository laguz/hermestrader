import sys
import os
sys.path.append(os.getcwd())

from services.container import Container
from services.ml_service import MLService
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import joblib

load_dotenv()

def debug_rf_predictions(symbol='SPY'):
    service = MLService(Container.get_tradier_service())
    
    print(f"Loading data for {symbol}...")
    df = pd.DataFrame(list(service.db['market_data'].find({"symbol": symbol}).sort("date", 1)))
    
    if df.empty:
        print("No data found.")
        return
        
    df['date'] = pd.to_datetime(df['date'])
    df = service.prepare_features(df)
    
    # Load model
    model_path = f"models/{symbol}_rf.pkl"
    
    if not os.path.exists(model_path):
        print("Model not found.")
        return
        
    model = joblib.load(model_path)
    
    # Load features
    import json
    with open(f"models/{symbol}_features.json", 'r') as f:
        features = json.load(f)
        
    print(f"Features: {features}")
    
    # Check max Close in first 80% (Training Set assumption)
    n = len(df)
    train_size = int(n * 0.8)
    train_df = df.iloc[:train_size]
    test_df = df.iloc[train_size:]
    
    max_train_close = train_df['close'].max()
    print(f"Max Training Close: {max_train_close}")
    print(f"Test Set Range: {test_df['close'].min()} - {test_df['close'].max()}")
    
    # Inspect last 10 evaluations
    print(f"\nInspecting last 10 evaluations...")
    eval_df = df.tail(10).copy()
    X = eval_df[features]
    predictions = model.predict(X)
    
    for i in range(10):
        actual = eval_df.iloc[i]['close']
        raw_pred = predictions[i]
        date = eval_df.iloc[i]['date']
        
        # Heuristic: if prediction is small (< 1), assume it's a return
        if abs(raw_pred) < 2.0: 
            # Reconstruct: close * (1 + return)
            # BUT wait, the target was RETURN for NEXT day.
            # So if we predict using features at T, we get Return T->T+1.
            # The Price at T+1 = Price T * (1 + pred_return)
            
            # Here we are comparing against 'actual' which is Close at T. 
            # We are verifying dynamic behavior, so precise alignment isn't critical 
            # as long as we see fluctuation.
            
            reconstructed_price = actual * (1 + raw_pred)
            print(f"Date: {date.strftime('%Y-%m-%d')} | Close: {actual:.2f} | Pred(Return): {raw_pred:.4f} -> Price: {reconstructed_price:.2f}")
        else:
            print(f"Date: {date.strftime('%Y-%m-%d')} | Close: {actual:.2f} | Pred(Price): {raw_pred:.2f}")

if __name__ == "__main__":
    debug_rf_predictions()
