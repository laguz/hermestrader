import sys
import os

# Add project root
sys.path.append(os.getcwd())

from dotenv import load_dotenv
load_dotenv()

from services.tradier_service import TradierService
from services.ml_service import MLService

def verify_lstm():
    print("Verifying LSTM Integration...")
    
    tradier = TradierService()
    ml = MLService(tradier)
    symbol = "SPY"
    
    print(f"1. Training LSTM for {symbol}...")
    try:
        train_result = ml.train_model(symbol, model_type='lstm')
        if "error" in train_result:
            print(f"Train Error: {train_result['error']}")
            return
        print(f"Training Success: MSE={train_result['mse']}")
    except Exception as e:
        print(f"Training Exception: {e}")
        import traceback
        traceback.print_exc()
        return

    print(f"2. Predicting next day with LSTM...")
    try:
        pred_result = ml.predict_next_day(symbol, model_type='lstm')
        if "error" in pred_result:
            print(f"Predict Error: {pred_result['error']}")
            return
        print(f"Prediction Success: {pred_result['predicted_price']} (Change: {pred_result['percent_change_str']})")
    except Exception as e:
        print(f"Prediction Exception: {e}")
        return

    print("LSTM Verification Complete!")

if __name__ == "__main__":
    verify_lstm()
