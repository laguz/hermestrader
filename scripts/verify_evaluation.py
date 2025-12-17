import requests
import os
import sys

# Add project root to sys.path
sys.path.append(os.getcwd())

from dotenv import load_dotenv
load_dotenv()

from services.tradier_service import TradierService
from services.ml_service import MLService

def test_evaluation():
    print("Testing MLService.evaluate_model()...")
    
    # 1. Initialize Services
    tradier = TradierService()
    ml = MLService(tradier)
    
    # 2. Check for Existing Model (or skip if none)
    symbol = "SPY"
    model_path = f"models/{symbol}_rf.pkl"
    if not os.path.exists(model_path):
        print(f"Model for {symbol} not found. Ensure it is trained first via UI or train_model().")
        # Attempt to train?
        # print("Attempting to train...")
        # ml.train_model(symbol)
        return

    # 3. Run Evaluation
    try:
        result = ml.evaluate_model(symbol, days=30)
        
        if "error" in result:
             print(f"Evaluation returned error: {result['error']}")
        else:
             print("Evaluation Successful!")
             print(f"Symbol: {result['symbol']}")
             print(f"MSE: {result['mse']}")
             print(f"MAE: {result['mae']}")
             print(f"Accuracy: {result['accuracy']}%")
             print(f"Predictions returned: {len(result['predictions'])}")
             if len(result['predictions']) > 0:
                 print(f"Sample prediction: {result['predictions'][0]}")

    except Exception as e:
        print(f"Evaluation failed with exception: {e}")

if __name__ == "__main__":
    test_evaluation()
