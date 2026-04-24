import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.tradier_service import TradierService
from services.ml_service import MLService
from dotenv import load_dotenv

def main():
    try:
        import sklearn
        print(f"✅ Scikit-learn {sklearn.__version__} installed.")
    except ImportError as e:
        print(f"❌ Dependency Error: {e}")
        return

    load_dotenv()
    
    # Check Tradier connection first
    tradier = TradierService()
    profile = tradier.get_user_profile()
    if not profile:
       print("⚠️ Tradier connection failed (check env vars). Skipping training test.")
       # Only warn, as user might not have API key set up in this session context
    
    print("Testing ML Service instantiation...")
    ml_service = MLService(tradier)
    print("✅ ML Service instantiated.")

    # We won't run full training as it takes too long and uses tokens.
    # But we can check if the model directory was created.
    if os.path.exists("models"):
        print("✅ Models directory exists.")
    else:
        print("❌ Models directory missing.")

if __name__ == "__main__":
    main()
