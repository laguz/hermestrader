
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
    symbol = 'NFLX'
    model_dir = 'models'
    
    # Load data
    df = joblib.load(r'/Users/laguz/Git/LaguzTechInvestment/tmp/ml_tmp_df_NFLX_rl.pkl')
    top_features = joblib.load(r'/Users/laguz/Git/LaguzTechInvestment/tmp/ml_tmp_features_NFLX_rl.pkl')
    
    predictor = RLPricePredictor(symbol, df, top_features, model_dir)
    predictor.train(timesteps=10_000)
    logger.info(f"RL model training for {symbol} completed successfully")
except Exception as e:
    logger.error(f"RL training subprocess failed: {e}")
    sys.exit(1)
