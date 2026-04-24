
import joblib
import os
import sys
import logging

# Ensure project root is in path
sys.path.append(os.getcwd())

from services.rl_price_predictor import RLPricePredictor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('rl_train_subprocess')

try:
    df = joblib.load(r'/Users/laguz/Git/LaguzTechInvestment/tmp/ml_tmp_df_TSLA.pkl')
    top_features = joblib.load(r'/Users/laguz/Git/LaguzTechInvestment/tmp/ml_tmp_features_TSLA.pkl')
    
    predictor = RLPricePredictor('TSLA', df, top_features, 'models')
    predictor.train(timesteps=10_000)
    logger.info("RL model training subprocess completed successfully")
except Exception as e:
    logger.error(f"RL training subprocess failed: {e}")
    exit(1)
