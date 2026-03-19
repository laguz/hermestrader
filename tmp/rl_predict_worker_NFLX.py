
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
    df = joblib.load(r'/Users/laguz/Git/LaguzTechInvestment/tmp/predict_tmp_df_NFLX.pkl')
    features = joblib.load(r'/Users/laguz/Git/LaguzTechInvestment/tmp/predict_tmp_features_NFLX.pkl')
    
    predictor = RLPricePredictor('NFLX', df, features, 'models')
    prediction = predictor.predict(df)
    
    with open(r'/Users/laguz/Git/LaguzTechInvestment/tmp/rl_predict_result_NFLX.json', 'w') as fh:
        json.dump({"prediction": prediction}, fh)
except Exception as e:
    import sys
    print(str(e), file=sys.stderr)
    exit(1)
