
import pandas as pd
import joblib
import os
import json
from services.rl_price_predictor import RLPricePredictor
import logging

logging.basicConfig(level=logging.ERROR)

try:
    df = joblib.load('predict_tmp_df.pkl')
    features = joblib.load('predict_tmp_features.pkl')
    
    predictor = RLPricePredictor('NFLX', df, features, 'models')
    prediction = predictor.predict(df)
    # Output only the prediction value
    print(prediction)
except Exception as e:
    # Print error but only to stderr
    import sys
    print(str(e), file=sys.stderr)
    exit(1)
