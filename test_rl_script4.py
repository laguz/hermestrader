import os
import sys

# Ensure project root is in path
sys.path.append(os.getcwd())

from services.rl_price_predictor import RLPricePredictor
from services.container import Container

container = Container()
ml_service = container.get_ml_service()

symbol = "RIOT"
days = 5
model_type = "rl"
df = ml_service._fetch_and_prepare_training_data(symbol)
top_features = ml_service.default_features

recent_df = df.dropna(subset=top_features + ['close']).copy()
if 'actual_future_price' not in recent_df.columns:
    recent_df['actual_future_price'] = recent_df['target']

predictor = RLPricePredictor(symbol, recent_df, top_features, ml_service.model_dir)

predictions = []
actuals = []

eval_start_idx = len(recent_df) - days
for i in range(max(0, eval_start_idx), len(recent_df)):
    basis_df = recent_df.iloc[:i+1]
    try:
        pred_price = predictor.predict(basis_df)
        predictions.append(pred_price)
        actuals.append(recent_df.iloc[i]['actual_future_price'])
    except Exception as e:
        print(f"FAILED AT {i}: {e}")
        
print("PREDICTIONS:", predictions)
print("ACTUALS:", actuals)

