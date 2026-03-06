import os
import sys
import pandas as pd
import json

# Ensure project root is in path
sys.path.append(os.getcwd())

from services.ml_service import MLService
from services.container import Container

container = Container()
ml_service = container.get_ml_service()

symbol = "RIOT"
days = 5
model_type = "rl"
df = ml_service._fetch_and_prepare_training_data(symbol)
top_features = ml_service.default_features

recent_df = df.dropna(subset=top_features + ['close']).copy()
print(f"Total rows in recent_df: {len(recent_df)}")

eval_start_idx = len(recent_df) - days
print(f"Eval start index: {eval_start_idx}")

range_list = list(range(max(0, eval_start_idx), len(recent_df)))
print(f"Loop range: {range_list}")

