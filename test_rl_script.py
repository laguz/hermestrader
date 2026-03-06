import os
import sys
import pandas as pd
import json

# mock data
dates = pd.date_range('2023-01-01', periods=10).strftime('%Y-%m-%d')
actuals = [100, 105, 110, 115, 120, 125, 130, 135, 140, 145]
predictions = [101, 106, 111, 116, 121, 123, 132, 137, 142, 147]
close_list = [95, 100, 105, 110, 115, 120, 125, 130, 135, 140]

import numpy as np

results = []
squared_errors = []
correct_direction = 0

for i in range(len(predictions)):
    actual = actuals[i]
    predicted = predictions[i]
    current_close = close_list[i]
    
    err = actual - predicted
    squared_errors.append(err ** 2)
    
    actual_move = actual - current_close
    predicted_move = predicted - current_close
    
    if (actual_move * predicted_move) > 0:
        correct_direction += 1
        
    results.append({
        "date": dates[i],
        "actual": round(actual, 2),
        "predicted": round(predicted, 2),
        "error": round(err, 2)
    })

mse = np.mean(squared_errors) if squared_errors else 0.0
mae = np.mean(np.abs([p['actual'] - p['predicted'] for p in results])) if results else 0.0
acc = (correct_direction / len(predictions) * 100) if predictions else 0.0

print(f"MSE: {mse}, MAE: {mae}, ACC: {acc}")
