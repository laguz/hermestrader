import os
import sys
import pandas as pd
import json

# Ensure project root is in path
sys.path.append(os.getcwd())

from services.ml_service import MLService
from services.tradier_service import TradierService
from services.container import Container

container = Container()
ml_service = container.get_ml_service()

# Run predict to see raw output values 
try:
    print("Evaluating RIOT...")
    eval_result = ml_service.evaluate_model("RIOT", days=5, model_type="rl")
    
    print("\nEvaluation Data:")
    for row in eval_result['predictions']:
        print(row)
        
    print(f"\nOverall MSE: {eval_result['mse']}")
    
except Exception as e:
    import traceback
    traceback.print_exc()

