import sys
import pandas as pd
from services.ml_service import MLService
from services.container import Container

class MockTradier:
    def get_historical_pricing(self, symbol, start, end): return []
Container.get_tradier_service = lambda: MockTradier()

ml = MLService(MockTradier())
# fetch enough data
df = pd.DataFrame()
try:
    res = ml.evaluate_model('TSLA', days=5, model_type='rl')
    print("SUCCESS", res)
except Exception as e:
    print("ERROR", e)
