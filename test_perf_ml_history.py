import time
from services.ml_service import MLService
from services.container import Container

class MockDB:
    def __init__(self):
        self.predictions = MockPredictions()
        self.market_data = MockMarketData()
    def __getitem__(self, item):
        if item == 'predictions': return self.predictions
        if item == 'market_data': return self.market_data

class MockPredictions:
    def find(self, *args, **kwargs):
        class Cursor:
            def sort(self, *args, **kwargs): return self
            def limit(self, *args, **kwargs): return self
            def __iter__(self):
                for i in range(100):
                    yield {"symbol": f"SYM{i}", "prediction_date": f"2023-10-01", "model_type": "LSTM"}
        return Cursor()
    def bulk_write(self, *args, **kwargs):
        time.sleep(0.01) # a bulk write takes a little longer than a single write but much less than 100 single writes
    def update_one(self, *args, **kwargs):
        time.sleep(0.001)

class MockMarketData:
    def find(self, *args, **kwargs):
        # find takes an array of arguments, returns a cursor
        class Cursor:
            def __iter__(self):
                for i in range(100):
                    yield {"symbol": f"SYM{i}", "date": f"2023-10-01", "close": "100.0"}
        time.sleep(0.01)
        return Cursor()
    def find_one(self, *args, **kwargs):
        time.sleep(0.001)
        return {"symbol": args[0]["symbol"], "date": args[0]["date"], "close": "100.0"}


class MockTradier:
    pass

def run_benchmark():
    Container.get_db = lambda: MockDB()
    ml = MLService(MockTradier())

    start = time.time()
    ml.get_prediction_history(limit=100)
    end = time.time()
    print(f"Time taken to get_prediction_history with 100 entries: {end - start:.4f}s")

if __name__ == "__main__":
    run_benchmark()
