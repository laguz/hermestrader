import sys
import os
sys.path.append(os.getcwd())

from app import app

def verify_evaluation():
    with app.app_context():
        client = app.test_client()
        
        print("Testing Evaluation for SPY (RF)...")
        resp = client.post('/api/evaluate', json={"symbol": "SPY", "model_type": "rf"})
        print(f"Status: {resp.status_code}")
        if resp.status_code != 200:
            print(f"Error Body: {resp.get_data(as_text=True)}")
        else:
            data = resp.get_json()
            if 'mae' in data:
                print(f"Success RF! MAE: {data['mae']}")
            else:
                print("Error: MAE missing in response.")

        print("\nTesting Evaluation for SPY (LSTM)...")
        resp = client.post('/api/evaluate', json={"symbol": "SPY", "model_type": "lstm"})
        print(f"Status: {resp.status_code}")
        if resp.status_code != 200:
            print(f"Error Body: {resp.get_data(as_text=True)}")
        else:
            print("Success LSTM!")

if __name__ == "__main__":
    verify_evaluation()
