import pandas as pd
import numpy as np
from services.rl_price_predictor import MarketEnv, RLPricePredictor

def test_market_env_step():
    # Create a tiny synthetic dataset
    data = {
        "close": [100, 101, 102, 103],
        "rsi": [30, 35, 40, 45],
        "atr": [1, 1.2, 1.1, 1.3],
    }
    df = pd.DataFrame(data)
    env = MarketEnv(df, feature_cols=["rsi", "atr"], target_col="close")
    obs, info = env.reset()
    assert obs.shape == (2,)
    action = np.array([101.5])
    obs, reward, terminated, truncated, info = env.step(action)
    # Reward should be negative squared error
    assert reward == -((101.5 - 101) ** 2)
    assert not terminated
    assert not truncated

def test_rl_training_and_prediction(tmp_path):
    # Synthetic data with a clear upward trend
    dates = pd.date_range(start="2020-01-01", periods=50)
    df = pd.DataFrame({
        "close": np.linspace(100, 150, 50),
        "rsi": np.random.rand(50) * 100,
        "atr": np.random.rand(50) * 2,
    })
    features = ["rsi", "atr"]
    predictor = RLPricePredictor("TEST", df, features, model_dir=tmp_path.as_posix())
    predictor.train(timesteps=2000)  # short training for test speed
    pred = predictor.predict(df.tail(1))
    assert isinstance(pred, float)
    # Prediction should be within a reasonable range of the last price
    assert 140 <= pred <= 160
