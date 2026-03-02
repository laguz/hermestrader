"""Reinforcement‑learning price‑prediction module.
Provides a Gym‑compatible environment (`MarketEnv`) that presents historical market
features as the state and defines a reward based on prediction error.
A PPO agent from `stable_baselines3` is trained to minimise the error.
"""

import os
import numpy as np
import pandas as pd
from gymnasium import Env
from gymnasium.spaces import Box
from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv

class MarketEnv(Env):
    """Gym environment for price‑prediction.
    * Observation: a vector of selected technical features for the current day.
    * Action: a continuous scalar representing the predicted next‑day price.
    * Reward: negative mean‑squared‑error between the action and the true price.
    """

    def __init__(self, df: pd.DataFrame, feature_cols: list, target_col: str = "close"):
        super().__init__()
        self.df = df.reset_index(drop=True)
        self.feature_cols = feature_cols
        self.target_col = target_col
        self.current_idx = 0
        # Observation space: normalized feature vector (no explicit bounds)
        self.observation_space = Box(low=-1e5, high=1e5, shape=(len(feature_cols),), dtype=np.float32)
        # Action space: predicted price (positive)
        self.action_space = Box(low=0, high=1e5, shape=(1,), dtype=np.float32)

    def reset(self, seed=None, options=None):
        super().reset(seed=seed)
        self.current_idx = 0
        return self._get_obs(), {}

    def step(self, action):
        # True next‑day price
        true_price = self.df.loc[self.current_idx + 1, self.target_col]
        # Reward is negative MSE (higher is better)
        reward = -((action[0] - true_price) ** 2)
        self.current_idx += 1
        done = bool(self.current_idx >= len(self.df) - 1)
        obs = self._get_obs() if not done else np.zeros(self.observation_space.shape, dtype=np.float32)
        # return obs, reward, terminated, truncated, info
        return obs, float(reward), done, False, {}

    def _get_obs(self):
        return self.df.loc[self.current_idx, self.feature_cols].values.astype(np.float32)

class RLPricePredictor:
    """Wrapper that trains / loads a PPO model and provides a `predict` method.
    The model is stored under ``models/<symbol>_rl.zip``.
    """

    def __init__(self, symbol: str, df: pd.DataFrame, feature_cols: list, model_dir: str = "models"):
        self.symbol = symbol.upper()
        self.df = df
        self.feature_cols = feature_cols
        self.model_path = os.path.join(model_dir, f"{self.symbol}_rl.zip")
        self.model = None
        self.model_dir = model_dir

    def train(self, timesteps: int = 10_000):
        env = DummyVecEnv([lambda: MarketEnv(self.df, self.feature_cols)])
        self.model = PPO("MlpPolicy", env, verbose=0)
        self.model.learn(total_timesteps=timesteps)
        os.makedirs(self.model_dir, exist_ok=True)
        self.model.save(self.model_path)

    def load(self):
        if os.path.exists(self.model_path):
            env = DummyVecEnv([lambda: MarketEnv(self.df, self.feature_cols)])
            self.model = PPO.load(self.model_path, env=env)
        else:
            raise FileNotFoundError(f"RL model not found at {self.model_path}")

    def predict(self, recent_df: pd.DataFrame) -> float:
        """Predict the next‑day price using the trained PPO model.
        ``recent_df`` must contain at least one row with the same feature columns.
        """
        if self.model is None:
            self.load()
        obs = recent_df[self.feature_cols].iloc[-1].values.astype(np.float32)
        action, _ = self.model.predict(obs.reshape(1, -1), deterministic=True)
        return float(action[0])
