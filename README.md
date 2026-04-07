# LaguzTechInvestment

Algorithmic trading platform built with Flask, integrating Tradier brokerage API, MongoDB, and machine learning models for options trading strategies.

## Features

- **Automated Options Trading** — Credit spreads, wheel strategy, and rule-based spread strategies with configurable watchlists and risk limits
- **ML Price Prediction (Daily Auto-Scheduling)** — Runs automatically every day for watchlist tickers. Supports LSTM and Reinforcement Learning (PPO) models with walk-forward validation, bias correction, and ensemble predictions.
- **SEC EDGAR Integration** — Fundamental analysis via Rule #1 "Sticker Price" calculator with CAGR, ROIC, and financial data parsing
- **Technical Analysis** — RSI, MACD, Bollinger Bands, ATR, ADX, OBV, VWAP, and more
- **Backtesting** — Full backtesting engine with mock services to simulate strategy performance
- **Authentication** — Password-based and Nostr (NIP-07/NIP-46) with encrypted vault storage
- **Position Tracking** — Real-time P&L monitoring, position sync, and trade history

## Requirements

- Python 3.13+
- MongoDB 7.0+
- Tradier API account (sandbox or production)

## Quick Start

```bash
# Clone and setup
git clone <repo-url> && cd LaguzTechInvestment
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Configure environment
cp .env.example .env  # Edit with your credentials

# Run locally
python app.py
```

## Docker

```bash
docker compose up --build
```

The app will be available at `http://localhost:8080`.

## Environment Variables

| Variable | Description | Required |
|---|---|---|
| `FLASK_SECRET_KEY` | Flask session secret key | Yes |
| `MONGODB_URI` | MongoDB connection string | Yes |
| `TRADIER_ACCESS_TOKEN` | Tradier API access token | Yes |
| `TRADIER_ACCOUNT_ID` | Tradier account ID | Yes |
| `TRADIER_ENDPOINT` | API endpoint (defaults to sandbox) | No |
| `SEC_USER_AGENT` | User-Agent for SEC EDGAR requests | No |
| `SECRET_KEY` | Flask session and encrypted cookie key | Yes |

## Project Structure

```
├── app.py                  # Flask application factory
├── routes/                 # API route blueprints
├── services/               # Business logic services
│   ├── container.py        # Dependency injection container
│   ├── tradier_service.py  # Tradier API client
│   ├── ml_service.py       # ML model training & prediction
│   ├── bot_service.py      # Trading bot lifecycle management
│   ├── auth_service.py     # Authentication & vault
│   ├── analysis_service.py # Market analysis
│   └── backtest_service.py # Strategy backtesting
├── bot/strategies/         # Trading strategy implementations
├── logic/                  # Financial calculators & SEC EDGAR
├── utils/                  # Technical indicators
├── models/                 # Saved ML models (gitignored)
└── tests/                  # Test suite
```

## Testing

```bash
pytest tests/ -v
pytest tests/ --cov=services --cov-report=term-missing
```

## License

Private
