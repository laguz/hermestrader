from flask import Flask
from dotenv import load_dotenv

# Import Blueprints
from routes.main_routes import main_bp
from routes.positions_routes import positions_bp
from routes.backtest_routes import backtest_bp
from routes.ml_routes import ml_bp
from routes.account_routes import account_bp
from routes.trading_routes import trading_bp

load_dotenv()

app = Flask(__name__)

# Register Blueprints
app.register_blueprint(main_bp)
app.register_blueprint(positions_bp)
app.register_blueprint(backtest_bp)
app.register_blueprint(ml_bp)
app.register_blueprint(account_bp)
app.register_blueprint(trading_bp)

from routes.market_routes import market_bp
app.register_blueprint(market_bp)

from routes.analysis_routes import analysis_bp
app.register_blueprint(analysis_bp)

if __name__ == '__main__':
    app.run(debug=True, port=8080)
