from flask import Blueprint, render_template, request
from flask_login import login_required
import logging
import yfinance as yf
from logic.edgar_client import get_company_facts
from logic.parser import extract_financials
from logic.calculator import analyze_stock
import time

rule1_bp = Blueprint('rule1', __name__, url_prefix='/rule1')

_yahoo_price_cache = {}
YAHOO_PRICE_CACHE_TTL = 300  # 5 minutes
MAX_CACHE_SIZE = 1000
logger = logging.getLogger(__name__)

@rule1_bp.route('/', methods=['GET', 'POST'])
@login_required
def index():
    result = None
    error = None
    ticker = ""
    
    if request.method == 'POST':
        raw_ticker = request.form.get('ticker', '').strip().upper()
        import re
        ticker = re.sub(r'[^A-Z]', '', raw_ticker)
        
        if not ticker:
            error = "Please enter a valid stock symbol (A-Z only)."
        else:
            try:
                # 1. Fetch Financials
                facts = get_company_facts(ticker)
                if not facts:
                    error = f"Could not fetch data for {ticker}. Check symbol or SEC availability."
                else:
                    df = extract_financials(facts)
                    if df is None or df.empty:
                        error = "Could not parse financial data."
                    else:
                        # Fetch splits and price
                        splits = None
                        current_price = None
                        try:
                            # Fetch splits and price
                            stock = yf.Ticker(ticker)
                            splits = stock.splits
                        except Exception as e:
                            logger.warning(f"Failed to fetch splits for {ticker}: {e}")
                            
                        try:
                            from services.container import Container
                            import requests
                            tradier = Container.get_tradier_service()
                            quote = tradier.get_quote(ticker)
                            if quote and 'last' in quote:
                                current_price = float(quote['last'])
                            else:
                                # Fallback to raw Yahoo Finance request due to yfinance bug
                                now = time.time()
                                cached_val = None
                                if ticker in _yahoo_price_cache:
                                    cached_price, timestamp = _yahoo_price_cache[ticker]
                                    if now - timestamp < YAHOO_PRICE_CACHE_TTL:
                                        cached_val = cached_price

                                if cached_val is not None:
                                    current_price = cached_val
                                else:
                                    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=1d"
                                    headers = {'User-Agent': 'Mozilla/5.0'}
                                    resp = requests.get(url, headers=headers, timeout=5)
                                    data = resp.json()
                                    fetched_price = float(data['chart']['result'][0]['meta']['regularMarketPrice'])
                                    current_price = fetched_price

                                    # Simple eviction if cache gets too big
                                    if len(_yahoo_price_cache) >= MAX_CACHE_SIZE:
                                        # Remove oldest 20%
                                        sorted_cache = sorted(_yahoo_price_cache.items(), key=lambda item: item[1][1])
                                        for key, _ in sorted_cache[:int(MAX_CACHE_SIZE * 0.2)]:
                                            _yahoo_price_cache.pop(key, None)

                                    _yahoo_price_cache[ticker] = (fetched_price, now)
                        except Exception as e:
                            logger.warning(f"Failed to fetch price for {ticker}: {e}")

                        # 2. Analyze
                        metrics, valuation = analyze_stock(ticker, df, splits=splits)
                        
                        if 'Error' in valuation:
                            # Edge case logic as requested
                            result = {
                                'recommendation': "Please don't invest",
                                'reason': valuation['Error'],
                                'sticker_price': "N/A",
                                'current_price': f"${current_price:.2f}" if current_price else "N/A"
                            }
                        else:
                            sticker_price = valuation.get('Sticker_Price', 0)
                            buy_price = valuation.get('Buy_Price', 0)
                            
                            recommendation = "Watch"
                            if current_price:
                                if current_price <= buy_price:
                                    recommendation = "BUY (Margin of Safety Reached!)"
                                elif current_price <= sticker_price:
                                    recommendation = "Fair Value (No Margin of Safety)"
                                else:
                                    recommendation = "Overvalued"
                            
                            result = {
                                'recommendation': recommendation,
                                'sticker_price': f"${sticker_price:.2f}",
                                'buy_price': f"${buy_price:.2f}",
                                'current_price': f"${current_price:.2f}" if current_price else "N/A",
                                'metrics': metrics,
                                'valuation': valuation
                            }
                            
            except Exception as e:
                import traceback
                traceback.print_exc()
                logger.error(f"Error processing {ticker}: {e}")
                error = f"An unexpected error occurred: {str(e)}"

    return render_template('rule1.html', result=result, error=error, ticker=ticker)
