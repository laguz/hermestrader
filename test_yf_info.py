import yfinance as yf
ticker = 'TSLA'
try:
    stock = yf.Ticker(ticker)
    cp = stock.info.get('currentPrice')
    print("stock.info.get('currentPrice'):", cp)
except Exception as e:
    print("Error:", e)
