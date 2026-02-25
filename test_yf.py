import yfinance as yf
try:
    data = yf.download('AAPL', period='1d', progress=False)
    print("Downloaded Close data:", data['Close'].iloc[-1].item())
except Exception as e:
    print("Error:", e)
try:
    stock = yf.Ticker('AAPL')
    df = stock.history(period="1d")
    print("Ticker History Close:", df['Close'].iloc[-1])
except Exception as e:
    print("Ticker history error:", e)
