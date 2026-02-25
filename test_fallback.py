import yfinance as yf
ticker = 'TSLA'
try:
    data = yf.download(ticker, period='1d', progress=False)
    if not data.empty:
        current_price = float(data['Close'].iloc[-1].item())
        print("Success:", current_price)
    else:
        print("Data is empty")
except Exception as e:
    import traceback
    traceback.print_exc()
    print("Error:", e)
