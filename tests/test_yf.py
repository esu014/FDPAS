import yfinance as yf

tickers = ["GC=F", "AAPL", "SPY", "EURUSD=X"]
for t in tickers:
    df = yf.download(t, period="5d", progress=False)
    print(f"{t}: {len(df)} registros")