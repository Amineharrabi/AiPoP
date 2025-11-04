import yfinance as yf
import pandas as pd
import os
from datetime import datetime

# List of tickers (customize as needed)
tickers = [
    'NVDA','MSFT','AAPL','AMZN','GOOGL','META','AVGO','TSM','ORCL','TSLA',
    'AMD','ADBE','CRM','IBM','ASML','INTC','QCOM','MU','AMAT','LRCX',
    'NOW','PLTR','SNOW','PATH','AI','SNPS','CDNS','ARM','SMCI','DELL'
]

RAW_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw', 'yfinance')
STAGING_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'staging')
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(STAGING_DIR, exist_ok=True)

all_data = []
for ticker in tickers:
    print(f"Downloading {ticker}...")
    df = yf.download(ticker, start="2020-01-01", end=datetime.today().strftime('%Y-%m-%d'))
    df.reset_index(inplace=True)
    df['ticker'] = ticker
    raw_path = os.path.join(RAW_DIR, f"{ticker}.csv")
    df.to_csv(raw_path, index=False)
    all_data.append(df)

# Combine all tickers into one DataFrame
combined = pd.concat(all_data, ignore_index=True)
staging_path = os.path.join(STAGING_DIR, "yfinance_clean.parquet")
combined.to_parquet(staging_path, index=False)
print(f"Saved cleaned yfinance data to {staging_path}")
