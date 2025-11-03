# save_ai_stock_data.py
import yfinance as yf
import pandas as pd
import os
from datetime import datetime

# ----------------------------
# 1. Configuration
# ----------------------------
START_DATE = "2020-01-01"
END_DATE = datetime.today().strftime("%Y-%m-%d")  

# Output directory
OUTPUT_DIR = "data/cache/SEC"
os.makedirs(OUTPUT_DIR, exist_ok=True)  # Create folder if not exists

# ----------------------------
# 2. Top 30 AI Companies (Ticker List)
# ----------------------------
tickers = [
    'NVDA', 'MSFT', 'AAPL', 'AMZN', 'GOOGL', 'META', 'AVGO', 'TSM', 'ORCL', 'TSLA',
    'AMD', 'ADBE', 'CRM', 'IBM', 'ASML', 'INTC', 'QCOM', 'MU', 'AMAT', 'LRCX',
    'NOW', 'PLTR', 'SNOW', 'PATH', 'AI', 'SNPS', 'CDNS', 'ARM', 'SMCI', 'DELL'
]

# ----------------------------
# 3. Download & Save Individual CSVs
# ----------------------------
print(f"Downloading data from {START_DATE} to {END_DATE}...")
print(f"Saving to: {os.path.abspath(OUTPUT_DIR)}")

all_data = {}  # For master file

for ticker in tickers:
    print(f"Downloading {ticker}...", end=" ")
    try:
        # Download data
        df = yf.download(
            ticker,
            start=START_DATE,
            end=END_DATE,
            progress=False,
            auto_adjust=True,  # Adjusted Close (handles splits/dividends)
            threads=True
        )

        if df.empty:
            print(f"[NO DATA]")
            continue

        # Reset index to make Date a column
        df = df.reset_index()

        # Save individual CSV
        file_path = os.path.join(OUTPUT_DIR, f"{ticker}_2020_onward.csv")
        df.to_csv(file_path, index=False)
        print(f"[SAVED] {len(df)} rows -> {file_path}")

        # Store for master file
        df['Ticker'] = ticker
        all_data[ticker] = df

    except Exception as e:
        print(f"[ERROR] {e}")

# ----------------------------
# 4. Save Master CSV (All Tickers)
# ----------------------------
if all_data:
    master_df = pd.concat(all_data.values(), ignore_index=True)
    master_path = os.path.join(OUTPUT_DIR, "all_ai_stocks_2020_onward.csv")
    master_df.to_csv(master_path, index=False)
    print(f"\nMaster CSV saved: {master_path}")
    print(f"   Total rows: {len(master_df):,}")
else:
    print("\nNo data downloaded. Check internet or ticker symbols.")

print("\nDone!")