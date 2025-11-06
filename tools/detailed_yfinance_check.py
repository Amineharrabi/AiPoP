"""Detailed check of yfinance import to understand count differences"""
import duckdb
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
STAGING_DIR = BASE_DIR / 'data' / 'staging'
WAREHOUSE_PATH = BASE_DIR / 'data' / 'warehouse' / 'ai_bubble.duckdb'

conn = duckdb.connect(str(WAREHOUSE_PATH))

print("=" * 80)
print("DETAILED YFINANCE VERIFICATION")
print("=" * 80)

# Load and normalize yfinance data
import sys
sys.path.insert(0, str(BASE_DIR))
from setup.load_warehouse import _normalize_yfinance

yfinance_path = STAGING_DIR / 'yfinance_clean.parquet'
if yfinance_path.exists():
    df_normalized = _normalize_yfinance(str(yfinance_path))
    print(f"\nNormalized yfinance DataFrame:")
    print(f"  Total rows: {len(df_normalized):,}")
    print(f"  Rows with close: {df_normalized['close'].notna().sum():,}")
    print(f"  Rows with volume: {df_normalized['volume'].notna().sum():,}")
    print(f"  Rows with both: {df_normalized[['close', 'volume']].notna().all(axis=1).sum():,}")
    print(f"  Unique tickers: {df_normalized['ticker'].nunique()}")
    print(f"  Unique dates: {df_normalized['date'].nunique():,}")
    print(f"  Date range: {df_normalized['date'].min()} to {df_normalized['date'].max()}")
    
    # Check warehouse counts
    print(f"\nWarehouse fact_reality_signals (yfinance):")
    close_count = conn.execute("""
        SELECT COUNT(*) FROM fact_reality_signals
        WHERE metric_name = 'stock_price' AND provenance = 'yfinance'
    """).fetchone()[0]
    volume_count = conn.execute("""
        SELECT COUNT(*) FROM fact_reality_signals
        WHERE metric_name = 'trading_volume' AND provenance = 'yfinance'
    """).fetchone()[0]
    print(f"  stock_price records: {close_count:,}")
    print(f"  trading_volume records: {volume_count:,}")
    print(f"  Total: {close_count + volume_count:,}")
    
    # Check for missing entities
    print(f"\nEntity matching:")
    staging_tickers = set(df_normalized['ticker'].unique())
    warehouse_tickers = conn.execute("""
        SELECT DISTINCT e.ticker
        FROM dim_entity e
        JOIN fact_reality_signals f ON e.entity_id = f.entity_id
        WHERE f.provenance = 'yfinance'
    """).fetchdf()['ticker'].tolist()
    warehouse_tickers_set = set(warehouse_tickers)
    
    missing = staging_tickers - warehouse_tickers_set
    extra = warehouse_tickers_set - staging_tickers
    
    print(f"  Staging tickers: {len(staging_tickers)}")
    print(f"  Warehouse tickers: {len(warehouse_tickers_set)}")
    if missing:
        print(f"  Missing in warehouse: {missing}")
    if extra:
        print(f"  Extra in warehouse: {extra}")
    
    # Check date coverage
    print(f"\nDate coverage:")
    staging_dates = set(pd.to_datetime(df_normalized['date']).dt.date)
    warehouse_dates = conn.execute("""
        SELECT DISTINCT t.date
        FROM dim_time t
        JOIN fact_reality_signals f ON t.time_id = f.time_id
        WHERE f.provenance = 'yfinance'
    """).fetchdf()['date'].tolist()
    warehouse_dates_set = set(warehouse_dates)
    
    print(f"  Staging unique dates: {len(staging_dates)}")
    print(f"  Warehouse dates with yfinance data: {len(warehouse_dates_set)}")

conn.close()

