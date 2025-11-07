import duckdb
from pathlib import Path
from datetime import datetime
import pandas as pd

STAGING='data/staging'
files=['yfinance_clean.parquet','reddit_clean.parquet','news_clean.parquet','arxiv_clean.parquet','github_clean.parquet','huggingface_clean.parquet']
conn=duckdb.connect()
today = datetime.now().date()

for f in files:
    p=Path(STAGING)/f
    if not p.exists():
        print(f, 'missing')
        continue
    try:
        cnt = conn.execute(f"SELECT COUNT(*) FROM parquet_scan('{p.as_posix()}')").fetchone()[0]
        print(f'\n{f}: rows={cnt}')
        
        if cnt>0:
            # Try to get date range
            df = conn.execute(f"SELECT * FROM parquet_scan('{p.as_posix()}')").fetchdf()
            
            # Check for date columns (various names)
            date_cols = [col for col in df.columns if any(x in str(col).lower() for x in ['date', 'published', 'created', 'updated', 'collected'])]
            
            if date_cols:
                for date_col in date_cols[:2]:  # Check first 2 date columns
                    try:
                        # Try to parse as date
                        if df[date_col].dtype == 'object':
                            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                        
                        if pd.api.types.is_datetime64_any_dtype(df[date_col]):
                            min_date = df[date_col].min()
                            max_date = df[date_col].max()
                            has_today = (df[date_col].dt.date == today).any() if hasattr(df[date_col].dt, 'date') else False
                            
                            print(f'  {date_col}: {min_date} to {max_date}')
                            if has_today:
                                print(f'  ✓ Has today\'s data ({today})')
                            else:
                                days_old = (today - max_date.date()).days if hasattr(max_date, 'date') else 'unknown'
                                print(f'  ✗ Latest data is {days_old} days old (latest: {max_date})')
                    except Exception as e:
                        pass
            
            # Show sample columns
            print(f'  columns: {list(df.columns)[:5]}...' if len(df.columns) > 5 else f'  columns: {list(df.columns)}')
            
    except Exception as e:
        print(f'{f} error reading: {e}')
        import traceback
        traceback.print_exc()

conn.close()
