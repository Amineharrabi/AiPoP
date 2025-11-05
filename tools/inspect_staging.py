import duckdb
from pathlib import Path
STAGING='data/staging'
files=['yfinance_clean.parquet','reddit_clean.parquet','news_clean.parquet','arxiv_clean.parquet','github_clean.parquet','huggingface_clean.parquet']
conn=duckdb.connect()
for f in files:
    p=Path(STAGING)/f
    if not p.exists():
        print(f, 'missing')
        continue
    try:
        cnt = conn.execute(f"SELECT COUNT(*) FROM parquet_scan('{p.as_posix()}')").fetchone()[0]
        print(f, 'rows=', cnt)
        if cnt>0:
            sample = conn.execute(f"SELECT * FROM parquet_scan('{p.as_posix()}') LIMIT 3").fetchdf()
            print('sample columns:', list(sample.columns))
    except Exception as e:
        print(f, 'error reading:', e)
conn.close()
