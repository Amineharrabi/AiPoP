"""Verify that all staging data is properly imported into the warehouse"""
import duckdb
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
STAGING_DIR = BASE_DIR / 'data' / 'staging'
WAREHOUSE_PATH = BASE_DIR / 'data' / 'warehouse' / 'ai_bubble.duckdb'

print("=" * 80)
print("WAREHOUSE IMPORT VERIFICATION")
print("=" * 80)

conn = duckdb.connect(str(WAREHOUSE_PATH))

# 1. Check yfinance data
print("\n1. YFINANCE DATA")
print("-" * 80)
yfinance_path = STAGING_DIR / 'yfinance_clean.parquet'
if yfinance_path.exists():
    staging_df = pd.read_parquet(yfinance_path)
    staging_count = len(staging_df)
    print(f"Staging parquet rows: {staging_count:,}")
    
    # Count unique date-ticker combinations in warehouse
    warehouse_count = conn.execute("""
        SELECT COUNT(DISTINCT time_id || '-' || entity_id) as count
        FROM fact_reality_signals
        WHERE metric_name IN ('stock_price', 'trading_volume')
        AND provenance = 'yfinance'
    """).fetchone()[0]
    print(f"Warehouse fact_reality_signals (yfinance): {warehouse_count:,}")
    
    # Count unique tickers
    staging_tickers = len(staging_df['ticker'].unique()) if 'ticker' in staging_df.columns else 0
    warehouse_tickers = conn.execute("""
        SELECT COUNT(DISTINCT e.ticker)
        FROM dim_entity e
        JOIN fact_reality_signals f ON e.entity_id = f.entity_id
        WHERE f.provenance = 'yfinance'
    """).fetchone()[0]
    print(f"Staging unique tickers: {staging_tickers}")
    print(f"Warehouse unique tickers (yfinance): {warehouse_tickers}")

# 2. Check Reddit data
print("\n2. REDDIT DATA")
print("-" * 80)
reddit_path = STAGING_DIR / 'reddit_clean.parquet'
if reddit_path.exists():
    staging_count = conn.execute(f"SELECT COUNT(*) FROM parquet_scan('{reddit_path}')").fetchone()[0]
    print(f"Staging parquet rows: {staging_count:,}")
    
    warehouse_count = conn.execute("""
        SELECT COUNT(*) FROM fact_hype_signals
        WHERE source_id IN (SELECT source_id FROM dim_source WHERE source_name = 'reddit')
    """).fetchone()[0]
    print(f"Warehouse fact_hype_signals (reddit): {warehouse_count:,}")
    
    # Check subreddits
    staging_subreddits = conn.execute(f"""
        SELECT COUNT(DISTINCT subreddit) FROM parquet_scan('{reddit_path}')
    """).fetchone()[0]
    warehouse_subreddits = conn.execute("""
        SELECT COUNT(DISTINCT e.entity_id)
        FROM dim_entity e
        JOIN fact_hype_signals f ON e.entity_id = f.entity_id
        JOIN dim_source s ON f.source_id = s.source_id
        WHERE s.source_name = 'reddit'
    """).fetchone()[0]
    print(f"Staging unique subreddits: {staging_subreddits}")
    print(f"Warehouse entities with reddit signals: {warehouse_subreddits}")

# 3. Check News data
print("\n3. NEWS DATA")
print("-" * 80)
news_path = STAGING_DIR / 'news_clean.parquet'
if news_path.exists():
    staging_count = conn.execute(f"SELECT COUNT(*) FROM parquet_scan('{news_path}')").fetchone()[0]
    print(f"Staging parquet rows: {staging_count:,}")
    
    warehouse_count = conn.execute("""
        SELECT COUNT(*) FROM fact_hype_signals
        WHERE source_id IN (SELECT source_id FROM dim_source WHERE source_name = 'news')
    """).fetchone()[0]
    print(f"Warehouse fact_hype_signals (news): {warehouse_count:,}")
    
    # Check companies
    staging_companies = conn.execute(f"""
        SELECT COUNT(DISTINCT company) FROM parquet_scan('{news_path}')
    """).fetchone()[0]
    warehouse_companies = conn.execute("""
        SELECT COUNT(DISTINCT e.entity_id)
        FROM dim_entity e
        JOIN fact_hype_signals f ON e.entity_id = f.entity_id
        JOIN dim_source s ON f.source_id = s.source_id
        WHERE s.source_name = 'news'
    """).fetchone()[0]
    print(f"Staging unique companies: {staging_companies}")
    print(f"Warehouse entities with news signals: {warehouse_companies}")

# 4. Check ArXiv data
print("\n4. ARXIV DATA")
print("-" * 80)
arxiv_path = STAGING_DIR / 'arxiv_clean.parquet'
if arxiv_path.exists():
    staging_count = conn.execute(f"SELECT COUNT(*) FROM parquet_scan('{arxiv_path}')").fetchone()[0]
    print(f"Staging parquet rows: {staging_count:,}")
    
    warehouse_count = conn.execute("""
        SELECT COUNT(*) FROM fact_hype_signals
        WHERE source_id IN (SELECT source_id FROM dim_source WHERE source_name = 'arxiv')
    """).fetchone()[0]
    print(f"Warehouse fact_hype_signals (arxiv): {warehouse_count:,}")
    
    # Check search terms
    staging_terms = conn.execute(f"""
        SELECT COUNT(DISTINCT search_term) FROM parquet_scan('{arxiv_path}')
    """).fetchone()[0]
    warehouse_terms = conn.execute("""
        SELECT COUNT(DISTINCT e.entity_id)
        FROM dim_entity e
        JOIN fact_hype_signals f ON e.entity_id = f.entity_id
        JOIN dim_source s ON f.source_id = s.source_id
        WHERE s.source_name = 'arxiv'
    """).fetchone()[0]
    print(f"Staging unique search_terms: {staging_terms}")
    print(f"Warehouse entities with arxiv signals: {warehouse_terms}")

# 5. Check GitHub data
print("\n5. GITHUB DATA")
print("-" * 80)
github_path = STAGING_DIR / 'github_clean.parquet'
if github_path.exists():
    staging_count = conn.execute(f"SELECT COUNT(*) FROM parquet_scan('{github_path}')").fetchone()[0]
    print(f"Staging parquet rows: {staging_count:,}")
    
    # Hype signals
    warehouse_hype = conn.execute("""
        SELECT COUNT(*) FROM fact_hype_signals
        WHERE source_id IN (SELECT source_id FROM dim_source WHERE source_name = 'github')
    """).fetchone()[0]
    print(f"Warehouse fact_hype_signals (github): {warehouse_hype:,}")
    
    # Reality signals
    warehouse_reality = conn.execute("""
        SELECT COUNT(*) FROM fact_reality_signals
        WHERE provenance = 'github'
    """).fetchone()[0]
    print(f"Warehouse fact_reality_signals (github): {warehouse_reality:,}")
    
    # Check repos
    staging_repos = conn.execute(f"""
        SELECT COUNT(DISTINCT name) FROM parquet_scan('{github_path}')
    """).fetchone()[0]
    warehouse_repos = conn.execute("""
        SELECT COUNT(DISTINCT e.entity_id)
        FROM dim_entity e
        JOIN fact_hype_signals f ON e.entity_id = f.entity_id
        JOIN dim_source s ON f.source_id = s.source_id
        WHERE s.source_name = 'github'
    """).fetchone()[0]
    print(f"Staging unique repos: {staging_repos}")
    print(f"Warehouse entities with github signals: {warehouse_repos}")

# 6. Check HuggingFace data
print("\n6. HUGGINGFACE DATA")
print("-" * 80)
hf_path = STAGING_DIR / 'huggingface_clean.parquet'
if hf_path.exists():
    staging_count = conn.execute(f"SELECT COUNT(*) FROM parquet_scan('{hf_path}')").fetchone()[0]
    print(f"Staging parquet rows: {staging_count:,}")
    
    # Hype signals
    warehouse_hype = conn.execute("""
        SELECT COUNT(*) FROM fact_hype_signals
        WHERE source_id IN (SELECT source_id FROM dim_source WHERE source_name = 'huggingface')
    """).fetchone()[0]
    print(f"Warehouse fact_hype_signals (huggingface): {warehouse_hype:,}")
    
    # Reality signals
    warehouse_reality = conn.execute("""
        SELECT COUNT(*) FROM fact_reality_signals
        WHERE provenance = 'huggingface'
    """).fetchone()[0]
    print(f"Warehouse fact_reality_signals (huggingface): {warehouse_reality:,}")
    
    # Check models
    staging_models = conn.execute(f"""
        SELECT COUNT(DISTINCT model_id) FROM parquet_scan('{hf_path}')
    """).fetchone()[0]
    warehouse_models = conn.execute("""
        SELECT COUNT(DISTINCT e.entity_id)
        FROM dim_entity e
        JOIN fact_hype_signals f ON e.entity_id = f.entity_id
        JOIN dim_source s ON f.source_id = s.source_id
        WHERE s.source_name = 'huggingface'
    """).fetchone()[0]
    print(f"Staging unique models: {staging_models}")
    print(f"Warehouse entities with huggingface signals: {warehouse_models}")

# 7. Check for missing joins
print("\n7. JOIN VERIFICATION")
print("-" * 80)

# Check for NULL joins in fact tables
null_time = conn.execute("""
    SELECT COUNT(*) FROM fact_hype_signals WHERE time_id IS NULL
    UNION ALL
    SELECT COUNT(*) FROM fact_reality_signals WHERE time_id IS NULL
""").fetchall()
print(f"NULL time_id in fact_hype_signals: {null_time[0][0]}")
print(f"NULL time_id in fact_reality_signals: {null_time[1][0]}")

null_entity = conn.execute("""
    SELECT COUNT(*) FROM fact_hype_signals WHERE entity_id IS NULL
    UNION ALL
    SELECT COUNT(*) FROM fact_reality_signals WHERE entity_id IS NULL
""").fetchall()
print(f"NULL entity_id in fact_hype_signals: {null_entity[0][0]}")
print(f"NULL entity_id in fact_reality_signals: {null_entity[1][0]}")

null_source = conn.execute("""
    SELECT COUNT(*) FROM fact_hype_signals WHERE source_id IS NULL
""").fetchone()[0]
print(f"NULL source_id in fact_hype_signals: {null_source}")

# 8. Summary statistics
print("\n8. SUMMARY STATISTICS")
print("-" * 80)
stats = conn.execute("""
    SELECT 
        (SELECT COUNT(*) FROM dim_time) as time_records,
        (SELECT COUNT(*) FROM dim_entity) as entity_records,
        (SELECT COUNT(*) FROM dim_source) as source_records,
        (SELECT COUNT(*) FROM fact_hype_signals) as hype_records,
        (SELECT COUNT(*) FROM fact_reality_signals) as reality_records
""").fetchone()

print(f"dim_time: {stats[0]:,} records")
print(f"dim_entity: {stats[1]:,} records")
print(f"dim_source: {stats[2]:,} records")
print(f"fact_hype_signals: {stats[3]:,} records")
print(f"fact_reality_signals: {stats[4]:,} records")

# 9. Check date ranges
print("\n9. DATE RANGE VERIFICATION")
print("-" * 80)
date_range = conn.execute("""
    SELECT 
        MIN(date) as min_date,
        MAX(date) as max_date,
        COUNT(DISTINCT date) as unique_dates
    FROM dim_time
""").fetchone()
print(f"Date range: {date_range[0]} to {date_range[1]}")
print(f"Unique dates: {date_range[2]:,}")

conn.close()
print("\n" + "=" * 80)
print("VERIFICATION COMPLETE")
print("=" * 80)

