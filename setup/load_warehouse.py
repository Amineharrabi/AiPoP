import os
import duckdb
from datetime import datetime
import pandas as pd

# Fixed paths for new structure
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
STAGING_DIR = os.path.join(BASE_DIR, 'data', 'staging')
WAREHOUSE_PATH = os.path.join(BASE_DIR, 'data', 'warehouse', 'ai_bubble.duckdb')

def load_staging_data():
    """Load all staged Parquet files into DuckDB warehouse with enhanced multi-source integration"""
    print(f"Loading data into warehouse: {WAREHOUSE_PATH}")
    
    # Connect to DuckDB
    conn = duckdb.connect(WAREHOUSE_PATH)
    
    try:
        # 1. Load time dimension (extract unique timestamps from all sources)
        print("\nLoading time dimension...")
        conn.execute("""
            INSERT INTO dim_time (
                time_id, date, year, quarter, month, week, day_of_week, is_business_day
            )
            WITH unified_dates AS (
                -- Stock data dates
                SELECT DISTINCT date FROM parquet_scan('{0}/yfinance_clean.parquet')
                UNION
                -- Reddit post dates
                SELECT DISTINCT date_trunc('day', timestamp_ms(created_utc*1000)) as date 
                FROM parquet_scan('{0}/reddit_clean.parquet')
                UNION
                -- News article dates
                SELECT DISTINCT date_trunc('day', publishedAt::timestamp) as date 
                FROM parquet_scan('{0}/news_clean.parquet')
                UNION
                -- ArXiv paper dates
                SELECT DISTINCT date_trunc('day', published::timestamp) as date 
                FROM parquet_scan('{0}/arxiv_clean.parquet')
                UNION
                -- GitHub/HF dates
                SELECT DISTINCT date_trunc('day', collected_at::timestamp) as date 
                FROM parquet_scan('{0}/github_clean.parquet')
            )
            SELECT 
                ROW_NUMBER() OVER (ORDER BY date) as time_id,
                date,
                EXTRACT(year FROM date) as year,
                EXTRACT(quarter FROM date) as quarter,
                EXTRACT(month FROM date) as month,
                EXTRACT(week FROM date) as week,
                EXTRACT(DOW FROM date) as day_of_week,
                CASE WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN FALSE ELSE TRUE END as is_business_day
            FROM unified_dates
            WHERE date IS NOT NULL
            AND date NOT IN (SELECT date FROM dim_time)
        """.format(STAGING_DIR))
        
        # 2. Load entity dimension
        print("Loading entity dimension...")
        conn.execute("""
            INSERT INTO dim_entity (
                entity_id, name, ticker, entity_type, industry
            )
            WITH unified_entities AS (
                -- Stock tickers
                SELECT DISTINCT 
                    ticker as name,
                    ticker,
                    'company' as entity_type,
                    'technology' as industry
                FROM parquet_scan('{0}/yfinance_clean.parquet')
                UNION
                -- AI Models from HuggingFace
                SELECT DISTINCT
                    name,
                    model_id as ticker,
                    'ai_model' as entity_type,
                    COALESCE(pipeline_tag, 'general') as industry
                FROM parquet_scan('{0}/huggingface_clean.parquet')
                UNION
                -- GitHub repositories
                SELECT DISTINCT
                    name,
                    name as ticker,
                    'software' as entity_type,
                    'ai_infrastructure' as industry
                FROM parquet_scan('{0}/github_clean.parquet')
                UNION
                -- ArXiv research topics
                SELECT DISTINCT
                    search_term as name,
                    search_term as ticker,
                    'research' as entity_type,
                    'academic' as industry
                FROM parquet_scan('{0}/arxiv_clean.parquet')
            )
            SELECT 
                ROW_NUMBER() OVER (ORDER BY name) as entity_id,
                name,
                ticker,
                entity_type,
                industry
            FROM unified_entities
            WHERE name IS NOT NULL
            AND name NOT IN (SELECT name FROM dim_entity)
        """.format(STAGING_DIR))
        
        # 3. Load source dimension
        print("Loading source dimension...")
        conn.execute("""
            INSERT INTO dim_source (
                source_id, source_name, source_type
            )
            SELECT 
                ROW_NUMBER() OVER (ORDER BY source_name) as source_id,
                source_name,
                source_type
            FROM (
                VALUES 
                    ('reddit', 'social_media'),
                    ('news', 'media'),
                    ('arxiv', 'academic'),
                    ('github', 'technical'),
                    ('huggingface', 'technical'),
                    ('yfinance', 'financial')
            ) AS s(source_name, source_type)
            WHERE source_name NOT IN (SELECT source_name FROM dim_source)
        """)
        
        # 4. Load enhanced hype signals fact table
        print("Loading enhanced hype signals...")
        conn.execute("""
            INSERT INTO fact_hype_signals (
                time_id, entity_id, source_id, signal_type, metric_name, metric_value, raw_text, url
            )
            -- Reddit sentiment with AI weighting
            SELECT 
                t.time_id,
                e.entity_id,
                s.source_id,
                'sentiment' as signal_type,
                'reddit_ai_sentiment' as metric_name,
                r.ai_sentiment_score as metric_value,
                r.title || ' ' || r.selftext as raw_text,
                r.url
            FROM parquet_scan('{0}/reddit_clean.parquet') r
            JOIN dim_time t ON date_trunc('day', timestamp_ms(r.created_utc*1000)) = t.date
            JOIN dim_entity e ON r.subreddit = e.name
            JOIN dim_source s ON s.source_name = 'reddit'
            UNION ALL
            -- News sentiment with AI weighting
            SELECT 
                t.time_id,
                e.entity_id,
                s.source_id,
                'sentiment' as signal_type,
                'news_ai_sentiment' as metric_name,
                n.ai_sentiment_score as metric_value,
                n.title || ' ' || n.description as raw_text,
                n.url
            FROM parquet_scan('{0}/news_clean.parquet') n
            JOIN dim_time t ON date_trunc('day', n.publishedAt::timestamp) = t.date
            JOIN dim_entity e ON n.company = e.name
            JOIN dim_source s ON s.source_name = 'news'
            UNION ALL
            -- GitHub trending activity
            SELECT 
                t.time_id,
                e.entity_id,
                s.source_id,
                'trending' as signal_type,
                'github_trending_score' as metric_name,
                g.trending_score as metric_value,
                NULL as raw_text,
                'https://github.com/' || g.name as url
            FROM parquet_scan('{0}/github_clean.parquet') g
            JOIN dim_time t ON date_trunc('day', g.collected_at::timestamp) = t.date
            JOIN dim_entity e ON g.name = e.name
            JOIN dim_source s ON s.source_name = 'github'
            UNION ALL
            -- HuggingFace trending activity
            SELECT 
                t.time_id,
                e.entity_id,
                s.source_id,
                'trending' as signal_type,
                'huggingface_trending_score' as metric_name,
                h.trending_score as metric_value,
                NULL as raw_text,
                'https://huggingface.co/' || h.model_id as url
            FROM parquet_scan('{0}/huggingface_clean.parquet') h
            JOIN dim_time t ON date_trunc('day', h.collected_at::timestamp) = t.date
            JOIN dim_entity e ON h.model_id = e.ticker
            JOIN dim_source s ON s.source_name = 'huggingface'
            UNION ALL
            -- ArXiv innovation
            SELECT 
                t.time_id,
                e.entity_id,
                s.source_id,
                'innovation' as signal_type,
                'arxiv_innovation_score' as metric_name,
                a.impact_score as metric_value,
                a.title || ' ' || a.abstract as raw_text,
                a.pdf_url as url
            FROM parquet_scan('{0}/arxiv_clean.parquet') a
            JOIN dim_time t ON date_trunc('day', a.published::timestamp) = t.date
            JOIN dim_entity e ON a.search_term = e.name
            JOIN dim_source s ON s.source_name = 'arxiv'
        """.format(STAGING_DIR))
        
        # 5. Load enhanced reality signals fact table
        print("Loading enhanced reality signals...")
        conn.execute("""
            INSERT INTO fact_reality_signals (
                time_id, entity_id, metric_name, metric_value, metric_unit, provenance
            )
            -- Stock market metrics
            SELECT 
                t.time_id,
                e.entity_id,
                'stock_price' as metric_name,
                y.close as metric_value,
                'USD' as metric_unit,
                'yfinance' as provenance
            FROM parquet_scan('{0}/yfinance_clean.parquet') y
            JOIN dim_time t ON y.date = t.date
            JOIN dim_entity e ON y.ticker = e.ticker
            UNION ALL
            -- Stock volume
            SELECT 
                t.time_id,
                e.entity_id,
                'trading_volume' as metric_name,
                y.volume as metric_value,
                'shares' as metric_unit,
                'yfinance' as provenance
            FROM parquet_scan('{0}/yfinance_clean.parquet') y
            JOIN dim_time t ON y.date = t.date
            JOIN dim_entity e ON y.ticker = e.ticker
            UNION ALL
            -- HuggingFace model adoption with deltas
            SELECT 
                t.time_id,
                e.entity_id,
                'model_downloads_delta' as metric_name,
                h.downloads_delta as metric_value,
                'count' as metric_unit,
                'huggingface' as provenance
            FROM parquet_scan('{0}/huggingface_clean.parquet') h
            JOIN dim_time t ON date_trunc('day', h.collected_at::timestamp) = t.date
            JOIN dim_entity e ON h.model_id = e.ticker
            UNION ALL
            -- GitHub project metrics with deltas
            SELECT 
                t.time_id,
                e.entity_id,
                'github_stars_delta' as metric_name,
                g.stars_delta as metric_value,
                'count' as metric_unit,
                'github' as provenance
            FROM parquet_scan('{0}/github_clean.parquet') g
            JOIN dim_time t ON date_trunc('day', g.collected_at::timestamp) = t.date
            JOIN dim_entity e ON g.name = e.name
            UNION ALL
            -- GitHub forks delta
            SELECT 
                t.time_id,
                e.entity_id,
                'github_forks_delta' as metric_name,
                g.forks_delta as metric_value,
                'count' as metric_unit,
                'github' as provenance
            FROM parquet_scan('{0}/github_clean.parquet') g
            JOIN dim_time t ON date_trunc('day', g.collected_at::timestamp) = t.date
            JOIN dim_entity e ON g.name = e.name
        """.format(STAGING_DIR))
        
        print("\nWarehouse loading completed successfully!")
        
        # Print some statistics
        print("\nWarehouse Statistics:")
        stats = {
            'dim_time': conn.execute("SELECT COUNT(*) FROM dim_time").fetchone()[0],
            'dim_entity': conn.execute("SELECT COUNT(*) FROM dim_entity").fetchone()[0],
            'dim_source': conn.execute("SELECT COUNT(*) FROM dim_source").fetchone()[0],
            'fact_hype_signals': conn.execute("SELECT COUNT(*) FROM fact_hype_signals").fetchone()[0],
            'fact_reality_signals': conn.execute("SELECT COUNT(*) FROM fact_reality_signals").fetchone()[0]
        }
        
        for table, count in stats.items():
            print(f"{table}: {count:,} rows")
            
    except Exception as e:
        print(f"Error loading warehouse: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    load_staging_data()