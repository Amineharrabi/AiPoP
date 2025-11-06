import os
import duckdb
from datetime import datetime
import pandas as pd
from pathlib import Path


def _flatten_col(col):
    # Convert tuple-like column names into a single string
    # Handles both actual tuples and string representations like "('Date', '')"
    try:
        if isinstance(col, tuple):
            parts = [str(c).strip() for c in col if c is not None and str(c).strip() != '']
            return '_'.join(parts) if parts else str(col)
        # Handle string representations of tuples like "('Date', '')" or "('Close', 'NVDA')"
        col_str = str(col).strip()
        if col_str.startswith('(') and col_str.endswith(')'):
            # Parse the tuple string
            import ast
            try:
                parsed = ast.literal_eval(col_str)
                if isinstance(parsed, tuple):
                    parts = [str(c).strip() for c in parsed if c is not None and str(c).strip() != '']
                    return '_'.join(parts) if parts else col_str
            except (ValueError, SyntaxError):
                pass
    except Exception:
        pass
    return str(col)


def _normalize_yfinance(path: str) -> pd.DataFrame:
    """Load a possibly pivoted yfinance parquet and return long-format DataFrame
    with columns: date (DATE), ticker, close, volume
    """
    df = pd.read_parquet(path)

    # Flatten multi-index column names if present
    df.columns = [_flatten_col(c) for c in df.columns]

    # Find date column
    date_col = None
    for c in df.columns:
        if c.lower().strip() in ('date', 'datetime') or c.lower().startswith('date'):
            date_col = c
            break
    if date_col is None:
        # try common fallback
        possible = [c for c in df.columns if 'date' in c.lower()]
        date_col = possible[0] if possible else df.columns[0]

    # Check if we have a ticker column - this might mean wide format with ticker-specific columns
    ticker_col = None
    for c in df.columns:
        if c.lower().strip() == 'ticker':
            ticker_col = c
            break
    
    # If we have a ticker column AND wide format columns (Close_NVDA, etc.), extract by ticker
    if ticker_col:
        # Check if we have ticker-specific columns (e.g., Close_NVDA, Close_MSFT)
        ticker_specific_close = [c for c in df.columns if 'close' in c.lower() and '_' in c]
        ticker_specific_vol = [c for c in df.columns if 'volume' in c.lower() and '_' in c]
        
        if ticker_specific_close or ticker_specific_vol:
            # Wide format: extract values based on ticker column
            records = []
            for idx, row in df.iterrows():
                ticker_val = str(row[ticker_col]).strip() if pd.notna(row[ticker_col]) else None
                if not ticker_val:
                    continue
                
                # Find matching columns for this ticker
                close_col = next((c for c in ticker_specific_close if c.endswith('_' + ticker_val)), None)
                vol_col = next((c for c in ticker_specific_vol if c.endswith('_' + ticker_val)), None)
                
                if close_col or vol_col:
                    record = {
                        'date': pd.to_datetime(row[date_col], errors='coerce'),
                        'ticker': ticker_val,
                        'close': row[close_col] if close_col and pd.notna(row.get(close_col)) else None,
                        'volume': row[vol_col] if vol_col and pd.notna(row.get(vol_col)) else None
                    }
                    records.append(record)
            
            if records:
                longdf = pd.DataFrame(records)
                longdf['date'] = pd.to_datetime(longdf['date'], errors='coerce').dt.normalize()
                return longdf[['date', 'ticker', 'close', 'volume']]
        
        # Otherwise, treat as long format
        long = df.rename(columns={date_col: 'date'})
        # Normalize column names
        cols = {c: c.lower() for c in long.columns}
        long = long.rename(columns=cols)
        if 'close' in long.columns and 'volume' in long.columns:
            return long[['date', 'ticker', 'close', 'volume']]
        # try to find close/volume variants
        close_col = next((c for c in long.columns if 'close' in c.lower() and c != 'close'), None)
        vol_col = next((c for c in long.columns if 'volume' in c.lower() and c != 'volume'), None)
        return long[['date', 'ticker'] + ([close_col] if close_col else []) + ([vol_col] if vol_col else [])]

    # Otherwise, wide/pivoted format: look for columns like Close_<TICKER> and Volume_<TICKER>
    close_cols = [c for c in df.columns if c.lower().startswith('close')]
    vol_cols = [c for c in df.columns if c.lower().startswith('volume')]

    # If no close columns, try variations
    if not close_cols:
        close_cols = [c for c in df.columns if 'close' in c.lower()]
    if not vol_cols:
        vol_cols = [c for c in df.columns if 'volume' in c.lower()]

    # Extract tickers from column names (primary source)
    # Column names like Close_NVDA, Volume_MSFT indicate tickers
    tickers = set()
    for c in close_cols:
        parts = c.split('_', 1)
        if len(parts) == 2 and parts[1].strip():
            tickers.add(parts[1])
    for c in vol_cols:
        parts = c.split('_', 1)
        if len(parts) == 2 and parts[1].strip():
            tickers.add(parts[1])
    
    # If no tickers found from column names but ticker column exists, use it
    if not tickers and 'ticker' in df.columns:
        tickers = set(df['ticker'].dropna().unique())

    records = []
    for tk in sorted(tickers):
        close_col = next((c for c in close_cols if c.endswith('_' + tk)), None)
        vol_col = next((c for c in vol_cols if c.endswith('_' + tk)), None)
        if close_col is None and vol_col is None:
            continue
        tmp = pd.DataFrame()
        tmp['date'] = pd.to_datetime(df[date_col], errors='coerce').dt.normalize()
        tmp['ticker'] = tk
        tmp['close'] = df[close_col] if close_col in df.columns else None
        tmp['volume'] = df[vol_col] if vol_col in df.columns else None
        records.append(tmp)

    if records:
        longdf = pd.concat(records, ignore_index=True)
        return longdf[['date', 'ticker', 'close', 'volume']]

    # Fallback: try to melt any remaining numeric columns by inferring tickers
    # As last resort, return empty DataFrame with expected columns
    return pd.DataFrame(columns=['date', 'ticker', 'close', 'volume'])

# Fixed paths for new structure
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
STAGING_DIR = os.path.join(BASE_DIR, 'data', 'staging')
WAREHOUSE_PATH = os.path.join(BASE_DIR, 'data', 'warehouse', 'ai_bubble.duckdb')

def load_staging_data():
    """Load all staged Parquet files into DuckDB warehouse with enhanced multi-source integration"""
    print(f"Loading data into warehouse: {WAREHOUSE_PATH}")
    
    # Connect to DuckDB
    conn = duckdb.connect(WAREHOUSE_PATH)

    # Debug: Print unique join keys and sample values from staging files
    import pandas as pd
    from pathlib import Path
    print("\n[DEBUG] Inspecting join keys in staging files...")
    try:
        # Debug: Print unique join keys and sample values from staging files
        try:
            reddit = pd.read_parquet(Path(STAGING_DIR) / 'reddit_clean.parquet')
            print("Reddit subreddits:", reddit['subreddit'].unique()[:10])
        except Exception as e:
            print("[DEBUG] Could not read reddit_clean.parquet:", e)
        try:
            news = pd.read_parquet(Path(STAGING_DIR) / 'news_clean.parquet')
            print("News companies:", news['company'].unique()[:10])
        except Exception as e:
            print("[DEBUG] Could not read news_clean.parquet:", e)
        try:
            arxiv = pd.read_parquet(Path(STAGING_DIR) / 'arxiv_clean.parquet')
            print("Arxiv search_terms:", arxiv['search_term'].unique()[:10])
        except Exception as e:
            print("[DEBUG] Could not read arxiv_clean.parquet:", e)
        try:
            github = pd.read_parquet(Path(STAGING_DIR) / 'github_clean.parquet')
            print("GitHub repo names:", github['name'].unique()[:10])
        except Exception as e:
            print("[DEBUG] Could not read github_clean.parquet:", e)
        try:
            hf = pd.read_parquet(Path(STAGING_DIR) / 'huggingface_clean.parquet')
            print("HF model_ids:", hf['model_id'].unique()[:10])
        except Exception as e:
            print("[DEBUG] Could not read huggingface_clean.parquet:", e)
        # Print dim_entity names after entity load
        try:
            if os.path.exists(WAREHOUSE_PATH):
                conn_check = duckdb.connect(WAREHOUSE_PATH)
                entities = conn_check.execute("SELECT name FROM dim_entity").fetchdf()
                print("[DEBUG] dim_entity names:", entities['name'].unique()[:10])
                conn_check.close()
        except Exception as e:
            print("[DEBUG] Could not read dim_entity from warehouse:", e)

        # 1. Load time dimension (extract unique timestamps from all sources)
        print("\nLoading time dimension...")

        # Normalize yfinance if needed and register as a temp table
        yfinance_path = Path(STAGING_DIR) / 'yfinance_clean.parquet'
        if yfinance_path.exists():
            try:
                yfinance_long = _normalize_yfinance(str(yfinance_path))
                if not yfinance_long.empty:
                    conn.register('yfinance_norm', yfinance_long)
            except Exception as e:
                print('Warning: could not normalize yfinance file:', e)

        # Build unified_dates using either the normalized yfinance or parquet scans
        yfinance_source = 'yfinance_norm' if 'yfinance_norm' in conn.execute("SHOW TABLES").fetchdf()['name'].tolist() else f"parquet_scan('{STAGING_DIR}/yfinance_clean.parquet')"

        # Get the maximum existing time_id to avoid conflicts
        max_time_id_result = conn.execute("SELECT COALESCE(MAX(time_id), 0) as max_id FROM dim_time").fetchone()
        max_time_id = max_time_id_result[0] if max_time_id_result else 0

        conn.execute(f"""
            INSERT INTO dim_time (
                time_id, date, unix_ts, year, month, week, is_business_day
            )
            WITH unified_dates AS (
                -- Stock data dates
                SELECT DISTINCT date FROM {yfinance_source}
                UNION
                -- Reddit post dates
                SELECT DISTINCT CAST(to_timestamp(created_utc) AS DATE) as date 
                FROM parquet_scan('{STAGING_DIR}/reddit_clean.parquet')
                UNION
                -- News article dates
                SELECT DISTINCT CAST(publishedAt AS DATE) as date 
                FROM parquet_scan('{STAGING_DIR}/news_clean.parquet')
                UNION
                -- ArXiv paper dates
                SELECT DISTINCT CAST(published AS DATE) as date 
                FROM parquet_scan('{STAGING_DIR}/arxiv_clean.parquet')
                UNION
                -- GitHub dates
                SELECT DISTINCT CAST(collected_at AS DATE) as date 
                FROM parquet_scan('{STAGING_DIR}/github_clean.parquet')
                UNION
                -- HuggingFace dates
                SELECT DISTINCT CAST(collected_at AS DATE) as date 
                FROM parquet_scan('{STAGING_DIR}/huggingface_clean.parquet')
            )
            SELECT 
                {max_time_id} + ROW_NUMBER() OVER (ORDER BY date) as time_id,
                date,
                CAST(EXTRACT(epoch FROM date) AS BIGINT) as unix_ts,
                EXTRACT(year FROM date) as year,
                EXTRACT(month FROM date) as month,
                EXTRACT(week FROM date) as week,
                CASE WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN FALSE ELSE TRUE END as is_business_day
            FROM unified_dates
            WHERE date IS NOT NULL
            AND date NOT IN (SELECT date FROM dim_time)
        """)
        
        # 2. Load entity dimension
        print("Loading entity dimension...")
        # Prefer normalized yfinance table if registered
        yfinance_source = 'yfinance_norm' if 'yfinance_norm' in conn.execute("SHOW TABLES").fetchdf()['name'].tolist() else f"parquet_scan('{STAGING_DIR}/yfinance_clean.parquet')"
        # NOTE: To ensure all join keys match, you may need to further normalize names (e.g., lowercase, strip, map aliases) for subreddits, companies, etc.
        # If you want to include all subreddits and companies as entities, you could UNION them in below as well.
        # Get the maximum existing entity_id to avoid conflicts
        max_entity_id_result = conn.execute("SELECT COALESCE(MAX(entity_id), 0) as max_id FROM dim_entity").fetchone()
        max_entity_id = max_entity_id_result[0] if max_entity_id_result else 0
        
        conn.execute(f"""
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
                FROM {yfinance_source}
                UNION
                -- AI Models from HuggingFace
                SELECT DISTINCT
                    name,
                    model_id as ticker,
                    'ai_model' as entity_type,
                    COALESCE(pipeline_tag, 'general') as industry
                    FROM parquet_scan('{STAGING_DIR}/huggingface_clean.parquet')
                UNION
                -- GitHub repositories
                SELECT DISTINCT
                    name,
                    name as ticker,
                    'software' as entity_type,
                    'ai_infrastructure' as industry
                    FROM parquet_scan('{STAGING_DIR}/github_clean.parquet')
                UNION
                -- ArXiv research topics
                SELECT DISTINCT
                    search_term as name,
                    search_term as ticker,
                    'research' as entity_type,
                    'academic' as industry
                    FROM parquet_scan('{STAGING_DIR}/arxiv_clean.parquet')
                UNION
                -- Reddit subreddits as entities (optional, for join compatibility)
                SELECT DISTINCT
                    subreddit as name,
                    subreddit as ticker,
                    'reddit' as entity_type,
                    'social' as industry
                    FROM parquet_scan('{STAGING_DIR}/reddit_clean.parquet')
                UNION
                -- News companies as entities (optional, for join compatibility)
                SELECT DISTINCT
                    company as name,
                    company as ticker,
                    'company' as entity_type,
                    'news' as industry
                    FROM parquet_scan('{STAGING_DIR}/news_clean.parquet')
            )
            SELECT 
                {max_entity_id} + ROW_NUMBER() OVER (ORDER BY name) as entity_id,
                name,
                ticker,
                entity_type,
                industry
            FROM unified_entities
            WHERE name IS NOT NULL
            AND name NOT IN (SELECT name FROM dim_entity)
        """)
        
        # 3. Load source dimension
        print("Loading source dimension...")
        # Get the maximum existing source_id to avoid conflicts
        max_source_id_result = conn.execute("SELECT COALESCE(MAX(source_id), 0) as max_id FROM dim_source").fetchone()
        max_source_id = max_source_id_result[0] if max_source_id_result else 0
        
        conn.execute(f"""
            INSERT INTO dim_source (
                source_id, source_name, source_type
            )
            SELECT 
                {max_source_id} + ROW_NUMBER() OVER (ORDER BY source_name) as source_id,
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
            JOIN dim_time t ON CAST(to_timestamp(r.created_utc) AS DATE) = t.date
            JOIN dim_entity e ON r.subreddit = e.name
            JOIN dim_source s ON s.source_name = 'reddit'
            WHERE r.ai_sentiment_score IS NOT NULL
            AND r.subreddit IS NOT NULL
            AND r.created_utc IS NOT NULL
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
            JOIN dim_time t ON CAST(n.publishedAt AS DATE) = t.date
            JOIN dim_entity e ON n.company = e.name
            JOIN dim_source s ON s.source_name = 'news'
            WHERE n.ai_sentiment_score IS NOT NULL
            AND n.company IS NOT NULL
            AND n.publishedAt IS NOT NULL
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
            JOIN dim_time t ON CAST(g.collected_at AS DATE) = t.date
            JOIN dim_entity e ON g.name = e.name
            JOIN dim_source s ON s.source_name = 'github'
            WHERE g.trending_score IS NOT NULL
            AND g.name IS NOT NULL
            AND g.collected_at IS NOT NULL
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
            JOIN dim_time t ON CAST(h.collected_at AS DATE) = t.date
            JOIN dim_entity e ON h.model_id = e.ticker
            JOIN dim_source s ON s.source_name = 'huggingface'
            WHERE h.trending_score IS NOT NULL
            AND h.model_id IS NOT NULL
            AND h.collected_at IS NOT NULL
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
            JOIN dim_time t ON CAST(a.published AS DATE) = t.date
            JOIN dim_entity e ON a.search_term = e.name
            JOIN dim_source s ON s.source_name = 'arxiv'
            WHERE a.impact_score IS NOT NULL
            AND a.search_term IS NOT NULL
            AND a.published IS NOT NULL
        """.format(STAGING_DIR))
        
        # 5. Load enhanced reality signals fact table
        print("Loading enhanced reality signals...")
        conn.execute(r"""
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
            FROM {yfinance_source} y
            JOIN dim_time t ON y.date = t.date
            JOIN dim_entity e ON y.ticker = e.ticker
            WHERE y.close IS NOT NULL
            AND y.ticker IS NOT NULL
            AND y.date IS NOT NULL
            UNION ALL
            -- Stock volume
            SELECT 
                t.time_id,
                e.entity_id,
                'trading_volume' as metric_name,
                y.volume as metric_value,
                'shares' as metric_unit,
                'yfinance' as provenance
            FROM {yfinance_source} y
            JOIN dim_time t ON y.date = t.date
            JOIN dim_entity e ON y.ticker = e.ticker
            WHERE y.volume IS NOT NULL
            AND y.ticker IS NOT NULL
            AND y.date IS NOT NULL
            UNION ALL
            -- HuggingFace model adoption with deltas
            SELECT 
                t.time_id,
                e.entity_id,
                'model_downloads_delta' as metric_name,
                h.downloads_delta as metric_value,
                'count' as metric_unit,
                'huggingface' as provenance
            FROM parquet_scan('{huggingface}') h
            JOIN dim_time t ON CAST(h.collected_at AS DATE) = t.date
            JOIN dim_entity e ON h.model_id = e.ticker
            WHERE h.downloads_delta IS NOT NULL
            AND h.model_id IS NOT NULL
            AND h.collected_at IS NOT NULL
            UNION ALL
            -- GitHub project metrics with deltas
            SELECT 
                t.time_id,
                e.entity_id,
                'github_stars_delta' as metric_name,
                g.stars_delta as metric_value,
                'count' as metric_unit,
                'github' as provenance
            FROM parquet_scan('{github}') g
            JOIN dim_time t ON CAST(g.collected_at AS DATE) = t.date
            JOIN dim_entity e ON g.name = e.name
            WHERE g.stars_delta IS NOT NULL
            AND g.name IS NOT NULL
            AND g.collected_at IS NOT NULL
            UNION ALL
            -- GitHub forks delta
            SELECT 
                t.time_id,
                e.entity_id,
                'github_forks_delta' as metric_name,
                g.forks_delta as metric_value,
                'count' as metric_unit,
                'github' as provenance
            FROM parquet_scan('{github}') g
            JOIN dim_time t ON CAST(g.collected_at AS DATE) = t.date
            JOIN dim_entity e ON g.name = e.name
            WHERE g.forks_delta IS NOT NULL
            AND g.name IS NOT NULL
            AND g.collected_at IS NOT NULL
        """.format(
            yfinance_source=yfinance_source,
            huggingface=str(Path(STAGING_DIR) / 'huggingface_clean.parquet'),
            github=str(Path(STAGING_DIR) / 'github_clean.parquet')
        ))

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
    finally:
        conn.close()

if __name__ == "__main__":
    load_staging_data()