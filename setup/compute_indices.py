import os
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

# Fixed paths for new structure
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
WAREHOUSE_PATH = os.path.join(BASE_DIR, 'data', 'warehouse', 'ai_bubble.duckdb')

def compute_hype_index(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Compute the enhanced AI Hype Index based on multi-source sentiment, trending, and innovation metrics
    Uses weighted averages of normalized metrics across entities from ALL sources
    """
    print("Computing Enhanced Multi-Source Hype Index...")
    
    conn.execute("""
    CREATE OR REPLACE TABLE hype_index AS
    WITH 
    -- Enhanced entity weights based on multi-source engagement
    -- Entity weights: if mention counts are not available in combined_features,
    -- fall back to using multi-source intensity and trending proxies.
    entity_weights AS (
        SELECT 
            entity_id,
            entity_name,
            entity_type,
            CASE 
                WHEN entity_type = 'company' THEN 
                    (AVG(COALESCE(multi_source_hype_intensity, 0)) + 
                     AVG(COALESCE(trending_intensity, 0))) * 2  -- Higher weight for companies
                WHEN entity_type = 'software' THEN 
                    (AVG(COALESCE(github_trending_score, 0)) + 
                     AVG(COALESCE(hf_trending_score, 0))) * 1.5
                WHEN entity_type = 'ai_model' THEN 
                    (AVG(COALESCE(hf_trending_score, 0)) + 
                     AVG(COALESCE(arxiv_innovation_score, 0) * 100)) * 1.3
                ELSE 
                    AVG(COALESCE(arxiv_innovation_score, 0)) + AVG(COALESCE(multi_source_hype_intensity, 0))
            END as entity_weight
        FROM combined_features
        GROUP BY entity_id, entity_name, entity_type
    ),
    -- Normalized multi-source daily metrics
    normalized_metrics AS (
        SELECT 
            cf.*,
            ew.entity_weight,
            -- Normalize all hype metrics to 0-1 scale within each entity
            COALESCE(multi_source_hype_intensity / NULLIF(MAX(multi_source_hype_intensity) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_hype_intensity,
            COALESCE(reddit_sentiment_momentum / NULLIF(MAX(ABS(reddit_sentiment_momentum)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_reddit_momentum,
            COALESCE(news_sentiment_momentum / NULLIF(MAX(ABS(news_sentiment_momentum)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_news_momentum,
            COALESCE(github_trending_momentum / NULLIF(MAX(ABS(github_trending_momentum)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_github_trending,
            COALESCE(trending_intensity / NULLIF(MAX(trending_intensity) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_trending_intensity,
            COALESCE(arxiv_innovation_score / NULLIF(MAX(arxiv_innovation_score) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_innovation
        FROM combined_features cf
        JOIN entity_weights ew ON cf.entity_id = ew.entity_id
    )
    SELECT 
        time_id,
        date,
        -- Weighted average of multi-source normalized metrics across entities
        SUM(
            entity_weight * (
                norm_hype_intensity * 0.25 +
                (norm_reddit_momentum + norm_news_momentum) * 0.2 +
                norm_github_trending * 0.15 +
                norm_trending_intensity * 0.15 +
                norm_innovation * 0.25
            )
        ) / NULLIF(SUM(entity_weight), 0) as hype_index,
        -- Component contributions
        SUM(entity_weight * norm_hype_intensity) / NULLIF(SUM(entity_weight), 0) as hype_intensity_component,
        SUM(entity_weight * (norm_reddit_momentum + norm_news_momentum) / 2) / NULLIF(SUM(entity_weight), 0) as sentiment_momentum_component,
        SUM(entity_weight * norm_github_trending) / NULLIF(SUM(entity_weight), 0) as github_trending_component,
        SUM(entity_weight * norm_trending_intensity) / NULLIF(SUM(entity_weight), 0) as trending_component,
        SUM(entity_weight * norm_innovation) / NULLIF(SUM(entity_weight), 0) as innovation_component,
        -- Additional statistics
    COUNT(DISTINCT entity_id) as entity_count,
    -- If mention count columns are not available, provide a proxy using intensity metrics
    SUM(COALESCE(multi_source_hype_intensity, 0) + COALESCE(trending_intensity, 0)) as total_mentions_proxy
    FROM normalized_metrics
    GROUP BY time_id, date
    ORDER BY date;
    """)

def compute_reality_index(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Compute the enhanced Reality Index based on stock performance, GitHub/HF deltas, and adoption metrics
    """
    print("Computing Enhanced Multi-Source Reality Index...")
    
    conn.execute("""
    CREATE OR REPLACE TABLE reality_index AS
    WITH 
    -- Enhanced entity weights based on market cap/adoption with delta metrics
    entity_weights AS (
        SELECT 
            entity_id,
            entity_name,
            entity_type,
            CASE 
                WHEN entity_type = 'company' THEN 
                    (AVG(COALESCE(stock_price, 0)) + 
                     AVG(ABS(github_stars_delta)) * 100 + 
                     AVG(ABS(hf_downloads_delta)) * 50) * 2  -- Higher weight for established companies
                WHEN entity_type = 'software' THEN 
                    (AVG(ABS(github_stars_delta)) * 100 + 
                     AVG(ABS(github_forks_delta)) * 50)
                WHEN entity_type = 'ai_model' THEN 
                    (AVG(hf_downloads_delta) * 50 + 
                     AVG(ABS(hf_downloads_delta)) * 100)
                ELSE 
                    AVG(ABS(github_stars_delta)) * 100 + AVG(ABS(hf_downloads_delta)) * 50
            END as entity_weight
        FROM combined_features
        GROUP BY entity_id, entity_name, entity_type
    ),
    -- Normalized daily metrics with delta integration
    normalized_metrics AS (
        SELECT 
            cf.*,
            ew.entity_weight,
            -- Normalize metrics to 0-1 scale within each entity
            COALESCE(reality_strength_score / NULLIF(MAX(reality_strength_score) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_reality_strength,
            COALESCE(price_30d_growth / NULLIF(MAX(ABS(price_30d_growth)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_price_growth,
            COALESCE(github_delta_30d_growth / NULLIF(MAX(ABS(github_delta_30d_growth)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_github_delta,
            COALESCE(hf_downloads_30d_growth / NULLIF(MAX(ABS(hf_downloads_30d_growth)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_hf_downloads,
            -- Cumulative impact normalization
            COALESCE(github_stars_cumulative_delta / NULLIF(MAX(ABS(github_stars_cumulative_delta)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_github_cumulative,
            COALESCE(hf_downloads_cumulative_delta / NULLIF(MAX(ABS(hf_downloads_cumulative_delta)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_hf_cumulative
        FROM combined_features cf
        JOIN entity_weights ew ON cf.entity_id = ew.entity_id
    )
    SELECT 
        time_id,
        date,
        -- Weighted average of normalized metrics across entities
        SUM(
            entity_weight * (
                norm_reality_strength * 0.3 +
                COALESCE(norm_price_growth, 0) * 0.2 +
                COALESCE(norm_github_delta, 0) * 0.25 +
                COALESCE(norm_hf_downloads, 0) * 0.25
            )
        ) / NULLIF(SUM(entity_weight), 0) as reality_index,
        -- Component contributions
        SUM(entity_weight * norm_reality_strength) / NULLIF(SUM(entity_weight), 0) as reality_strength_component,
        SUM(entity_weight * COALESCE(norm_price_growth, 0)) / NULLIF(SUM(entity_weight), 0) as price_growth_component,
        SUM(entity_weight * COALESCE(norm_github_delta, 0)) / NULLIF(SUM(entity_weight), 0) as github_delta_component,
        SUM(entity_weight * COALESCE(norm_hf_downloads, 0)) / NULLIF(SUM(entity_weight), 0) as hf_downloads_component,
        -- Cumulative impact component
        SUM(entity_weight * (COALESCE(norm_github_cumulative, 0) + COALESCE(norm_hf_cumulative, 0)) / 2) / NULLIF(SUM(entity_weight), 0) as cumulative_impact_component,
        -- Additional statistics
        COUNT(DISTINCT entity_id) as entity_count,
        AVG(CASE WHEN entity_type = 'company' THEN price_30d_growth ELSE NULL END) as avg_company_growth,
        AVG(ABS(github_delta_30d_growth)) as avg_github_momentum,
        AVG(ABS(hf_downloads_30d_growth)) as avg_hf_momentum
    FROM normalized_metrics
    GROUP BY time_id, date
    ORDER BY date;
    """)

def compute_bubble_metrics(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Compute enhanced bubble detection metrics combining multi-source Hype and Reality indices
    Ensures exactly ONE point per date for clean visualization
    """
    print("Computing enhanced bubble metrics with multi-source integration...")
    
    # Drop and recreate table to ensure clean structure with proper primary key
    conn.execute("DROP TABLE IF EXISTS bubble_metrics;")
    conn.execute("""
    CREATE TABLE bubble_metrics (
        date DATE PRIMARY KEY,
        time_id INTEGER,
        computed_at TIMESTAMP,
        hype_index DOUBLE,
        reality_index DOUBLE,
        hype_7d_avg DOUBLE,
        reality_7d_avg DOUBLE,
        hype_30d_avg DOUBLE,
        reality_30d_avg DOUBLE,
        hype_reality_gap DOUBLE,
        hype_reality_ratio DOUBLE,
        bubble_momentum DOUBLE,
        sentiment_bubble_momentum DOUBLE,
        github_trending_bubble_momentum DOUBLE,
        trending_reality_divergence DOUBLE,
        hype_score DOUBLE,
        reality_score DOUBLE,
        bubble_risk_score DOUBLE
    );
    """)
    
    # Compute and insert/update metrics ensuring one point per date
    conn.execute("""
    CREATE OR REPLACE TEMP TABLE temp_bubble_metrics AS
    WITH 
    -- Get the latest time_id for each date to ensure one point per day
    daily_data AS (
        SELECT DISTINCT ON (h.date)
            h.time_id,
            h.date,
            h.hype_index,
            h.sentiment_momentum_component,
            h.github_trending_component,
            h.trending_component,
            h.innovation_component,
            r.reality_index,
            r.reality_strength_component,
            r.github_delta_component,
            r.hf_downloads_component,
            r.cumulative_impact_component
        FROM hype_index h
        JOIN reality_index r ON h.date = r.date
        ORDER BY h.date, h.time_id DESC
    ),
    -- Compute rolling averages with one point per day
    rolling_metrics AS (
        SELECT 
            time_id,
            date,
            hype_index,
            sentiment_momentum_component,
            github_trending_component,
            trending_component,
            innovation_component,
            reality_index,
            reality_strength_component,
            github_delta_component,
            hf_downloads_component,
            cumulative_impact_component,
            -- 7-day rolling averages
            AVG(hype_index) OVER (
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as hype_7d_avg,
            AVG(reality_index) OVER (
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as reality_7d_avg,
            -- 30-day rolling averages
            AVG(hype_index) OVER (
                ORDER BY date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as hype_30d_avg,
            AVG(reality_index) OVER (
                ORDER BY date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as reality_30d_avg,
            -- Component rolling averages
            AVG(sentiment_momentum_component) OVER (
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as sentiment_7d_avg,
            AVG(github_trending_component) OVER (
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as github_trend_7d_avg,
            AVG(reality_strength_component) OVER (
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as reality_strength_7d_avg,
            AVG(github_delta_component) OVER (
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as reality_github_7d_avg
        FROM daily_data
    )
    SELECT 
        date,  -- Date is now the primary key
        time_id,
        CURRENT_TIMESTAMP as computed_at,
        hype_index,
        reality_index,
        hype_7d_avg,
        reality_7d_avg,
        hype_30d_avg,
        reality_30d_avg,
        -- Enhanced divergence metrics
        (hype_index - reality_index) as hype_reality_gap,
        CASE 
            WHEN reality_index = 0 THEN 0
            ELSE hype_index / NULLIF(reality_index, 0)
        END as hype_reality_ratio,
        -- Bubble indicators with multi-source components
        (hype_7d_avg - reality_7d_avg) - (hype_30d_avg - reality_30d_avg) as bubble_momentum,
        -- Component-specific momentum gaps
        (sentiment_7d_avg - reality_strength_7d_avg) as sentiment_bubble_momentum,
        (github_trend_7d_avg - reality_github_7d_avg) as github_trending_bubble_momentum,
        -- Trending divergence indicator
        (trending_component - cumulative_impact_component) as trending_reality_divergence,
        -- Normalized scores (0-100 scale)
        CASE 
            WHEN MAX(hype_index) OVER () = MIN(hype_index) OVER () THEN 50.0
            ELSE 100 * (hype_index - MIN(hype_index) OVER ()) / 
                NULLIF(MAX(hype_index) OVER () - MIN(hype_index) OVER (), 0)
        END as hype_score,
        CASE 
            WHEN MAX(reality_index) OVER () = MIN(reality_index) OVER () THEN 50.0
            ELSE 100 * (reality_index - MIN(reality_index) OVER ()) / 
                NULLIF(MAX(reality_index) OVER () - MIN(reality_index) OVER (), 0)
        END as reality_score,
        -- Composite bubble risk score
        (
            ABS(hype_index - reality_index) * 0.4 +
            ABS((hype_7d_avg - reality_7d_avg) - (hype_30d_avg - reality_30d_avg)) * 0.3 +
            ABS(trending_component - cumulative_impact_component) * 0.3
        ) * 100 as bubble_risk_score
    FROM rolling_metrics
    ORDER BY date;
    """)
    
    # Use INSERT OR REPLACE to ensure only one row per date
    conn.execute("""
    INSERT OR REPLACE INTO bubble_metrics
    SELECT * FROM temp_bubble_metrics;
    """)
    
    print(f"Bubble metrics computed successfully")

def compute_entity_bubble_metrics(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Compute per-entity bubble metrics with multi-source integration
    """
    print("Computing entity-level bubble metrics with multi-source integration...")

    conn.execute("""
    CREATE OR REPLACE TABLE entity_bubble_metrics AS
    WITH base AS (
        SELECT
            cf.time_id,
            cf.date,
            cf.entity_id,
            cf.entity_name,
            cf.entity_type,
            cf.multi_source_hype_intensity,
            cf.reality_strength_score,
            cf.hype_reality_gap,
            -- Multi-source entity metrics
            cf.reddit_sentiment_score,
            cf.github_trending_score,
            cf.hf_trending_score,
            cf.arxiv_innovation_score,
            cf.github_stars_delta,
            cf.hf_downloads_delta
        FROM combined_features cf
    ),
    rolling AS (
        SELECT
            *,
            AVG(multi_source_hype_intensity) OVER (
                PARTITION BY entity_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as hype_7d_avg,
            AVG(reality_strength_score) OVER (
                PARTITION BY entity_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as reality_7d_avg,
            AVG(multi_source_hype_intensity) OVER (
                PARTITION BY entity_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as hype_30d_avg,
            AVG(reality_strength_score) OVER (
                PARTITION BY entity_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as reality_30d_avg
        FROM base
    )
    SELECT
        time_id,
        date,
        entity_id,
        entity_name,
        entity_type,
        multi_source_hype_intensity,
        reality_strength_score,
        hype_7d_avg,
        reality_7d_avg,
        hype_30d_avg,
        reality_30d_avg,
        hype_reality_gap,
        CASE WHEN reality_strength_score = 0 THEN 0 ELSE multi_source_hype_intensity / NULLIF(reality_strength_score,0) END as hype_reality_ratio,
        (hype_7d_avg - reality_7d_avg) - (hype_30d_avg - reality_30d_avg) as bubble_momentum,
        -- Multi-source component tracking
        reddit_sentiment_score,
        github_trending_score,
        hf_trending_score,
        arxiv_innovation_score,
        github_stars_delta,
        hf_downloads_delta,
        -- Entity-specific trending momentum
        (github_trending_score - LAG(github_trending_score, 7) OVER (PARTITION BY entity_id ORDER BY date)) as github_trending_momentum,
        (hf_trending_score - LAG(hf_trending_score, 7) OVER (PARTITION BY entity_id ORDER BY date)) as hf_trending_momentum,
        -- Per-entity normalized scores (0-100)
        -- Handle case where min = max (all values same) by returning 50 as neutral score
        CASE 
            WHEN MAX(multi_source_hype_intensity) OVER (PARTITION BY entity_id) = MIN(multi_source_hype_intensity) OVER (PARTITION BY entity_id) THEN 50.0
            ELSE 100 * (multi_source_hype_intensity - MIN(multi_source_hype_intensity) OVER (PARTITION BY entity_id)) /
                NULLIF((MAX(multi_source_hype_intensity) OVER (PARTITION BY entity_id) - MIN(multi_source_hype_intensity) OVER (PARTITION BY entity_id)), 0)
        END as hype_score,
        CASE 
            WHEN MAX(reality_strength_score) OVER (PARTITION BY entity_id) = MIN(reality_strength_score) OVER (PARTITION BY entity_id) THEN 50.0
            ELSE 100 * (reality_strength_score - MIN(reality_strength_score) OVER (PARTITION BY entity_id)) /
                NULLIF((MAX(reality_strength_score) OVER (PARTITION BY entity_id) - MIN(reality_strength_score) OVER (PARTITION BY entity_id)), 0)
        END as reality_score
    FROM rolling
    ORDER BY entity_id, date;
    """)

def main():
    """Main function to compute all enhanced indices"""
    print(f"Computing enhanced multi-source indices from warehouse: {WAREHOUSE_PATH}")
    
    conn = duckdb.connect(WAREHOUSE_PATH)
    
    try:
        # Ensure we have the required enhanced features
        if not conn.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'combined_features')").fetchone()[0]:
            raise Exception("Combined features table not found. Please run compute_features.py first.")
        
        # Compute enhanced indices with multi-source integration
        compute_hype_index(conn)
        compute_reality_index(conn)
        compute_bubble_metrics(conn)
        compute_entity_bubble_metrics(conn)
        
        # Print enhanced statistics
        print("\nEnhanced Index Statistics:")
        latest_metrics = conn.execute("""
            SELECT 
                hype_score,
                reality_score,
                hype_reality_gap,
                bubble_momentum,
                trending_reality_divergence,
                bubble_risk_score
            FROM bubble_metrics 
            WHERE computed_at IS NOT NULL
            ORDER BY computed_at DESC 
            LIMIT 1
        """).fetchone()
        
        if latest_metrics:
            print(f"Latest Hype Score: {latest_metrics[0]:.2f}")
            print(f"Latest Reality Score: {latest_metrics[1]:.2f}")
            print(f"Current Hype-Reality Gap: {latest_metrics[2]:.2f}")
            print(f"Bubble Momentum: {latest_metrics[3]:.2f}")
            print(f"Trending Reality Divergence: {latest_metrics[4]:.2f}")
            print(f"Bubble Risk Score: {latest_metrics[5]:.2f}")
        
        # Show sample of multi-source bubble detection
        print("\nTop entities by bubble risk score (latest date per entity):")
        top_risks = conn.execute("""
            WITH latest_metrics AS (
                SELECT 
                    entity_id,
                    MAX(date) as latest_date
                FROM entity_bubble_metrics
                WHERE COALESCE(entity_name, '') != ''
                GROUP BY entity_id
            ),
            ranked_entities AS (
                SELECT 
                    e.entity_id,
                    COALESCE(e.entity_name, 'Unknown') as entity_name,
                    e.entity_type,
                    e.hype_score,
                    e.reality_score,
                    e.bubble_momentum,
                    e.github_trending_score,
                    e.hf_trending_score,
                    e.reddit_sentiment_score,
                    ROW_NUMBER() OVER (PARTITION BY e.entity_id ORDER BY e.date DESC) as rn
                FROM entity_bubble_metrics e
                JOIN latest_metrics lm ON e.entity_id = lm.entity_id AND e.date = lm.latest_date
                WHERE COALESCE(e.entity_name, '') != ''
                AND (e.hype_score IS NOT NULL OR e.reality_score IS NOT NULL)
            )
            SELECT 
                entity_name,
                entity_type,
                ROUND(COALESCE(hype_score, 0), 2) as hype_score,
                ROUND(COALESCE(reality_score, 0), 2) as reality_score,
                ROUND(COALESCE(bubble_momentum, 0), 4) as bubble_momentum,
                ROUND(COALESCE(github_trending_score, 0), 2) as github_trending_score,
                ROUND(COALESCE(hf_trending_score, 0), 2) as hf_trending_score,
                ROUND(COALESCE(reddit_sentiment_score, 0), 2) as reddit_sentiment_score
            FROM ranked_entities
            WHERE rn = 1
            ORDER BY ABS(COALESCE(bubble_momentum, 0)) DESC 
            LIMIT 10
        """).fetchdf()
        
        # Format the output nicely with proper display options
        if not top_risks.empty:
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 200)
            pd.set_option('display.max_colwidth', 40)
            pd.set_option('display.float_format', lambda x: f'{x:.2f}' if pd.notna(x) else 'N/A')
            print("\n" + top_risks.to_string(index=False))
        else:
            print("No entities found with sufficient data.")
        
        print("\nEnhanced index computation completed successfully!")
        
    except Exception as e:
        print(f"Error computing indices: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()