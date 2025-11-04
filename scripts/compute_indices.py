import os
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Tuple

# Setup paths
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
WAREHOUSE_PATH = os.path.join(BASE_DIR, 'data', 'warehouse', 'ai_bubble.duckdb')

def compute_hype_index(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Compute the AI Hype Index based on sentiment, mentions, and innovation metrics
    Uses weighted averages of normalized metrics across entities
    """
    print("Computing Hype Index...")
    
    conn.execute("""
    CREATE OR REPLACE TABLE hype_index AS
    WITH 
    -- Entity weights based on market cap/popularity
    entity_weights AS (
        SELECT 
            entity_id,
            entity_name,
            entity_type,
            CASE 
                WHEN entity_type = 'company' THEN 
                    AVG(COALESCE(mention_count, 0)) * 2  -- Higher weight for companies
                ELSE 
                    AVG(COALESCE(mention_count, 0))
            END as entity_weight
        FROM combined_features
        GROUP BY entity_id, entity_name, entity_type
    ),
    -- Normalized daily metrics
    normalized_metrics AS (
        SELECT 
            cf.*,
            ew.entity_weight,
            -- Normalize metrics to 0-1 scale within each entity
            hype_intensity_score / NULLIF(MAX(hype_intensity_score) OVER (PARTITION BY cf.entity_id), 0) as norm_hype_intensity,
            sentiment_momentum / NULLIF(MAX(ABS(sentiment_momentum)) OVER (PARTITION BY cf.entity_id), 0) as norm_sentiment_momentum,
            mention_momentum / NULLIF(MAX(ABS(mention_momentum)) OVER (PARTITION BY cf.entity_id), 0) as norm_mention_momentum
        FROM combined_features cf
        JOIN entity_weights ew ON cf.entity_id = ew.entity_id
    )
    SELECT 
        time_id,
        date,
        -- Weighted average of normalized metrics across entities
        SUM(
            entity_weight * (
                norm_hype_intensity * 0.4 +
                norm_sentiment_momentum * 0.3 +
                norm_mention_momentum * 0.3
            )
        ) / NULLIF(SUM(entity_weight), 0) as hype_index,
        -- Component contributions
        SUM(entity_weight * norm_hype_intensity) / NULLIF(SUM(entity_weight), 0) as hype_intensity_component,
        SUM(entity_weight * norm_sentiment_momentum) / NULLIF(SUM(entity_weight), 0) as sentiment_momentum_component,
        SUM(entity_weight * norm_mention_momentum) / NULLIF(SUM(entity_weight), 0) as mention_momentum_component,
        -- Additional statistics
        COUNT(DISTINCT entity_id) as entity_count,
        SUM(mention_count) as total_mentions
    FROM normalized_metrics
    GROUP BY time_id, date
    ORDER BY date;
    """)

def compute_reality_index(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Compute the Reality Index based on stock performance, adoption metrics, and real-world impact
    """
    print("Computing Reality Index...")
    
    conn.execute("""
    CREATE OR REPLACE TABLE reality_index AS
    WITH 
    -- Entity weights based on market cap/adoption
    entity_weights AS (
        SELECT 
            entity_id,
            entity_name,
            entity_type,
            CASE 
                WHEN entity_type = 'company' THEN 
                    AVG(COALESCE(stock_price, 0)) * 2  -- Higher weight for established companies
                ELSE 
                    AVG(COALESCE(daily_downloads, 0))
            END as entity_weight
        FROM combined_features
        GROUP BY entity_id, entity_name, entity_type
    ),
    -- Normalized daily metrics
    normalized_metrics AS (
        SELECT 
            cf.*,
            ew.entity_weight,
            -- Normalize metrics to 0-1 scale within each entity
            reality_strength_score / NULLIF(MAX(reality_strength_score) OVER (PARTITION BY cf.entity_id), 0) as norm_reality_strength,
            price_30d_growth / NULLIF(MAX(ABS(price_30d_growth)) OVER (PARTITION BY cf.entity_id), 0) as norm_price_growth,
            stars_30d_growth / NULLIF(MAX(ABS(stars_30d_growth)) OVER (PARTITION BY cf.entity_id), 0) as norm_adoption_growth
        FROM combined_features cf
        JOIN entity_weights ew ON cf.entity_id = ew.entity_id
    )
    SELECT 
        time_id,
        date,
        -- Weighted average of normalized metrics across entities
        SUM(
            entity_weight * (
                norm_reality_strength * 0.4 +
                COALESCE(norm_price_growth, 0) * 0.3 +
                COALESCE(norm_adoption_growth, 0) * 0.3
            )
        ) / NULLIF(SUM(entity_weight), 0) as reality_index,
        -- Component contributions
        SUM(entity_weight * norm_reality_strength) / NULLIF(SUM(entity_weight), 0) as reality_strength_component,
        SUM(entity_weight * COALESCE(norm_price_growth, 0)) / NULLIF(SUM(entity_weight), 0) as price_growth_component,
        SUM(entity_weight * COALESCE(norm_adoption_growth, 0)) / NULLIF(SUM(entity_weight), 0) as adoption_growth_component,
        -- Additional statistics
        COUNT(DISTINCT entity_id) as entity_count,
        AVG(CASE WHEN entity_type = 'company' THEN price_30d_growth ELSE NULL END) as avg_company_growth
    FROM normalized_metrics
    GROUP BY time_id, date
    ORDER BY date;
    """)

def compute_bubble_metrics(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Compute bubble detection metrics combining Hype and Reality indices
    """
    print("Computing bubble metrics...")
    
    conn.execute("""
    CREATE OR REPLACE TABLE bubble_metrics AS
    WITH 
    -- Join indices and compute rolling averages
    rolling_metrics AS (
        SELECT 
            h.time_id,
            h.date,
            h.hype_index,
            r.reality_index,
            -- 7-day rolling averages
            AVG(h.hype_index) OVER (
                ORDER BY h.date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as hype_7d_avg,
            AVG(r.reality_index) OVER (
                ORDER BY h.date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as reality_7d_avg,
            -- 30-day rolling averages
            AVG(h.hype_index) OVER (
                ORDER BY h.date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as hype_30d_avg,
            AVG(r.reality_index) OVER (
                ORDER BY h.date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as reality_30d_avg
        FROM hype_index h
        JOIN reality_index r ON h.time_id = r.time_id
    )
    SELECT 
        time_id,
        date,
        hype_index,
        reality_index,
        hype_7d_avg,
        reality_7d_avg,
        hype_30d_avg,
        reality_30d_avg,
        -- Divergence metrics
        (hype_index - reality_index) as hype_reality_gap,
        CASE 
            WHEN reality_index = 0 THEN 0
            ELSE hype_index / NULLIF(reality_index, 0)
        END as hype_reality_ratio,
        -- Bubble indicators
        (hype_7d_avg - reality_7d_avg) - (hype_30d_avg - reality_30d_avg) as bubble_momentum,
        -- Normalized scores (0-100 scale)
        100 * (hype_index - MIN(hype_index) OVER()) / 
            NULLIF((MAX(hype_index) OVER() - MIN(hype_index) OVER()), 0) as hype_score,
        100 * (reality_index - MIN(reality_index) OVER()) / 
            NULLIF((MAX(reality_index) OVER() - MIN(reality_index) OVER()), 0) as reality_score
    FROM rolling_metrics
    ORDER BY date;
    """)


def compute_entity_bubble_metrics(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Compute per-entity bubble metrics to support entity-level detection.
    Creates `entity_bubble_metrics` with per-entity time series.
    """
    print("Computing entity-level bubble metrics...")

    conn.execute("""
    CREATE OR REPLACE TABLE entity_bubble_metrics AS
    WITH base AS (
        SELECT
            cf.time_id,
            cf.date,
            cf.entity_id,
            cf.entity_name,
            cf.entity_type,
            cf.hype_intensity_score,
            cf.reality_strength_score
        FROM combined_features cf
    ),
    rolling AS (
        SELECT
            *,
            AVG(hype_intensity_score) OVER (
                PARTITION BY entity_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as hype_7d_avg,
            AVG(reality_strength_score) OVER (
                PARTITION BY entity_id ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as reality_7d_avg,
            AVG(hype_intensity_score) OVER (
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
        hype_intensity_score,
        reality_strength_score,
        hype_7d_avg,
        reality_7d_avg,
        hype_30d_avg,
        reality_30d_avg,
        (hype_intensity_score - reality_strength_score) as hype_reality_gap,
        CASE WHEN reality_strength_score = 0 THEN 0 ELSE hype_intensity_score / NULLIF(reality_strength_score,0) END as hype_reality_ratio,
        (hype_7d_avg - reality_7d_avg) - (hype_30d_avg - reality_30d_avg) as bubble_momentum,
        -- Per-entity normalized scores (0-100)
        100 * (hype_intensity_score - MIN(hype_intensity_score) OVER (PARTITION BY entity_id)) /
            NULLIF((MAX(hype_intensity_score) OVER (PARTITION BY entity_id) - MIN(hype_intensity_score) OVER (PARTITION BY entity_id)), 0) as hype_score,
        100 * (reality_strength_score - MIN(reality_strength_score) OVER (PARTITION BY entity_id)) /
            NULLIF((MAX(reality_strength_score) OVER (PARTITION BY entity_id) - MIN(reality_strength_score) OVER (PARTITION BY entity_id)), 0) as reality_score
    FROM rolling
    ORDER BY entity_id, date;
    """)

def main():
    """Main function to compute all indices"""
    print(f"Computing indices from warehouse: {WAREHOUSE_PATH}")
    
    conn = duckdb.connect(WAREHOUSE_PATH)
    
    try:
        # Ensure we have the required features
        if not conn.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'combined_features')").fetchone()[0]:
            raise Exception("Combined features table not found. Please run compute_features.py first.")
        
        # Compute indices
        compute_hype_index(conn)
        compute_reality_index(conn)
        compute_bubble_metrics(conn)
        compute_entity_bubble_metrics(conn)
        
        # Print some statistics
        print("\nIndex Statistics:")
        latest_metrics = conn.execute("""
            SELECT 
                hype_score,
                reality_score,
                hype_reality_gap,
                bubble_momentum
            FROM bubble_metrics 
            ORDER BY date DESC 
            LIMIT 1
        """).fetchone()
        
        print(f"Latest Hype Score: {latest_metrics[0]:.2f}")
        print(f"Latest Reality Score: {latest_metrics[1]:.2f}")
        print(f"Current Hype-Reality Gap: {latest_metrics[2]:.2f}")
        print(f"Bubble Momentum: {latest_metrics[3]:.2f}")
        
        print("\nIndex computation completed successfully!")
        
    except Exception as e:
        print(f"Error computing indices: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()