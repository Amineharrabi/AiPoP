import os
import duckdb
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict

# Setup paths
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
WAREHOUSE_PATH = os.path.join(BASE_DIR, 'data', 'warehouse', 'ai_bubble.duckdb')

def compute_hype_features(conn: duckdb.DuckDBPyConnection) -> None:
    """Compute hype-related features from fact_hype_signals"""
    print("Computing hype features...")
    
    conn.execute("""
    -- Create a temporary view for hype features
    CREATE OR REPLACE TABLE hype_features AS
    WITH 
    -- Daily aggregated sentiment and activity metrics
    daily_metrics AS (
        SELECT 
            t.time_id,
            t.date,
            e.entity_id,
            e.name as entity_name,
            e.entity_type,
            -- Sentiment metrics
            AVG(CASE WHEN signal_type = 'sentiment' THEN metric_value ELSE NULL END) as avg_sentiment,
            COUNT(CASE WHEN signal_type = 'sentiment' THEN 1 ELSE NULL END) as sentiment_mentions,
            -- Activity metrics
            AVG(CASE WHEN signal_type = 'activity' THEN metric_value ELSE NULL END) as avg_activity,
            -- Research/Innovation metrics
            AVG(CASE WHEN signal_type = 'research' THEN metric_value ELSE NULL END) as avg_innovation,
            COUNT(CASE WHEN signal_type = 'research' THEN 1 ELSE NULL END) as research_mentions
        FROM fact_hype_signals h
        JOIN dim_time t ON h.time_id = t.time_id
        JOIN dim_entity e ON h.entity_id = e.entity_id
        GROUP BY t.time_id, t.date, e.entity_id, e.name, e.entity_type
    ),
    -- Rolling metrics (7-day and 30-day windows)
    rolling_metrics AS (
        SELECT 
            *,
            -- 7-day rolling averages
            AVG(avg_sentiment) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as sentiment_7d_avg,
            AVG(sentiment_mentions) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as mentions_7d_avg,
            -- 30-day rolling averages
            AVG(avg_sentiment) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as sentiment_30d_avg,
            AVG(sentiment_mentions) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as mentions_30d_avg,
            -- Momentum and Volatility
            (avg_sentiment - LAG(avg_sentiment, 7) OVER (PARTITION BY entity_id ORDER BY date)) 
                / NULLIF(LAG(avg_sentiment, 7) OVER (PARTITION BY entity_id ORDER BY date), 0) 
                as sentiment_momentum,
            (sentiment_mentions - LAG(sentiment_mentions, 7) OVER (PARTITION BY entity_id ORDER BY date)) 
                / NULLIF(LAG(sentiment_mentions, 7) OVER (PARTITION BY entity_id ORDER BY date), 0) 
                as mention_momentum,
            -- Sentiment volatility (standard deviation)
            STDDEV(avg_sentiment) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as sentiment_volatility
    ),
    -- Cross-entity correlation
    entity_correlations AS (
        SELECT 
            rm1.time_id,
            rm1.entity_id,
            AVG(
                CORR(rm1.avg_sentiment, rm2.avg_sentiment) OVER (
                    ORDER BY rm1.date 
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                )
            ) as sentiment_correlation
        FROM rolling_metrics rm1
        JOIN rolling_metrics rm2 
            ON rm1.date = rm2.date 
            AND rm1.entity_id != rm2.entity_id
        GROUP BY rm1.time_id, rm1.entity_id
    )
    SELECT 
        time_id,
        date,
        entity_id,
        entity_name,
        entity_type,
        -- Raw metrics
        COALESCE(avg_sentiment, 0) as sentiment_score,
        COALESCE(sentiment_mentions, 0) as mention_count,
        COALESCE(avg_activity, 0) as activity_score,
        COALESCE(avg_innovation, 0) as innovation_score,
        COALESCE(research_mentions, 0) as research_count,
        -- Rolling averages
        COALESCE(sentiment_7d_avg, 0) as sentiment_7d_avg,
        COALESCE(mentions_7d_avg, 0) as mentions_7d_avg,
        COALESCE(sentiment_30d_avg, 0) as sentiment_30d_avg,
        COALESCE(mentions_30d_avg, 0) as mentions_30d_avg,
        -- Momentum indicators
        COALESCE(sentiment_momentum, 0) as sentiment_momentum,
        COALESCE(mention_momentum, 0) as mention_momentum,
        -- Composite scores
        (COALESCE(sentiment_7d_avg, 0) * 0.4 + 
         COALESCE(mentions_7d_avg / NULLIF(MAX(mentions_7d_avg) OVER (PARTITION BY entity_id), 0), 0) * 0.3 +
         COALESCE(sentiment_momentum, 0) * 0.3) as hype_intensity_score
    FROM rolling_metrics
    ORDER BY date, entity_id;
    """)

def compute_reality_features(conn: duckdb.DuckDBPyConnection) -> None:
    """Compute reality-related features from fact_reality_signals"""
    print("Computing reality features...")
    
    conn.execute("""
    -- Create a temporary view for reality features
    CREATE OR REPLACE TABLE reality_features AS
    WITH 
    -- Daily aggregated metrics
    daily_metrics AS (
        SELECT 
            t.time_id,
            t.date,
            e.entity_id,
            e.name as entity_name,
            e.entity_type,
            -- Stock metrics
            AVG(CASE WHEN metric_name = 'stock_price' THEN metric_value ELSE NULL END) as stock_price,
            AVG(CASE WHEN metric_name = 'volume' THEN metric_value ELSE NULL END) as trading_volume,
            -- Adoption metrics
            SUM(CASE WHEN metric_name = 'model_downloads' THEN metric_value ELSE NULL END) as daily_downloads,
            SUM(CASE WHEN metric_name = 'github_stars' THEN metric_value ELSE NULL END) as github_stars
        FROM fact_reality_signals r
        JOIN dim_time t ON r.time_id = t.time_id
        JOIN dim_entity e ON r.entity_id = e.entity_id
        GROUP BY t.time_id, t.date, e.entity_id, e.name, e.entity_type
    ),
    -- Rolling metrics and growth rates
    rolling_metrics AS (
        SELECT 
            *,
            -- 7-day rolling averages
            AVG(stock_price) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as price_7d_avg,
            AVG(daily_downloads) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as downloads_7d_avg,
            -- 30-day rolling averages
            AVG(stock_price) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as price_30d_avg,
            -- Growth rates
            (stock_price - LAG(stock_price, 30) OVER (PARTITION BY entity_id ORDER BY date)) 
                / NULLIF(LAG(stock_price, 30) OVER (PARTITION BY entity_id ORDER BY date), 0) 
                as price_30d_growth,
            (github_stars - LAG(github_stars, 30) OVER (PARTITION BY entity_id ORDER BY date)) 
                / NULLIF(LAG(github_stars, 30) OVER (PARTITION BY entity_id ORDER BY date), 0) 
                as stars_30d_growth
    )
    SELECT 
        time_id,
        date,
        entity_id,
        entity_name,
        entity_type,
        -- Raw metrics
        COALESCE(stock_price, 0) as stock_price,
        COALESCE(daily_downloads, 0) as daily_downloads,
        COALESCE(github_stars, 0) as github_stars,
        -- Rolling averages
        COALESCE(price_7d_avg, 0) as price_7d_avg,
        COALESCE(downloads_7d_avg, 0) as downloads_7d_avg,
        COALESCE(price_30d_avg, 0) as price_30d_avg,
        -- Growth metrics
        COALESCE(price_30d_growth, 0) as price_30d_growth,
        COALESCE(stars_30d_growth, 0) as stars_30d_growth,
        -- Composite score
        (CASE 
            WHEN entity_type = 'company' THEN 
                COALESCE(price_30d_growth, 0) * 0.7 + COALESCE(stars_30d_growth, 0) * 0.3
            WHEN entity_type = 'ai_model' THEN 
                COALESCE(downloads_7d_avg / NULLIF(MAX(downloads_7d_avg) OVER (PARTITION BY entity_id), 0), 0)
            ELSE 
                COALESCE(stars_30d_growth, 0)
        END) as reality_strength_score
    FROM rolling_metrics
    ORDER BY date, entity_id;
    """)

def compute_combined_features(conn: duckdb.DuckDBPyConnection) -> None:
    """Compute combined features joining hype and reality metrics"""
    print("Computing combined features...")
    
    conn.execute("""
    -- Create final features table combining hype and reality metrics
    CREATE OR REPLACE TABLE combined_features AS
    SELECT 
        h.time_id,
        h.date,
        h.entity_id,
        h.entity_name,
        h.entity_type,
        -- Hype metrics
        h.sentiment_score,
        h.mention_count,
        h.activity_score,
        h.innovation_score,
        h.sentiment_7d_avg,
        h.mentions_7d_avg,
        h.sentiment_momentum,
        h.mention_momentum,
        h.hype_intensity_score,
        -- Reality metrics
        r.stock_price,
        r.daily_downloads,
        r.github_stars,
        r.price_7d_avg,
        r.downloads_7d_avg,
        r.price_30d_growth,
        r.stars_30d_growth,
        r.reality_strength_score,
        -- Divergence metrics
        (h.hype_intensity_score - r.reality_strength_score) as hype_reality_gap,
        CASE 
            WHEN r.reality_strength_score = 0 THEN 0
            ELSE h.hype_intensity_score / NULLIF(r.reality_strength_score, 0)
        END as hype_reality_ratio
    FROM hype_features h
    JOIN reality_features r 
        ON h.time_id = r.time_id 
        AND h.entity_id = r.entity_id
    ORDER BY date, entity_id;
    """)

def main():
    """Main function to compute all features"""
    print(f"Computing features from warehouse: {WAREHOUSE_PATH}")
    
    conn = duckdb.connect(WAREHOUSE_PATH)
    
    try:
        # Compute feature sets
        compute_hype_features(conn)
        compute_reality_features(conn)
        compute_combined_features(conn)
        
        # Print some statistics
        print("\nFeature Statistics:")
        stats = {
            'hype_features': conn.execute("SELECT COUNT(*) FROM hype_features").fetchone()[0],
            'reality_features': conn.execute("SELECT COUNT(*) FROM reality_features").fetchone()[0],
            'combined_features': conn.execute("SELECT COUNT(*) FROM combined_features").fetchone()[0]
        }
        
        for table, count in stats.items():
            print(f"{table}: {count:,} rows")
            
        print("\nFeature computation completed successfully!")
        
    except Exception as e:
        print(f"Error computing features: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()