import os
import duckdb
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict

# Fixed paths for new structure
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
WAREHOUSE_PATH = os.path.join(BASE_DIR, 'data', 'warehouse', 'ai_bubble.duckdb')

def compute_hype_features(conn: duckdb.DuckDBPyConnection) -> None:
    """Compute comprehensive hype-related features from all fact_hype_signals"""
    print("Computing enhanced hype features from all data sources...")
    
    conn.execute("""
    -- Create a temporary view for comprehensive hype features
    CREATE OR REPLACE TABLE hype_features AS
    WITH 
    -- Daily aggregated sentiment and activity metrics from ALL sources
    daily_metrics AS (
        SELECT 
            t.time_id,
            t.date,
            e.entity_id,
            e.name as entity_name,
            e.entity_type,
            -- Multi-source sentiment metrics
            AVG(CASE WHEN metric_name = 'reddit_ai_sentiment' THEN metric_value ELSE NULL END) as reddit_sentiment,
            AVG(CASE WHEN metric_name = 'news_ai_sentiment' THEN metric_value ELSE NULL END) as news_sentiment,
            COUNT(CASE WHEN metric_name = 'reddit_ai_sentiment' THEN 1 ELSE NULL END) as reddit_mentions,
            COUNT(CASE WHEN metric_name = 'news_ai_sentiment' THEN 1 ELSE NULL END) as news_mentions,
            
            -- Multi-source trending metrics
            AVG(CASE WHEN metric_name = 'github_trending_score' THEN metric_value ELSE NULL END) as github_trending_score,
            AVG(CASE WHEN metric_name = 'huggingface_trending_score' THEN metric_value ELSE NULL END) as hf_trending_score,
            COUNT(CASE WHEN metric_name = 'github_trending_score' THEN 1 ELSE NULL END) as github_mentions,
            COUNT(CASE WHEN metric_name = 'huggingface_trending_score' THEN 1 ELSE NULL END) as hf_mentions,
            
            -- Innovation metrics
            AVG(CASE WHEN metric_name = 'arxiv_innovation_score' THEN metric_value ELSE NULL END) as arxiv_innovation,
            COUNT(CASE WHEN metric_name = 'arxiv_innovation_score' THEN 1 ELSE NULL END) as research_mentions
        FROM fact_hype_signals h
        JOIN dim_time t ON h.time_id = t.time_id
        JOIN dim_entity e ON h.entity_id = e.entity_id
        GROUP BY t.time_id, t.date, e.entity_id, e.name, e.entity_type
    ),
    -- Rolling metrics (7-day and 30-day windows) for ALL metrics
    rolling_metrics AS (
        SELECT 
            *,
            -- 7-day rolling averages for all sentiment sources
            AVG(reddit_sentiment) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as reddit_sentiment_7d_avg,
            AVG(news_sentiment) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as news_sentiment_7d_avg,
            
            -- 7-day rolling averages for trending scores
            AVG(github_trending_score) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as github_trend_7d_avg,
            AVG(hf_trending_score) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as hf_trend_7d_avg,
            
            -- 30-day rolling averages
            AVG(reddit_sentiment) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as reddit_sentiment_30d_avg,
            AVG(news_sentiment) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as news_sentiment_30d_avg,
            
            -- Sentiment momentum calculations
            (reddit_sentiment - LAG(reddit_sentiment, 7) OVER (PARTITION BY entity_id ORDER BY date)) 
                / NULLIF(LAG(reddit_sentiment, 7) OVER (PARTITION BY entity_id ORDER BY date), 0) 
                as reddit_sentiment_momentum,
            (news_sentiment - LAG(news_sentiment, 7) OVER (PARTITION BY entity_id ORDER BY date)) 
                / NULLIF(LAG(news_sentiment, 7) OVER (PARTITION BY entity_id ORDER by date), 0) 
                as news_sentiment_momentum,
            
            -- Trending momentum
            (github_trending_score - LAG(github_trending_score, 7) OVER (PARTITION BY entity_id ORDER BY date)) 
                / NULLIF(LAG(github_trending_score, 7) OVER (PARTITION BY entity_id ORDER BY date), 0) 
                as github_trending_momentum,
            
            -- Sentiment volatility (standard deviation)
            STDDEV(reddit_sentiment) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as reddit_sentiment_volatility,
            STDDEV(news_sentiment) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as news_sentiment_volatility
    ),
    -- Cross-entity correlation for trending metrics
    entity_correlations AS (
        SELECT 
            rm1.time_id,
            rm1.entity_id,
            AVG(
                CORR(rm1.github_trending_score, rm2.github_trending_score) OVER (
                    ORDER BY rm1.date 
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                )
            ) as github_trend_correlation,
            AVG(
                CORR(rm1.hf_trending_score, rm2.hf_trending_score) OVER (
                    ORDER BY rm1.date 
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                )
            ) as hf_trend_correlation
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
        -- Multi-source raw metrics
        COALESCE(reddit_sentiment, 0) as reddit_sentiment_score,
        COALESCE(news_sentiment, 0) as news_sentiment_score,
        COALESCE(github_trending_score, 0) as github_trending_score,
        COALESCE(hf_trending_score, 0) as hf_trending_score,
        COALESCE(arxiv_innovation, 0) as arxiv_innovation_score,
        
        -- Multi-source mentions counts
        COALESCE(reddit_mentions, 0) as reddit_mentions,
        COALESCE(news_mentions, 0) as news_mentions,
        COALESCE(github_mentions, 0) as github_mentions,
        COALESCE(hf_mentions, 0) as hf_mentions,
        COALESCE(research_mentions, 0) as research_mentions,
        
        -- Rolling averages
        COALESCE(reddit_sentiment_7d_avg, 0) as reddit_sentiment_7d_avg,
        COALESCE(news_sentiment_7d_avg, 0) as news_sentiment_7d_avg,
        COALESCE(reddit_sentiment_30d_avg, 0) as reddit_sentiment_30d_avg,
        COALESCE(news_sentiment_30d_avg, 0) as news_sentiment_30d_avg,
        COALESCE(github_trend_7d_avg, 0) as github_trend_7d_avg,
        COALESCE(hf_trend_7d_avg, 0) as hf_trend_7d_avg,
        
        -- Momentum indicators
        COALESCE(reddit_sentiment_momentum, 0) as reddit_sentiment_momentum,
        COALESCE(news_sentiment_momentum, 0) as news_sentiment_momentum,
        COALESCE(github_trending_momentum, 0) as github_trending_momentum,
        
        -- Composite scores with multi-source integration
        (COALESCE(reddit_sentiment_7d_avg, 0) * 0.25 + 
         COALESCE(news_sentiment_7d_avg, 0) * 0.25 +
         COALESCE(github_trend_7d_avg, 0) * 0.25 +
         COALESCE(hf_trend_7d_avg, 0) * 0.25) as multi_source_hype_intensity,
         
        -- Trending intensity (weighted by mentions)
        ((COALESCE(github_trend_7d_avg, 0) * COALESCE(github_mentions, 1)) + 
         (COALESCE(hf_trend_7d_avg, 0) * COALESCE(hf_mentions, 1))) / 
         NULLIF(COALESCE(github_mentions, 1) + COALESCE(hf_mentions, 1), 0) as trending_intensity
    FROM rolling_metrics
    ORDER BY date, entity_id;
    """)

def compute_reality_features(conn: duckdb.DuckDBPyConnection) -> None:
    """Compute comprehensive reality-related features from all fact_reality_signals"""
    print("Computing enhanced reality features from all data sources...")
    
    conn.execute("""
    -- Create a temporary view for comprehensive reality features
    CREATE OR REPLACE TABLE reality_features AS
    WITH 
    -- Daily aggregated metrics from ALL sources
    daily_metrics AS (
        SELECT 
            t.time_id,
            t.date,
            e.entity_id,
            e.name as entity_name,
            e.entity_type,
            -- Stock metrics
            AVG(CASE WHEN metric_name = 'stock_price' THEN metric_value ELSE NULL END) as stock_price,
            AVG(CASE WHEN metric_name = 'trading_volume' THEN metric_value ELSE NULL END) as trading_volume,
            
            -- Delta metrics (GitHub/HuggingFace trending)
            AVG(CASE WHEN metric_name = 'github_stars_delta' THEN metric_value ELSE NULL END) as github_stars_delta,
            AVG(CASE WHEN metric_name = 'github_forks_delta' THEN metric_value ELSE NULL END) as github_forks_delta,
            AVG(CASE WHEN metric_name = 'model_downloads_delta' THEN metric_value ELSE NULL END) as hf_downloads_delta
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
            AVG(github_stars_delta) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as github_stars_7d_delta,
            AVG(hf_downloads_delta) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) as hf_downloads_7d_delta,
            
            -- 30-day rolling averages
            AVG(stock_price) OVER (
                PARTITION BY entity_id 
                ORDER BY date 
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) as price_30d_avg,
            
            -- Growth rates with delta integration
            (stock_price - LAG(stock_price, 30) OVER (PARTITION BY entity_id ORDER BY date)) 
                / NULLIF(LAG(stock_price, 30) OVER (PARTITION BY entity_id ORDER BY date), 0) 
                as price_30d_growth,
            (github_stars_delta - LAG(github_stars_delta, 30) OVER (PARTITION BY entity_id ORDER BY date)) 
                / NULLIF(LAG(github_stars_delta, 30) OVER (PARTITION BY entity_id ORDER BY date), 0) 
                as github_delta_30d_growth,
            (hf_downloads_delta - LAG(hf_downloads_delta, 30) OVER (PARTITION BY entity_id ORDER BY date)) 
                / NULLIF(LAG(hf_downloads_delta, 30) OVER (PARTITION BY entity_id ORDER BY date), 0) 
                as hf_downloads_30d_growth,
                
            -- Cumulative delta metrics
            SUM(github_stars_delta) OVER (PARTITION BY entity_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as github_stars_cumulative_delta,
            SUM(hf_downloads_delta) OVER (PARTITION BY entity_id ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as hf_downloads_cumulative_delta
    )
    SELECT 
        time_id,
        date,
        entity_id,
        entity_name,
        entity_type,
        -- Raw metrics
        COALESCE(stock_price, 0) as stock_price,
        COALESCE(trading_volume, 0) as trading_volume,
        COALESCE(github_stars_delta, 0) as github_stars_delta,
        COALESCE(github_forks_delta, 0) as github_forks_delta,
        COALESCE(hf_downloads_delta, 0) as hf_downloads_delta,
        
        -- Rolling averages
        COALESCE(price_7d_avg, 0) as price_7d_avg,
        COALESCE(github_stars_7d_delta, 0) as github_stars_7d_delta,
        COALESCE(hf_downloads_7d_delta, 0) as hf_downloads_7d_delta,
        COALESCE(price_30d_avg, 0) as price_30d_avg,
        
        -- Growth metrics
        COALESCE(price_30d_growth, 0) as price_30d_growth,
        COALESCE(github_delta_30d_growth, 0) as github_delta_30d_growth,
        COALESCE(hf_downloads_30d_growth, 0) as hf_downloads_30d_growth,
        
        -- Cumulative impact metrics
        COALESCE(github_stars_cumulative_delta, 0) as github_stars_cumulative_delta,
        COALESCE(hf_downloads_cumulative_delta, 0) as hf_downloads_cumulative_delta,
        
        -- Enhanced composite reality score with delta integration
        (CASE 
            WHEN entity_type = 'company' THEN 
                (COALESCE(price_30d_growth, 0) * 0.4 + 
                 COALESCE(github_delta_30d_growth, 0) * 0.3 + 
                 COALESCE(hf_downloads_30d_growth, 0) * 0.3)
            WHEN entity_type = 'software' THEN 
                COALESCE(github_delta_30d_growth, 0)
            WHEN entity_type = 'ai_model' THEN 
                COALESCE(hf_downloads_30d_growth, 0)
            ELSE 
                COALESCE(github_delta_30d_growth, 0) + COALESCE(hf_downloads_30d_growth, 0)
        END) as reality_strength_score
    FROM rolling_metrics
    ORDER BY date, entity_id;
    """)

def compute_combined_features(conn: duckdb.DuckDBPyConnection) -> None:
    """Compute combined features joining all hype and reality metrics"""
    print("Computing enhanced combined features with multi-source integration...")
    
    conn.execute("""
    -- Create final features table combining all hype and reality metrics
    CREATE OR REPLACE TABLE combined_features AS
    SELECT 
        h.time_id,
        h.date,
        h.entity_id,
        h.entity_name,
        h.entity_type,
        
        -- Multi-source hype metrics
        h.reddit_sentiment_score,
        h.news_sentiment_score,
        h.github_trending_score,
        h.hf_trending_score,
        h.arxiv_innovation_score,
        
        -- Multi-source sentiment momentum
        h.reddit_sentiment_momentum,
        h.news_sentiment_momentum,
        h.github_trending_momentum,
        
        -- Multi-source hype intensity
        h.multi_source_hype_intensity,
        h.trending_intensity,
        
        -- Reality metrics with deltas
        r.stock_price,
        r.trading_volume,
        r.github_stars_delta,
        r.github_forks_delta,
        r.hf_downloads_delta,
        
        -- Reality growth metrics
        r.price_30d_growth,
        r.github_delta_30d_growth,
        r.hf_downloads_30d_growth,
        
        -- Cumulative impact metrics
        r.github_stars_cumulative_delta,
        r.hf_downloads_cumulative_delta,
        
        -- Enhanced composite scores
        h.multi_source_hype_intensity as hype_intensity_score,
        r.reality_strength_score,
        
        -- Enhanced divergence metrics
        (h.multi_source_hype_intensity - r.reality_strength_score) as hype_reality_gap,
        CASE 
            WHEN r.reality_strength_score = 0 THEN 0
            ELSE h.multi_source_hype_intensity / NULLIF(r.reality_strength_score, 0)
        END as hype_reality_ratio,
        
        -- NEW: Trending momentum divergence
        (h.trending_intensity - r.github_delta_30d_growth) as trending_reality_gap,
        
        -- NEW: Innovation vs Adoption gap
        (h.arxiv_innovation_score - COALESCE(r.hf_downloads_30d_growth, 0)) as innovation_adoption_gap
    FROM hype_features h
    JOIN reality_features r 
        ON h.time_id = r.time_id 
        AND h.entity_id = r.entity_id
    ORDER BY date, entity_id;
    """)

def main():
    """Main function to compute all enhanced features"""
    print(f"Computing enhanced features from warehouse: {WAREHOUSE_PATH}")
    
    conn = duckdb.connect(WAREHOUSE_PATH)
    
    try:
        # Compute feature sets with multi-source integration
        compute_hype_features(conn)
        compute_reality_features(conn)
        compute_combined_features(conn)
        
        # Print some statistics
        print("\nEnhanced Feature Statistics:")
        stats = {
            'hype_features': conn.execute("SELECT COUNT(*) FROM hype_features").fetchone()[0],
            'reality_features': conn.execute("SELECT COUNT(*) FROM reality_features").fetchone()[0],
            'combined_features': conn.execute("SELECT COUNT(*) FROM combined_features").fetchone()[0]
        }
        
        for table, count in stats.items():
            print(f"{table}: {count:,} rows")
            
        # Show sample of multi-source integration
        print("\nSample of multi-source integration:")
        sample = conn.execute("""
            SELECT 
                entity_name,
                entity_type,
                reddit_sentiment_score,
                news_sentiment_score,
                github_trending_score,
                hf_trending_score,
                multi_source_hype_intensity,
                reality_strength_score,
                hype_reality_gap
            FROM combined_features 
            ORDER BY multi_source_hype_intensity DESC 
            LIMIT 5
        """).fetchdf()
        
        print(sample)
            
        print("\nEnhanced feature computation completed successfully!")
        
    except Exception as e:
        print(f"Error computing features: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()