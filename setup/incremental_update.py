#!/usr/bin/env python3
"""
Incremental data update pipeline
Only processes new data and adds new points to the graph
"""
import os
import sys
import duckdb
from datetime import datetime, date, timedelta
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
WAREHOUSE_PATH = os.path.join(BASE_DIR, 'data', 'warehouse', 'ai_bubble.duckdb')

def get_latest_data_date(conn):
    """Get the latest date we have data for"""
    try:
        result = conn.execute("""
            SELECT MAX(date) as latest_date 
            FROM bubble_metrics
        """).fetchone()
        
        if result and result[0]:
            return result[0]
        else:
            # If no data exists, start from 30 days ago
            return (date.today() - timedelta(days=30))
    except:
        return (date.today() - timedelta(days=30))

def check_new_data_available(conn, latest_date):
    """Check if there's new data in staging that needs processing"""
    today = date.today()
    
    # Check if we already have today's data
    if latest_date >= today:
        logger.info(f"Already have data for today ({today}). No update needed.")
        return False
    
    # Check if there's new raw data
    try:
        # Check combined_features for new dates
        result = conn.execute("""
            SELECT COUNT(*) as new_rows
            FROM combined_features
            WHERE date > ?
        """, [latest_date]).fetchone()
        
        if result and result[0] > 0:
            logger.info(f"Found {result[0]} new rows in combined_features after {latest_date}")
            return True
        
        logger.info(f"No new data found after {latest_date}")
        return False
        
    except Exception as e:
        logger.error(f"Error checking for new data: {e}")
        # If combined_features doesn't exist, we need to run full pipeline
        return True

def compute_incremental_indices(conn, start_date):
    """
    Compute indices only for dates after start_date
    This efficiently adds new points without recomputing old ones
    """
    logger.info(f"Computing incremental indices from {start_date}...")
    
    # Get all data we need for rolling averages (need history for 30-day averages)
    # We'll compute indices for new dates but need old data for context
    
    # First, ensure we have combined_features
    try:
        conn.execute("SELECT COUNT(*) FROM combined_features").fetchone()
    except:
        logger.error("combined_features table not found. Run full pipeline first.")
        return False
    
    # Compute hype_index for new dates only
    logger.info("Computing hype_index for new dates...")
    conn.execute("""
    INSERT INTO hype_index
    WITH 
    entity_weights AS (
        SELECT 
            entity_id,
            entity_name,
            entity_type,
            CASE 
                WHEN entity_type = 'company' THEN 
                    (AVG(COALESCE(multi_source_hype_intensity, 0)) + 
                     AVG(COALESCE(trending_intensity, 0))) * 2
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
    normalized_metrics AS (
        SELECT 
            cf.*,
            ew.entity_weight,
            COALESCE(multi_source_hype_intensity / NULLIF(MAX(multi_source_hype_intensity) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_hype_intensity,
            COALESCE(reddit_sentiment_momentum / NULLIF(MAX(ABS(reddit_sentiment_momentum)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_reddit_momentum,
            COALESCE(news_sentiment_momentum / NULLIF(MAX(ABS(news_sentiment_momentum)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_news_momentum,
            COALESCE(github_trending_momentum / NULLIF(MAX(ABS(github_trending_momentum)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_github_trending,
            COALESCE(trending_intensity / NULLIF(MAX(trending_intensity) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_trending_intensity,
            COALESCE(arxiv_innovation_score / NULLIF(MAX(arxiv_innovation_score) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_innovation
        FROM combined_features cf
        JOIN entity_weights ew ON cf.entity_id = ew.entity_id
        WHERE cf.date > ?
    )
    SELECT 
        time_id,
        date,
        SUM(
            entity_weight * (
                norm_hype_intensity * 0.25 +
                (norm_reddit_momentum + norm_news_momentum) * 0.2 +
                norm_github_trending * 0.15 +
                norm_trending_intensity * 0.15 +
                norm_innovation * 0.25
            )
        ) / NULLIF(SUM(entity_weight), 0) as hype_index,
        SUM(entity_weight * norm_hype_intensity) / NULLIF(SUM(entity_weight), 0) as hype_intensity_component,
        SUM(entity_weight * (norm_reddit_momentum + norm_news_momentum) / 2) / NULLIF(SUM(entity_weight), 0) as sentiment_momentum_component,
        SUM(entity_weight * norm_github_trending) / NULLIF(SUM(entity_weight), 0) as github_trending_component,
        SUM(entity_weight * norm_trending_intensity) / NULLIF(SUM(entity_weight), 0) as trending_component,
        SUM(entity_weight * norm_innovation) / NULLIF(SUM(entity_weight), 0) as innovation_component,
        COUNT(DISTINCT entity_id) as entity_count,
        SUM(COALESCE(multi_source_hype_intensity, 0) + COALESCE(trending_intensity, 0)) as total_mentions_proxy
    FROM normalized_metrics
    GROUP BY time_id, date
    ORDER BY date;
    """, [start_date])
    
    # Compute reality_index for new dates only
    logger.info("Computing reality_index for new dates...")
    conn.execute("""
    INSERT INTO reality_index
    WITH 
    entity_weights AS (
        SELECT 
            entity_id,
            entity_name,
            entity_type,
            CASE 
                WHEN entity_type = 'company' THEN 
                    (AVG(COALESCE(stock_price, 0)) + 
                     AVG(ABS(github_stars_delta)) * 100 + 
                     AVG(ABS(hf_downloads_delta)) * 50) * 2
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
    normalized_metrics AS (
        SELECT 
            cf.*,
            ew.entity_weight,
            COALESCE(reality_strength_score / NULLIF(MAX(reality_strength_score) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_reality_strength,
            COALESCE(price_30d_growth / NULLIF(MAX(ABS(price_30d_growth)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_price_growth,
            COALESCE(github_delta_30d_growth / NULLIF(MAX(ABS(github_delta_30d_growth)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_github_delta,
            COALESCE(hf_downloads_30d_growth / NULLIF(MAX(ABS(hf_downloads_30d_growth)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_hf_downloads,
            COALESCE(github_stars_cumulative_delta / NULLIF(MAX(ABS(github_stars_cumulative_delta)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_github_cumulative,
            COALESCE(hf_downloads_cumulative_delta / NULLIF(MAX(ABS(hf_downloads_cumulative_delta)) OVER (PARTITION BY cf.entity_id), 0), 0) as norm_hf_cumulative
        FROM combined_features cf
        JOIN entity_weights ew ON cf.entity_id = ew.entity_id
        WHERE cf.date > ?
    )
    SELECT 
        time_id,
        date,
        SUM(
            entity_weight * (
                norm_reality_strength * 0.3 +
                COALESCE(norm_price_growth, 0) * 0.2 +
                COALESCE(norm_github_delta, 0) * 0.25 +
                COALESCE(norm_hf_downloads, 0) * 0.25
            )
        ) / NULLIF(SUM(entity_weight), 0) as reality_index,
        SUM(entity_weight * norm_reality_strength) / NULLIF(SUM(entity_weight), 0) as reality_strength_component,
        SUM(entity_weight * COALESCE(norm_price_growth, 0)) / NULLIF(SUM(entity_weight), 0) as price_growth_component,
        SUM(entity_weight * COALESCE(norm_github_delta, 0)) / NULLIF(SUM(entity_weight), 0) as github_delta_component,
        SUM(entity_weight * COALESCE(norm_hf_downloads, 0)) / NULLIF(SUM(entity_weight), 0) as hf_downloads_component,
        SUM(entity_weight * (COALESCE(norm_github_cumulative, 0) + COALESCE(norm_hf_cumulative, 0)) / 2) / NULLIF(SUM(entity_weight), 0) as cumulative_impact_component,
        COUNT(DISTINCT entity_id) as entity_count,
        AVG(CASE WHEN entity_type = 'company' THEN price_30d_growth ELSE NULL END) as avg_company_growth,
        AVG(ABS(github_delta_30d_growth)) as avg_github_momentum,
        AVG(ABS(hf_downloads_30d_growth)) as avg_hf_momentum
    FROM normalized_metrics
    GROUP BY time_id, date
    ORDER BY date;
    """, [start_date])
    
    # Compute bubble_metrics for new dates only
    logger.info("Computing bubble_metrics for new dates...")
    conn.execute("""
    INSERT OR REPLACE INTO bubble_metrics
    WITH daily_data AS (
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
        WHERE h.date > ?
        ORDER BY h.date, h.time_id DESC
    ),
    all_historical_data AS (
        SELECT date, hype_index, reality_index FROM bubble_metrics
        UNION ALL
        SELECT date, hype_index, reality_index FROM daily_data
    ),
    rolling_metrics AS (
        SELECT 
            dd.time_id,
            dd.date,
            dd.hype_index,
            dd.sentiment_momentum_component,
            dd.github_trending_component,
            dd.trending_component,
            dd.innovation_component,
            dd.reality_index,
            dd.reality_strength_component,
            dd.github_delta_component,
            dd.hf_downloads_component,
            dd.cumulative_impact_component,
            (SELECT AVG(hype_index) FROM all_historical_data ahd 
             WHERE ahd.date <= dd.date AND ahd.date > dd.date - INTERVAL '7 days') as hype_7d_avg,
            (SELECT AVG(reality_index) FROM all_historical_data ahd 
             WHERE ahd.date <= dd.date AND ahd.date > dd.date - INTERVAL '7 days') as reality_7d_avg,
            (SELECT AVG(hype_index) FROM all_historical_data ahd 
             WHERE ahd.date <= dd.date AND ahd.date > dd.date - INTERVAL '30 days') as hype_30d_avg,
            (SELECT AVG(reality_index) FROM all_historical_data ahd 
             WHERE ahd.date <= dd.date AND ahd.date > dd.date - INTERVAL '30 days') as reality_30d_avg,
            (SELECT AVG(sentiment_momentum_component) FROM daily_data d2 
             WHERE d2.date <= dd.date AND d2.date > dd.date - INTERVAL '7 days') as sentiment_7d_avg,
            (SELECT AVG(github_trending_component) FROM daily_data d2 
             WHERE d2.date <= dd.date AND d2.date > dd.date - INTERVAL '7 days') as github_trend_7d_avg,
            (SELECT AVG(reality_strength_component) FROM daily_data d2 
             WHERE d2.date <= dd.date AND d2.date > dd.date - INTERVAL '7 days') as reality_strength_7d_avg,
            (SELECT AVG(github_delta_component) FROM daily_data d2 
             WHERE d2.date <= dd.date AND d2.date > dd.date - INTERVAL '7 days') as reality_github_7d_avg
        FROM daily_data dd
    )
    SELECT 
        date,
        time_id,
        CURRENT_TIMESTAMP as computed_at,
        hype_index,
        reality_index,
        hype_7d_avg,
        reality_7d_avg,
        hype_30d_avg,
        reality_30d_avg,
        (hype_index - reality_index) as hype_reality_gap,
        CASE 
            WHEN reality_index = 0 THEN 0
            ELSE hype_index / NULLIF(reality_index, 0)
        END as hype_reality_ratio,
        (hype_7d_avg - reality_7d_avg) - (hype_30d_avg - reality_30d_avg) as bubble_momentum,
        (sentiment_7d_avg - reality_strength_7d_avg) as sentiment_bubble_momentum,
        (github_trend_7d_avg - reality_github_7d_avg) as github_trending_bubble_momentum,
        (trending_component - cumulative_impact_component) as trending_reality_divergence,
        CASE 
            WHEN (SELECT MAX(hype_index) FROM bubble_metrics) = (SELECT MIN(hype_index) FROM bubble_metrics) THEN 50.0
            ELSE 100 * (hype_index - (SELECT MIN(hype_index) FROM bubble_metrics)) / 
                NULLIF((SELECT MAX(hype_index) FROM bubble_metrics) - (SELECT MIN(hype_index) FROM bubble_metrics), 0)
        END as hype_score,
        CASE 
            WHEN (SELECT MAX(reality_index) FROM bubble_metrics) = (SELECT MIN(reality_index) FROM bubble_metrics) THEN 50.0
            ELSE 100 * (reality_index - (SELECT MIN(reality_index) FROM bubble_metrics)) / 
                NULLIF((SELECT MAX(reality_index) FROM bubble_metrics) - (SELECT MIN(reality_index) FROM bubble_metrics), 0)
        END as reality_score,
        (
            ABS(hype_index - reality_index) * 0.4 +
            ABS((hype_7d_avg - reality_7d_avg) - (hype_30d_avg - reality_30d_avg)) * 0.3 +
            ABS(trending_component - cumulative_impact_component) * 0.3
        ) * 100 as bubble_risk_score
    FROM rolling_metrics
    ORDER BY date;
    """, [start_date])
    
    logger.info("✓ Incremental indices computed successfully")
    return True

def run_incremental_update():
    """Main incremental update function"""
    logger.info("=" * 60)
    logger.info("Starting Incremental Data Update")
    logger.info("=" * 60)
    
    if not os.path.exists(WAREHOUSE_PATH):
        logger.error(f"Warehouse not found: {WAREHOUSE_PATH}")
        logger.error("Run full pipeline first: python setup/setup_duckdb.py")
        return False
    
    conn = duckdb.connect(WAREHOUSE_PATH)
    
    try:
        # Get latest data date
        latest_date = get_latest_data_date(conn)
        logger.info(f"Latest data date in warehouse: {latest_date}")
        
        # Check if new data is available
        if not check_new_data_available(conn, latest_date):
            logger.info("No new data to process. Exiting.")
            return True
        
        # Compute incremental indices
        success = compute_incremental_indices(conn, latest_date)
        
        if success:
            # Get new date range
            new_latest = conn.execute("SELECT MAX(date) FROM bubble_metrics").fetchone()[0]
            logger.info(f"✓ Updated data range: {latest_date} → {new_latest}")
            
            # Show stats
            total_points = conn.execute("SELECT COUNT(*) FROM bubble_metrics").fetchone()[0]
            logger.info(f"✓ Total data points: {total_points}")
            
            logger.info("=" * 60)
            logger.info("Incremental Update Complete!")
            logger.info("=" * 60)
            return True
        else:
            logger.error("Incremental update failed")
            return False
            
    except Exception as e:
        logger.error(f"Error during incremental update: {e}", exc_info=True)
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    success = run_incremental_update()
    sys.exit(0 if success else 1)