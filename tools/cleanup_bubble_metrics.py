import duckdb
import os

# Get the path to the DuckDB database
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
WAREHOUSE_PATH = os.path.join(BASE_DIR, 'data', 'warehouse', 'ai_bubble.duckdb')

print(f"Connecting to database: {WAREHOUSE_PATH}")
conn = duckdb.connect(WAREHOUSE_PATH)

try:
    print("Cleaning up bubble_metrics table...")
    conn.execute("""
    CREATE TABLE bubble_metrics_clean AS
    SELECT DISTINCT ON (date)
        time_id, date, computed_at, hype_index, reality_index,
        hype_7d_avg, reality_7d_avg, hype_30d_avg, reality_30d_avg,
        hype_reality_gap, hype_reality_ratio, bubble_momentum,
        sentiment_bubble_momentum, github_trending_bubble_momentum,
        trending_reality_divergence, hype_score, reality_score, bubble_risk_score
    FROM bubble_metrics
    ORDER BY date, computed_at DESC;

    DROP TABLE bubble_metrics;
    ALTER TABLE bubble_metrics_clean RENAME TO bubble_metrics;
    """)
    print("Successfully cleaned up bubble_metrics table!")
    
    # Verify the results
    result = conn.execute("SELECT COUNT(*) as count, COUNT(DISTINCT date) as distinct_dates FROM bubble_metrics").fetchone()
    print(f"\nBubble metrics table now has:")
    print(f"Total rows: {result[0]}")
    print(f"Distinct dates: {result[1]}")
    
except Exception as e:
    print(f"Error cleaning up bubble_metrics: {str(e)}")
finally:
    conn.close()