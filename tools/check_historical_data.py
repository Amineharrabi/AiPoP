"""Check historical data in the bubble metrics table"""
import duckdb
import os
import pandas as pd
from datetime import datetime, timedelta

# Setup paths
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
WAREHOUSE_PATH = os.path.join(BASE_DIR, 'data', 'warehouse', 'ai_bubble.duckdb')

def main():
    conn = duckdb.connect(WAREHOUSE_PATH)
    
    # Print available tables
    print("\nAvailable tables:")
    print("-" * 80)
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchdf()
    print(tables)
    
    # Get data from bubble_metrics
    print("\nBubble metrics data:")
    print("-" * 80)
    try:
        df = conn.execute("""
            SELECT 
                date,
                computed_at,
                hype_index,
                reality_index,
                hype_reality_gap,
                bubble_momentum
            FROM bubble_metrics
            ORDER BY COALESCE(computed_at, date) DESC
            LIMIT 100
        """).fetchdf()
        print(df)
    except Exception as e:
        print(f"Error querying bubble_metrics: {str(e)}")
    
    conn.close()

if __name__ == "__main__":
    main()