import os
import sys
import duckdb
import pandas as pd

# Try to find the warehouse file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE_PATH = os.path.join(BASE_DIR, '..', 'data', 'warehouse', 'ai_bubble.duckdb')

# Alternative paths if running from different locations
if not os.path.exists(WAREHOUSE_PATH):
    WAREHOUSE_PATH = os.path.join(BASE_DIR, 'data', 'warehouse', 'ai_bubble.duckdb')
if not os.path.exists(WAREHOUSE_PATH):
    WAREHOUSE_PATH = 'data/warehouse/ai_bubble.duckdb'

if not os.path.exists(WAREHOUSE_PATH):
    print(f"ERROR: Could not find database file!")
    print(f"Looked in: {WAREHOUSE_PATH}")
    print(f"Current directory: {os.getcwd()}")
    print(f"Script directory: {BASE_DIR}")
    sys.exit(1)

print(f"Using database: {WAREHOUSE_PATH}\n")

def diagnose_database():
    """Diagnose what's in the database"""
    print(f"Diagnosing database: {WAREHOUSE_PATH}\n")
    
    conn = duckdb.connect(WAREHOUSE_PATH)
    
    try:
        # Check bubble_metrics
        print("=" * 60)
        print("BUBBLE_METRICS TABLE")
        print("=" * 60)
        
        try:
            count = conn.execute("SELECT COUNT(*) FROM bubble_metrics").fetchone()[0]
            print(f"Total rows: {count}")
            
            dates = conn.execute("""
                SELECT 
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    COUNT(DISTINCT date) as unique_dates
                FROM bubble_metrics
            """).fetchone()
            print(f"Date range: {dates[0]} to {dates[1]}")
            print(f"Unique dates: {dates[2]}")
            print(f"Duplicates per date: {count / max(dates[2], 1):.1f} avg")
            
            # Show all dates with counts
            print("\nAll dates in bubble_metrics:")
            all_dates = conn.execute("""
                SELECT date, COUNT(*) as count
                FROM bubble_metrics
                GROUP BY date
                ORDER BY date
            """).fetchdf()
            print(all_dates.to_string(index=False))
            
        except Exception as e:
            print(f"Error reading bubble_metrics: {e}")
        
        # Check source tables
        print("\n" + "=" * 60)
        print("SOURCE TABLES (hype_index & reality_index)")
        print("=" * 60)
        
        try:
            hype_dates = conn.execute("""
                SELECT 
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    COUNT(DISTINCT date) as unique_dates,
                    COUNT(*) as total_rows
                FROM hype_index
            """).fetchone()
            print(f"\nHYPE_INDEX:")
            print(f"  Total rows: {hype_dates[3]}")
            print(f"  Date range: {hype_dates[0]} to {hype_dates[1]}")
            print(f"  Unique dates: {hype_dates[2]}")
            
            reality_dates = conn.execute("""
                SELECT 
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    COUNT(DISTINCT date) as unique_dates,
                    COUNT(*) as total_rows
                FROM reality_index
            """).fetchone()
            print(f"\nREALITY_INDEX:")
            print(f"  Total rows: {reality_dates[3]}")
            print(f"  Date range: {reality_dates[0]} to {reality_dates[1]}")
            print(f"  Unique dates: {reality_dates[2]}")
            
            # Show sample of dates from source tables
            print("\nSample dates from hype_index (first 10):")
            sample_hype = conn.execute("""
                SELECT DISTINCT date
                FROM hype_index
                ORDER BY date
                LIMIT 10
            """).fetchdf()
            print(sample_hype.to_string(index=False))
            
        except Exception as e:
            print(f"Error reading source tables: {e}")
        
        # Check combined_features (the real source)
        print("\n" + "=" * 60)
        print("COMBINED_FEATURES TABLE (Original Data)")
        print("=" * 60)
        
        try:
            cf_dates = conn.execute("""
                SELECT 
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    COUNT(DISTINCT date) as unique_dates,
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT entity_id) as unique_entities
                FROM combined_features
            """).fetchone()
            print(f"Total rows: {cf_dates[3]}")
            print(f"Date range: {cf_dates[0]} to {cf_dates[1]}")
            print(f"Unique dates: {cf_dates[2]}")
            print(f"Unique entities: {cf_dates[4]}")
            
            # Show dates with entity counts
            print("\nDates with entity counts:")
            date_entities = conn.execute("""
                SELECT date, COUNT(DISTINCT entity_id) as entities
                FROM combined_features
                GROUP BY date
                ORDER BY date
                LIMIT 20
            """).fetchdf()
            print(date_entities.to_string(index=False))
            
        except Exception as e:
            print(f"Error reading combined_features: {e}")
        
        # Check time dimension
        print("\n" + "=" * 60)
        print("DIM_TIME TABLE")
        print("=" * 60)
        
        try:
            time_dates = conn.execute("""
                SELECT 
                    MIN(date) as min_date,
                    MAX(date) as max_date,
                    COUNT(DISTINCT date) as unique_dates
                FROM dim_time
            """).fetchone()
            print(f"Date range: {time_dates[0]} to {time_dates[1]}")
            print(f"Unique dates: {time_dates[2]}")
            
        except Exception as e:
            print(f"Error reading dim_time: {e}")
        
    finally:
        conn.close()
    
    print("\n" + "=" * 60)
    print("DIAGNOSIS COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    diagnose_database()