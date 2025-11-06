"""Analyze entity data distribution to understand insufficient data issue"""
import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
WAREHOUSE_PATH = BASE_DIR / 'data' / 'warehouse' / 'ai_bubble.duckdb'

conn = duckdb.connect(str(WAREHOUSE_PATH))

print("=" * 80)
print("ENTITY DATA DISTRIBUTION ANALYSIS")
print("=" * 80)

# Check entity_bubble_metrics
print("\n1. ENTITY_BUBBLE_METRICS DATA:")
print("-" * 80)
result = conn.execute("""
    SELECT 
        entity_id,
        COUNT(*) as cnt,
        MIN(date) as min_date,
        MAX(date) as max_date,
        COUNT(DISTINCT date) as unique_dates,
        DATEDIFF('day', MIN(date), MAX(date)) as date_span_days
    FROM entity_bubble_metrics
    GROUP BY entity_id
    ORDER BY cnt DESC
""").fetchdf()

print(f"Total entities: {len(result)}")
print(f"Entities with <5 samples: {len(result[result['cnt'] < 5])}")
print(f"Entities with 5-10 samples: {len(result[(result['cnt'] >= 5) & (result['cnt'] < 10)])}")
print(f"Entities with >=10 samples: {len(result[result['cnt'] >= 10])}")
print(f"Entities with >=30 samples: {len(result[result['cnt'] >= 30])}")

print("\nSample of entities with <5 samples:")
print(result[result['cnt'] < 5].head(10))

print("\n2. DATA SOURCE ANALYSIS:")
print("-" * 80)
print("\nCombined features (source of entity_bubble_metrics):")
cf_stats = conn.execute("""
    SELECT 
        entity_id,
        COUNT(*) as cnt,
        MIN(date) as min_date,
        MAX(date) as max_date
    FROM combined_features
    GROUP BY entity_id
    ORDER BY cnt DESC
    LIMIT 10
""").fetchdf()
print(cf_stats)

print("\nDate range in combined_features:")
date_range = conn.execute("""
    SELECT 
        MIN(date) as min_date,
        MAX(date) as max_date,
        COUNT(DISTINCT date) as unique_dates,
        DATEDIFF('day', MIN(date), MAX(date)) as date_span_days
    FROM combined_features
""").fetchone()
print(f"Date range: {date_range[0]} to {date_range[1]}")
print(f"Unique dates: {date_range[2]}")
print(f"Total span: {date_range[3]} days")

print("\n3. SOLUTION OPTIONS:")
print("-" * 80)
print("Option 1: Increase lookback window (currently 90 days)")
current_90d = conn.execute("""
    SELECT COUNT(DISTINCT entity_id)
    FROM entity_bubble_metrics
    WHERE date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY entity_id
    HAVING COUNT(*) >= 5
""").fetchone()
print(f"  - Entities with >=5 samples in last 90 days: {current_90d[0] if current_90d else 0}")

all_time = conn.execute("""
    SELECT COUNT(DISTINCT entity_id)
    FROM entity_bubble_metrics
    GROUP BY entity_id
    HAVING COUNT(*) >= 5
""").fetchone()
print(f"  - Entities with >=5 samples in ALL time: {all_time[0] if all_time else 0}")

print("\nOption 2: Filter entities early (only include entities with sufficient data)")
print("Option 3: Use forward-fill/interpolation for missing dates")
print("Option 4: Reduce min_samples requirement further (already at 5)")

conn.close()

