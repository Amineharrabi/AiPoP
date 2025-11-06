"""Check entity_bubble_metrics for issues"""
import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
WAREHOUSE_PATH = BASE_DIR / 'data' / 'warehouse' / 'ai_bubble.duckdb'

conn = duckdb.connect(str(WAREHOUSE_PATH))

print("=" * 80)
print("ENTITY_BUBBLE_METRICS DATA CHECK")
print("=" * 80)

# Check for empty entity names
print("\n1. Entities with empty/NULL names:")
empty_names = conn.execute("""
    SELECT entity_id, entity_name, entity_type, COUNT(*) as cnt
    FROM entity_bubble_metrics
    WHERE entity_name IS NULL OR entity_name = ''
    GROUP BY entity_id, entity_name, entity_type
    LIMIT 10
""").fetchdf()
print(empty_names)

# Check for duplicates
print("\n2. Duplicate entity_id + date combinations:")
dupes = conn.execute("""
    SELECT entity_id, date, COUNT(*) as cnt
    FROM entity_bubble_metrics
    GROUP BY entity_id, date
    HAVING COUNT(*) > 1
    LIMIT 10
""").fetchdf()
print(dupes)

# Check for NaN scores
print("\n3. Entities with NaN scores:")
nan_scores = conn.execute("""
    SELECT entity_id, entity_name, entity_type, 
           COUNT(*) as total_rows,
           SUM(CASE WHEN hype_score IS NULL OR hype_score != hype_score THEN 1 ELSE 0 END) as nan_hype,
           SUM(CASE WHEN reality_score IS NULL OR reality_score != reality_score THEN 1 ELSE 0 END) as nan_reality
    FROM entity_bubble_metrics
    GROUP BY entity_id, entity_name, entity_type
    HAVING nan_hype > 0 OR nan_reality > 0
    LIMIT 10
""").fetchdf()
print(nan_scores)

# Check sample of latest data
print("\n4. Sample of latest data per entity:")
sample = conn.execute("""
    WITH latest AS (
        SELECT entity_id, MAX(date) as latest_date
        FROM entity_bubble_metrics
        GROUP BY entity_id
    )
    SELECT e.entity_id, e.entity_name, e.entity_type, e.date,
           e.hype_score, e.reality_score, e.bubble_momentum
    FROM entity_bubble_metrics e
    JOIN latest l ON e.entity_id = l.entity_id AND e.date = l.latest_date
    WHERE e.entity_name IS NOT NULL AND e.entity_name != ''
    ORDER BY ABS(e.bubble_momentum) DESC
    LIMIT 10
""").fetchdf()
print(sample)

conn.close()

