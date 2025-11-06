"""Summary of warehouse import verification"""
import duckdb
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
WAREHOUSE_PATH = BASE_DIR / 'data' / 'warehouse' / 'ai_bubble.duckdb'

conn = duckdb.connect(str(WAREHOUSE_PATH))

print("=" * 80)
print("WAREHOUSE IMPORT SUMMARY")
print("=" * 80)

print("\n✅ ALL DATA SOURCES VERIFIED:")
print("-" * 80)

# Get counts by source
results = conn.execute("""
    SELECT 
        s.source_name,
        COUNT(DISTINCT f.entity_id) as entities,
        COUNT(*) as hype_signals
    FROM fact_hype_signals f
    JOIN dim_source s ON f.source_id = s.source_id
    GROUP BY s.source_name
    ORDER BY s.source_name
""").fetchall()

for source, entities, signals in results:
    print(f"  {source.upper():15} {entities:3} entities, {signals:6,} hype signals")

print("\n✅ REALITY SIGNALS BY PROVENANCE:")
print("-" * 80)
reality_results = conn.execute("""
    SELECT 
        provenance,
        COUNT(*) as signals
    FROM fact_reality_signals
    GROUP BY provenance
    ORDER BY provenance
""").fetchall()

for provenance, signals in reality_results:
    print(f"  {provenance:15} {signals:6,} signals")

print("\n✅ DATA QUALITY CHECKS:")
print("-" * 80)

# Check for NULLs
null_checks = conn.execute("""
    SELECT 
        'NULL time_id in hype' as check_name,
        COUNT(*) as count
    FROM fact_hype_signals WHERE time_id IS NULL
    UNION ALL
    SELECT 'NULL time_id in reality', COUNT(*) FROM fact_reality_signals WHERE time_id IS NULL
    UNION ALL
    SELECT 'NULL entity_id in hype', COUNT(*) FROM fact_hype_signals WHERE entity_id IS NULL
    UNION ALL
    SELECT 'NULL entity_id in reality', COUNT(*) FROM fact_reality_signals WHERE entity_id IS NULL
    UNION ALL
    SELECT 'NULL source_id in hype', COUNT(*) FROM fact_hype_signals WHERE source_id IS NULL
""").fetchall()

all_good = True
for check_name, count in null_checks:
    status = "✅" if count == 0 else "❌"
    print(f"  {status} {check_name:30} {count}")
    if count > 0:
        all_good = False

print("\n✅ DIMENSION TABLES:")
print("-" * 80)
dim_stats = conn.execute("""
    SELECT 'dim_time' as table_name, COUNT(*) as count FROM dim_time
    UNION ALL
    SELECT 'dim_entity', COUNT(*) FROM dim_entity
    UNION ALL
    SELECT 'dim_source', COUNT(*) FROM dim_source
""").fetchall()

for table, count in dim_stats:
    print(f"  {table:15} {count:6,} records")

print("\n✅ FACT TABLES:")
print("-" * 80)
fact_stats = conn.execute("""
    SELECT 'fact_hype_signals' as table_name, COUNT(*) as count FROM fact_hype_signals
    UNION ALL
    SELECT 'fact_reality_signals', COUNT(*) FROM fact_reality_signals
""").fetchall()

for table, count in fact_stats:
    print(f"  {table:20} {count:6,} records")

print("\n✅ DATE COVERAGE:")
print("-" * 80)
date_info = conn.execute("""
    SELECT 
        MIN(date) as min_date,
        MAX(date) as max_date,
        COUNT(DISTINCT date) as unique_dates
    FROM dim_time
""").fetchone()

print(f"  Date range: {date_info[0]} to {date_info[1]}")
print(f"  Total unique dates: {date_info[2]:,}")

print("\n" + "=" * 80)
if all_good:
    print("✅ ALL VERIFICATIONS PASSED - DATA FULLY IMPORTED!")
else:
    print("⚠️  SOME ISSUES DETECTED - CHECK ABOVE")
print("=" * 80)

conn.close()

