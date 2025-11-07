#!/usr/bin/env python3
"""
Cleanup script to run before incremental updates
Ensures data integrity and removes any duplicate dates
"""
import os
import sys
import duckdb
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
WAREHOUSE_PATH = os.path.join(BASE_DIR, 'data', 'warehouse', 'ai_bubble.duckdb')

def cleanup_duplicates(conn):
    """Remove duplicate dates from all index tables"""
    logger.info("Checking for duplicate dates...")
    
    tables_to_check = ['hype_index', 'reality_index', 'bubble_metrics']
    
    for table in tables_to_check:
        try:
            # Check for duplicates
            result = conn.execute(f"""
                SELECT COUNT(*) as total, COUNT(DISTINCT date) as unique_dates
                FROM {table}
            """).fetchone()
            
            total, unique = result
            duplicates = total - unique
            
            if duplicates > 0:
                logger.warning(f"{table}: Found {duplicates} duplicate dates")
                
                # Clean up duplicates - keep most recent for each date
                if table == 'bubble_metrics':
                    # For bubble_metrics, keep the one with latest computed_at
                    conn.execute(f"""
                        CREATE TEMP TABLE {table}_clean AS
                        WITH ranked AS (
                            SELECT *,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY date 
                                       ORDER BY computed_at DESC NULLS LAST, time_id DESC
                                   ) as rn
                            FROM {table}
                        )
                        SELECT * EXCEPT(rn)
                        FROM ranked
                        WHERE rn = 1;
                    """)
                else:
                    # For hype_index and reality_index, keep most recent time_id
                    conn.execute(f"""
                        CREATE TEMP TABLE {table}_clean AS
                        WITH ranked AS (
                            SELECT *,
                                   ROW_NUMBER() OVER (
                                       PARTITION BY date 
                                       ORDER BY time_id DESC
                                   ) as rn
                            FROM {table}
                        )
                        SELECT * EXCEPT(rn)
                        FROM ranked
                        WHERE rn = 1;
                    """)
                
                # Replace original table
                conn.execute(f"DROP TABLE {table};")
                conn.execute(f"ALTER TABLE {table}_clean RENAME TO {table};")
                
                logger.info(f"✓ Cleaned {table}: removed {duplicates} duplicates")
            else:
                logger.info(f"✓ {table}: No duplicates found")
                
        except Exception as e:
            logger.error(f"Error cleaning {table}: {e}")

def verify_data_integrity(conn):
    """Verify that data is consistent and complete"""
    logger.info("Verifying data integrity...")
    
    try:
        # Check that bubble_metrics has matching dates in hype/reality indices
        result = conn.execute("""
            SELECT 
                COUNT(*) as bubble_count,
                (SELECT COUNT(*) FROM hype_index) as hype_count,
                (SELECT COUNT(*) FROM reality_index) as reality_count
            FROM bubble_metrics
        """).fetchone()
        
        bubble_count, hype_count, reality_count = result
        
        logger.info(f"Data points - Bubble: {bubble_count}, Hype: {hype_count}, Reality: {reality_count}")
        
        if bubble_count > hype_count or bubble_count > reality_count:
            logger.warning("bubble_metrics has more points than source indices - may need recomputation")
        
        # Check for gaps in dates
        gaps = conn.execute("""
            WITH date_series AS (
                SELECT date
                FROM bubble_metrics
                ORDER BY date
            ),
            with_next AS (
                SELECT 
                    date,
                    LEAD(date) OVER (ORDER BY date) as next_date,
                    LEAD(date) OVER (ORDER BY date) - date as gap_days
                FROM date_series
            )
            SELECT COUNT(*) as gap_count
            FROM with_next
            WHERE gap_days > 7 AND next_date IS NOT NULL
        """).fetchone()[0]
        
        if gaps > 0:
            logger.warning(f"Found {gaps} date gaps larger than 7 days")
        else:
            logger.info("✓ No significant date gaps found")
            
    except Exception as e:
        logger.error(f"Error verifying integrity: {e}")

def optimize_database(conn):
    """Optimize database for better performance"""
    logger.info("Optimizing database...")
    
    try:
        # Vacuum to reclaim space
        conn.execute("CHECKPOINT;")
        logger.info("✓ Database checkpointed")
        
    except Exception as e:
        logger.error(f"Error optimizing database: {e}")

def main():
    """Main cleanup function"""
    logger.info("=" * 60)
    logger.info("Pre-Update Cleanup")
    logger.info("=" * 60)
    
    if not os.path.exists(WAREHOUSE_PATH):
        logger.warning(f"Warehouse not found: {WAREHOUSE_PATH}")
        logger.info("This is normal for first run. Cleanup will be skipped.")
        return True
    
    conn = duckdb.connect(WAREHOUSE_PATH)
    
    try:
        cleanup_duplicates(conn)
        verify_data_integrity(conn)
        optimize_database(conn)
        
        logger.info("=" * 60)
        logger.info("Cleanup Complete!")
        logger.info("=" * 60)
        return True
        
    except Exception as e:
        logger.error(f"Cleanup failed: {e}", exc_info=True)
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)