import os
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import pytz

# Setup paths
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
WAREHOUSE_PATH = os.path.join(BASE_DIR, 'data', 'warehouse', 'ai_bubble.duckdb')

def create_test_data():
    """Create test data for debugging"""
    conn = duckdb.connect(WAREHOUSE_PATH)
    
    try:
        # Check if we have entities
        entity_count = conn.execute("SELECT COUNT(*) FROM dim_entity").fetchone()[0]
        if entity_count == 0:
            print("Creating test entities...")
            conn.execute("""
                INSERT INTO dim_entity (entity_id, name, ticker, entity_type) VALUES
                (1, 'NVIDIA Corporation', 'NVDA', 'company'),
                (2, 'Microsoft Corporation', 'MSFT', 'company'),
                (3, 'Tesla Inc', 'TSLA', 'company')
            """)
        
        # Create test time data
        now = datetime.now(pytz.UTC)
        start_date = now - timedelta(days=30)
        
        print("Creating test time data...")
        time_data = []
        current = start_date
        time_id = 1
        while current <= now:
            time_data.append({
                'time_id': time_id,
                'date': current.date(),
                'year': current.year,
                'month': current.month,
                'day': current.day,
                'weekday': current.weekday()
            })
            current += timedelta(days=1)
            time_id += 1
        
        # Clear existing time data and insert new
        conn.execute("DELETE FROM dim_time")
        conn.register('time_df', pd.DataFrame(time_data))
        conn.execute("INSERT INTO dim_time SELECT * FROM time_df")
        
        # Create test hype signals
        print("Creating test hype signals...")
        hype_data = []
        for time_id in range(1, min(31, len(time_data) + 1)):
            for entity_id in range(1, 4):  # 3 entities
                # Sentiment data
                hype_data.extend([
                    {
                        'time_id': time_id,
                        'entity_id': entity_id,
                        'source_id': 1,
                        'signal_type': 'sentiment',
                        'metric_name': 'sentiment_score',
                        'metric_value': 0.5 + (time_id * 0.01),  # Trending up
                        'raw_text': f'Test sentiment for entity {entity_id} on day {time_id}',
                        'url': f'https://test.com/entity{entity_id}/day{time_id}'
                    },
                    {
                        'time_id': time_id,
                        'entity_id': entity_id,
                        'source_id': 2,
                        'signal_type': 'activity',
                        'metric_name': 'mention_count',
                        'metric_value': 10 + time_id,  # Increasing mentions
                        'raw_text': f'Test activity for entity {entity_id} on day {time_id}',
                        'url': f'https://test.com/activity{entity_id}/day{time_id}'
                    }
                ])
        
        conn.execute("DELETE FROM fact_hype_signals")
        conn.register('hype_df', pd.DataFrame(hype_data))
        conn.execute("INSERT INTO fact_hype_signals SELECT * FROM hype_df")
        
        # Create test reality signals
        print("Creating test reality signals...")
        reality_data = []
        base_price = 100
        for time_id in range(1, min(31, len(time_data) + 1)):
            for entity_id in range(1, 4):  # 3 entities
                price_variation = (entity_id * 5) + (time_id * 2)  # Different trends per entity
                volume = 1000000 + (time_id * 10000)  # Increasing volume
                
                reality_data.extend([
                    {
                        'time_id': time_id,
                        'entity_id': entity_id,
                        'metric_name': 'stock_price',
                        'metric_value': base_price + price_variation,
                        'metric_unit': 'USD',
                        'provenance': 'yfinance'
                    },
                    {
                        'time_id': time_id,
                        'entity_id': entity_id,
                        'metric_name': 'volume',
                        'metric_value': volume,
                        'metric_unit': 'shares',
                        'provenance': 'yfinance'
                    }
                ])
        
        conn.execute("DELETE FROM fact_reality_signals")
        conn.register('reality_df', pd.DataFrame(reality_data))
        conn.execute("INSERT INTO fact_reality_signals SELECT * FROM reality_df")
        
        print("Test data created successfully!")
        
    except Exception as e:
        print(f"Error creating test data: {str(e)}")
        raise
    finally:
        conn.close()

def test_simple_query():
    """Test a simple version of the query"""
    conn = duckdb.connect(WAREHOUSE_PATH)
    
    try:
        # Test the JOIN first
        print("Testing simple JOIN...")
        result = conn.execute("""
            SELECT 
                h.time_id,
                h.entity_id,
                h.metric_value as sentiment,
                r.metric_value as price
            FROM fact_hype_signals h
            JOIN fact_reality_signals r 
                ON h.time_id = r.time_id 
                AND h.entity_id = r.entity_id
            LIMIT 5
        """).fetchall()
        
        print(f"Simple JOIN returned {len(result)} rows")
        
        # Test just the daily metrics part
        print("Testing daily metrics...")
        result = conn.execute("""
            SELECT 
                t.time_id,
                t.date,
                e.entity_id,
                e.name as entity_name,
                e.entity_type,
                AVG(CASE WHEN signal_type = 'sentiment' THEN metric_value ELSE NULL END) as avg_sentiment,
                COUNT(CASE WHEN signal_type = 'sentiment' THEN 1 ELSE NULL END) as sentiment_mentions
            FROM fact_hype_signals h
            JOIN dim_time t ON h.time_id = t.time_id
            JOIN dim_entity e ON h.entity_id = e.entity_id
            GROUP BY t.time_id, t.date, e.entity_id, e.name, e.entity_type
            LIMIT 5
        """).fetchall()
        
        print(f"Daily metrics returned {len(result)} rows")
        
    except Exception as e:
        print(f"Error in test query: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    print(f"Working with warehouse: {WAREHOUSE_PATH}")
    create_test_data()
    test_simple_query()