import duckdb
import pandas as pd
from datetime import datetime
import pytz

def check_latest_data():
    conn = duckdb.connect('data/warehouse/ai_bubble.duckdb')
    
    # Check latest data for each source
    sources = [
        ('Reddit posts', "SELECT time_id, metric_name, COUNT(*) as count FROM fact_hype_signals WHERE signal_type = 'reddit' GROUP BY time_id, metric_name ORDER BY time_id DESC LIMIT 5"),
        ('News articles', "SELECT time_id, metric_name, COUNT(*) as count FROM fact_hype_signals WHERE signal_type = 'news' GROUP BY time_id, metric_name ORDER BY time_id DESC LIMIT 5"),
        ('GitHub/HF activity', "SELECT time_id, metric_name, COUNT(*) as count FROM fact_hype_signals WHERE signal_type IN ('github', 'huggingface') GROUP BY time_id, metric_name ORDER BY time_id DESC LIMIT 5"),
        ('Stock prices', "SELECT time_id, metric_name, COUNT(*) as count FROM fact_reality_signals GROUP BY time_id, metric_name ORDER BY time_id DESC LIMIT 5")
    ]
    
    print("\nLatest Data Points Summary")
    print("=" * 50)
    
    for source_name, query in sources:
        print(f'\n=== {source_name} ===')
        try:
            result = conn.execute(query).fetchdf()
            if not result.empty:
                # Convert time_id to datetime
                result['date'] = pd.to_datetime(result['time_id'], unit='s')
                result = result.drop('time_id', axis=1)
                print(result)
            else:
                print("No data found")
        except Exception as e:
            print(f"Error querying {source_name}: {str(e)}")
    
    conn.close()

if __name__ == "__main__":
    check_latest_data()