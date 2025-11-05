import duckdb
import os

# Path to DuckDB database
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'data', 'warehouse', 'ai_bubble.duckdb')
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

con = duckdb.connect(DB_PATH)

# Create tables
con.execute('''
CREATE TABLE IF NOT EXISTS dim_time (
  time_id INTEGER PRIMARY KEY,
  date DATE,
  unix_ts BIGINT,
  week INTEGER,
  month INTEGER,
  year INTEGER,
  is_business_day BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_entity (
  entity_id INTEGER PRIMARY KEY,
  name TEXT,
  ticker TEXT,
  entity_type TEXT,
  industry TEXT
);

CREATE TABLE IF NOT EXISTS dim_source (
  source_id INTEGER PRIMARY KEY,
  source_name TEXT,
  source_type TEXT
);

CREATE TABLE IF NOT EXISTS fact_hype_signals (
  time_id INTEGER,
  entity_id INTEGER,
  source_id INTEGER,
  signal_type TEXT,
  metric_name TEXT,
  metric_value DOUBLE,
  raw_text TEXT,
  url TEXT
);

CREATE TABLE IF NOT EXISTS fact_reality_signals (
  time_id INTEGER,
  entity_id INTEGER,
  metric_name TEXT,
  metric_value DOUBLE,
  metric_unit TEXT,
  provenance TEXT
);

CREATE TABLE IF NOT EXISTS fact_indices (
  time_id INTEGER,
  entity_id INTEGER,
  hype_index DOUBLE,
  reality_index DOUBLE,
  divergence DOUBLE,
  confidence DOUBLE
);
''')

print('DuckDB schema created at:', DB_PATH)
con.close()
