import duckdb
conn = duckdb.connect('data/warehouse/ai_bubble.duckdb')
try:
    df = conn.execute("PRAGMA table_info('combined_features')").fetchdf()
    if df.empty:
        print('combined_features: missing or no columns')
    else:
        print('combined_features PRAGMA output:')
        print(df.to_string(index=False))
finally:
    conn.close()
