import duckdb
conn=duckdb.connect('data/warehouse/ai_bubble.duckdb')
for t in ['dim_time','dim_entity','fact_hype_signals','fact_reality_signals','combined_features','hype_index','bubble_metrics']:
    try:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    except Exception as e:
        cnt = f'missing ({e})'
    print(f"{t}: {cnt}")
conn.close()
