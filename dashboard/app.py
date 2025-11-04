import os
import duckdb
import pandas as pd
import streamlit as st
from datetime import date

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
WAREHOUSE_PATH = os.path.join(BASE_DIR, 'data', 'warehouse', 'ai_bubble.duckdb')


def connect_db(path: str):
    if not os.path.exists(path):
        st.error(f"DuckDB warehouse not found at: {path}\nRun `scripts/setup_duckdb.py` and ingest pipeline first.")
        return None
    return duckdb.connect(path)


def load_global_indices(conn):
    try:
        df = conn.execute("SELECT date, hype_index, reality_index FROM hype_index JOIN reality_index USING(time_id, date) ORDER BY date").fetchdf()
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        return df
    except Exception:
        return pd.DataFrame()


def load_alerts(conn, limit: int = 50):
    try:
        q = f"SELECT a.timestamp, a.entity_id, a.alert_level, a.divergence_score, a.acceleration_score, a.pattern_score, e.name as entity_name FROM bubble_alerts a LEFT JOIN dim_entity e ON a.entity_id = e.entity_id ORDER BY a.timestamp DESC LIMIT {limit}"
        df = conn.execute(q).fetchdf()
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception:
        return pd.DataFrame()


def main():
    st.set_page_config(page_title="AiPoP — Hype vs Reality Dashboard", layout='wide')
    st.title("AiPoP — Hype vs Reality")

    conn = connect_db(WAREHOUSE_PATH)
    if conn is None:
        return

    st.sidebar.header("Controls")
    with st.sidebar.expander("Date range"):
        today = date.today()
        start = st.date_input("Start date", today.replace(year=today.year - 1))
        end = st.date_input("End date", today)

    st.header("Global Indices")
    indices_df = load_global_indices(conn)
    if indices_df.empty:
        st.warning("No index data available. Run `scripts/compute_indices.py` after loading features.")
    else:
        # Filter by date
        mask = (indices_df.index.date >= start) & (indices_df.index.date <= end)
        st.line_chart(indices_df.loc[mask, ['hype_index', 'reality_index']])

    st.header("Recent Bubble Alerts")
    alerts_df = load_alerts(conn)
    if alerts_df.empty:
        st.info("No alerts found. Run `scripts/detect_bubble.py` to generate bubble_alerts.")
    else:
        st.dataframe(alerts_df)

    st.sidebar.markdown("---")
    st.sidebar.markdown("Run order:\n1. `scripts/setup_duckdb.py`\n2. ingest scripts -> `data/staging`\n3. `scripts/load_warehouse.py`\n4. `scripts/compute_features.py`\n5. `scripts/compute_indices.py`\n6. `scripts/detect_bubble.py`\n")


if __name__ == '__main__':
    main()
