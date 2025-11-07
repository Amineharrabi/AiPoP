import os
import duckdb
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from datetime import date, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
WAREHOUSE_PATH = os.path.join(BASE_DIR, 'data', 'warehouse', 'ai_bubble.duckdb')

def connect_db(path: str):
    if not os.path.exists(path):
        st.error(f"DuckDB warehouse not found at: {path}\nRun `scripts/setup_duckdb.py` and ingest pipeline first.")
        return None
    return duckdb.connect(path, read_only=True)

def load_global_indices(conn):
    """Load global indices ensuring one point per date"""
    try:
        # Try to load from bubble_metrics first (preferred method)
        df = conn.execute("""
            SELECT 
                date,
                hype_index,
                reality_index,
                hype_7d_avg,
                reality_7d_avg,
                hype_reality_gap,
                bubble_risk_score
            FROM bubble_metrics
            ORDER BY date
        """).fetchdf()
        
        if not df.empty:
            # Convert date to datetime explicitly and ensure no timezone issues
            df['date'] = pd.to_datetime(df['date'], utc=False)
            # Ensure no duplicate dates
            df = df.drop_duplicates(subset=['date'], keep='last')
            # Sort by date before setting index
            df = df.sort_values('date')
            df = df.set_index('date')
            return df
            
    except Exception as e:
        st.warning(f"Could not load from bubble_metrics: {e}")
        
        # Fallback to computing from hype_index and reality_index
        try:
            df = conn.execute("""
                WITH daily_data AS (
                    SELECT DISTINCT ON (h.date)
                        h.date,
                        h.hype_index,
                        r.reality_index
                    FROM hype_index h
                    JOIN reality_index r ON h.date = r.date
                    ORDER BY h.date, h.time_id DESC
                )
                SELECT * FROM daily_data ORDER BY date
            """).fetchdf()
            
            if not df.empty:
                df['date'] = pd.to_datetime(df['date'], utc=False)
                df = df.sort_values('date')
                df = df.set_index('date')
                return df
        except Exception as e:
            st.error(f"Error loading indices: {e}")
    
    return pd.DataFrame()

def load_alerts(conn, limit: int = 50):
    try:
        q = f"""
            SELECT 
                a.timestamp, 
                a.entity_id, 
                a.alert_level, 
                a.divergence_score, 
                a.acceleration_score, 
                a.pattern_score, 
                e.name as entity_name 
            FROM bubble_alerts a 
            LEFT JOIN dim_entity e ON a.entity_id = e.entity_id 
            ORDER BY a.timestamp DESC 
            LIMIT {limit}
        """
        df = conn.execute(q).fetchdf()
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception:
        return pd.DataFrame()

def main():
    st.set_page_config(page_title="AiPoP — Hype vs Reality Dashboard", layout='wide')
    st.title("🚀 AiPoP — Hype vs Reality")
    
    conn = connect_db(WAREHOUSE_PATH)
    if conn is None:
        return
    
    # Sidebar controls
    st.sidebar.header("Controls")
    
    # Load data first to get available date range
    indices_df = load_global_indices(conn)
    
    if not indices_df.empty:
        min_date = indices_df.index.min().date()
        max_date = indices_df.index.max().date()
        
        with st.sidebar.expander("Date Range", expanded=True):
            st.write(f"Available data: {min_date} to {max_date}")
            start = st.date_input("Start date", min_date)
            end = st.date_input("End date", max_date)
    else:
        today = date.today()
        with st.sidebar.expander("Date Range", expanded=True):
            start = st.date_input("Start date", today - timedelta(days=30))
            end = st.date_input("End date", today)
    
    # Global Indices Section
    st.header("📊 Global Indices")
    
    if indices_df.empty:
        st.warning("No index data available. Run `scripts/compute_indices.py` after loading features.")
    else:
        # Filter by date range
        mask = (indices_df.index.date >= start) & (indices_df.index.date <= end)
        filtered_df = indices_df.loc[mask]
        
        # Display metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Data Points", len(filtered_df))
        with col2:
            st.metric("Latest Hype Index", f"{filtered_df['hype_index'].iloc[-1]:.2f}" if not filtered_df.empty else "N/A")
        with col3:
            st.metric("Latest Reality Index", f"{filtered_df['reality_index'].iloc[-1]:.2f}" if not filtered_df.empty else "N/A")
        with col4:
            if 'hype_reality_gap' in filtered_df.columns:
                st.metric("Hype-Reality Gap", f"{filtered_df['hype_reality_gap'].iloc[-1]:.2f}" if not filtered_df.empty else "N/A")
        
        # Main chart - use plotly for better control
        st.subheader("Hype vs Reality Over Time")
        
        # Create Plotly figure for better date handling
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=filtered_df.index,
            y=filtered_df['hype_index'],
            mode='lines+markers',
            name='Hype Index',
            line=dict(color='#1f77b4', width=2),
            marker=dict(size=6)
        ))
        
        fig.add_trace(go.Scatter(
            x=filtered_df.index,
            y=filtered_df['reality_index'],
            mode='lines+markers',
            name='Reality Index',
            line=dict(color='#ff7f0e', width=2),
            marker=dict(size=6)
        ))
        
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Index Value",
            hovermode='x unified',
            height=500,
            xaxis=dict(
                tickformat='%Y-%m-%d',
                tickmode='auto',
                nticks=20
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Also show simple line chart as backup
        with st.expander("Alternative View (Streamlit Native)"):
            chart_data = filtered_df[['hype_index', 'reality_index']].copy()
            chart_data.columns = ['Hype Index', 'Reality Index']
            st.line_chart(chart_data, use_container_width=True)
        
        # Show data table
        with st.expander("View Raw Data"):
            st.write(f"Shape: {filtered_df.shape}")
            st.write(f"Index type: {type(filtered_df.index)}")
            st.write(f"Index sample: {filtered_df.index[:5].tolist()}")
            st.dataframe(filtered_df)
    
    # Alerts Section
    st.header("⚠️ Recent Bubble Alerts")
    alerts_df = load_alerts(conn)
    
    if alerts_df.empty:
        st.info("No alerts found. Run `scripts/detect_bubble.py` to generate bubble_alerts.")
    else:
        st.dataframe(alerts_df)
    
    # Instructions
    st.sidebar.markdown("---")
    st.sidebar.markdown("""
    ### Run Order:
    1. `scripts/setup_duckdb.py`
    2. Ingest scripts → `data/staging`
    3. `scripts/load_warehouse.py`
    4. `scripts/compute_features.py`
    5. `scripts/compute_indices.py`
    6. `scripts/detect_bubble.py`
    """)

if __name__ == '__main__':
    main()