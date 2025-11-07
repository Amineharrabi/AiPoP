import streamlit as st
import os
import sys
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import logging
import re
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Setup paths
BASE_DIR = os.path.dirname(__file__)
WAREHOUSE_PATH = os.path.join(BASE_DIR, 'data', 'warehouse', 'ai_bubble.duckdb')

def check_data_status():
    """Check if required data is available"""
    if not os.path.exists(WAREHOUSE_PATH):
        return False
        
    try:
        conn = duckdb.connect(WAREHOUSE_PATH, read_only=True)
        required_tables = [
            'bubble_metrics',
            'entity_bubble_metrics',
            'bubble_alerts'
        ]
        
        for table in required_tables:
            if not conn.execute(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table}')"
            ).fetchone()[0]:
                conn.close()
                return False
                
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Error checking data status: {str(e)}")
        return False

def run_pipeline():
    """Run the data pipeline"""
    try:
        from setup.run_pipeline import Pipeline
        with st.spinner('Running pipeline...'):
            pipeline = Pipeline()
            pipeline.run(incremental=True)
        st.success('Pipeline completed successfully!')
        st.rerun()
    except Exception as e:
        st.error(f'Pipeline failed: {str(e)}')
        logger.error(f"Pipeline error: {str(e)}", exc_info=True)

def load_global_indices(conn):
    """Load global indices ensuring one point per date"""
    try:
        # Load from bubble_metrics
        df = conn.execute("""
            SELECT 
                date,
                hype_index,
                reality_index,
                hype_7d_avg,
                reality_7d_avg,
                hype_reality_gap,
                bubble_momentum,
                bubble_risk_score
            FROM bubble_metrics
            ORDER BY date
        """).fetchdf()
        
        if not df.empty:
            # Convert date to datetime explicitly
            df['date'] = pd.to_datetime(df['date'], utc=False)
            # Ensure no duplicate dates
            df = df.drop_duplicates(subset=['date'], keep='last')
            # Sort by date
            df = df.sort_values('date')
            df = df.set_index('date')
            return df
            
    except Exception as e:
        logger.error(f"Error loading indices: {e}")
        st.error(f"Error loading data: {e}")
    
    return pd.DataFrame()

def show_alerts(conn):
    """Display bubble alerts"""
    try:
        df = conn.execute("""
            SELECT 
                a.timestamp,
                COALESCE(e.name, 'Unknown') as entity_name,
                a.alert_level,
                ROUND(a.divergence_score, 2) as divergence_score,
                ROUND(COALESCE(a.acceleration_score, 0.0), 2) as acceleration_score,
                ROUND(a.pattern_score, 2) as pattern_score,
                ROUND(a.confidence, 2) as confidence,
                a.contributing_factors
            FROM bubble_alerts a
            LEFT JOIN dim_entity e ON a.entity_id = e.entity_id
            WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY timestamp DESC, confidence DESC
            LIMIT 50
        """).fetchdf()
        
        if len(df) > 0:
            st.subheader('⚠️ Recent Bubble Alerts')
            
            # Deduplicate
            df = df.drop_duplicates(subset=['entity_name', 'timestamp'], keep='first')
            
            # Format contributing_factors
            def format_factors(factors_str):
                if pd.isna(factors_str) or not factors_str:
                    return "N/A"
                try:
                    factors_clean = re.sub(r'np\.float64\(([^)]+)\)', r'\1', str(factors_str))
                    factors_clean = factors_clean.replace("'", '"')
                    factors = json.loads(factors_clean)
                    div = factors.get('divergence', 0)
                    accel = factors.get('acceleration', 0)
                    pattern = factors.get('pattern_similarity', 0)
                    return f"Div: {div:.2f}, Accel: {accel:.2f}, Pattern: {pattern:.2f}"
                except:
                    numbers = re.findall(r'-?\d+\.?\d*', str(factors_str))
                    if len(numbers) >= 3:
                        return f"Div: {float(numbers[0]):.2f}, Accel: {float(numbers[1]):.2f}, Pattern: {float(numbers[2]):.2f}"
                    return str(factors_str)[:50]
            
            df['contributing_factors'] = df['contributing_factors'].apply(format_factors)
            
            # Display
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.info('No recent alerts')
            
    except Exception as e:
        logger.error(f"Error loading alerts: {e}")
        st.info('No alerts available')

def main():
    st.set_page_config(
        page_title='AiPoP — Hype vs Reality',
        page_icon='🚀',
        layout='wide'
    )
    
    st.title('🚀 AiPoP — Hype vs Reality')
    
    # Check if data is available
    has_data = check_data_status()
    
    # Sidebar
    st.sidebar.header("Controls")
    
    # Add pipeline control if available
    try:
        if st.sidebar.button('🔄 Run Pipeline'):
            run_pipeline()
    except:
        pass
    
    if not has_data:
        st.sidebar.warning("""
        ⚠️ No data available. 
        
        Run order:
        1. `scripts/setup_duckdb.py`
        2. Ingest scripts → `data/staging`
        3. `scripts/load_warehouse.py`
        4. `scripts/compute_features.py`
        5. `scripts/compute_indices.py`
        6. `scripts/detect_bubble.py`
        """)
    
    if has_data:
        try:
            conn = duckdb.connect(WAREHOUSE_PATH, read_only=True)
            
            # Load data
            indices_df = load_global_indices(conn)
            
            if indices_df.empty:
                st.warning("No index data available. Run `scripts/compute_indices.py` after loading features.")
            else:
                # Date range selector
                with st.sidebar.expander("Date Range", expanded=True):
                    min_date = indices_df.index.min().date()
                    max_date = indices_df.index.max().date()
                    st.write(f"Available: {min_date} to {max_date}")
                    
                    start_date = st.date_input("Start date", min_date)
                    end_date = st.date_input("End date", max_date)
                
                # Filter data
                mask = (indices_df.index.date >= start_date) & (indices_df.index.date <= end_date)
                filtered_df = indices_df.loc[mask]
                
                # Display metrics
                st.header("📊 Global Indices")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("Data Points", len(filtered_df))
                with col2:
                    if not filtered_df.empty:
                        st.metric("Latest Hype Index", f"{filtered_df['hype_index'].iloc[-1]:.2f}")
                with col3:
                    if not filtered_df.empty:
                        st.metric("Latest Reality Index", f"{filtered_df['reality_index'].iloc[-1]:.2f}")
                with col4:
                    if not filtered_df.empty and 'hype_reality_gap' in filtered_df.columns:
                        st.metric("Hype-Reality Gap", f"{filtered_df['hype_reality_gap'].iloc[-1]:.2f}")
                
                # Main chart
                st.subheader("Hype vs Reality Over Time")
                
                # Create Plotly figure
                fig = go.Figure()
                
                fig.add_trace(go.Scatter(
                    x=filtered_df.index,
                    y=filtered_df['hype_index'],
                    mode='lines+markers',
                    name='Hype Index',
                    line=dict(color='#ff4444', width=2),
                    marker=dict(size=6)
                ))
                
                fig.add_trace(go.Scatter(
                    x=filtered_df.index,
                    y=filtered_df['reality_index'],
                    mode='lines+markers',
                    name='Reality Index',
                    line=dict(color='#4444ff', width=2),
                    marker=dict(size=6)
                ))
                
                # Add 7-day averages if available
                if 'hype_7d_avg' in filtered_df.columns and filtered_df['hype_7d_avg'].notna().any():
                    fig.add_trace(go.Scatter(
                        x=filtered_df.index,
                        y=filtered_df['hype_7d_avg'],
                        mode='lines',
                        name='Hype 7d Avg',
                        line=dict(color='#ff8888', width=1, dash='dash'),
                        opacity=0.6
                    ))
                    
                    fig.add_trace(go.Scatter(
                        x=filtered_df.index,
                        y=filtered_df['reality_7d_avg'],
                        mode='lines',
                        name='Reality 7d Avg',
                        line=dict(color='#8888ff', width=1, dash='dash'),
                        opacity=0.6
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
                
                # Bubble indicators
                if 'hype_reality_gap' in filtered_df.columns and 'bubble_momentum' in filtered_df.columns:
                    st.subheader("Bubble Indicators")
                    
                    fig2 = go.Figure()
                    
                    fig2.add_trace(go.Scatter(
                        x=filtered_df.index,
                        y=filtered_df['hype_reality_gap'],
                        mode='lines+markers',
                        name='Hype-Reality Gap',
                        line=dict(color='#ff8800', width=2),
                        marker=dict(size=5),
                        fill='tozeroy',
                        fillcolor='rgba(255, 136, 0, 0.1)'
                    ))
                    
                    fig2.add_trace(go.Scatter(
                        x=filtered_df.index,
                        y=filtered_df['bubble_momentum'],
                        mode='lines+markers',
                        name='Bubble Momentum',
                        line=dict(color='#aa00ff', width=2),
                        marker=dict(size=5)
                    ))
                    
                    fig2.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.5)
                    
                    fig2.update_layout(
                        xaxis_title="Date",
                        yaxis_title="Indicator Value",
                        hovermode='x unified',
                        height=400,
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
                    
                    st.plotly_chart(fig2, use_container_width=True)
                
                # Show raw data
                with st.expander("📋 View Raw Data"):
                    st.write(f"Shape: {filtered_df.shape}")
                    st.dataframe(filtered_df, use_container_width=True)
            
            # Show alerts
            st.header("⚠️ Recent Bubble Alerts")
            show_alerts(conn)
            
            conn.close()
            
        except Exception as e:
            st.error(f'Error loading data: {str(e)}')
            logger.error(f"Error in main: {str(e)}", exc_info=True)
    else:
        st.header('📊 Global Indices')
        st.warning('No index data available. Please run the data pipeline first.')
        
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown("""
    ### 📚 Pipeline Order:
    1. `setup_duckdb.py`
    2. Ingest scripts
    3. `load_warehouse.py`
    4. `compute_features.py`
    5. `compute_indices.py`
    6. `detect_bubble.py`
    """)

if __name__ == "__main__":
    main()