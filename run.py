import streamlit as st
import os
import sys
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import logging
from setup.run_pipeline import Pipeline

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
        conn = duckdb.connect(WAREHOUSE_PATH)
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
        with st.spinner('Running pipeline...'):
            pipeline = Pipeline()
            pipeline.run(incremental=True)
        st.success('Pipeline completed successfully!')
        st.rerun()
    except Exception as e:
        st.error(f'Pipeline failed: {str(e)}')

def plot_indices(conn):
    """Plot global indices"""
    df = conn.execute("""
        SELECT 
            date,
            hype_index,
            reality_index,
            hype_reality_gap,
            bubble_momentum
        FROM bubble_metrics
        ORDER BY date
    """).fetchdf()
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Indices', 'Bubble Indicators'),
        vertical_spacing=0.12
    )
    
    # Plot indices
    fig.add_trace(
        go.Scatter(
            x=df['date'],
            y=df['hype_index'],
            name='Hype Index',
            line=dict(color='red')
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=df['date'],
            y=df['reality_index'],
            name='Reality Index',
            line=dict(color='blue')
        ),
        row=1, col=1
    )
    
    # Plot bubble indicators
    fig.add_trace(
        go.Scatter(
            x=df['date'],
            y=df['hype_reality_gap'],
            name='Hype-Reality Gap',
            line=dict(color='orange')
        ),
        row=2, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=df['date'],
            y=df['bubble_momentum'],
            name='Bubble Momentum',
            line=dict(color='purple')
        ),
        row=2, col=1
    )
    
    # Update layout
    fig.update_layout(
        height=800,
        showlegend=True,
        title_text='AI Bubble Detection Indices'
    )
    
    st.plotly_chart(fig, width='stretch')

def show_alerts(conn):
    """Display bubble alerts"""
    df = conn.execute("""
        SELECT 
            a.*,
            e.name as entity_name
        FROM bubble_alerts a
        LEFT JOIN dim_entity e ON a.entity_id = e.entity_id
        WHERE timestamp >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY timestamp DESC, confidence DESC
    """).fetchdf()
    
    if len(df) > 0:
        st.subheader('Recent Bubble Alerts')
        
        # Color code alert levels
        def color_alert(val):
            colors = {
                'critical': 'background-color: red; color: white',
                'warning': 'background-color: orange',
                'watch': 'background-color: yellow'
            }
            return colors.get(val, '')
            
        st.dataframe(
            df.style.applymap(
                color_alert,
                subset=['alert_level']
            )
        )
    else:
        st.info('No recent alerts')

def main():
    st.set_page_config(
        page_title='AI Bubble Detection',
        page_icon='📊',
        layout='wide'
    )
    
    st.title('AI Bubble Detection Dashboard')
    
    # Check if data is available
    has_data = check_data_status()
    
    # Add pipeline control
    with st.sidebar:
        st.subheader('Pipeline Control')
        if st.button('Run Pipeline'):
            run_pipeline()
            
        if not has_data:
            st.warning("""
            No index data available. Click 'Run Pipeline' to process data.
            
            Run order:
            1. scripts/setup_duckdb.py
            2. ingest scripts → data/staging
            3. scripts/load_warehouse.py
            4. scripts/compute_features.py
            5. scripts/compute_indices.py
            6. scripts/detect_bubble.py
            """)
    
    if has_data:
        try:
            conn = duckdb.connect(WAREHOUSE_PATH)
            
            # Display global indices
            st.header('Global Indices')
            plot_indices(conn)
            
            # Show alerts
            show_alerts(conn)
            
            conn.close()
            
        except Exception as e:
            st.error(f'Error loading data: {str(e)}')
    else:
        st.header('Global Indices')
        st.warning('No index data available. Use Pipeline Control to process data.')

if __name__ == "__main__":
    main()