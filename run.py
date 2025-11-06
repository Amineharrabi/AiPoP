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
    """Plot global indices with hourly granularity"""
    # Check if computed_at column exists
    try:
        columns = conn.execute("DESCRIBE bubble_metrics").fetchdf()
        has_computed_at = 'computed_at' in columns['column_name'].values
    except:
        has_computed_at = False
    
    if has_computed_at:
        # Use computed_at for hourly granularity
        df = conn.execute("""
            SELECT 
                computed_at,
                date,
                hype_index,
                reality_index,
                hype_reality_gap,
                bubble_momentum
            FROM bubble_metrics
            WHERE computed_at IS NOT NULL
            ORDER BY computed_at
        """).fetchdf()
        
        if df.empty:
            st.warning("No bubble metrics data available")
            return
        
        # Convert computed_at to datetime if it's not already
        df['computed_at'] = pd.to_datetime(df['computed_at'])
        time_col = 'computed_at'
    else:
        # Fallback to date column (daily granularity)
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
        
        if df.empty:
            st.warning("No bubble metrics data available")
            return
        
        # Convert date to datetime
        df['date'] = pd.to_datetime(df['date'])
        time_col = 'date'
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Indices', 'Bubble Indicators'),
        vertical_spacing=0.12
    )
    
    # Plot indices using timestamp (computed_at or date) for granularity
    fig.add_trace(
        go.Scatter(
            x=df[time_col],
            y=df['hype_index'],
            name='Hype Index',
            line=dict(color='red'),
            mode='lines+markers'
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=df[time_col],
            y=df['reality_index'],
            name='Reality Index',
            line=dict(color='blue'),
            mode='lines+markers'
        ),
        row=1, col=1
    )
    
    # Plot bubble indicators
    fig.add_trace(
        go.Scatter(
            x=df[time_col],
            y=df['hype_reality_gap'],
            name='Hype-Reality Gap',
            line=dict(color='orange'),
            mode='lines+markers'
        ),
        row=2, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=df[time_col],
            y=df['bubble_momentum'],
            name='Bubble Momentum',
            line=dict(color='purple'),
            mode='lines+markers'
        ),
        row=2, col=1
    )
    
    # Update layout with proper datetime formatting
    title_suffix = " (Hourly Updates)" if has_computed_at else " (Daily Updates)"
    tick_format = '%Y-%m-%d %H:%M' if has_computed_at else '%Y-%m-%d'
    
    fig.update_layout(
        height=800,
        showlegend=True,
        title_text=f'AI Bubble Detection Indices{title_suffix}',
        xaxis=dict(
            title='Time',
            type='date',
            tickformat=tick_format
        ),
        xaxis2=dict(
            title='Time',
            type='date',
            tickformat=tick_format
        )
    )
    
    st.plotly_chart(fig, width='stretch')

def show_alerts(conn):
    """Display bubble alerts"""
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
        st.subheader('Recent Bubble Alerts')
        
        # Deduplicate: keep only the most recent alert per entity
        df = df.drop_duplicates(subset=['entity_name', 'timestamp'], keep='first')
        
        # Format contributing_factors for better display
        def format_factors(factors_str):
            """Parse and format contributing factors"""
            if pd.isna(factors_str) or not factors_str:
                return "N/A"
            try:
                # Handle string representation of dict
                # Remove numpy type annotations
                factors_clean = re.sub(r'np\.float64\(([^)]+)\)', r'\1', str(factors_str))
                factors_clean = factors_clean.replace("'", '"')
                
                # Try to parse as JSON
                factors = json.loads(factors_clean)
                div = factors.get('divergence', 0)
                accel = factors.get('acceleration', 0)
                pattern = factors.get('pattern_similarity', 0)
                return f"Div: {div:.2f}, Accel: {accel:.2f}, Pattern: {pattern:.2f}"
            except:
                # Fallback: try to extract numbers using regex
                numbers = re.findall(r'-?\d+\.?\d*', str(factors_str))
                if len(numbers) >= 3:
                    return f"Div: {float(numbers[0]):.2f}, Accel: {float(numbers[1]):.2f}, Pattern: {float(numbers[2]):.2f}"
                return str(factors_str)[:50]  # Truncate if can't parse
        
        df['contributing_factors'] = df['contributing_factors'].apply(format_factors)
        
        # Reorder columns for better display
        display_cols = ['timestamp', 'entity_name', 'alert_level', 'divergence_score', 
                        'acceleration_score', 'pattern_score', 'confidence', 'contributing_factors']
        df = df[display_cols]
        
        # Color code alert levels
        def color_alert(val):
            colors = {
                'critical': 'background-color: #ff4444; color: white; font-weight: bold',
                'warning': 'background-color: #ff8800; color: white',
                'watch': 'background-color: #ffdd00'
            }
            return colors.get(val, '')
            
        st.dataframe(
            df.style.applymap(
                color_alert,
                subset=['alert_level']
            ),
            use_container_width=True
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