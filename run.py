def plot_indices(conn):
    """Plot global indices with proper date handling - one point per date"""
    
    # Always use date column for cleaner visualization (one point per day)
    # This prevents clustering when multiple updates happen on the same day
    df = conn.execute("""
        SELECT 
            date,
            hype_index,
            reality_index,
            hype_reality_gap,
            bubble_momentum,
            hype_7d_avg,
            reality_7d_avg
        FROM bubble_metrics
        ORDER BY date
    """).fetchdf()
    
    if df.empty:
        st.warning("No bubble metrics data available")
        return
    
    # Convert date to datetime and ensure proper sorting
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    # Display data range info
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Data Points", len(df))
    with col2:
        st.metric("Date Range", f"{df['date'].min().strftime('%Y-%m-%d')} to {df['date'].max().strftime('%Y-%m-%d')}")
    with col3:
        st.metric("Days Covered", len(df))
    
    # Create subplots
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=('Hype vs Reality Indices', 'Bubble Indicators'),
        vertical_spacing=0.15,
        row_heights=[0.6, 0.4]
    )
    
    # Plot indices
    fig.add_trace(
        go.Scatter(
            x=df['date'],
            y=df['hype_index'],
            name='Hype Index',
            line=dict(color='#ff4444', width=2),
            mode='lines+markers',
            marker=dict(size=6),
            hovertemplate='<b>Date:</b> %{x|%Y-%m-%d}<br><b>Hype:</b> %{y:.4f}<extra></extra>'
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=df['date'],
            y=df['reality_index'],
            name='Reality Index',
            line=dict(color='#4444ff', width=2),
            mode='lines+markers',
            marker=dict(size=6),
            hovertemplate='<b>Date:</b> %{x|%Y-%m-%d}<br><b>Reality:</b> %{y:.4f}<extra></extra>'
        ),
        row=1, col=1
    )
    
    # Add 7-day moving averages
    if 'hype_7d_avg' in df.columns and df['hype_7d_avg'].notna().any():
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['hype_7d_avg'],
                name='Hype 7d Avg',
                line=dict(color='#ff8888', width=1, dash='dash'),
                mode='lines',
                hovertemplate='<b>Date:</b> %{x|%Y-%m-%d}<br><b>7d Avg:</b> %{y:.4f}<extra></extra>'
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['reality_7d_avg'],
                name='Reality 7d Avg',
                line=dict(color='#8888ff', width=1, dash='dash'),
                mode='lines',
                hovertemplate='<b>Date:</b> %{x|%Y-%m-%d}<br><b>7d Avg:</b> %{y:.4f}<extra></extra>'
            ),
            row=1, col=1
        )
    
    # Plot bubble indicators
    fig.add_trace(
        go.Scatter(
            x=df['date'],
            y=df['hype_reality_gap'],
            name='Hype-Reality Gap',
            line=dict(color='#ff8800', width=2),
            mode='lines+markers',
            marker=dict(size=5),
            hovertemplate='<b>Date:</b> %{x|%Y-%m-%d}<br><b>Gap:</b> %{y:.4f}<extra></extra>',
            fill='tozeroy',
            fillcolor='rgba(255, 136, 0, 0.1)'
        ),
        row=2, col=1
    )
    
    fig.add_trace(
        go.Scatter(
            x=df['date'],
            y=df['bubble_momentum'],
            name='Bubble Momentum',
            line=dict(color='#aa00ff', width=2),
            mode='lines+markers',
            marker=dict(size=5),
            hovertemplate='<b>Date:</b> %{x|%Y-%m-%d}<br><b>Momentum:</b> %{y:.4f}<extra></extra>'
        ),
        row=2, col=1
    )
    
    # Add zero line for momentum
    fig.add_hline(y=0, line_dash="dot", line_color="gray", opacity=0.5, row=2, col=1)
    
    # Update layout with proper datetime formatting
    fig.update_layout(
        height=800,
        showlegend=True,
        title_text='AI Bubble Detection Indices (Daily)',
        hovermode='x unified',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        )
    )
    
    # Format x-axes for dates
    fig.update_xaxes(
        title_text="Date",
        type='date',
        tickformat='%Y-%m-%d',
        tickmode='auto',
        nticks=20,
        row=1, col=1
    )
    
    fig.update_xaxes(
        title_text="Date",
        type='date',
        tickformat='%Y-%m-%d',
        tickmode='auto',
        nticks=20,
        row=2, col=1
    )
    
    # Update y-axes titles
    fig.update_yaxes(title_text="Index Value", row=1, col=1)
    fig.update_yaxes(title_text="Indicator Value", row=2, col=1)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Show data table in expander
    with st.expander("📊 View Raw Data"):
        display_df = df.copy()
        display_df['date'] = display_df['date'].dt.strftime('%Y-%m-%d')
        
        # Round numeric columns for display
        numeric_cols = display_df.select_dtypes(include=['float64', 'float32']).columns
        display_df[numeric_cols] = display_df[numeric_cols].round(4)
        
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )