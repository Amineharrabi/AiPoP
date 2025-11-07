import os
import logging
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Optional
import sys

# Add the project root to path to import DataManager
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from setup.data_manager import DataManager
import duckdb

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockDataIngester:
    def __init__(self):
        self.data_manager = DataManager()
        self.warehouse_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'data', 'warehouse', 'ai_bubble.duckdb'
        )
        
        # Setup staging directory
        self.base_dir = os.path.dirname(os.path.dirname(__file__))
        self.staging_dir = os.path.join(self.base_dir, 'data', 'staging')
        os.makedirs(self.staging_dir, exist_ok=True)
        
        # AI-related stock tickers
        self.tickers = [
            'NVDA','MSFT','AAPL','AMZN','GOOGL','META','AVGO','TSM','ORCL','TSLA',
            'AMD','ADBE','CRM','IBM','ASML','INTC','QCOM','MU','AMAT','LRCX',
            'NOW','PLTR','SNOW','PATH','AI','SNPS','CDNS','ARM','SMCI','DELL'
        ]

    def fetch_incremental_data(self, ticker: str) -> Optional[pd.DataFrame]:
        """Fetch only new data since last update"""
        try:
            # Get last update time for this ticker
            last_update = self.data_manager.get_last_update(f'stock_prices_{ticker}')
            
            now = datetime.now(pytz.UTC)
            
            # Yahoo Finance limits: 1m data = 8 days max, 1h data = 730 days max, 1d data = unlimited
            # For incremental updates, prefer 1h interval for good granularity with longer history
            # If we need more than 730 days, use 1d interval instead
            
            if last_update:
                start_time = datetime.fromisoformat(last_update)
                # Add small overlap to ensure no data gaps
                start_time -= timedelta(hours=1)
                days_since_update = (now - start_time).days
            else:
                # If no previous data, start from 30 days ago for initial data
                start_time = now - timedelta(days=30)
                days_since_update = 30

            # For initial load, always use daily data to ensure we get all trading days
            if not last_update:
                interval = '1d'
            # Use 1-hour data if within 730 days, otherwise use daily
            elif days_since_update <= 730:
                interval = '1h'
                # Ensure we don't exceed 730 days
                max_start = now - timedelta(days=730)
                if start_time < max_start:
                    start_time = max_start
            else:
                # For older data, use daily interval (no limit)
                interval = '1d'
                logger.info(f"Using daily interval for {ticker} (last update was {days_since_update} days ago)")
            
            # Download data
            stock = yf.Ticker(ticker)
            df = stock.history(
                start=start_time,
                end=now,
                interval=interval
            )

            # If hourly data is empty (market closed), try daily interval for last few days
            used_daily_fallback = False
            if df.empty and interval == '1h':
                logger.info(f"No hourly data for {ticker}, trying daily interval for last 5 days")
                # Try daily data for last 5 days to get latest available
                daily_start = now - timedelta(days=5)
                df = stock.history(
                    start=daily_start,
                    end=now,
                    interval='1d'
                )
                if not df.empty:
                    used_daily_fallback = True
                    logger.info(f"Got daily data for {ticker} (latest: {df.index.max()})")

            if df.empty:
                logger.info(f"No new data for {ticker}")
                return None

            # Convert index to UTC
            df.index = df.index.tz_convert(pytz.UTC)
            
            # Filter to only data after last_update
            # If we used daily fallback (market closed), we still want to get the latest day
            # so we'll be more lenient with the filtering
            if last_update:
                last_update_dt = datetime.fromisoformat(last_update)
                
                if used_daily_fallback:
                    # For daily fallback (market closed), we want to include the latest available trading day
                    # Strategy: 
                    # 1. Get the latest date in the fetched data (most recent trading day available)
                    # 2. Include all data from that latest date (to ensure we get the full day's data)
                    # 3. Also include any data from last_update date or newer (to catch any new days)
                    last_update_date = last_update_dt.date() if hasattr(last_update_dt, 'date') else last_update_dt.date()
                    
                    # Get the latest date in the fetched data
                    latest_fetched_date = df.index.max().date() if hasattr(df.index.max(), 'date') else pd.Timestamp(df.index.max()).date()
                    
                    # Include data if:
                    # 1. It's from the latest available date (always include latest trading day)
                    # 2. OR it's from last_update date or newer (to catch any new days since last update)
                    mask = []
                    for idx in df.index:
                        idx_date = pd.Timestamp(idx).date()
                        # Always include latest available date, or any date >= last_update
                        mask.append(idx_date == latest_fetched_date or idx_date >= last_update_date)
                    
                    df = df[mask]
                else:
                    # For hourly data, only get data after last_update
                    df = df[df.index > last_update_dt]
            
            if df.empty:
                logger.info(f"No new data for {ticker} after filtering")
                return None
                
            logger.info(f"Fetched {len(df)} new records for {ticker} (from {df.index.min()} to {df.index.max()})")
            return df

        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {str(e)}")
            return None

    def update_warehouse(self, ticker: str, df: pd.DataFrame):
        """Update DuckDB warehouse with new stock data"""
        conn = None
        try:
            # Use consistent connection settings - no read_only parameter to avoid config mismatch
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    conn = duckdb.connect(self.warehouse_path)
                    break
                except Exception as e:
                    if attempt < max_retries - 1:
                        import time
                        time.sleep(0.5)  # Wait before retry
                        logger.warning(f"Retry {attempt + 1} connecting to warehouse for {ticker}")
                    else:
                        raise
            
            # Prepare data for fact_reality_signals
            facts = []
            for timestamp, row in df.iterrows():
                facts.extend([
                    {
                        'time_id': int(timestamp.timestamp()),
                        'entity_id': self.get_entity_id(ticker),
                        'metric_name': 'stock_price',
                        'metric_value': row['Close'],
                        'metric_unit': 'USD',
                        'provenance': 'yfinance'
                    },
                    {
                        'time_id': int(timestamp.timestamp()),
                        'entity_id': self.get_entity_id(ticker),
                        'metric_name': 'trading_volume',
                        'metric_value': row['Volume'],
                        'metric_unit': 'shares',
                        'provenance': 'yfinance'
                    }
                ])

            # Convert to DataFrame
            facts_df = pd.DataFrame(facts)
            
            # Insert facts using pandas to duckdb
            conn.register('facts_df', facts_df)
            conn.execute("""
                INSERT INTO fact_reality_signals 
                SELECT 
                    time_id,
                    entity_id,
                    metric_name,
                    metric_value,
                    metric_unit,
                    provenance
                FROM facts_df
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM fact_reality_signals 
                    WHERE 
                        time_id = facts_df.time_id AND 
                        entity_id = facts_df.entity_id AND
                        metric_name = facts_df.metric_name
                )
            """)
            
            logger.info(f"Updated warehouse with new data for {ticker}")

        except Exception as e:
            logger.error(f"Error updating warehouse for {ticker}: {str(e)}")
            raise
        finally:
            if conn is not None:
                try:
                    conn.close()
                except:
                    pass

    def get_entity_id(self, ticker: str) -> int:
        """Get entity_id for a ticker from dim_entity"""
        conn = None
        try:
            # Use same connection config as update_warehouse to avoid config mismatch
            conn = duckdb.connect(self.warehouse_path)
            result = conn.execute(
                "SELECT entity_id FROM dim_entity WHERE ticker = ?",
                [ticker]
            ).fetchone()
            
            if result:
                return result[0]
            else:
                raise ValueError(f"Entity not found for ticker {ticker}")
        
        finally:
            if conn is not None:
                try:
                    conn.close()
                except:
                    pass

    def process_ticker(self, ticker: str, df: pd.DataFrame = None):
        """Process a single ticker
        
        Args:
            ticker: Stock ticker symbol
            df: Optional DataFrame with data. If None, will fetch new data.
        """
        try:
            # Fetch new data if not provided
            if df is None:
                df = self.fetch_incremental_data(ticker)
                if df is None or df.empty:
                    return

            # Update historical data
            self.data_manager.update_price_data(df, ticker)
            
            # Update warehouse
            self.update_warehouse(ticker, df)
            
            # Update last update time
            self.data_manager.update_last_update(
                f'stock_prices_{ticker}',
                datetime.now(pytz.UTC).isoformat()
            )

        except Exception as e:
            logger.error(f"Error processing ticker {ticker}: {str(e)}")
            raise

def update_stock_data():
    """Main function to update stock data"""
    ingester = StockDataIngester()
    
    # Collect all data for staging file
    all_ticker_data = []
    
    for ticker in ingester.tickers:
        try:
            logger.info(f"Processing {ticker}")
            
            # Fetch new data once
            df = ingester.fetch_incremental_data(ticker)
            if df is not None and not df.empty:
                # Process ticker (updates warehouse and historical)
                # Pass the df we already fetched to avoid duplicate fetch
                ingester.process_ticker(ticker, df)
                
                # Also collect data for staging parquet file
                # Resample to daily and add ticker column
                daily_df = df.resample('D').agg({
                    'Open': 'first',
                    'High': 'max',
                    'Low': 'min',
                    'Close': 'last',
                    'Volume': 'sum'
                })
                daily_df['ticker'] = ticker
                all_ticker_data.append(daily_df)
            else:
                # No new data - don't call process_ticker to avoid duplicate fetch
                logger.info(f"No new data for {ticker}, skipping")
                
        except Exception as e:
            logger.error(f"Failed to process {ticker}: {str(e)}")
            continue
    
    # Save combined data to staging parquet file
    # First, try to load existing staging file and merge with new data
    staging_file = os.path.join(ingester.staging_dir, 'yfinance_clean.parquet')
    existing_df = None
    if os.path.exists(staging_file):
        try:
            existing_df = pd.read_parquet(staging_file)
            logger.info(f"Loaded existing staging file with {len(existing_df)} rows")
        except Exception as e:
            logger.warning(f"Could not load existing staging file: {e}")
    
    if all_ticker_data:
        try:
            # Combine all ticker data
            combined_df = pd.concat(all_ticker_data, ignore_index=False)
            combined_df = combined_df.reset_index()
            combined_df = combined_df.rename(columns={combined_df.columns[0]: 'Date'})
            
            # Pivot to wide format (Date as index, tickers as columns) for compatibility with load_warehouse
            if 'Date' in combined_df.columns and 'ticker' in combined_df.columns:
                # Create wide format with Date as index
                combined_df['Date'] = pd.to_datetime(combined_df['Date'])
                new_wide = combined_df.pivot_table(
                    index='Date',
                    columns='ticker',
                    values=['Close', 'High', 'Low', 'Open', 'Volume'],
                    aggfunc='last'  # Take last value if duplicates
                )
                # Flatten column names to match expected format
                new_wide.columns = [f"{col[0]}_{col[1]}" if col[1] else col[0] for col in new_wide.columns]
                new_wide = new_wide.reset_index()
                new_wide = new_wide.rename(columns={'Date': ('Date', '')})
                
                # Merge with existing data if available
                if existing_df is not None:
                    # Ensure both dataframes have the same date column name
                    existing_date_col = ('Date', '') if ('Date', '') in existing_df.columns else 'Date'
                    new_date_col = ('Date', '') if ('Date', '') in new_wide.columns else 'Date'
                    
                    # If column names don't match, rename to match existing
                    if existing_date_col != new_date_col:
                        if new_date_col == 'Date':
                            new_wide = new_wide.rename(columns={'Date': existing_date_col})
                        elif existing_date_col == 'Date':
                            existing_df = existing_df.rename(columns={existing_date_col: new_date_col})
                            existing_date_col = new_date_col
                    
                    # Merge on Date column
                    staging_df = pd.concat([existing_df, new_wide]).drop_duplicates(subset=[existing_date_col], keep='last').sort_values(existing_date_col)
                else:
                    staging_df = new_wide
                
                staging_df.to_parquet(staging_file, index=False)
                logger.info(f"Saved {len(staging_df)} rows to staging file: {staging_file}")
                date_col = ('Date', '') if ('Date', '') in staging_df.columns else 'Date'
                if date_col in staging_df.columns:
                    logger.info(f"Date range: {staging_df[date_col].min()} to {staging_df[date_col].max()}")
            else:
                logger.warning("Could not create wide format staging file - missing Date or ticker columns")
            
        except Exception as e:
            logger.error(f"Error saving staging file: {str(e)}")
            import traceback
            traceback.print_exc()
    elif existing_df is not None:
        logger.info(f"No new data fetched, keeping existing staging file with {len(existing_df)} rows")
    else:
        logger.warning("No data collected and no existing staging file found")

if __name__ == "__main__":

    try:
        update_stock_data()
    except Exception as e:
        logger.error(f"Fatal error in stock data update: {str(e)}")
        raise
