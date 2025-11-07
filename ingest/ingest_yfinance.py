import os
import logging
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Optional
import sys

# Add the setup directory to path to import DataManager
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'setup'))
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
            
            if last_update:
                start_time = datetime.fromisoformat(last_update)
            else:
                # If no previous data, start from 2020
                start_time = datetime(2020, 1, 1, tzinfo=pytz.UTC)

            # Add small overlap to ensure no data gaps
            start_time -= timedelta(minutes=30)
            
            # Download data
            stock = yf.Ticker(ticker)
            df = stock.history(
                start=start_time,
                end=datetime.now(pytz.UTC),
                interval='1m'  # Get 1-minute data to ensure we don't miss anything
            )

            if df.empty:
                logger.info(f"No new data for {ticker}")
                return None

            # Convert index to UTC
            df.index = df.index.tz_convert(pytz.UTC)
            return df

        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {str(e)}")
            return None

    def update_warehouse(self, ticker: str, df: pd.DataFrame):
        """Update DuckDB warehouse with new stock data"""
        try:
            conn = duckdb.connect(self.warehouse_path)
            
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
            
            conn.commit()
            logger.info(f"Updated warehouse with new data for {ticker}")

        except Exception as e:
            logger.error(f"Error updating warehouse for {ticker}: {str(e)}")
            raise
        finally:
            conn.close()

    def get_entity_id(self, ticker: str) -> int:
        """Get entity_id for a ticker from dim_entity"""
        try:
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
            conn.close()

    def process_ticker(self, ticker: str):
        """Process a single ticker"""
        try:
            # Fetch new data
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
    
    for ticker in ingester.tickers:
        try:
            logger.info(f"Processing {ticker}")
            ingester.process_ticker(ticker)
        except Exception as e:
            logger.error(f"Failed to process {ticker}: {str(e)}")
            continue

if __name__ == "__main__":
    try:
        update_stock_data()
    except Exception as e:
        logger.error(f"Fatal error in stock data update: {str(e)}")
        raise
