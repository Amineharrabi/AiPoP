import os
import logging
import json
import time
import re
from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Optional
import pandas as pd
from sec_api import QueryApi, RenderApi
from dotenv import load_dotenv
from setup.data_manager import DataManager
import duckdb
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from tqdm import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# AI-related keywords for content filtering
AI_KEYWORDS = [
    'artificial intelligence', 'machine learning', 'deep learning', 'neural network',
    'AI', 'ML', 'generative AI', 'LLM', 'large language model', 'transformer',
    'GPU', 'semiconductor', 'chip', 'data center', 'cloud computing',
    'automation', 'robotics', 'autonomous', 'cognitive computing'
]

load_dotenv()

class SECDataIngester:
    def __init__(self):
        self.data_manager = DataManager()
        self.warehouse_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'data', 'warehouse', 'ai_bubble.duckdb'
        )
        
        # Initialize SEC API clients
        self.api_key = os.getenv('SEC_API_KEY')
        if not self.api_key:
            raise ValueError("SEC_API_KEY environment variable not set")
            
        self.query_api = QueryApi(api_key=self.api_key)
        self.render_api = RenderApi(api_key=self.api_key)
        
        # Initialize sentiment analyzer
        self.analyzer = SentimentIntensityAnalyzer()
        
        # Filing types to track
        self.forms = ["8-K", "10-Q", "10-K"]
        
        # AI-related companies to track
        self.tickers = [
            'NVDA','MSFT','AAPL','AMZN','GOOGL','META','AVGO','TSM','ORCL','TSLA',
            'AMD','ADBE','CRM','IBM','ASML','INTC','QCOM','MU','AMAT','LRCX',
            'NOW','PLTR','SNOW','PATH','AI','SNPS','CDNS','ARM','SMCI','DELL'
        ]

    def clean_text(self, txt: str) -> str:
        """Clean and normalize text"""
        return " ".join(txt.split()) if txt else ""

    def extract_ai_mentions(self, text: str) -> List[str]:
        """Extract AI-related sentences from text"""
        if not text:
            return []
        sentences = re.split(r'[.!?]+', text)
        return [s.strip() for s in sentences if s and any(kw.lower() in s.lower() for kw in AI_KEYWORDS)]

    def extract_market_metrics(self, text: str) -> Dict:
        """Extract financial metrics from text"""
        metrics = {}
        if not text:
            return metrics
            
        # Revenue patterns (billion, million, plain)
        rev_patterns = [
            (r"revenue.*?\$?\s*([\d,]+\.?\d*)\s*billion", 1e9),
            (r"revenue.*?\$?\s*([\d,]+\.?\d*)\s*million", 1e6),
            (r"revenue.*?\$?\s*([\d,]+\.?\d*)", 1)
        ]
        
        # Try to find revenue mentions
        for pattern, multiplier in rev_patterns:
            match = re.search(pattern, text, re.I)
            if match:
                try:
                    value = float(match.group(1).replace(',','')) * multiplier
                    metrics['revenue'] = value
                    break
                except ValueError:
                    continue
                    
        return metrics

    def fetch_incremental_filings(self, ticker: str) -> Optional[pd.DataFrame]:
        """Fetch new SEC filings since last update"""
        try:
            # Get last update time
            last_update = self.data_manager.get_last_update(f'sec_{ticker}')
            
            if last_update:
                start_time = datetime.fromisoformat(last_update)
            else:
                # If no previous data, start from 2020
                start_time = datetime(2020, 1, 1, tzinfo=pytz.UTC)

            # Convert to SEC API date format
            start_str = start_time.strftime("%Y-%m-%d")
            end_str = datetime.now(pytz.UTC).strftime("%Y-%m-%d")

            all_filings = []
            for form in self.forms:
                query = {
                    "query": {
                        "query_string": {
                            "query": f'ticker:"{ticker}" AND formType:"{form}" AND filedAt:[{start_str} TO {end_str}]'
                        }
                    },
                    "from": 0,
                    "size": 200,
                    "sort": [{"filedAt": {"order": "desc"}}]
                }

                try:
                    resp = self.query_api.get_filings(query)
                    filings = resp.get("filings", [])
                    
                    for filing in tqdm(filings, desc=f"{ticker}-{form}", leave=False):
                        link = filing["linkToFilingDetails"]
                        try:
                            # Get full text
                            txt = self.render_api.get_filing(link)
                            
                            # Extract AI mentions and sentiment
                            ai_mentions = self.extract_ai_mentions(txt)
                            if ai_mentions:  # Only process if AI-related
                                sentiment = self.analyzer.polarity_scores(txt)
                                metrics = self.extract_market_metrics(txt)
                                
                                all_filings.append({
                                    "ticker": ticker,
                                    "form": filing["formType"],
                                    "filed_at": datetime.fromisoformat(filing["filedAt"].replace('Z', '+00:00')),
                                    "accession_no": filing["accessionNo"],
                                    "url": link,
                                    "ai_mentions": ai_mentions,
                                    "sentiment_compound": sentiment['compound'],
                                    "revenue": metrics.get('revenue'),
                                    "filing_text": self.clean_text(txt)
                                })
                            
                            # Be nice to SEC API
                            time.sleep(0.3)
                            
                        except Exception as e:
                            logger.warning(f"Error processing filing {link}: {str(e)}")
                            continue

                except Exception as e:
                    logger.error(f"Error querying SEC API for {ticker}-{form}: {str(e)}")
                    continue

            if not all_filings:
                logger.info(f"No new filings for {ticker}")
                return None

            return pd.DataFrame(all_filings)

        except Exception as e:
            logger.error(f"Error fetching SEC filings for {ticker}: {str(e)}")
            return None

    def update_warehouse(self, df: pd.DataFrame):
        """Update DuckDB warehouse with new SEC filing data"""
        try:
            conn = duckdb.connect(self.warehouse_path)
            
            # Prepare data for fact tables
            hype_signals = []
            reality_signals = []
            
            for _, row in df.iterrows():
                # Add sentiment and mentions to hype signals
                hype_signals.append({
                    'time_id': int(row['filed_at'].timestamp()),
                    'entity_id': self.get_entity_id(row['ticker']),
                    'source_id': self.get_source_id('sec'),
                    'signal_type': 'sentiment',
                    'metric_name': 'sec_filing_sentiment',
                    'metric_value': row['sentiment_compound'],
                    'raw_text': '\n'.join(row['ai_mentions']),
                    'url': row['url']
                })
                
                # Add revenue to reality signals if available
                if pd.notna(row['revenue']):
                    reality_signals.append({
                        'time_id': int(row['filed_at'].timestamp()),
                        'entity_id': self.get_entity_id(row['ticker']),
                        'metric_name': 'revenue',
                        'metric_value': row['revenue'],
                        'metric_unit': 'USD',
                        'provenance': f"SEC-{row['form']}"
                    })

            # Convert to DataFrames
            hype_df = pd.DataFrame(hype_signals)
            reality_df = pd.DataFrame(reality_signals)
            
            # Update fact_hype_signals
            if not hype_df.empty:
                conn.execute("""
                    INSERT INTO fact_hype_signals 
                    SELECT * FROM hype_df
                    WHERE NOT EXISTS (
                        SELECT 1 
                        FROM fact_hype_signals 
                        WHERE 
                            time_id = hype_df.time_id AND 
                            entity_id = hype_df.entity_id AND
                            source_id = hype_df.source_id AND
                            url = hype_df.url
                    )
                """)
            
            # Update fact_reality_signals
            if not reality_df.empty:
                conn.execute("""
                    INSERT INTO fact_reality_signals 
                    SELECT * FROM reality_df
                    WHERE NOT EXISTS (
                        SELECT 1 
                        FROM fact_reality_signals 
                        WHERE 
                            time_id = reality_df.time_id AND 
                            entity_id = reality_df.entity_id AND
                            metric_name = reality_df.metric_name
                    )
                """)
            
            conn.commit()
            logger.info("Updated warehouse with new SEC data")

        except Exception as e:
            logger.error(f"Error updating warehouse with SEC data: {str(e)}")
            raise
        finally:
            conn.close()

    def get_entity_id(self, ticker: str) -> int:
        """Get entity_id from dim_entity based on ticker"""
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

    def get_source_id(self, source_name: str) -> int:
        """Get source_id from dim_source"""
        try:
            conn = duckdb.connect(self.warehouse_path)
            result = conn.execute(
                "SELECT source_id FROM dim_source WHERE source_name = ?",
                [source_name]
            ).fetchone()
            
            if result:
                return result[0]
            else:
                raise ValueError(f"Source not found: {source_name}")
        
        finally:
            conn.close()

    def process_ticker(self, ticker: str):
        """Process SEC filings for a single ticker"""
        try:
            # Fetch new filings
            df = self.fetch_incremental_filings(ticker)
            if df is None or df.empty:
                return

            # Update historical data
            self.data_manager.update_text_data(df, f'sec_{ticker}')
            
            # Update warehouse
            self.update_warehouse(df)
            
            # Update last update time
            self.data_manager.update_last_update(
                f'sec_{ticker}',
                datetime.now(pytz.UTC).isoformat()
            )

        except Exception as e:
            logger.error(f"Error processing SEC data for {ticker}: {str(e)}")
            raise

def update_sec_data():
    """Main function to update SEC filing data"""
    try:
        ingester = SECDataIngester()
        
        for ticker in ingester.tickers:
            try:
                logger.info(f"Processing SEC filings for {ticker}")
                ingester.process_ticker(ticker)
            except Exception as e:
                logger.error(f"Failed to process SEC data for {ticker}: {str(e)}")
                continue
                
    except Exception as e:
        logger.error(f"Fatal error in SEC data update: {str(e)}")
        raise

if __name__ == "__main__":
    update_sec_data()
