import os
import time
import schedule
import logging
from datetime import datetime
import pytz
from setup.data_manager import DataManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataUpdateOrchestrator:
    def __init__(self):
        self.data_manager = DataManager()
        self.setup_logging()
        
    def run_processing_pipeline(self):
        """Run the full processing pipeline"""
        try:
            logger.info("Running processing pipeline...")
            from run_pipeline import Pipeline
            pipeline = Pipeline()
            pipeline.run(incremental=True)
            logger.info("Processing pipeline completed successfully")
        except Exception as e:
            logger.error(f"Error running processing pipeline: {str(e)}")

    def setup_logging(self):
        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
        
        # Add file handler
        fh = logging.FileHandler('logs/update_service.log')
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    def update_stock_data(self):
        """Update stock price data every 10 minutes"""
        if self.data_manager.should_update('stock_prices'):
            try:
                logger.info("Updating stock price data...")
                # Import and run the stock data update script
                from ingest_yfinance import update_stock_data
                update_stock_data()
                
                self.data_manager.update_last_update(
                    'stock_prices',
                    datetime.now(pytz.UTC).isoformat()
                )
            except Exception as e:
                logger.error(f"Error updating stock data: {str(e)}")

    def update_reddit_data(self):
        """Update Reddit data hourly"""
        if self.data_manager.should_update('reddit'):
            try:
                logger.info("Updating Reddit data...")
                from ingest_reddit import update_reddit_data
                update_reddit_data()
                
                self.data_manager.update_last_update(
                    'reddit',
                    datetime.now(pytz.UTC).isoformat()
                )
            except Exception as e:
                logger.error(f"Error updating Reddit data: {str(e)}")

    def update_news_data(self):
        """Update news data hourly"""
        if self.data_manager.should_update('news'):
            try:
                logger.info("Updating news data...")
                from ingest_news import update_news_data
                update_news_data()
                
                self.data_manager.update_last_update(
                    'news',
                    datetime.now(pytz.UTC).isoformat()
                )
            except Exception as e:
                logger.error(f"Error updating news data: {str(e)}")

    def update_github_data(self):
        """Update GitHub data every 6 hours"""
        if self.data_manager.should_update('github'):
            try:
                logger.info("Updating GitHub data...")
                from ingest_github_hf import update_github_data
                update_github_data()
                
                self.data_manager.update_last_update(
                    'github',
                    datetime.now(pytz.UTC).isoformat()
                )
            except Exception as e:
                logger.error(f"Error updating GitHub data: {str(e)}")

    def update_huggingface_data(self):
        """Update HuggingFace data every 6 hours"""
        if self.data_manager.should_update('huggingface'):
            try:
                logger.info("Updating HuggingFace data...")
                from ingest_github_hf import update_huggingface_data
                update_huggingface_data()
                
                self.data_manager.update_last_update(
                    'huggingface',
                    datetime.now(pytz.UTC).isoformat()
                )
            except Exception as e:
                logger.error(f"Error updating HuggingFace data: {str(e)}")

    def update_arxiv_data(self):
        """Update arXiv data daily"""
        if self.data_manager.should_update('arxiv'):
            try:
                logger.info("Updating arXiv data...")
                from ingest_arxiv import update_arxiv_data
                update_arxiv_data()
                
                self.data_manager.update_last_update(
                    'arxiv',
                    datetime.now(pytz.UTC).isoformat()
                )
                
                # Run processing pipeline after arxiv update since it's the last daily update
                self.run_processing_pipeline()
            except Exception as e:
                logger.error(f"Error updating arXiv data: {str(e)}")

    def schedule_jobs(self):
        """Schedule all update jobs"""
        # Stock prices every 10 minutes
        schedule.every(10).minutes.do(self.update_stock_data)
        
        # Reddit and News hourly
        schedule.every(1).hours.do(self.update_reddit_data)
        schedule.every(1).hours.do(self.update_news_data)
        
        # GitHub and HuggingFace every 6 hours
        schedule.every(6).hours.do(self.update_github_data)
        schedule.every(6).hours.do(self.update_huggingface_data)
        
        # ArXiv daily
        schedule.every(1).days.at("00:00").do(self.update_arxiv_data)
        
        # Run processing pipeline every 6 hours (after major data updates)
        schedule.every(6).hours.do(self.run_processing_pipeline)

    def run(self):
        """Run the update service"""
        logger.info("Starting data update service...")
        self.schedule_jobs()
        
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}")
                time.sleep(300)  # Wait 5 minutes on error

if __name__ == "__main__":
    orchestrator = DataUpdateOrchestrator()
    orchestrator.run()