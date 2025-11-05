import os
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pytz

class DataManager:
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.historical_dir = os.path.join(self.base_dir, 'data', 'historical')
        self.latest_state_file = os.path.join(self.historical_dir, 'latest_state.json')
        self.initialize_directories()

    def initialize_directories(self):
        """Create necessary directories if they don't exist"""
        os.makedirs(self.historical_dir, exist_ok=True)
        
        # Initialize state file if it doesn't exist
        if not os.path.exists(self.latest_state_file):
            self.save_state({
                'last_update': {
                    'stock_prices': {},
                    'reddit': {},
                    'news': {},
                    'github': {},
                    'huggingface': {},
                    'arxiv': {}
                }
                ,
                # store last known metrics for deltas
                'github_metrics': {},
                'huggingface_metrics': {}
            })

    def load_state(self) -> Dict:
        """Load the latest state of data collection"""
        if os.path.exists(self.latest_state_file):
            with open(self.latest_state_file, 'r') as f:
                return json.load(f)
        return {}

    def save_state(self, state: Dict):
        """Save the current state of data collection"""
        with open(self.latest_state_file, 'w') as f:
            json.dump(state, f, indent=4)

    def get_last_update(self, data_type: str) -> Optional[str]:
        """Get the timestamp of last update for a specific data type"""
        state = self.load_state()
        return state.get('last_update', {}).get(data_type)

    def update_last_update(self, data_type: str, timestamp: str):
        """Update the timestamp for a specific data type"""
        state = self.load_state()
        if 'last_update' not in state:
            state['last_update'] = {}
        state['last_update'][data_type] = timestamp
        self.save_state(state)

    def get_historical_file_path(self, data_type: str, timeframe: str) -> str:
        """Get the path for historical data file"""
        return os.path.join(self.historical_dir, f'{data_type}_{timeframe}.parquet')

    def load_historical_data(self, data_type: str, timeframe: str) -> pd.DataFrame:
        """Load historical data from parquet file"""
        file_path = self.get_historical_file_path(data_type, timeframe)
        if os.path.exists(file_path):
            return pd.read_parquet(file_path)
        return pd.DataFrame()

    def save_historical_data(self, data: pd.DataFrame, data_type: str, timeframe: str):
        """Save historical data to parquet file"""
        file_path = self.get_historical_file_path(data_type, timeframe)
        data.to_parquet(file_path, index=True)

    def update_price_data(self, new_data: pd.DataFrame, ticker: str):
        """Update price data with different timeframes"""
        timeframes = {
            '10min': '10T',
            '1hour': 'H',
            '1day': 'D'
        }

        for timeframe, freq in timeframes.items():
            # Load existing data
            hist_data = self.load_historical_data(f'price_{ticker}', timeframe)
            
            # Resample new data to desired frequency
            resampled_data = new_data.resample(freq).agg({
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last',
                'Volume': 'sum'
            })

            # Combine and deduplicate
            if not hist_data.empty:
                combined_data = pd.concat([hist_data, resampled_data])
                combined_data = combined_data[~combined_data.index.duplicated(keep='last')]
            else:
                combined_data = resampled_data

            # Save updated data
            self.save_historical_data(combined_data, f'price_{ticker}', timeframe)

    def update_text_data(self, new_data: pd.DataFrame, data_type: str):
        """Update text-based data (Reddit, News, etc.)"""
        # Load existing data
        hist_data = self.load_historical_data(data_type, 'full')
        
        # Combine and deduplicate based on URL or ID
        if not hist_data.empty:
            id_col = 'url' if data_type == 'news' else 'id'
            combined_data = pd.concat([hist_data, new_data])
            combined_data = combined_data.drop_duplicates(subset=[id_col], keep='last')
        else:
            combined_data = new_data

        # Save updated data
        self.save_historical_data(combined_data, data_type, 'full')

    def get_update_interval(self, data_type: str) -> timedelta:
        """Get the update interval for different data types"""
        intervals = {
            'stock_prices': timedelta(minutes=10),
            'reddit': timedelta(hours=1),
            'news': timedelta(hours=1),
            'github': timedelta(hours=6),
            'huggingface': timedelta(hours=6),
            'arxiv': timedelta(days=1)
        }
        return intervals.get(data_type, timedelta(hours=1))

    def should_update(self, data_type: str) -> bool:
        """Check if it's time to update a specific data type"""
        last_update = self.get_last_update(data_type)
        if not last_update:
            return True
            
        last_update_dt = datetime.fromisoformat(last_update)
        current_time = datetime.now(pytz.UTC)
        interval = self.get_update_interval(data_type)
        
        return current_time - last_update_dt >= interval

    def compute_github_deltas(self, current_metrics: Dict) -> Dict:
        """Compute deltas for GitHub repository metrics and persist the current snapshot.

        Returns a dict containing at least: stars_delta, forks_delta, activity_change, momentum_score
        """
        name = current_metrics.get('name')
        if not name:
            return {'stars_delta': 0, 'forks_delta': 0, 'activity_change': 0, 'momentum_score': 0}

        state = self.load_state()
        github_metrics = state.get('github_metrics', {})
        prev = github_metrics.get(name, {})

        prev_stars = prev.get('stars', 0)
        prev_forks = prev.get('forks', 0)
        prev_activity = prev.get('activity_score', 0)

        stars_delta = int(current_metrics.get('stars', 0) - prev_stars)
        forks_delta = int(current_metrics.get('forks', 0) - prev_forks)
        activity_change = float(current_metrics.get('activity_score', 0) - prev_activity)

        # Simple momentum score: activity change scaled by sign of stars change
        momentum_score = activity_change * (1 if stars_delta >= 0 else -1)

        # Persist current snapshot for future delta calculations
        github_metrics[name] = {
            'stars': int(current_metrics.get('stars', 0)),
            'forks': int(current_metrics.get('forks', 0)),
            'activity_score': float(current_metrics.get('activity_score', 0)),
            'collected_at': datetime.now().isoformat()
        }
        state['github_metrics'] = github_metrics
        self.save_state(state)

        return {
            'stars_delta': stars_delta,
            'forks_delta': forks_delta,
            'activity_change': activity_change,
            'momentum_score': momentum_score
        }

    def compute_hf_deltas(self, current_metrics: Dict) -> Dict:
        """Compute deltas for HuggingFace model metrics and persist the current snapshot.

        Returns a dict containing at least: downloads_delta, likes_delta, trending_score
        """
        model_id = current_metrics.get('model_id')
        if not model_id:
            return {'downloads_delta': 0, 'likes_delta': 0, 'trending_score': 0}

        state = self.load_state()
        hf_metrics = state.get('huggingface_metrics', {})
        prev = hf_metrics.get(model_id, {})

        prev_downloads = prev.get('downloads', 0)
        prev_likes = prev.get('likes', 0)

        downloads_delta = int(current_metrics.get('downloads', 0) - prev_downloads)
        likes_delta = int(current_metrics.get('likes', 0) - prev_likes)

        # Simple trending score: normalized sum of deltas
        trending_score = 0.0
        try:
            trending_score = (downloads_delta / max(prev_downloads, 1)) * 0.6 + (likes_delta / max(prev_likes, 1)) * 0.4
        except Exception:
            trending_score = float(downloads_delta + likes_delta)

        # Persist current snapshot
        hf_metrics[model_id] = {
            'downloads': int(current_metrics.get('downloads', 0)),
            'likes': int(current_metrics.get('likes', 0)),
            'collected_at': datetime.now().isoformat()
        }
        state['huggingface_metrics'] = hf_metrics
        self.save_state(state)

        return {
            'downloads_delta': downloads_delta,
            'likes_delta': likes_delta,
            'trending_score': trending_score
        }