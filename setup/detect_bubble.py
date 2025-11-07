import os
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Tuple, Optional
import duckdb
from sklearn.preprocessing import RobustScaler
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
from scipy.stats import zscore
from dtaidistance import dtw
from ruptures import Binseg
import statsmodels.api as sm
from dataclasses import dataclass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class BubbleAlert:
    level: str  # 'watch', 'warning', 'critical'
    timestamp: datetime
    entity_id: int
    divergence_score: float
    acceleration_score: float
    pattern_score: float
    confidence: float
    contributing_factors: Dict[str, float]




class BubbleDetector:
    def __init__(self):
        self.warehouse_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            'data', 'warehouse', 'ai_bubble.duckdb'
        )
        
        # Model parameters
        self.lookback_window = 90  # days
        self.min_samples = 5  # Reduced from 30 - data is sparse, need at least 5 points for basic analysis
        self.contamination = 0.05
        
        # Alert thresholds
        self.watch_threshold = 1.5
        self.warning_threshold = 2.5
        self.critical_threshold = 3.0
        
        # Bubble score weights
        self.alpha = 0.5  # divergence weight
        self.beta = 0.3   # acceleration weight
        self.gamma = 0.2  # pattern match weight
        
        # Historical bubble templates
        self.templates = self.load_bubble_templates()
        
        # Initialize detection models
        self.isolation_forest = IsolationForest(
            contamination=self.contamination,
            random_state=42
        )
        
        self.one_class_svm = OneClassSVM(
            nu=self.contamination,
            kernel='rbf'
        )

    def load_bubble_templates(self) -> Dict[str, pd.DataFrame]:
        """Load historical bubble templates"""
        templates = {}
        template_path = os.path.join(
            os.path.dirname(__file__), 
            'data', 
            'templates'
        )
        
        # Load each template file
        for bubble in ['dotcom', 'housing', 'crypto']:
            try:
                path = os.path.join(template_path, f'{bubble}_bubble.parquet')
                if os.path.exists(path):
                    templates[bubble] = pd.read_parquet(path)
            except Exception as e:
                logger.warning(f"Could not load template for {bubble}: {str(e)}")
                
        return templates

    def compute_divergence(self, hype: pd.Series, reality: pd.Series) -> pd.Series:
        """Compute normalized divergence between hype and reality"""
        # Check if inputs are pandas Series, if not convert them
        if not isinstance(hype, pd.Series):
            hype = pd.Series(hype)
        if not isinstance(reality, pd.Series):
            reality = pd.Series(reality)

        # Make sure both series have an index
        if hype.index is None:
            hype.index = np.arange(len(hype))
        if reality.index is None:
            reality.index = np.arange(len(reality))

        # Normalize using robust scaling
        scaler = RobustScaler()
        hype_norm = scaler.fit_transform(hype.values.reshape(-1, 1)).flatten()
        reality_norm = scaler.fit_transform(reality.values.reshape(-1, 1)).flatten()
        
        # Compute divergence using the index from hype
        divergence = pd.Series(
            hype_norm - reality_norm,
            index=hype.index
        )
        
        return divergence

    def compute_acceleration(self, divergence: pd.Series) -> pd.Series:
        """Compute acceleration (second derivative) of divergence"""
        # Ensure input is a pandas Series
        if not isinstance(divergence, pd.Series):
            if isinstance(divergence, np.ndarray):
                divergence = pd.Series(divergence)
            else:
                divergence = pd.Series(np.array(divergence))
        
        # First derivative (velocity)
        velocity = divergence.diff()
        
        # Second derivative (acceleration)
        acceleration = velocity.diff()
        
        # Apply EWMA smoothing
        acceleration = acceleration.ewm(span=7).mean()
        
        return acceleration

    def compute_pattern_match(self, 
                            current_pattern: pd.Series,
                            normalize: bool = True) -> float:
        """Compute DTW distance to historical bubble patterns"""
        # Ensure input is a pandas Series
        if not isinstance(current_pattern, pd.Series):
            if isinstance(current_pattern, np.ndarray):
                current_pattern = pd.Series(current_pattern)
            else:
                current_pattern = pd.Series(np.array(current_pattern))
        
        if normalize:
            # Handle zero standard deviation case
            std = current_pattern.std()
            if std == 0:
                current = pd.Series(np.zeros_like(current_pattern))
            else:
                current = (current_pattern - current_pattern.mean()) / std
        else:
            current = current_pattern
            
        distances = []
        for name, template in self.templates.items():
            try:
                # Ensure template is a pandas Series/DataFrame
                if isinstance(template, np.ndarray):
                    template = pd.Series(template)
                
                # Handle zero standard deviation case for template
                template_std = template.std()
                if template_std == 0:
                    template_norm = pd.Series(np.zeros_like(template))
                else:
                    template_norm = (template - template.mean()) / template_std
                
                # Convert to numpy arrays for DTW
                current_array = current.values
                template_array = template_norm.values
                
                # Compute DTW distance
                distance = dtw.distance(current_array, template_array)
                distances.append(distance)
            except Exception as e:
                logger.warning(f"Error computing DTW for {name}: {str(e)}")
                
        if distances:
            # Convert min distance to similarity score (0-1)
            min_dist = min(distances)
            return 1 / (1 + min_dist)
        return 0.0

    def detect_changepoints(self, series: pd.Series) -> List[int]:
        """Detect significant changes in the time series"""
        # Use binary segmentation for change point detection
        model = Binseg(model="l2").fit(series.values)
        
        # Find optimal number of change points
        changes = model.predict(n_bkps=5)
        
        return changes

    def compute_granger_causality(self, 
                                leading_indicators: pd.DataFrame,
                                target: pd.Series,
                                max_lag: int = 5) -> Dict[str, float]:
        """Compute Granger causality between indicators and target"""
        results = {}
        
        for col in leading_indicators.columns:
            try:
                # Prepare data
                data = pd.DataFrame({
                    'y': target,
                    'x': leading_indicators[col]
                }).dropna()
                
                # Test Granger causality
                gc_res = sm.tsa.stattools.grangercausalitytests(
                    data,
                    maxlag=max_lag,
                    verbose=False
                )
                
                # Get minimum p-value across lags
                min_p_value = min(
                    gc_res[lag][0]['ssr_chi2test'][1]
                    for lag in range(1, max_lag + 1)
                )
                
                results[col] = 1 - min_p_value  # Convert to causality score
                
            except Exception as e:
                logger.warning(f"Error in Granger causality for {col}: {str(e)}")
                
        return results

    def generate_alerts(self, 
                    entity_id: int,
                    divergence: pd.Series,
                    acceleration: pd.Series,
                    pattern_score: float) -> List[BubbleAlert]:
        """Generate multi-tier alerts based on detection results - only for latest/most significant point"""
        alerts = []
        
        # Handle NaN values in acceleration
        acceleration_clean = acceleration.dropna()
        if len(acceleration_clean) == 0:
            acceleration_clean = pd.Series([0.0] * len(divergence), index=divergence.index)
        else:
            acceleration_clean = acceleration_clean  # already clean

        # Align acceleration with divergence
        acceleration_aligned = acceleration.reindex(divergence.index, method='ffill').fillna(0.0)
        
        # Compute z-scores for divergence
        div_clean = divergence.dropna()
        if len(div_clean) == 0:
            return alerts

        div_z = zscore(div_clean)
        div_z_series = pd.Series(div_z, index=div_clean.index)
        div_z_mapped = div_z_series.reindex(divergence.index, fill_value=0.0)
        
        # Compute z-scores for acceleration
        acc_clean = acceleration_aligned.dropna()
        if len(acc_clean) > 0:
            acc_z = zscore(acc_clean)
            acc_z_series = pd.Series(acc_z, index=acc_clean.index)
            acc_z_mapped = acc_z_series.reindex(divergence.index, fill_value=0.0)
        else:
            acc_z_mapped = pd.Series([0.0] * len(divergence), index=divergence.index)
        
        # Only generate alert for the latest timestamp
        latest_timestamp = divergence.index[-1]
        div_score = div_z_mapped[latest_timestamp]
        acc_score = acc_z_mapped[latest_timestamp]
        
        # Determine alert level
        if div_score > self.critical_threshold and pattern_score > 0.8:
            level = 'critical'
        elif div_score > self.warning_threshold and pattern_score > 0.6:
            level = 'warning'
        elif div_score > self.watch_threshold or acc_score > 2.0:
            level = 'watch'
        else:
            return alerts
        
        # Create alert
        alert = BubbleAlert(
            level=level,
            timestamp=latest_timestamp,
            entity_id=entity_id,
            divergence_score=float(div_score),
            acceleration_score=float(acc_score),
            pattern_score=float(pattern_score),
            confidence=self.compute_confidence(div_score, acc_score, pattern_score),
            contributing_factors={
                'divergence': float(div_score),
                'acceleration': float(acc_score),
                'pattern_similarity': float(pattern_score)
            }
        )
        alerts.append(alert)
            
        return alerts

    def compute_confidence(self,
                         divergence_score: float,
                         acceleration_score: float,
                         pattern_score: float) -> float:
        """Compute confidence score for bubble detection"""
        # Weighted combination of signals
        confidence = (
            self.alpha * abs(divergence_score) +
            self.beta * abs(acceleration_score) +
            self.gamma * pattern_score
        )
        
        # Normalize to 0-1 range
        confidence = 1 / (1 + np.exp(-confidence + 5))  # Sigmoid transformation
        
        return confidence

    def save_alerts(self, alerts: List[BubbleAlert]):
        """Save alerts to DuckDB warehouse"""
        try:
            conn = duckdb.connect(self.warehouse_path)
            
            # Convert alerts to DataFrame
            alerts_df = pd.DataFrame([
                {
                    'timestamp': alert.timestamp,
                    'entity_id': alert.entity_id,
                    'alert_level': alert.level,
                    'divergence_score': alert.divergence_score,
                    'acceleration_score': alert.acceleration_score,
                    'pattern_score': alert.pattern_score,
                    'confidence': alert.confidence,
                    'contributing_factors': str(alert.contributing_factors)
                }
                for alert in alerts
            ])
            
            # Ensure timestamp column is datetime type and timezone-naive
            alerts_df['timestamp'] = pd.to_datetime(alerts_df['timestamp'])
            if alerts_df['timestamp'].dt.tz is not None:
                # Remove timezone if present
                alerts_df['timestamp'] = alerts_df['timestamp'].dt.tz_localize(None)
            
            # Update alerts table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bubble_alerts (
                    timestamp TIMESTAMP,
                    entity_id INTEGER,
                    alert_level VARCHAR,
                    divergence_score DOUBLE,
                    acceleration_score DOUBLE,
                    pattern_score DOUBLE,
                    confidence DOUBLE,
                    contributing_factors VARCHAR
                )
            """)
            
            # Handle NaN values in DataFrame before insertion
            alerts_df['acceleration_score'] = alerts_df['acceleration_score'].fillna(0.0)
            alerts_df['divergence_score'] = alerts_df['divergence_score'].fillna(0.0)
            alerts_df['pattern_score'] = alerts_df['pattern_score'].fillna(0.0)
            alerts_df['confidence'] = alerts_df['confidence'].fillna(0.0)
            
            # Deduplicate: remove existing alerts for same (timestamp, entity_id, alert_level) before inserting
            if len(alerts_df) > 0:
                conn.register('alerts_df', alerts_df)
                # Delete existing alerts that match the new ones
                conn.execute("""
                    DELETE FROM bubble_alerts
                    WHERE EXISTS (
                        SELECT 1 FROM alerts_df
                        WHERE alerts_df.timestamp = bubble_alerts.timestamp
                        AND alerts_df.entity_id = bubble_alerts.entity_id
                        AND alerts_df.alert_level = bubble_alerts.alert_level
                    )
                """)
                
                # Insert new alerts
                conn.execute("""
                    INSERT INTO bubble_alerts 
                    SELECT 
                        timestamp,
                        entity_id,
                        alert_level,
                        divergence_score,
                        acceleration_score,
                        pattern_score,
                        confidence,
                        contributing_factors
                    FROM alerts_df
                """)
            logger.info(f"Saved {len(alerts)} alerts to warehouse")
            
        except Exception as e:
            logger.error(f"Error saving alerts: {str(e)}")
            raise
        finally:
            conn.close()

    def detect_bubbles(self, lookback_days: int = None):
        """
        Main function to detect bubbles across all entities
        
        Args:
            lookback_days: Number of days to look back. If None, uses all available data.
        """
        try:
            logger.info("Starting bubble detection...")
            conn = duckdb.connect(self.warehouse_path)
            
            # Build date filter - use all data if lookback_days is None
            date_filter = ""
            if lookback_days is not None:
                date_filter = f"AND eb.date >= CURRENT_DATE - INTERVAL '{lookback_days} days'"
            
            # First, identify entities with sufficient data
            where_clause = date_filter.replace("AND", "WHERE") if date_filter else ""
            sufficient_entities_query = f"""
                SELECT entity_id, COUNT(*) as cnt
                FROM entity_bubble_metrics eb
                {where_clause}
                GROUP BY entity_id
                HAVING COUNT(*) >= {self.min_samples}
            """
            sufficient_entities = conn.execute(sufficient_entities_query).fetchdf()
            
            if len(sufficient_entities) == 0:
                logger.warning(f"No entities have sufficient data (>= {self.min_samples} samples)")
                return []
            
            logger.info(f"Found {len(sufficient_entities)} entities with sufficient data (>= {self.min_samples} samples)")
            
            # Get recent data for entities with sufficient data
            entity_list = ', '.join(map(str, sufficient_entities['entity_id'].tolist()))
            query = f"""
                WITH recent_entity_data AS (
                    SELECT
                        eb.time_id,
                        eb.date,
                        eb.entity_id,
                        eb.entity_name,
                        eb.multi_source_hype_intensity as hype_index,
                        eb.reality_strength_score as reality_index,
                        eb.hype_reality_gap,
                        eb.bubble_momentum
                    FROM entity_bubble_metrics eb
                    WHERE eb.entity_id IN ({entity_list})
                    {date_filter}
                ),
                recent_global_data AS (
                    SELECT
                        b.time_id,
                        b.date,
                        NULL::INTEGER as entity_id,
                        NULL::VARCHAR as entity_name,
                        b.hype_index,
                        b.reality_index,
                        b.hype_reality_gap,
                        b.bubble_momentum
                    FROM bubble_metrics b
                    {f"WHERE b.date >= CURRENT_DATE - INTERVAL '{lookback_days} days'" if lookback_days is not None else ""}
                ),
                recent_data AS (
                    SELECT * FROM recent_entity_data
                    UNION ALL
                    SELECT * FROM recent_global_data
                )
                SELECT * FROM recent_data
                ORDER BY date
            """

            df = conn.execute(query).fetchdf()
            
            if df.empty:
                logger.warning("No data found for bubble detection")
                return []
            
            all_alerts = []
            # Filter out NULL entity_ids and process each entity
            # Note: We already filtered for sufficient entities, so all should have enough data
            entity_ids = df['entity_id'].dropna().unique()
            
            for entity_id in entity_ids:
                entity_data = df[df['entity_id'] == entity_id].copy()
                
                # Double-check (shouldn't happen since we pre-filtered, but safety check)
                if len(entity_data) < self.min_samples:
                    logger.debug(f"Entity {entity_id} had insufficient data after filtering (has {len(entity_data)} rows, needs {self.min_samples})")
                    continue
                
                # Set date as index for proper timestamp handling
                entity_data = entity_data.set_index('date')
                entity_data.index = pd.to_datetime(entity_data.index)
                
                # Ensure numeric columns are properly typed and handle NaN values
                numeric_columns = ['hype_index', 'reality_index', 'hype_reality_gap']
                for col in numeric_columns:
                    entity_data[col] = pd.to_numeric(entity_data[col], errors='coerce').fillna(0)
                
                # Convert to pandas Series with proper index
                hype_series = pd.Series(entity_data['hype_index'], index=entity_data.index)
                reality_series = pd.Series(entity_data['reality_index'], index=entity_data.index)
                
                # Compute metrics
                divergence = self.compute_divergence(hype_series, reality_series)
                
                acceleration = self.compute_acceleration(divergence)
                
                pattern_score = self.compute_pattern_match(
                    entity_data['hype_reality_gap']
                )
                
                # Generate alerts
                entity_alerts = self.generate_alerts(
                    entity_id,
                    divergence,
                    acceleration,
                    pattern_score
                )
                
                all_alerts.extend(entity_alerts)
            
            # Save alerts
            if all_alerts:
                self.save_alerts(all_alerts)
                
            return all_alerts
            
        except Exception as e:
            logger.error(f"Error in bubble detection: {str(e)}")
            raise

def main():
    """Run bubble detection"""
    try:
        detector = BubbleDetector()
        # Use None to process all available data (not just last 90 days)
        # This ensures we include entities with sufficient historical data
        alerts = detector.detect_bubbles(lookback_days=None)
        
        # Log summary
        alert_counts = {
            'watch': len([a for a in alerts if a.level == 'watch']),
            'warning': len([a for a in alerts if a.level == 'warning']),
            'critical': len([a for a in alerts if a.level == 'critical'])
        }
        
        logger.info("Bubble Detection Summary:")
        logger.info(f"Total Alerts: {len(alerts)}")
        for level, count in alert_counts.items():
            logger.info(f"{level.title()}: {count}")
            
    except Exception as e:
        logger.error(f"Fatal error in bubble detection: {str(e)}")
        raise

if __name__ == "__main__":
    main()