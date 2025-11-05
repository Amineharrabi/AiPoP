#!/usr/bin/env python3
"""
Comprehensive Test Script for AiPoP Reorganized Pipeline
Tests all scripts in the new ingest/setup structure with multi-source integration
"""

import os
import sys
import subprocess
import pandas as pd
import duckdb
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add paths for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'setup'))

class PipelineTester:
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.data_dir = os.path.join(self.base_dir, 'data')
        self.warehouse_path = os.path.join(self.data_dir, 'warehouse', 'ai_bubble.duckdb')
        self.staging_dir = os.path.join(self.data_dir, 'staging')
        
    def setup_test_environment(self):
        """Setup test directories and environment"""
        logger.info("Setting up test environment...")
        
        # Create directories
        for dir_path in [self.data_dir, self.staging_dir, 
                        os.path.join(self.data_dir, 'raw'),
                        os.path.join(self.data_dir, 'historical')]:
            os.makedirs(dir_path, exist_ok=True)
        
        logger.info("Test environment setup complete")
        
    def test_setup_scripts(self):
        """Test all setup scripts"""
        logger.info("Testing setup scripts...")
        
        # Test DuckDB setup
        try:
            logger.info("Testing setup_duckdb.py...")
            result = subprocess.run([
                sys.executable, 
                os.path.join(self.base_dir, 'setup', 'setup_duckdb.py')
            ], capture_output=True, text=True, cwd=self.base_dir)
            
            if result.returncode == 0:
                logger.info("✓ DuckDB setup successful")
            else:
                logger.error(f"✗ DuckDB setup failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"✗ DuckDB setup error: {str(e)}")
            return False
            
        # Test DataManager
        try:
            logger.info("Testing DataManager...")
            from setup.data_manager import DataManager
            
            dm = DataManager()
            
            # Test basic functionality
            test_state = {
                'test_repo': {'stars': 1000, 'forks': 100},
                'test_model': {'downloads': 5000, 'likes': 100}
            }
            
            # Test GitHub delta calculation
            deltas = dm.compute_github_deltas({
                'name': 'test_repo',
                'stars': 1100,  # +100 stars
                'forks': 120,   # +20 forks
                'activity_score': 0.5
            })
            
            assert deltas['stars_delta'] == 100
            assert deltas['forks_delta'] == 20
            logger.info("✓ DataManager GitHub delta calculation working")
            
            # Test HF delta calculation
            hf_deltas = dm.compute_hf_deltas({
                'model_id': 'test_model',
                'downloads': 6000,  # +1000 downloads
                'likes': 150        # +50 likes
            })
            
            assert hf_deltas['downloads_delta'] == 1000
            assert hf_deltas['likes_delta'] == 50
            logger.info("✓ DataManager HF delta calculation working")
            
        except Exception as e:
            logger.error(f"✗ DataManager test failed: {str(e)}")
            return False
            
        return True
        
    def test_sample_data_loading(self):
        """Test loading sample data to verify warehouse structure"""
        logger.info("Testing sample data loading...")
        
        try:
            conn = duckdb.connect(self.warehouse_path)
            
            # Test if tables exist
            tables = conn.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'main'
            """).fetchall()
            
            expected_tables = [
                'dim_time', 'dim_entity', 'dim_source', 
                'fact_hype_signals', 'fact_reality_signals'
            ]
            
            existing_tables = [t[0] for t in tables]
            
            for table in expected_tables:
                if table not in existing_tables:
                    logger.error(f"✗ Missing table: {table}")
                    return False
                    
            logger.info(f"✓ All expected tables exist: {existing_tables}")
            
            # Test table schemas
            schema_tests = [
                ("dim_time", "time_id"),
                ("dim_entity", "entity_id"), 
                ("fact_hype_signals", "metric_name"),
                ("fact_reality_signals", "metric_value")
            ]
            
            for table, column in schema_tests:
                result = conn.execute(f"""
                    SELECT COUNT(*) FROM information_schema.columns 
                    WHERE table_name = ? AND column_name = ?
                """, [table, column]).fetchone()
                
                if result[0] == 0:
                    logger.error(f"✗ Missing column {column} in {table}")
                    return False
                    
            logger.info("✓ Table schemas validated")
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"✗ Sample data loading test failed: {str(e)}")
            return False
            
    def test_enhanced_features_computation(self):
        """Test feature computation with sample data"""
        logger.info("Testing enhanced features computation...")
        
        try:
            # Create sample data for testing
            conn = duckdb.connect(self.warehouse_path)
            
            # Insert sample time data
            conn.execute("""
                INSERT INTO dim_time (time_id, date, year, month, week, day_of_week, is_business_day)
                VALUES 
                    (1, '2024-01-01', 2024, 1, 1, 1, true),
                    (2, '2024-01-02', 2024, 1, 1, 2, true)
            """)
            
            # Insert sample entity data
            conn.execute("""
                INSERT INTO dim_entity (entity_id, name, ticker, entity_type, industry)
                VALUES 
                    (1, 'NVIDIA', 'NVDA', 'company', 'technology'),
                    (2, 'transformers', 'transformers', 'software', 'ai_infrastructure')
            """)
            
            # Insert sample source data
            conn.execute("""
                INSERT INTO dim_source (source_id, source_name, source_type)
                VALUES 
                    (1, 'reddit', 'social_media'),
                    (2, 'github', 'technical'),
                    (3, 'huggingface', 'technical')
            """)
            
            # Insert sample hype signals
            conn.execute("""
                INSERT INTO fact_hype_signals (time_id, entity_id, source_id, signal_type, metric_name, metric_value, raw_text, url)
                VALUES 
                    (1, 1, 1, 'sentiment', 'reddit_ai_sentiment', 0.5, 'Great AI news about NVDA', 'https://reddit.com/r/NVDA'),
                    (1, 2, 2, 'trending', 'github_trending_score', 0.8, NULL, 'https://github.com/huggingface/transformers'),
                    (2, 1, 3, 'trending', 'huggingface_trending_score', 0.6, NULL, 'https://huggingface.co/models')
            """)
            
            # Insert sample reality signals
            conn.execute("""
                INSERT INTO fact_reality_signals (time_id, entity_id, metric_name, metric_value, metric_unit, provenance)
                VALUES 
                    (1, 1, 'stock_price', 495.0, 'USD', 'yfinance'),
                    (1, 1, 'github_stars_delta', 50, 'count', 'github'),
                    (2, 1, 'model_downloads_delta', 1000, 'count', 'huggingface')
            """)
            
            conn.close()
            logger.info("✓ Sample data inserted")
            
            # Test features computation
            from setup.compute_features import compute_hype_features, compute_reality_features, compute_combined_features
            
            conn = duckdb.connect(self.warehouse_path)
            compute_hype_features(conn)
            logger.info("✓ Hype features computed")
            
            compute_reality_features(conn)
            logger.info("✓ Reality features computed")
            
            compute_combined_features(conn)
            logger.info("✓ Combined features computed")
            
            # Verify features were created
            tables = conn.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_name LIKE '%features'
            """).fetchall()
            
            if len(tables) >= 3:
                logger.info(f"✓ All feature tables created: {[t[0] for t in tables]}")
            else:
                logger.error(f"✗ Missing feature tables. Found: {[t[0] for t in tables]}")
                return False
                
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"✗ Features computation test failed: {str(e)}")
            return False
            
    def test_enhanced_indices_computation(self):
        """Test indices computation with sample features"""
        logger.info("Testing enhanced indices computation...")
        
        try:
            from setup.compute_indices import compute_hype_index, compute_reality_index, compute_bubble_metrics
            
            conn = duckdb.connect(self.warehouse_path)
            
            # Test indices computation
            compute_hype_index(conn)
            logger.info("✓ Hype index computed")
            
            compute_reality_index(conn)
            logger.info("✓ Reality index computed")
            
            compute_bubble_metrics(conn)
            logger.info("✓ Bubble metrics computed")
            
            # Verify indices were created
            tables = conn.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_name LIKE '%index%' OR table_name LIKE '%bubble_metrics'
            """).fetchall()
            
            if len(tables) >= 3:
                logger.info(f"✓ All index tables created: {[t[0] for t in tables]}")
            else:
                logger.error(f"✗ Missing index tables. Found: {[t[0] for t in tables]}")
                return False
                
            # Test sample query
            sample = conn.execute("""
                SELECT 
                    hype_index,
                    reality_index,
                    hype_reality_gap,
                    bubble_risk_score
                FROM bubble_metrics 
                LIMIT 1
            """).fetchone()
            
            if sample:
                logger.info(f"✓ Sample indices query successful: Hype={sample[0]:.3f}, Reality={sample[1]:.3f}")
            else:
                logger.warning("No bubble metrics data found (expected for test data)")
                
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"✗ Indices computation test failed: {str(e)}")
            return False
            
    def test_path_references(self):
        """Test that all scripts have correct path references"""
        logger.info("Testing path references...")
        
        # Test that all required directories exist
        required_dirs = [
            'data',
            'data/raw',
            'data/staging', 
            'data/warehouse',
            'data/historical',
            'ingest',
            'setup'
        ]
        
        for dir_path in required_dirs:
            full_path = os.path.join(self.base_dir, dir_path)
            if os.path.exists(full_path):
                logger.info(f"✓ Directory exists: {dir_path}")
            else:
                logger.error(f"✗ Missing directory: {dir_path}")
                return False
                
        # Test that key scripts exist
        key_scripts = [
            'ingest/ingest_yfinance.py',
            'ingest/ingest_reddit.py', 
            'ingest/ingest_github_hf.py',
            'ingest/ingest_news.py',
            'ingest/ingest_arxiv.py',
            'setup/setup_duckdb.py',
            'setup/data_manager.py',
            'setup/load_warehouse.py',
            'setup/compute_features.py',
            'setup/compute_indices.py'
        ]
        
        for script in key_scripts:
            script_path = os.path.join(self.base_dir, script)
            if os.path.exists(script_path):
                logger.info(f"✓ Script exists: {script}")
            else:
                logger.error(f"✗ Missing script: {script}")
                return False
                
        return True
        
    def run_all_tests(self):
        """Run all tests"""
        logger.info("=" * 60)
        logger.info("STARTING AIPOOP PIPELINE TESTS")
        logger.info("=" * 60)
        
        tests = [
            ("Path References", self.test_path_references),
            ("Setup Scripts", self.test_setup_scripts),
            ("Sample Data Loading", self.test_sample_data_loading),
            ("Enhanced Features", self.test_enhanced_features_computation),
            ("Enhanced Indices", self.test_enhanced_indices_computation)
        ]
        
        passed = 0
        total = len(tests)
        
        for test_name, test_func in tests:
            logger.info(f"\n{'=' * 40}")
            logger.info(f"TESTING: {test_name}")
            logger.info(f"{'=' * 40}")
            
            try:
                if test_func():
                    passed += 1
                    logger.info(f"✓ {test_name} PASSED")
                else:
                    logger.error(f"✗ {test_name} FAILED")
            except Exception as e:
                logger.error(f"✗ {test_name} FAILED with exception: {str(e)}")
                
        logger.info(f"\n{'=' * 60}")
        logger.info(f"TEST SUMMARY: {passed}/{total} tests passed")
        logger.info(f"{'=' * 60}")
        
        if passed == total:
            logger.info("🎉 ALL TESTS PASSED! Pipeline reorganization successful!")
            return True
        else:
            logger.error(f"❌ {total - passed} tests failed. Please review the errors above.")
            return False

if __name__ == "__main__":
    tester = PipelineTester()
    tester.setup_test_environment()
    success = tester.run_all_tests()
    
    if success:
        logger.info("\n🚀 PIPELINE READY FOR USE!")
        logger.info("\nNext steps:")
        logger.info("1. Run ingest scripts to collect data from APIs")
        logger.info("2. Run setup/load_warehouse.py to load data into warehouse")
        logger.info("3. Run setup/compute_features.py to compute features")
        logger.info("4. Run setup/compute_indices.py to compute indices")
        logger.info("5. Run setup/detect_bubble.py for bubble detection")
    else:
        logger.error("\n❌ Pipeline tests failed. Please fix the issues before proceeding.")
        sys.exit(1)