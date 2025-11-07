import os
import sys
import logging
import importlib.util
from datetime import datetime
from typing import List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Pipeline:
    def __init__(self):
        self.setup_dir = os.path.dirname(__file__)
        self.project_dir = os.path.dirname(self.setup_dir)
        self.ingest_dir = os.path.join(self.project_dir, 'ingest')
        
        # Add project root to Python path for package imports
        if self.project_dir not in sys.path:
            sys.path.insert(0, self.project_dir)
        
        # Define pipeline stages and their dependencies
        self.pipeline_stages = [
            {
                'name': 'setup_duckdb',
                'script': 'setup_duckdb.py',
                'script_dir': 'setup',
                'dependencies': []
            },
            {
                'name': 'ingest_yfinance',
                'script': 'ingest_yfinance.py',
                'script_dir': 'ingest',
                'dependencies': ['setup_duckdb']
            },
            {
                'name': 'ingest_github_hf',
                'script': 'ingest_github_hf.py',
                'script_dir': 'ingest',
                'dependencies': ['setup_duckdb']
            },
            {
                'name': 'ingest_sec',
                'script': 'ingest_sec.py',
                'script_dir': 'ingest',
                'dependencies': ['setup_duckdb']
            },
            {
                'name': 'ingest_reddit',
                'script': 'ingest_reddit.py',
                'script_dir': 'ingest',
                'dependencies': ['setup_duckdb']
            },
            {
                'name': 'ingest_news',
                'script': 'ingest_news.py',
                'script_dir': 'ingest',
                'dependencies': ['setup_duckdb']
            },
            {
                'name': 'ingest_arxiv',
                'script': 'ingest_arxiv.py',
                'script_dir': 'ingest',
                'dependencies': ['setup_duckdb']
            },
            
            {
                'name': 'load_warehouse',
                'script': 'load_warehouse.py',
                'script_dir': 'setup',
                'dependencies': ['ingest_yfinance', 'ingest_sec', 'ingest_github_hf']
            },
            {
                'name': 'compute_features',
                'script': 'compute_features.py',
                'script_dir': 'setup',
                'dependencies': ['load_warehouse']
            },
            {
                'name': 'compute_indices',
                'script': 'compute_indices.py',
                'script_dir': 'setup',
                'dependencies': ['compute_features']
            },
            {
                'name': 'detect_bubble',
                'script': 'detect_bubble.py',
                'script_dir': 'setup',
                'dependencies': ['compute_indices']
            }
        ]

    def _import_script(self, script_path: str):
        """Import a Python script as a module"""
        try:
            spec = importlib.util.spec_from_file_location(
                os.path.splitext(os.path.basename(script_path))[0],
                script_path
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return module
        except Exception as e:
            logger.error(f"Error importing {script_path}: {str(e)}")
            raise

    def _run_stage(self, stage: dict) -> bool:
        """Run a single pipeline stage"""
        script_dir = self.setup_dir if stage['script_dir'] == 'setup' else self.ingest_dir
        script_path = os.path.join(script_dir, stage['script'])
        
        try:
            logger.info(f"Running {stage['name']}...")
            start_time = datetime.now()
            
            # Import and run the script
            try:
                module = self._import_script(script_path)
            except Exception as e:
                # If the import fails due to missing optional dependencies (e.g. scikit-learn
                # required by detect_bubble), log and skip the stage instead of failing the
                # entire pipeline. Other import errors will still be logged.
                msg = str(e)
                if 'No module named' in msg:
                    logger.warning(f"Skipping {stage['name']} due to missing dependency: {msg}")
                    return True
                else:
                    raise

            if hasattr(module, 'main'):
                module.main()
            
            duration = datetime.now() - start_time
            logger.info(f"Completed {stage['name']} in {duration}")
            return True
            
        except Exception as e:
            logger.error(f"Error in {stage['name']}: {str(e)}")
            return False

    def _get_completed_stages(self) -> List[str]:
        """Check which stages have been completed by verifying their outputs"""
        completed = []
        warehouse_path = os.path.join(self.project_dir, 'data', 'warehouse', 'ai_bubble.duckdb')
        
        if not os.path.exists(warehouse_path):
            return completed
            
        import duckdb
        try:
            conn = duckdb.connect(warehouse_path)
            
            # Check for key tables to determine completion
            # Note: compute_indices is intentionally NOT marked as completed here
            # because it needs to run every time to add new data points to bubble_metrics.
            # The table existing doesn't mean the stage is "done" - it needs to run
            # incrementally to append new points.
            tables = {
                'setup_duckdb': ['dim_entity', 'dim_time'],
                'load_warehouse': ['fact_metrics', 'fact_signals'],
                'compute_features': ['combined_features'],
                # 'compute_indices' intentionally omitted - always runs to add new points
                'detect_bubble': ['bubble_alerts']
            }
            
            for stage, required_tables in tables.items():
                all_exist = all(
                    conn.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table}')").fetchone()[0]
                    for table in required_tables
                )
                if all_exist:
                    completed.append(stage)
                    
            conn.close()
        except Exception as e:
            logger.warning(f"Error checking completion status: {str(e)}")
            
        return completed

    def run(self, incremental: bool = True, direct: bool = False):
        """Run the complete pipeline

        Args:
            incremental: whether to skip already completed stages (default True)
            direct: if True, skip the DuckDB initialization and ingestion stages and
                    start directly at the load_warehouse stage (useful when staging
                    data and schema are already present).
        """
        logger.info("Starting AI Bubble Detection pipeline...")

        # If direct is requested, mark setup and ingestion stages as completed so
        # the pipeline begins at load_warehouse. This intentionally overrides
        # checking the DB for completed stages when direct=True.
        # Note: In direct mode, we always run processing stages (load_warehouse, 
        # compute_features, compute_indices, detect_bubble) to ensure new data points
        # are added to the graph with each update.
        if direct:
            completed_stages = [
                'setup_duckdb',
                'ingest_yfinance',
                'ingest_github_hf',
                'ingest_sec',
                'ingest_reddit',
                'ingest_news',
                'ingest_arxiv',
            ]
            logger.info(f"Direct mode enabled - skipping ingestion stages: {', '.join(completed_stages)}")
            logger.info("Processing stages (load_warehouse, compute_features, compute_indices, detect_bubble) will always run to add new data points")
        else:
            # Get already completed stages if running incrementally
            completed_stages = self._get_completed_stages() if incremental else []
            if completed_stages and incremental:
                logger.info(f"Found completed stages: {', '.join(completed_stages)}")
        
        # Run each stage in order
        success = True
        for stage in self.pipeline_stages:
            # Skip if already completed and running incrementally
            if incremental and stage['name'] in completed_stages:
                logger.info(f"Skipping completed stage: {stage['name']}")
                continue
                
            # Check dependencies
            deps_met = all(
                dep in completed_stages 
                for dep in stage['dependencies']
            )
            
            if not deps_met:
                logger.error(
                    f"Dependencies not met for {stage['name']}: "
                    f"requires {', '.join(stage['dependencies'])}"
                )
                success = False
                break
            
            # Run the stage
            if not self._run_stage(stage):
                success = False
                break
                
            completed_stages.append(stage['name'])
        
        if success:
            logger.info("Pipeline completed successfully!")
        else:
            logger.error("Pipeline failed!")
            sys.exit(1)

def main():
    """Run the pipeline with command line arguments"""
    import argparse
    parser = argparse.ArgumentParser(description='Run the AI Bubble Detection pipeline')
    parser.add_argument(
        '--full-refresh',
        action='store_true',
        help='Run full pipeline without checking for completed stages'
    )
    parser.add_argument(
        '--direct',
        action='store_true',
        help='Skip DuckDB initialization and ingestion stages and start at load_warehouse'
    )
    args = parser.parse_args()
    
    pipeline = Pipeline()
    pipeline.run(incremental=not args.full_refresh, direct=args.direct)

if __name__ == "__main__":
    main()