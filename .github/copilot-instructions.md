# Copilot Instructions for AiPoP

## Project Overview

- **AiPoP** analyzes the gap between AI hype (social/media/market sentiment) and real technological progress using multi-source data and custom indices.
- Data flows from ingestion (various sources) → feature engineering → index calculation → bubble detection → dashboard visualization.

## Key Components & Data Flow

- **Ingestion scripts** (`ingest/`):
  - `ingest_yfinance.py`, `ingest_sec.py`, `ingest_news.py`, `ingest_reddit.py`, `ingest_arxiv.py`, `ingest_github_hf.py` collect raw data into `data/raw/` and stage to `data/staging/`.
  - Each script expects relevant API keys in `.env` (see `.env.example`).
- **Feature & Index Calculation** (`setup/`):
  - `compute_features.py` and `compute_indices.py` process staged data into features and indices, storing results in DuckDB (`data/warehouse/ai_bubble.duckdb`).
  - `detect_bubble.py` computes bubble signals using rolling stats and pattern matching.
- **Orchestration**:
  - `setup/run_pipeline.py` defines and runs the full pipeline with dependency management.
- **Visualization**:
  - `dashboard/app.py` (Streamlit) visualizes indices and alerts. Run with `streamlit run dashboard/app.py`.

## Developer Workflows

- **Setup**:
  - Copy `.env.example` to `.env` and fill in API keys.
  - Install dependencies: `pip install -r requirements.txt`
- **Run full pipeline**:
  - `python setup/run_pipeline.py` (preferred for end-to-end run)
  - Or run individual scripts in order: ingestion → features → indices → detection
- **Testing**:
  - Run all tests: `pytest tests/`
  - Minimal tests check script presence and basic API, not full data flow.
- **Data update automation**:
  - See `.github/workflows/data_update.yml` for scheduled/triggered runs and auto-commit of new data.

## Project Conventions & Patterns

- **Data folders**: `data/raw/`, `data/staging/`, `data/warehouse/` are not versioned; generate with scripts.
- **Environment variables**: All secrets/API keys must be set in `.env` or environment.
- **DuckDB**: Used for all intermediate and final data storage; schema evolves with features.
- **Modular pipeline**: Each stage is a script with clear inputs/outputs; dependencies managed in `run_pipeline.py`.
- **Error handling**: Ingest scripts exit with clear instructions if credentials are missing.
- **Extensibility**: Add new data sources by following the pattern in `ingest/` and updating the pipeline config.

## Integration Points

- **APIs**: SEC, Reddit, News, GitHub, HuggingFace, Yahoo Finance (see `.env.example` for required keys).
- **CI/CD**: GitHub Actions for data update and (optionally) test workflows.

## References

- See `README.md` and `ABOUT.md` for high-level goals and methodology.
- See `project_schema.json` for detailed script purposes, inputs/outputs, and dependencies.
- For new features, follow the modular script pattern and update the pipeline as needed.
