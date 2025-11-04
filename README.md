# AiPoP - AI Progress Vs Hype Detector

[![Python Version](https://img.shields.io/badge/python-3.13-blue.svg)](https://python.org)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.9.2-orange.svg)](https://duckdb.org)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

AiPoP (AI Progress or Propaganda) is a comprehensive analysis system designed to detect and measure potential AI hype bubbles by comparing public sentiment and market excitement against concrete technological progress and real-world adoption metrics.

## Project Goals

- Track and analyze AI-related hype across multiple data sources
- Measure concrete technological progress and adoption metrics
- Detect divergence between hype and reality
- Provide early warning signals for potential AI bubbles
- Enable data-driven decision making in AI investments and adoption

## Architecture

```
AiPoP/
├── data/
│   ├── raw/          # Raw data from various sources
│   ├── staging/      # Processed Parquet files
│   └── warehouse/    # DuckDB warehouse
├── scripts/
│   ├── ingest_*.py   # Data ingestion scripts
│   ├── compute_features.py
│   ├── compute_indices.py
│   └── detect_bubble.py
├── dashboard/
│   └── app.py        # Streamlit dashboard
└── requirements.txt
```

## Data Sources

1. Market Data:
   - Stock prices and trading volumes
   - SEC filings and financial metrics
2. Social Sentiment:
   - Reddit discussions and sentiment
   - Tech news coverage and tone
3. Technical Progress:
   - GitHub activity and stars
   - ArXiv paper submissions
   - HuggingFace model releases and downloads

## Pipeline Workflow

1. **Data Collection**

   - Regular ingestion from multiple sources
   - Raw data storage and versioning

2. **Feature Engineering**

   - Sentiment analysis
   - Activity metrics
   - Progress indicators

3. **Index Calculation**

   - HypeIndex: Measures market excitement and social buzz
   - RealityIndex: Tracks concrete progress and adoption
   - Bubble detection algorithms

4. **Visualization**
   - Interactive Streamlit dashboard
   - Real-time monitoring and alerts
   - Historical trend analysis

## Getting Started

1. Clone the repository:

```bash
git clone https://github.com/Amineharrabi/AiPoP-.git
cd AiPoP
```

2. Create and activate a virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
.\venv\Scripts\activate   # Windows
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. Set up environment variables:

```bash
cp .env.example .env
# Edit .env with your API keys and configurations
```

5. Run the pipeline:

```bash
python scripts/ingest_*.py     # Data collection
python scripts/compute_features.py
python scripts/compute_indices.py
python scripts/detect_bubble.py
```

6. Launch the dashboard:

```bash
streamlit run dashboard/app.py
```

## Features

- **Multi-source Data Integration**: Unified analysis of market, social, and technical metrics
- **Advanced Analytics**: Sentiment analysis, trend detection, and anomaly identification
- **Real-time Monitoring**: Continuous tracking of AI industry dynamics
- **Interactive Visualization**: Rich dashboards for exploring trends and patterns
- **Algorithmic Detection**: Sophisticated bubble detection using multiple indicators

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 📧 Contact

Amine Harrabi - [@amineharrabi](mailto://hrissa69@gmail.com)

Project Link: [https://github.com/Amineharrabi/AiPoP](https://github.com/Amineharrabi/AiPoP)
