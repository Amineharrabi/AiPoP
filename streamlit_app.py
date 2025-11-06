"""
Streamlit app entry point for Streamlit Community Cloud
This file is an alias to run.py for Streamlit Cloud deployment
"""
import sys
import os

# Add the project root to the path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import and run the main app
from run import main

if __name__ == "__main__":
    main()

