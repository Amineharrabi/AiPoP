import pandas as pd
import os
from datetime import datetime

def check_staging_files():
    staging_dir = 'data/staging'
    files_config = [
        ('news_clean.parquet', 'publishedAt'),
        ('reddit_clean.parquet', 'created_utc'),
        ('github_clean.parquet', 'collected_at'),
        ('huggingface_clean.parquet', 'collected_at'),
        ('yfinance_clean.parquet', ('Date', ''))
    ]

    print("\nStaging Data Summary")
    print("=" * 50)

    for file, date_col in files_config:
        path = os.path.join(staging_dir, file)
        print(f"\n=== {file} ===")
        try:
            df = pd.read_parquet(path)

            if date_col:
                try:
                    latest_date = df[date_col].max()
                    earliest_date = df[date_col].min()
                    row_count = len(df)
                    print(f"Date range: {earliest_date} to {latest_date}")
                    print(f"Total rows: {row_count}")
                    
                    # Show a sample of columns and latest records
                    print("\nColumns:", df.columns.tolist())
                    print("\nLatest 5 records:")
                    print(df.sort_values(by=date_col, ascending=False).head())
                except Exception as e:
                    print(f"Error processing dates: {str(e)}")
                    print("\nFirst few records:")
                    print(df.head())
            else:
                print("No date column found")
                print("Columns available:", df.columns.tolist())
                print("Total rows:", len(df))
        except Exception as e:
            print(f"Error reading file: {str(e)}")

if __name__ == "__main__":
    check_staging_files()