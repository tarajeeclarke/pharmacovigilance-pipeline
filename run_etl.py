"""
run_etl.py
----------
Single entry point to run the full pipeline:
  1. Extract from openFDA
  2. Transform + load into PostgreSQL

Usage:
    python run_etl.py
"""

from etl.extract import extract_all
from etl.transform_load import run_full_etl

if __name__ == "__main__":
    print("Starting pharmacovigilance ETL pipeline...")
    print("Step 1/2: Extracting from openFDA...")
    data = extract_all()

    print("\nStep 2/2: Transforming and loading into PostgreSQL...")
    stats = run_full_etl(data)

    print(f"\nPipeline complete.")
    print(f"  Loaded : {stats['loaded']}")
    print(f"  Skipped: {stats['skipped']}")
    print(f"  Errors : {stats['errors']}")
