"""Shared constants for the reviews pipeline DAGs.

Single source of truth for the processed-file path and the Dataset object
that ties `process_reviews` and `load_reviews_to_mongo` together. Both DAGs
import from here instead of redefining the same path independently, so the
two can never silently drift apart.
"""

from pathlib import Path

from airflow.datasets import Dataset

RAW_FILE = Path("/opt/airflow/data/raw/tiktok_google_play_reviews.csv")
WORK_DIR = Path("/opt/airflow/data/work")
PROCESSED_DIR = Path("/opt/airflow/data/processed")

FINAL_FILE = PROCESSED_DIR / "reviews_final.csv"
FINAL_DATASET = Dataset(f"file://{FINAL_FILE}")
