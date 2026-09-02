from datetime import datetime, timezone
from urllib.parse import quote_plus

import pandas as pd
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from pymongo import MongoClient

from common.config import FINAL_DATASET, FINAL_FILE

MONGO_CONN_ID = "mongo_default"
MONGO_DB = "reviews_db"
MONGO_COLLECTION = "google_play_reviews"


def _build_mongo_client() -> MongoClient:
    """Create a MongoDB client from the Airflow connection settings."""
    connection = BaseHook.get_connection(MONGO_CONN_ID)
    username = quote_plus(connection.login or "")
    password = quote_plus(connection.password or "")
    host = connection.host or "mongo"
    port = connection.port or 27017
    auth_source = connection.extra_dejson.get("authSource", connection.schema or "admin")

    uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource={auth_source}"
    return MongoClient(uri)


def load_to_mongo() -> None:
    """Load the processed dataset into MongoDB in batches."""
    if not FINAL_FILE.exists():
        raise FileNotFoundError(f"Processed file was not found: {FINAL_FILE}")

    df = pd.read_csv(FINAL_FILE)

    if "created_date" in df.columns:
        df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")

    df = df.where(pd.notnull(df), None)

    records = df.to_dict(orient="records")
    for record in records:
        created_date = record.get("created_date")
        if pd.notna(created_date):
            record["created_date"] = pd.Timestamp(created_date).to_pydatetime()

    client = _build_mongo_client()
    try:
        collection = client[MONGO_DB][MONGO_COLLECTION]
        collection.drop()

        batch_size = 2000
        for start in range(0, len(records), batch_size):
            batch = records[start : start + batch_size]
            if batch:
                collection.insert_many(batch, ordered=False)

        collection.create_index("created_date")
    finally:
        client.close()


with DAG(
    dag_id="load_reviews_to_mongo",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=[FINAL_DATASET],
    catchup=False,
    is_paused_upon_creation=False,
    tags=["reviews", "mongo", "loading"],
    default_args={"owner": "airflow"},
) as dag:
    load_data = PythonOperator(
        task_id="load_data_to_mongo",
        python_callable=load_to_mongo,
    )
