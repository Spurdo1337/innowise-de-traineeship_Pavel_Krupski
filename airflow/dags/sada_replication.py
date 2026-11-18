"""Replicate the Pagila and Sakila databases into Snowflake with Airbyte.

The DAG triggers the two Airbyte connections created by
``airbyte/setup_airbyte.py``, waits for the sync jobs and verifies that the
raw tables in Snowflake contain the expected number of rows.
"""

from datetime import datetime

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import DAG, Asset, task
from common.airbyte import find_connection, trigger_sync, wait_for_job
from common.checks import (
    PAGILA_EXPECTED_COUNTS,
    SAKILA_EXPECTED_COUNTS,
    assert_row_counts,
)

PAGILA_ASSET = Asset("pagila_raw")
SAKILA_ASSET = Asset("sakila_raw")

DATABASE = "SADA"


def _raw_counts(schema: str, expected: dict[str, int]) -> dict[str, int]:
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    counts: dict[str, int] = {}
    for table in expected:
        value = hook.get_first(f"select count(*) from {DATABASE}.{schema}.{table.upper()}")
        counts[table] = int(value[0]) if value else 0
    return counts


with DAG(
    dag_id="sada_replication",
    description="Replicate Pagila and Sakila into Snowflake with Airbyte",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["airbyte", "snowflake"],
) as dag:

    @task
    def replicate(connection_name: str) -> None:
        connection = find_connection(connection_name)
        job_id = trigger_sync(connection["connectionId"])
        wait_for_job(job_id)

    @task(outlets=[PAGILA_ASSET])
    def verify_pagila() -> None:
        counts = _raw_counts("PAGILA_RAW", PAGILA_EXPECTED_COUNTS)
        assert_row_counts(counts, PAGILA_EXPECTED_COUNTS, "Pagila (raw)")

    @task(outlets=[SAKILA_ASSET])
    def verify_sakila() -> None:
        counts = _raw_counts("SAKILA_RAW", SAKILA_EXPECTED_COUNTS)
        assert_row_counts(counts, SAKILA_EXPECTED_COUNTS, "Sakila (raw)")

    pagila_sync = replicate.override(task_id="replicate_pagila")("pagila_to_snowflake")
    sakila_sync = replicate.override(task_id="replicate_sakila")("sakila_to_snowflake")

    pagila_sync >> verify_pagila()
    sakila_sync >> verify_sakila()
