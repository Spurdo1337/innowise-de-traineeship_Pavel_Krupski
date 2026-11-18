"""Provision the Pagila source database from the SQL dumps.

The DAG rebuilds the ``public`` schema from scratch on every run, loads the
dump files through ``psql`` (the data file uses ``COPY ... FROM stdin``, which
a database driver cannot execute directly) and creates the read-only user that
Airbyte uses for replication.
"""

import os
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task
from common.checks import PAGILA_EXPECTED_COUNTS, assert_row_counts

SQL_DIR = "/opt/airflow/sources/pagila"
PSQL = "psql -v ON_ERROR_STOP=1 -h pagila-db -U postgres -d pagila"
PGPASSWORD = "postgres"

with DAG(
    dag_id="pagila_setup",
    description="Recreate and load the Pagila source database",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["pagila", "setup"],
) as dag:
    reset_database = BashOperator(
        task_id="reset_database",
        bash_command=f'{PSQL} -c "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;"',
        env={"PGPASSWORD": PGPASSWORD},
        append_env=True,
    )

    load_schema = BashOperator(
        task_id="load_schema",
        bash_command=f"{PSQL} -f {SQL_DIR}/01-schema.sql",
        env={"PGPASSWORD": PGPASSWORD},
        append_env=True,
    )

    load_data = BashOperator(
        task_id="load_data",
        bash_command=f"{PSQL} -f {SQL_DIR}/02-data.sql",
        env={"PGPASSWORD": PGPASSWORD},
        append_env=True,
    )

    @task
    def ensure_replication_user() -> None:
        """Create or refresh the read-only user used by the Airbyte source."""
        password = os.environ["PAGILA_READER_PASSWORD"]
        hook = PostgresHook(postgres_conn_id="pagila_default")
        hook.run(
            "DO $$ BEGIN "
            "IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airbyte_reader') THEN "
            f"CREATE ROLE airbyte_reader LOGIN PASSWORD '{password}'; "
            "ELSE "
            f"ALTER ROLE airbyte_reader PASSWORD '{password}'; "
            "END IF; END $$;"
        )
        hook.run("GRANT USAGE ON SCHEMA public TO airbyte_reader")
        hook.run("GRANT SELECT ON ALL TABLES IN SCHEMA public TO airbyte_reader")
        hook.run(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA public "
            "GRANT SELECT ON TABLES TO airbyte_reader"
        )

    @task
    def verify_row_counts() -> None:
        hook = PostgresHook(postgres_conn_id="pagila_default")
        actual = {
            table: hook.get_first(f"SELECT count(*) FROM {table}")[0]
            for table in PAGILA_EXPECTED_COUNTS
        }
        assert_row_counts(actual, PAGILA_EXPECTED_COUNTS, "Pagila")

    reset_database >> load_schema >> load_data >> ensure_replication_user() >> verify_row_counts()
