"""Provision the Sakila source database from the official MySQL dumps.

The DAG recreates the ``sakila`` database, loads the schema and data through
the ``mysql`` client and creates the read-only user that Airbyte uses for
replication.
"""

import os
from datetime import datetime

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task
from common.checks import SAKILA_EXPECTED_COUNTS, assert_row_counts

SQL_DIR = "/opt/airflow/sources/sakila"
MYSQL = "mysql -h sakila-db -u root --default-character-set=utf8mb4"
MYSQL_PWD = "root"

with DAG(
    dag_id="sakila_setup",
    description="Recreate and load the Sakila source database",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["sakila", "setup"],
) as dag:
    reset_database = BashOperator(
        task_id="reset_database",
        bash_command=(
            f'{MYSQL} -e "DROP DATABASE IF EXISTS sakila; '
            'CREATE DATABASE sakila CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"'
        ),
        env={"MYSQL_PWD": MYSQL_PWD},
        append_env=True,
    )

    load_schema = BashOperator(
        task_id="load_schema",
        bash_command=f"{MYSQL} sakila < {SQL_DIR}/sakila-schema.sql",
        env={"MYSQL_PWD": MYSQL_PWD},
        append_env=True,
    )

    load_data = BashOperator(
        task_id="load_data",
        bash_command=f"{MYSQL} sakila < {SQL_DIR}/sakila-data.sql",
        env={"MYSQL_PWD": MYSQL_PWD},
        append_env=True,
    )

    @task
    def ensure_replication_user() -> None:
        """Create or refresh the read-only user used by the Airbyte source."""
        password = os.environ["SAKILA_READER_PASSWORD"]
        hook = MySqlHook(mysql_conn_id="sakila_default")
        hook.run(
            "CREATE USER IF NOT EXISTS 'airbyte_reader'@'%' "
            f"IDENTIFIED WITH mysql_native_password BY '{password}'"
        )
        hook.run(
            "ALTER USER 'airbyte_reader'@'%' "
            f"IDENTIFIED WITH mysql_native_password BY '{password}'"
        )
        hook.run("GRANT SELECT ON sakila.* TO 'airbyte_reader'@'%'")

    @task
    def verify_row_counts() -> None:
        hook = MySqlHook(mysql_conn_id="sakila_default")
        actual = {
            table: hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]
            for table in SAKILA_EXPECTED_COUNTS
        }
        assert_row_counts(actual, SAKILA_EXPECTED_COUNTS, "Sakila")

    reset_database >> load_schema >> load_data >> ensure_replication_user() >> verify_row_counts()
