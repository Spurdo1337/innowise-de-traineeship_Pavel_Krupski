"""Build the Pagila dbt project after the raw data landed in Snowflake.

The DAG runs ``dbt source freshness``, ``dbt build`` (models and tests only)
and verifies that Airflow is connected with the development role and that the
analytics models produced rows.
"""

from datetime import datetime

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset, task

DBT_DIR = "/opt/airflow/dbt/pagila_analytics"
DATABASE = "SADA"

ANALYTICS_MODELS = [
    "film_category_distribution",
    "top_rented_actors",
    "highest_revenue_category",
    "films_not_in_inventory",
    "top_children_actors",
    "customer_distribution_by_city",
    "category_rental_hours_analysis",
]

with DAG(
    dag_id="pagila_dbt_analytics",
    description="Run dbt models, tests and analytics checks for Pagila",
    start_date=datetime(2026, 1, 1),
    schedule=[Asset("pagila_raw")],
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "analytics"],
) as dag:

    # Artifacts go to /tmp so the container does not mix its Linux manifest
    # with the one a developer may generate on the host.
    dbt_flags = "--profiles-dir . --target-path /tmp/dbt-target --log-path /tmp/dbt-logs"

    source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && dbt source freshness {dbt_flags}",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build {dbt_flags}",
    )

    @task
    def verify_role() -> None:
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        role = hook.get_first("select current_role()")[0]
        if role != "SADA_DEV":
            raise ValueError(f"Airflow must use SADA_DEV, got {role}")

    @task
    def verify_analytics() -> None:
        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
        empty = []
        for model in ANALYTICS_MODELS:
            value = hook.get_first(f"select count(*) from {DATABASE}.ANALYTICS.{model.upper()}")
            if not value or int(value[0]) == 0:
                empty.append(model)
        if empty:
            raise ValueError(f"Analytics models returned no rows: {', '.join(empty)}")

    source_freshness >> dbt_build >> verify_role() >> verify_analytics()
