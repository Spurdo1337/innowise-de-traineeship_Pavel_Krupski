import csv
import re
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

BASE_DIR = Path("/opt/airflow")
SOURCE_FILE = BASE_DIR / "data" / "airline_dataset.csv"

# Airflow run ids use letters, digits and _ - : + . Anything else is refused
# before the id is written into SQL text.
RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9_\-:+.]+")


def _current_run_id() -> str:
    run_id = get_current_context()["run_id"]
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(f"Unexpected run id format: {run_id}")
    return run_id


def _count_source_rows(path: Path) -> int:
    """Number of data rows in the CSV (the header is not counted)."""
    with path.open(newline="", encoding="utf-8") as handle:
        return sum(1 for _ in csv.reader(handle)) - 1


@dag(
    dag_id="airline_dwh_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    # Runs are started by hand, one at a time, so parallel runs are not expected.
    # max_active_runs=1 only guards against an accidental double trigger.
    max_active_runs=1,
    tags=["snowflake", "airflow", "dwh"],
    doc_md="Loads the airline CSV into Snowflake through bronze, silver, and gold.",
)
def airline_dwh_pipeline():
    @task.snowpark
    def upload_source_file(session) -> dict:
        session.file.put(
            str(SOURCE_FILE),
            "@AIRLINE_DWH.BRONZE.RAW_STAGE",
            overwrite=True,
            auto_compress=False,
        )
        return {
            "file_name": SOURCE_FILE.name,
            "expected_rows": _count_source_rows(SOURCE_FILE),
        }

    @task.snowpark
    def load_bronze(source: dict, session) -> str:
        run_id = _current_run_id()
        result = session.sql(
            f"CALL AIRLINE_DWH.BRONZE.SP_LOAD_RAW_FROM_STAGE('{source['file_name']}', '{run_id}')"
        ).collect()
        return result[0][0]

    @task.snowpark
    def load_silver(session) -> str:
        run_id = _current_run_id()
        result = session.sql(f"CALL AIRLINE_DWH.SILVER.SP_RAW_TO_SILVER('{run_id}')").collect()
        return result[0][0]

    @task.snowpark
    def load_gold(session) -> str:
        run_id = _current_run_id()
        result = session.sql(f"CALL AIRLINE_DWH.GOLD.SP_SILVER_TO_GOLD('{run_id}')").collect()
        return result[0][0]

    @task.snowpark
    def validate_pipeline(source: dict, session) -> str:
        row = session.sql(
            """
            SELECT
                (SELECT COUNT(*) FROM AIRLINE_DWH.BRONZE.RAW_FLIGHTS) AS BRONZE_ROWS,
                (SELECT COUNT(*) FROM AIRLINE_DWH.SILVER.STG_FLIGHTS) AS SILVER_ROWS,
                (SELECT COUNT(*) FROM AIRLINE_DWH.GOLD.FACT_FLIGHTS) AS GOLD_ROWS,
                (SELECT COUNT(*) FROM AIRLINE_DWH.GOLD.DIM_PASSENGER) AS DIM_PASSENGER_ROWS,
                (SELECT COUNT(*) FROM AIRLINE_DWH.GOLD.DIM_AIRPORT) AS DIM_AIRPORT_ROWS,
                (SELECT COUNT(*) FROM AIRLINE_DWH.GOLD.DIM_PILOT) AS DIM_PILOT_ROWS,
                (SELECT COUNT(*) FROM AIRLINE_DWH.GOLD.DIM_DATE) AS DIM_DATE_ROWS,
                (SELECT COUNT(*) FROM AIRLINE_DWH.AUDIT.ETL_AUDIT_LOG) AS AUDIT_ROWS,
                (SELECT COUNT(*) FROM AIRLINE_DWH.SILVER.REJECTED_FLIGHTS) AS REJECTED_ROWS,
                (SELECT COUNT(*) FROM AIRLINE_DWH.SILVER.STG_FLIGHTS WHERE AGE_GROUP = 'Unknown')
                    AS UNKNOWN_AGE_ROWS,
                (
                    SELECT COUNT(*) FROM AIRLINE_DWH.GOLD.FACT_FLIGHTS
                    WHERE PASSENGER_SK IS NULL
                       OR AIRPORT_SK IS NULL
                       OR PILOT_SK IS NULL
                       OR DATE_SK IS NULL
                ) AS FACT_NULL_FK_ROWS
            """
        ).collect()[0]

        expected_rows = int(source["expected_rows"])
        summary = (
            f"source_file_rows={expected_rows}, bronze={row['BRONZE_ROWS']}, "
            f"silver={row['SILVER_ROWS']}, rejected={row['REJECTED_ROWS']}, "
            f"unknown_age={row['UNKNOWN_AGE_ROWS']}, gold={row['GOLD_ROWS']}, "
            f"dim_passenger={row['DIM_PASSENGER_ROWS']}, "
            f"dim_airport={row['DIM_AIRPORT_ROWS']}, dim_pilot={row['DIM_PILOT_ROWS']}, "
            f"dim_date={row['DIM_DATE_ROWS']}, audit={row['AUDIT_ROWS']}, "
            f"fact_null_fk_rows={row['FACT_NULL_FK_ROWS']}"
        )

        errors = []
        if row["BRONZE_ROWS"] == 0:
            errors.append("AIRLINE_DWH.BRONZE.RAW_FLIGHTS is empty")
        if row["BRONZE_ROWS"] != expected_rows:
            errors.append(
                f"AIRLINE_DWH.BRONZE.RAW_FLIGHTS has {row['BRONZE_ROWS']} rows, "
                f"but the source file has {expected_rows} data rows"
            )
        if row["SILVER_ROWS"] == 0:
            errors.append("AIRLINE_DWH.SILVER.STG_FLIGHTS is empty")
        if row["SILVER_ROWS"] + row["REJECTED_ROWS"] != row["BRONZE_ROWS"]:
            errors.append(
                f"Silver ({row['SILVER_ROWS']}) plus rejected ({row['REJECTED_ROWS']}) "
                f"does not equal bronze ({row['BRONZE_ROWS']}). Rows are unaccounted for."
            )
        if row["GOLD_ROWS"] == 0:
            errors.append("AIRLINE_DWH.GOLD.FACT_FLIGHTS is empty")
        if row["GOLD_ROWS"] != row["SILVER_ROWS"]:
            errors.append(
                f"AIRLINE_DWH.GOLD.FACT_FLIGHTS row count ({row['GOLD_ROWS']}) "
                f"does not match AIRLINE_DWH.SILVER.STG_FLIGHTS ({row['SILVER_ROWS']})"
            )
        if row["FACT_NULL_FK_ROWS"] > 0:
            errors.append(
                f"{row['FACT_NULL_FK_ROWS']} row(s) in AIRLINE_DWH.GOLD.FACT_FLIGHTS "
                "have a NULL foreign key (PASSENGER_SK / AIRPORT_SK / PILOT_SK / DATE_SK)"
            )
        for dim_name, count in (
            ("DIM_PASSENGER", row["DIM_PASSENGER_ROWS"]),
            ("DIM_AIRPORT", row["DIM_AIRPORT_ROWS"]),
            ("DIM_PILOT", row["DIM_PILOT_ROWS"]),
            ("DIM_DATE", row["DIM_DATE_ROWS"]),
        ):
            if count == 0:
                errors.append(f"AIRLINE_DWH.GOLD.{dim_name} is empty")
        if row["AUDIT_ROWS"] == 0:
            errors.append("AIRLINE_DWH.AUDIT.ETL_AUDIT_LOG has no rows at all")

        if errors:
            raise ValueError(
                "Pipeline validation failed: " + "; ".join(errors) + f" | counts: {summary}"
            )

        return summary

    source = upload_source_file()
    bronze_result = load_bronze(source)
    silver_result = load_silver()
    gold_result = load_gold()
    validation_result = validate_pipeline(source)

    # load_bronze(source) and validate_pipeline(source) already depend on
    # upload_source_file through their arguments. load_silver and load_gold
    # take no data, so these edges are what enforces their order.
    bronze_result >> silver_result
    silver_result >> gold_result
    gold_result >> validation_result


airline_dwh_pipeline()
