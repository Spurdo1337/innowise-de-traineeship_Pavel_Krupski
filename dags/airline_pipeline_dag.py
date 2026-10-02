from pathlib import Path

import pendulum
from airflow.decorators import dag, task

BASE_DIR = Path("/opt/airflow")
SOURCE_FILE = BASE_DIR / "data" / "airline_dataset.csv"


@dag(
    dag_id="airline_dwh_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["snowflake", "airflow", "dwh"],
    doc_md="Loads the airline CSV into Snowflake through bronze, silver, and gold.",
)
def airline_dwh_pipeline():
    @task.snowpark
    def upload_source_file(session) -> str:
        session.file.put(
            str(SOURCE_FILE),
            "@AIRLINE_DWH.BRONZE.RAW_STAGE",
            overwrite=True,
            auto_compress=False,
        )
        return SOURCE_FILE.name

    @task.snowpark
    def load_bronze(file_name: str, session) -> str:
        result = session.sql(
            f"CALL AIRLINE_DWH.BRONZE.SP_LOAD_RAW_FROM_STAGE('{file_name}')"
        ).collect()
        return result[0][0]

    @task.snowpark
    def load_silver(session) -> str:
        result = session.sql("CALL AIRLINE_DWH.SILVER.SP_RAW_TO_SILVER()").collect()
        return result[0][0]

    @task.snowpark
    def load_gold(session) -> str:
        result = session.sql("CALL AIRLINE_DWH.GOLD.SP_SILVER_TO_GOLD()").collect()
        return result[0][0]

    @task.snowpark
    def validate_pipeline(session) -> str:
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
                (
                    SELECT COUNT(*) FROM AIRLINE_DWH.GOLD.FACT_FLIGHTS
                    WHERE PASSENGER_SK IS NULL
                       OR AIRPORT_SK IS NULL
                       OR PILOT_SK IS NULL
                       OR DATE_SK IS NULL
                ) AS FACT_NULL_FK_ROWS
            """
        ).collect()[0]

        summary = (
            f"bronze={row['BRONZE_ROWS']}, silver={row['SILVER_ROWS']}, "
            f"gold={row['GOLD_ROWS']}, dim_passenger={row['DIM_PASSENGER_ROWS']}, "
            f"dim_airport={row['DIM_AIRPORT_ROWS']}, dim_pilot={row['DIM_PILOT_ROWS']}, "
            f"dim_date={row['DIM_DATE_ROWS']}, audit={row['AUDIT_ROWS']}, "
            f"fact_null_fk_rows={row['FACT_NULL_FK_ROWS']}"
        )

        errors = []
        if row["BRONZE_ROWS"] == 0:
            errors.append("AIRLINE_DWH.BRONZE.RAW_FLIGHTS is empty")
        if row["SILVER_ROWS"] == 0:
            errors.append("AIRLINE_DWH.SILVER.STG_FLIGHTS is empty")
        if row["GOLD_ROWS"] == 0:
            errors.append("AIRLINE_DWH.GOLD.FACT_FLIGHTS is empty")
        if row["GOLD_ROWS"] != row["SILVER_ROWS"]:
            errors.append(
                f"AIRLINE_DWH.GOLD.FACT_FLIGHTS row count ({row['GOLD_ROWS']}) "
                f"does not match AIRLINE_DWH.SILVER.STG_FLIGHTS "
                f"({row['SILVER_ROWS']})"
            )
        if row["FACT_NULL_FK_ROWS"] > 0:
            errors.append(
                f"{row['FACT_NULL_FK_ROWS']} row(s) in "
                "AIRLINE_DWH.GOLD.FACT_FLIGHTS have a NULL foreign key "
                "(PASSENGER_SK / AIRPORT_SK / PILOT_SK / DATE_SK)"
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

    file_name = upload_source_file()
    bronze_result = load_bronze(file_name)
    silver_result = load_silver()
    gold_result = load_gold()
    validation_result = validate_pipeline()

    # load_bronze(file_name) already creates bronze's dependency on
    # upload_source_file by taking its return value as an argument - no
    # explicit edge needed there. load_silver/load_gold/validate_pipeline
    # take no arguments, though, so they have no implicit dependency on
    # anything - these three edges are the ones that actually enforce
    # execution order and are not redundant.
    bronze_result >> silver_result
    silver_result >> gold_result
    gold_result >> validation_result


airline_dwh_pipeline()
