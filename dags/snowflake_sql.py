# SQL/DDL used by the Airflow DAGs. Split into small, logically-grouped
# lists (instead of one flat BOOTSTRAP_STATEMENTS list) so the bootstrap
# DAG can run each group as its own Airflow task - see
# airline_bootstrap_dag.py.

# Fully-qualified name of the row access policy created by
# SECURITY_STATEMENTS. Needed before SECURITY_STATEMENTS runs, to detach
# the policy from anything it may already be attached to - see
# airline_bootstrap_dag.py:_detach_row_access_policy_everywhere.
ROW_ACCESS_POLICY_FQN = "AIRLINE_DWH.SECURITY.RAP_FACT_FLIGHTS_COUNTRY"

# Stage that holds the shared Python module (snowflake/python_procs/audit_utils.py)
# imported by all three stored procedures below.
UTIL_STAGE_FQN = "AIRLINE_DWH.AUDIT.UTIL_STAGE"

# ============================================================
# 1. Warehouse, database, schemas, file format, stage, sequences
# ============================================================
INFRA_STATEMENTS = [
    """
    CREATE WAREHOUSE IF NOT EXISTS AIRLINE_DWH_WH
      WAREHOUSE_SIZE = XSMALL
      AUTO_SUSPEND = 60
      AUTO_RESUME = TRUE
      INITIALLY_SUSPENDED = TRUE
    """,
    """
    CREATE DATABASE IF NOT EXISTS AIRLINE_DWH
    """,
    """
    CREATE SCHEMA IF NOT EXISTS AIRLINE_DWH.BRONZE
    """,
    """
    CREATE SCHEMA IF NOT EXISTS AIRLINE_DWH.SILVER
    """,
    """
    CREATE SCHEMA IF NOT EXISTS AIRLINE_DWH.GOLD
    """,
    """
    CREATE SCHEMA IF NOT EXISTS AIRLINE_DWH.AUDIT
    """,
    """
    CREATE SCHEMA IF NOT EXISTS AIRLINE_DWH.SECURITY
    """,
    """
    CREATE SCHEMA IF NOT EXISTS AIRLINE_DWH.ANALYTICS
    """,
    """
    CREATE OR REPLACE FILE FORMAT AIRLINE_DWH.BRONZE.CSV_FF
      TYPE = CSV
      SKIP_HEADER = 1
      FIELD_OPTIONALLY_ENCLOSED_BY = '"'
      TRIM_SPACE = TRUE
      NULL_IF = ('', 'NULL')
    """,
    """
    CREATE OR REPLACE STAGE AIRLINE_DWH.BRONZE.RAW_STAGE
      FILE_FORMAT = (FORMAT_NAME = 'AIRLINE_DWH.BRONZE.CSV_FF')
    """,
    """
    -- Holds snowflake/python_procs/audit_utils.py, imported by the three
    -- Python stored procedures below (see UTIL_STAGE_FQN).
    CREATE STAGE IF NOT EXISTS AIRLINE_DWH.AUDIT.UTIL_STAGE
    """,
    """
    CREATE OR REPLACE SEQUENCE AIRLINE_DWH.GOLD.SEQ_PASSENGER_SK START = 1 INCREMENT = 1
    """,
    """
    CREATE OR REPLACE SEQUENCE AIRLINE_DWH.GOLD.SEQ_AIRPORT_SK START = 1 INCREMENT = 1
    """,
    """
    CREATE OR REPLACE SEQUENCE AIRLINE_DWH.GOLD.SEQ_PILOT_SK START = 1 INCREMENT = 1
    """,
    """
    CREATE OR REPLACE SEQUENCE AIRLINE_DWH.GOLD.SEQ_DATE_SK START = 1 INCREMENT = 1
    """,
    """
    CREATE OR REPLACE SEQUENCE AIRLINE_DWH.GOLD.SEQ_FACT_SK START = 1 INCREMENT = 1
    """,
]

# ============================================================
# 2. Tables
# ============================================================
TABLE_STATEMENTS = [
    """
    -- Bronze intentionally has no NOT NULL constraints: it should be able
    -- to store a row exactly as the source file gave it, blanks included.
    CREATE OR REPLACE TABLE AIRLINE_DWH.BRONZE.RAW_FLIGHTS (
      PASSENGER_ID STRING,
      FIRST_NAME STRING,
      LAST_NAME STRING,
      GENDER STRING,
      AGE NUMBER(3,0),
      NATIONALITY STRING,
      AIRPORT_NAME STRING,
      AIRPORT_COUNTRY_CODE STRING,
      COUNTRY_NAME STRING,
      AIRPORT_CONTINENT STRING,
      CONTINENTS STRING,
      DEPARTURE_DATE STRING,
      ARRIVAL_AIRPORT STRING,
      PILOT_NAME STRING,
      FLIGHT_STATUS STRING,
      TICKET_TYPE STRING,
      PASSENGER_STATUS STRING,
      SOURCE_FILE_NAME STRING,
      LOADED_AT TIMESTAMP_NTZ,
      INGESTION_RUN_ID STRING
    )
    """,
    """
    CREATE OR REPLACE TABLE AIRLINE_DWH.SILVER.STG_FLIGHTS (
      PASSENGER_ID STRING NOT NULL,
      FIRST_NAME STRING,
      LAST_NAME STRING,
      GENDER STRING,
      AGE NUMBER(3,0),
      AGE_GROUP STRING,
      NATIONALITY STRING,
      AIRPORT_NAME STRING,
      AIRPORT_COUNTRY_CODE STRING,
      COUNTRY_NAME STRING,
      AIRPORT_CONTINENT STRING,
      CONTINENTS STRING,
      DEPARTURE_DATE DATE NOT NULL,
      ARRIVAL_AIRPORT STRING NOT NULL,
      PILOT_NAME STRING,
      FLIGHT_STATUS STRING,
      TICKET_TYPE STRING,
      PASSENGER_STATUS STRING,
      SOURCE_FILE_NAME STRING,
      LOADED_AT TIMESTAMP_NTZ,
      TRANSFORMED_AT TIMESTAMP_NTZ,
      CONSTRAINT PK_STG_FLIGHTS PRIMARY KEY (PASSENGER_ID, DEPARTURE_DATE, ARRIVAL_AIRPORT)
    )
    """,
    """
    CREATE OR REPLACE TABLE AIRLINE_DWH.GOLD.DIM_PASSENGER (
      PASSENGER_SK NUMBER(38,0) NOT NULL,
      PASSENGER_ID STRING NOT NULL,
      FIRST_NAME STRING,
      LAST_NAME STRING,
      GENDER STRING,
      AGE NUMBER(3,0),
      AGE_GROUP STRING,
      NATIONALITY STRING,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      UPDATED_AT TIMESTAMP_NTZ,
      CONSTRAINT PK_DIM_PASSENGER PRIMARY KEY (PASSENGER_SK),
      CONSTRAINT UQ_DIM_PASSENGER_ID UNIQUE (PASSENGER_ID)
    )
    """,
    """
    CREATE OR REPLACE TABLE AIRLINE_DWH.GOLD.DIM_AIRPORT (
      AIRPORT_SK NUMBER(38,0) NOT NULL,
      AIRPORT_NAME STRING NOT NULL,
      AIRPORT_COUNTRY_CODE STRING NOT NULL,
      COUNTRY_NAME STRING,
      AIRPORT_CONTINENT STRING,
      CONTINENTS STRING,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      UPDATED_AT TIMESTAMP_NTZ,
      CONSTRAINT PK_DIM_AIRPORT PRIMARY KEY (AIRPORT_SK),
      CONSTRAINT UQ_DIM_AIRPORT_NATURAL UNIQUE (AIRPORT_NAME, AIRPORT_COUNTRY_CODE)
    )
    """,
    """
    CREATE OR REPLACE TABLE AIRLINE_DWH.GOLD.DIM_PILOT (
      PILOT_SK NUMBER(38,0) NOT NULL,
      PILOT_NAME STRING NOT NULL,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      UPDATED_AT TIMESTAMP_NTZ,
      CONSTRAINT PK_DIM_PILOT PRIMARY KEY (PILOT_SK),
      CONSTRAINT UQ_DIM_PILOT_NAME UNIQUE (PILOT_NAME)
    )
    """,
    """
    CREATE OR REPLACE TABLE AIRLINE_DWH.GOLD.DIM_DATE (
      DATE_SK NUMBER(38,0) NOT NULL,
      FLIGHT_DATE DATE NOT NULL,
      CALENDAR_YEAR NUMBER(4,0),
      CALENDAR_QUARTER NUMBER(1,0),
      CALENDAR_MONTH NUMBER(2,0),
      CALENDAR_DAY NUMBER(2,0),
      DAY_OF_WEEK NUMBER(1,0),
      WEEK_OF_YEAR NUMBER(2,0),
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      UPDATED_AT TIMESTAMP_NTZ,
      CONSTRAINT PK_DIM_DATE PRIMARY KEY (DATE_SK),
      CONSTRAINT UQ_DIM_DATE_FLIGHT_DATE UNIQUE (FLIGHT_DATE)
    )
    """,
    """
    CREATE OR REPLACE TABLE AIRLINE_DWH.GOLD.FACT_FLIGHTS (
      FACT_FLIGHT_SK NUMBER(38,0) NOT NULL,
      PASSENGER_SK NUMBER(38,0) NOT NULL,
      AIRPORT_SK NUMBER(38,0) NOT NULL,
      PILOT_SK NUMBER(38,0) NOT NULL,
      DATE_SK NUMBER(38,0) NOT NULL,
      PASSENGER_ID STRING NOT NULL,
      DEPARTURE_DATE DATE NOT NULL,
      ARRIVAL_AIRPORT STRING NOT NULL,
      COUNTRY_NAME STRING,
      FLIGHT_STATUS STRING,
      TICKET_TYPE STRING,
      PASSENGER_STATUS STRING,
      SOURCE_FILE_NAME STRING,
      LOADED_AT TIMESTAMP_NTZ,
      CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      UPDATED_AT TIMESTAMP_NTZ,
      CONSTRAINT PK_FACT_FLIGHTS PRIMARY KEY (FACT_FLIGHT_SK),
      CONSTRAINT UQ_FACT_FLIGHTS_NATURAL UNIQUE (PASSENGER_ID, DEPARTURE_DATE, ARRIVAL_AIRPORT),
      CONSTRAINT FK_FACT_PASSENGER FOREIGN KEY (PASSENGER_SK) REFERENCES AIRLINE_DWH.GOLD.DIM_PASSENGER (PASSENGER_SK),
      CONSTRAINT FK_FACT_AIRPORT FOREIGN KEY (AIRPORT_SK) REFERENCES AIRLINE_DWH.GOLD.DIM_AIRPORT (AIRPORT_SK),
      CONSTRAINT FK_FACT_PILOT FOREIGN KEY (PILOT_SK) REFERENCES AIRLINE_DWH.GOLD.DIM_PILOT (PILOT_SK),
      CONSTRAINT FK_FACT_DATE FOREIGN KEY (DATE_SK) REFERENCES AIRLINE_DWH.GOLD.DIM_DATE (DATE_SK)
    )
    """,
    """
    CREATE OR REPLACE TABLE AIRLINE_DWH.AUDIT.ETL_AUDIT_LOG (
      AUDIT_ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
      RUN_ID STRING NOT NULL,
      PROCEDURE_NAME STRING NOT NULL,
      TARGET_OBJECT STRING NOT NULL,
      SOURCE_OBJECT STRING,
      ROWS_INSERTED NUMBER(38,0),
      ROWS_UPDATED NUMBER(38,0),
      ROWS_DELETED NUMBER(38,0),
      ROWS_AFFECTED NUMBER(38,0),
      STATUS STRING NOT NULL,
      DETAILS STRING,
      EXECUTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      CONSTRAINT PK_ETL_AUDIT_LOG PRIMARY KEY (AUDIT_ID)
    )
    """,
    """
    CREATE OR REPLACE TABLE AIRLINE_DWH.SECURITY.ALLOWED_COUNTRY_ROLES (
      ROLE_NAME STRING NOT NULL,
      ALLOWED_COUNTRY_NAME STRING NOT NULL
    )
    """,
    """
    -- The row access policy already grants ACCOUNTADMIN unconditional
    -- access via CURRENT_ROLE() = 'ACCOUNTADMIN'. The read-only role used by
    -- Power BI (AIRLINE_BI) is listed here for the three visible countries.
    INSERT INTO AIRLINE_DWH.SECURITY.ALLOWED_COUNTRY_ROLES (ROLE_NAME, ALLOWED_COUNTRY_NAME)
    SELECT * FROM VALUES
      ('AIRLINE_BI', 'United States'),
      ('AIRLINE_BI', 'Canada'),
      ('AIRLINE_BI', 'United Kingdom')
    """,
    """
    CREATE OR REPLACE TABLE AIRLINE_DWH.SILVER.REJECTED_FLIGHTS (
      RUN_ID STRING NOT NULL,
      REJECT_REASON STRING NOT NULL,
      SOURCE_FILE_NAME STRING,
      PASSENGER_ID STRING,
      ARRIVAL_AIRPORT STRING,
      DEPARTURE_DATE_RAW STRING,
      REJECTED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """,
]

# ============================================================
# 3. Streams
# ============================================================
STREAM_STATEMENTS = [
    """
    CREATE OR REPLACE STREAM AIRLINE_DWH.BRONZE.RAW_FLIGHTS_STREAM
      ON TABLE AIRLINE_DWH.BRONZE.RAW_FLIGHTS
      APPEND_ONLY = TRUE
    """,
    """
    CREATE OR REPLACE STREAM AIRLINE_DWH.SILVER.STG_FLIGHTS_STREAM
      ON TABLE AIRLINE_DWH.SILVER.STG_FLIGHTS
    """,
]

# ============================================================
# 4. Row access policy, secure view, grants
# ============================================================
SECURITY_STATEMENTS = [
    """
    CREATE OR REPLACE ROW ACCESS POLICY AIRLINE_DWH.SECURITY.RAP_FACT_FLIGHTS_COUNTRY
      AS (COUNTRY_NAME STRING)
      RETURNS BOOLEAN ->
        CURRENT_ROLE() = 'ACCOUNTADMIN'
        OR EXISTS (
          SELECT 1
          FROM AIRLINE_DWH.SECURITY.ALLOWED_COUNTRY_ROLES R
          WHERE UPPER(R.ROLE_NAME) = UPPER(CURRENT_ROLE())
            AND UPPER(R.ALLOWED_COUNTRY_NAME) = UPPER(COUNTRY_NAME)
        )
    """,
    """
    ALTER TABLE AIRLINE_DWH.GOLD.FACT_FLIGHTS
      ADD ROW ACCESS POLICY AIRLINE_DWH.SECURITY.RAP_FACT_FLIGHTS_COUNTRY
      ON (COUNTRY_NAME)
    """,
    """
    CREATE OR REPLACE SECURE VIEW AIRLINE_DWH.ANALYTICS.SECURE_FACT_FLIGHTS_V AS
    SELECT
      F.FACT_FLIGHT_SK,
      F.PASSENGER_ID,
      P.FIRST_NAME,
      P.LAST_NAME,
      P.GENDER,
      P.AGE,
      P.AGE_GROUP,
      P.NATIONALITY,
      A.AIRPORT_NAME,
      A.AIRPORT_COUNTRY_CODE,
      A.COUNTRY_NAME,
      A.AIRPORT_CONTINENT,
      A.CONTINENTS,
      D.FLIGHT_DATE,
      D.CALENDAR_YEAR,
      D.CALENDAR_QUARTER,
      D.CALENDAR_MONTH,
      D.CALENDAR_DAY,
      D.DAY_OF_WEEK,
      D.WEEK_OF_YEAR,
      PIL.PILOT_NAME,
      F.ARRIVAL_AIRPORT,
      F.FLIGHT_STATUS,
      F.TICKET_TYPE,
      F.PASSENGER_STATUS,
      F.SOURCE_FILE_NAME,
      F.LOADED_AT
    FROM AIRLINE_DWH.GOLD.FACT_FLIGHTS F
    LEFT JOIN AIRLINE_DWH.GOLD.DIM_PASSENGER P ON F.PASSENGER_SK = P.PASSENGER_SK
    LEFT JOIN AIRLINE_DWH.GOLD.DIM_AIRPORT A ON F.AIRPORT_SK = A.AIRPORT_SK
    LEFT JOIN AIRLINE_DWH.GOLD.DIM_PILOT PIL ON F.PILOT_SK = PIL.PILOT_SK
    LEFT JOIN AIRLINE_DWH.GOLD.DIM_DATE D ON F.DATE_SK = D.DATE_SK
    """,
    """
    CREATE ROLE IF NOT EXISTS AIRLINE_BI
    """,
    """
    GRANT SELECT ON VIEW AIRLINE_DWH.ANALYTICS.SECURE_FACT_FLIGHTS_V TO ROLE AIRLINE_BI
    """,
    """
    GRANT USAGE ON WAREHOUSE AIRLINE_DWH_WH TO ROLE AIRLINE_BI
    """,
    """
    GRANT USAGE ON DATABASE AIRLINE_DWH TO ROLE AIRLINE_BI
    """,
    """
    GRANT USAGE ON SCHEMA AIRLINE_DWH.ANALYTICS TO ROLE AIRLINE_BI
    """,
]

# ============================================================
# 5. Stored procedures (Python, sharing snowflake/python_procs/audit_utils.py)
# ============================================================

BRONZE_LOAD_PROC = '''
CREATE OR REPLACE PROCEDURE AIRLINE_DWH.BRONZE.SP_LOAD_RAW_FROM_STAGE(P_LOCAL_FILE_NAME STRING, P_RUN_ID STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS = ('@__UTIL_STAGE__/audit_utils.py')
HANDLER = 'main'
AS
$$
from audit_utils import log_audit

# Uses COPY INTO rather than INSERT ... SELECT: Snowflake keeps per-table
# load metadata (file name, size, ETag) and skips a staged file it already
# loaded into this table. When that happens COPY INTO returns ONE row with
# ONE column, "Copy executed with 0 files processed." - not the usual
# per-file row (file, status, rows_parsed, rows_loaded, ...) - so the
# result parsing below has to handle both shapes.
#
# Every row of one file gets the same INGESTION_RUN_ID: the Airflow run id.

def main(session, p_local_file_name, p_run_id):
    safe_file_name = p_local_file_name.replace("'", "''")
    safe_run_id = p_run_id.replace("'", "''")

    sql = f"""
        COPY INTO AIRLINE_DWH.BRONZE.RAW_FLIGHTS (
            PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE, NATIONALITY,
            AIRPORT_NAME, AIRPORT_COUNTRY_CODE, COUNTRY_NAME, AIRPORT_CONTINENT, CONTINENTS,
            DEPARTURE_DATE, ARRIVAL_AIRPORT, PILOT_NAME, FLIGHT_STATUS, TICKET_TYPE,
            PASSENGER_STATUS, SOURCE_FILE_NAME, LOADED_AT, INGESTION_RUN_ID
        )
        FROM (
            SELECT
                t.$2::STRING, t.$3::STRING, t.$4::STRING, t.$5::STRING,
                t.$6::NUMBER(3,0), t.$7::STRING, t.$8::STRING, t.$9::STRING,
                t.$10::STRING, t.$11::STRING, t.$12::STRING, t.$13::STRING,
                t.$14::STRING, t.$15::STRING, t.$16::STRING, t.$17::STRING,
                t.$18::STRING, '{safe_file_name}'::STRING, CURRENT_TIMESTAMP(), '{safe_run_id}'::STRING
            FROM @AIRLINE_DWH.BRONZE.RAW_STAGE t
        )
        FILES = ('{safe_file_name}')
        FILE_FORMAT = (FORMAT_NAME = 'AIRLINE_DWH.BRONZE.CSV_FF')
    """

    procedure = "AIRLINE_DWH.BRONZE.SP_LOAD_RAW_FROM_STAGE"
    target = "AIRLINE_DWH.BRONZE.RAW_FLIGHTS"
    source = "@AIRLINE_DWH.BRONZE.RAW_STAGE"

    try:
        rows = session.sql(sql).collect()
        total_loaded = 0
        file_statuses = []
        for r in rows:
            if len(r) >= 4:
                # per-file row: file, status, rows_parsed, rows_loaded, ...
                file_statuses.append(str(r[0]) + "=" + str(r[1]))
                total_loaded += int(r[3] or 0)
            else:
                # already-loaded file: single "status" column
                file_statuses.append(str(r[0]))

        details = "COPY INTO result: " + ", ".join(file_statuses)
        log_audit(
            session, p_run_id, procedure, target, source,
            total_loaded, 0, 0, "SUCCESS", details,
        )
        return "Bronze COPY INTO finished (" + ", ".join(file_statuses) + "), rows loaded: " + str(total_loaded)
    except Exception as err:
        log_audit(
            session, p_run_id, procedure, target, source,
            0, 0, 0, "FAILED", str(err),
        )
        raise
$$
'''.replace("__UTIL_STAGE__", UTIL_STAGE_FQN)

# Silver: the bronze stream is consumed inside one explicit transaction, together
# with the reject insert and the MERGE. A failure rolls all of it back, so the
# stream still holds the same rows and a retry processes them again.
#
# Two notes on the Snowflake rules this relies on:
#  - DDL statements (CREATE, DROP) commit any open transaction, so the batch
#    table is created before BEGIN TRANSACTION and dropped after COMMIT.
#  - Only DML and queries belong inside the transaction.
SILVER_PROC = '''
CREATE OR REPLACE PROCEDURE AIRLINE_DWH.SILVER.SP_RAW_TO_SILVER(P_RUN_ID STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS = ('@__UTIL_STAGE__/audit_utils.py')
HANDLER = 'main'
AS
$$
from audit_utils import log_audit, run_merge_and_log

RAW_COLUMNS = (
    "PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE, NATIONALITY, "
    "AIRPORT_NAME, AIRPORT_COUNTRY_CODE, COUNTRY_NAME, AIRPORT_CONTINENT, CONTINENTS, "
    "DEPARTURE_DATE, ARRIVAL_AIRPORT, PILOT_NAME, FLIGHT_STATUS, TICKET_TYPE, "
    "PASSENGER_STATUS, SOURCE_FILE_NAME, LOADED_AT, INGESTION_RUN_ID"
)

BATCH = "AIRLINE_DWH.SILVER.TMP_BRONZE_BATCH"
SOURCE = "AIRLINE_DWH.BRONZE.RAW_FLIGHTS_STREAM"
TARGET = "AIRLINE_DWH.SILVER.STG_FLIGHTS"
PROCEDURE = "AIRLINE_DWH.SILVER.SP_RAW_TO_SILVER"

MERGE_SQL = """
      MERGE INTO AIRLINE_DWH.SILVER.STG_FLIGHTS T
      USING (
        SELECT
          PASSENGER_ID,
          FIRST_NAME,
          LAST_NAME,
          GENDER,
          AGE,
          CASE
            WHEN AGE IS NULL THEN 'Unknown'
            WHEN AGE < 18 THEN 'Under 18'
            WHEN AGE < 30 THEN '18-29'
            WHEN AGE < 45 THEN '30-44'
            WHEN AGE < 60 THEN '45-59'
            ELSE '60+'
          END AS AGE_GROUP,
          NATIONALITY,
          AIRPORT_NAME,
          AIRPORT_COUNTRY_CODE,
          COUNTRY_NAME,
          AIRPORT_CONTINENT,
          CONTINENTS,
          TRY_TO_DATE(DEPARTURE_DATE, 'MM/DD/YYYY') AS DEPARTURE_DATE,
          ARRIVAL_AIRPORT,
          PILOT_NAME,
          FLIGHT_STATUS,
          TICKET_TYPE,
          PASSENGER_STATUS,
          SOURCE_FILE_NAME,
          LOADED_AT,
          CURRENT_TIMESTAMP() AS TRANSFORMED_AT
        FROM AIRLINE_DWH.SILVER.TMP_BRONZE_BATCH
        WHERE PASSENGER_ID IS NOT NULL
          AND ARRIVAL_AIRPORT IS NOT NULL
          AND TRY_TO_DATE(DEPARTURE_DATE, 'MM/DD/YYYY') IS NOT NULL
        -- Keeps exactly one row per natural key even if the batch contains
        -- more than one row for it, so the MERGE below can't fail with
        -- "duplicate row detected during DML action".
        QUALIFY ROW_NUMBER() OVER (
          PARTITION BY PASSENGER_ID, DEPARTURE_DATE, ARRIVAL_AIRPORT
          ORDER BY LOADED_AT DESC
        ) = 1
      ) S
      ON T.PASSENGER_ID = S.PASSENGER_ID
         AND T.DEPARTURE_DATE = S.DEPARTURE_DATE
         AND T.ARRIVAL_AIRPORT = S.ARRIVAL_AIRPORT
      WHEN MATCHED THEN UPDATE SET
        FIRST_NAME = S.FIRST_NAME,
        LAST_NAME = S.LAST_NAME,
        GENDER = S.GENDER,
        AGE = S.AGE,
        AGE_GROUP = S.AGE_GROUP,
        NATIONALITY = S.NATIONALITY,
        AIRPORT_NAME = S.AIRPORT_NAME,
        AIRPORT_COUNTRY_CODE = S.AIRPORT_COUNTRY_CODE,
        COUNTRY_NAME = S.COUNTRY_NAME,
        AIRPORT_CONTINENT = S.AIRPORT_CONTINENT,
        CONTINENTS = S.CONTINENTS,
        PILOT_NAME = S.PILOT_NAME,
        FLIGHT_STATUS = S.FLIGHT_STATUS,
        TICKET_TYPE = S.TICKET_TYPE,
        PASSENGER_STATUS = S.PASSENGER_STATUS,
        SOURCE_FILE_NAME = S.SOURCE_FILE_NAME,
        LOADED_AT = S.LOADED_AT,
        TRANSFORMED_AT = S.TRANSFORMED_AT
      WHEN NOT MATCHED THEN INSERT (
        PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE, AGE_GROUP, NATIONALITY,
        AIRPORT_NAME, AIRPORT_COUNTRY_CODE, COUNTRY_NAME, AIRPORT_CONTINENT, CONTINENTS,
        DEPARTURE_DATE, ARRIVAL_AIRPORT, PILOT_NAME, FLIGHT_STATUS, TICKET_TYPE,
        PASSENGER_STATUS, SOURCE_FILE_NAME, LOADED_AT, TRANSFORMED_AT
      )
      VALUES (
        S.PASSENGER_ID, S.FIRST_NAME, S.LAST_NAME, S.GENDER, S.AGE, S.AGE_GROUP, S.NATIONALITY,
        S.AIRPORT_NAME, S.AIRPORT_COUNTRY_CODE, S.COUNTRY_NAME, S.AIRPORT_CONTINENT, S.CONTINENTS,
        S.DEPARTURE_DATE, S.ARRIVAL_AIRPORT, S.PILOT_NAME, S.FLIGHT_STATUS, S.TICKET_TYPE,
        S.PASSENGER_STATUS, S.SOURCE_FILE_NAME, S.LOADED_AT, S.TRANSFORMED_AT
      )
    
"""


def rollback_quietly(session):
    try:
        session.sql("ROLLBACK").collect()
    except Exception:
        pass


def main(session, p_run_id):
    try:
        # DDL, outside the transaction (it would commit an open transaction).
        session.sql(
            f"CREATE OR REPLACE TRANSIENT TABLE {BATCH} LIKE AIRLINE_DWH.BRONZE.RAW_FLIGHTS"
        ).collect()

        session.sql("BEGIN TRANSACTION").collect()

        # DML: this is what consumes the bronze stream. A rollback restores it.
        session.sql(
            f"INSERT INTO {BATCH} ({RAW_COLUMNS}) SELECT {RAW_COLUMNS} FROM {SOURCE}"
        ).collect()

        # Rows that cannot be loaded are kept with a reason, not dropped silently.
        session.sql(
            f"""
            INSERT INTO AIRLINE_DWH.SILVER.REJECTED_FLIGHTS
              (RUN_ID, REJECT_REASON, SOURCE_FILE_NAME, PASSENGER_ID, ARRIVAL_AIRPORT, DEPARTURE_DATE_RAW)
            SELECT
              ?,
              CASE
                WHEN PASSENGER_ID IS NULL THEN 'PASSENGER_ID is NULL'
                WHEN ARRIVAL_AIRPORT IS NULL THEN 'ARRIVAL_AIRPORT is NULL'
                ELSE 'DEPARTURE_DATE is not a valid MM/DD/YYYY date'
              END,
              SOURCE_FILE_NAME, PASSENGER_ID, ARRIVAL_AIRPORT, DEPARTURE_DATE
            FROM {BATCH}
            WHERE PASSENGER_ID IS NULL
               OR ARRIVAL_AIRPORT IS NULL
               OR TRY_TO_DATE(DEPARTURE_DATE, 'MM/DD/YYYY') IS NULL
            """,
            params=[p_run_id],
        ).collect()

        affected = run_merge_and_log(
            session, p_run_id, MERGE_SQL, PROCEDURE, TARGET, SOURCE,
            "Merged the bronze batch into the silver layer.",
        )

        session.sql("COMMIT").collect()
        session.sql(f"DROP TABLE IF EXISTS {BATCH}").collect()
        return f"Merged bronze to silver: {affected} rows affected"
    except Exception as err:
        rollback_quietly(session)
        log_audit(session, p_run_id, PROCEDURE, TARGET, SOURCE, 0, 0, 0, "FAILED", str(err))
        raise
$$
'''.replace("__UTIL_STAGE__", UTIL_STAGE_FQN)

# Gold: same pattern as Silver. The silver stream is consumed inside one
# explicit transaction together with all five MERGEs.

GOLD_PROC = '''
CREATE OR REPLACE PROCEDURE AIRLINE_DWH.GOLD.SP_SILVER_TO_GOLD(P_RUN_ID STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS = ('@__UTIL_STAGE__/audit_utils.py')
HANDLER = 'main'
AS
$$
from audit_utils import log_audit, run_merge_and_log

DIM_PASSENGER_SQL = """
  MERGE INTO AIRLINE_DWH.GOLD.DIM_PASSENGER T
  USING (
    SELECT PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE, AGE_GROUP, NATIONALITY
    FROM AIRLINE_DWH.GOLD.TMP_SILVER_BATCH
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PASSENGER_ID ORDER BY TRANSFORMED_AT DESC) = 1
  ) S
  ON T.PASSENGER_ID = S.PASSENGER_ID
  WHEN MATCHED THEN UPDATE SET
    FIRST_NAME = S.FIRST_NAME, LAST_NAME = S.LAST_NAME, GENDER = S.GENDER,
    AGE = S.AGE, AGE_GROUP = S.AGE_GROUP, NATIONALITY = S.NATIONALITY,
    UPDATED_AT = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (PASSENGER_SK, PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE, AGE_GROUP, NATIONALITY)
  VALUES (AIRLINE_DWH.GOLD.SEQ_PASSENGER_SK.NEXTVAL, S.PASSENGER_ID, S.FIRST_NAME, S.LAST_NAME, S.GENDER, S.AGE, S.AGE_GROUP, S.NATIONALITY)
"""

DIM_AIRPORT_SQL = """
  MERGE INTO AIRLINE_DWH.GOLD.DIM_AIRPORT T
  USING (
    SELECT AIRPORT_NAME, AIRPORT_COUNTRY_CODE, COUNTRY_NAME, AIRPORT_CONTINENT, CONTINENTS
    FROM AIRLINE_DWH.GOLD.TMP_SILVER_BATCH
    QUALIFY ROW_NUMBER() OVER (PARTITION BY AIRPORT_NAME, AIRPORT_COUNTRY_CODE ORDER BY TRANSFORMED_AT DESC) = 1
  ) S
  ON T.AIRPORT_NAME = S.AIRPORT_NAME AND T.AIRPORT_COUNTRY_CODE = S.AIRPORT_COUNTRY_CODE
  WHEN MATCHED THEN UPDATE SET
    COUNTRY_NAME = S.COUNTRY_NAME, AIRPORT_CONTINENT = S.AIRPORT_CONTINENT,
    CONTINENTS = S.CONTINENTS, UPDATED_AT = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (AIRPORT_SK, AIRPORT_NAME, AIRPORT_COUNTRY_CODE, COUNTRY_NAME, AIRPORT_CONTINENT, CONTINENTS)
  VALUES (AIRLINE_DWH.GOLD.SEQ_AIRPORT_SK.NEXTVAL, S.AIRPORT_NAME, S.AIRPORT_COUNTRY_CODE, S.COUNTRY_NAME, S.AIRPORT_CONTINENT, S.CONTINENTS)
"""

DIM_PILOT_SQL = """
  MERGE INTO AIRLINE_DWH.GOLD.DIM_PILOT T
  USING (
    SELECT PILOT_NAME FROM AIRLINE_DWH.GOLD.TMP_SILVER_BATCH
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PILOT_NAME ORDER BY TRANSFORMED_AT DESC) = 1
  ) S
  ON T.PILOT_NAME = S.PILOT_NAME
  WHEN MATCHED THEN UPDATE SET UPDATED_AT = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (PILOT_SK, PILOT_NAME)
  VALUES (AIRLINE_DWH.GOLD.SEQ_PILOT_SK.NEXTVAL, S.PILOT_NAME)
"""

DIM_DATE_SQL = """
  MERGE INTO AIRLINE_DWH.GOLD.DIM_DATE T
  USING (
    SELECT
      DEPARTURE_DATE AS FLIGHT_DATE,
      YEAR(DEPARTURE_DATE) AS CALENDAR_YEAR,
      QUARTER(DEPARTURE_DATE) AS CALENDAR_QUARTER,
      MONTH(DEPARTURE_DATE) AS CALENDAR_MONTH,
      DAY(DEPARTURE_DATE) AS CALENDAR_DAY,
      DAYOFWEEK(DEPARTURE_DATE) AS DAY_OF_WEEK,
      WEEKOFYEAR(DEPARTURE_DATE) AS WEEK_OF_YEAR
    FROM AIRLINE_DWH.GOLD.TMP_SILVER_BATCH
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DEPARTURE_DATE ORDER BY TRANSFORMED_AT DESC) = 1
  ) S
  ON T.FLIGHT_DATE = S.FLIGHT_DATE
  WHEN MATCHED THEN UPDATE SET
    CALENDAR_YEAR = S.CALENDAR_YEAR, CALENDAR_QUARTER = S.CALENDAR_QUARTER,
    CALENDAR_MONTH = S.CALENDAR_MONTH, CALENDAR_DAY = S.CALENDAR_DAY,
    DAY_OF_WEEK = S.DAY_OF_WEEK, WEEK_OF_YEAR = S.WEEK_OF_YEAR, UPDATED_AT = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (DATE_SK, FLIGHT_DATE, CALENDAR_YEAR, CALENDAR_QUARTER, CALENDAR_MONTH, CALENDAR_DAY, DAY_OF_WEEK, WEEK_OF_YEAR)
  VALUES (AIRLINE_DWH.GOLD.SEQ_DATE_SK.NEXTVAL, S.FLIGHT_DATE, S.CALENDAR_YEAR, S.CALENDAR_QUARTER, S.CALENDAR_MONTH, S.CALENDAR_DAY, S.DAY_OF_WEEK, S.WEEK_OF_YEAR)
"""

FACT_SQL = """
  MERGE INTO AIRLINE_DWH.GOLD.FACT_FLIGHTS T
  USING (
    SELECT
      P.PASSENGER_SK, A.AIRPORT_SK, PIL.PILOT_SK, D.DATE_SK,
      S.PASSENGER_ID, S.DEPARTURE_DATE, S.ARRIVAL_AIRPORT, S.COUNTRY_NAME,
      S.FLIGHT_STATUS, S.TICKET_TYPE, S.PASSENGER_STATUS, S.SOURCE_FILE_NAME, S.LOADED_AT
    FROM AIRLINE_DWH.GOLD.TMP_SILVER_BATCH S
    LEFT JOIN AIRLINE_DWH.GOLD.DIM_PASSENGER P ON P.PASSENGER_ID = S.PASSENGER_ID
    LEFT JOIN AIRLINE_DWH.GOLD.DIM_AIRPORT A
      ON A.AIRPORT_NAME = S.AIRPORT_NAME AND A.AIRPORT_COUNTRY_CODE = S.AIRPORT_COUNTRY_CODE
    LEFT JOIN AIRLINE_DWH.GOLD.DIM_PILOT PIL ON PIL.PILOT_NAME = S.PILOT_NAME
    LEFT JOIN AIRLINE_DWH.GOLD.DIM_DATE D ON D.FLIGHT_DATE = S.DEPARTURE_DATE
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY S.PASSENGER_ID, S.DEPARTURE_DATE, S.ARRIVAL_AIRPORT ORDER BY S.LOADED_AT DESC
    ) = 1
  ) S
  ON T.PASSENGER_ID = S.PASSENGER_ID AND T.DEPARTURE_DATE = S.DEPARTURE_DATE AND T.ARRIVAL_AIRPORT = S.ARRIVAL_AIRPORT
  WHEN MATCHED THEN UPDATE SET
    PASSENGER_SK = S.PASSENGER_SK, AIRPORT_SK = S.AIRPORT_SK, PILOT_SK = S.PILOT_SK, DATE_SK = S.DATE_SK,
    COUNTRY_NAME = S.COUNTRY_NAME, FLIGHT_STATUS = S.FLIGHT_STATUS, TICKET_TYPE = S.TICKET_TYPE,
    PASSENGER_STATUS = S.PASSENGER_STATUS, SOURCE_FILE_NAME = S.SOURCE_FILE_NAME,
    LOADED_AT = S.LOADED_AT, UPDATED_AT = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (
    FACT_FLIGHT_SK, PASSENGER_SK, AIRPORT_SK, PILOT_SK, DATE_SK, PASSENGER_ID,
    DEPARTURE_DATE, ARRIVAL_AIRPORT, COUNTRY_NAME, FLIGHT_STATUS, TICKET_TYPE,
    PASSENGER_STATUS, SOURCE_FILE_NAME, LOADED_AT
  )
  VALUES (
    AIRLINE_DWH.GOLD.SEQ_FACT_SK.NEXTVAL, S.PASSENGER_SK, S.AIRPORT_SK, S.PILOT_SK, S.DATE_SK, S.PASSENGER_ID,
    S.DEPARTURE_DATE, S.ARRIVAL_AIRPORT, S.COUNTRY_NAME, S.FLIGHT_STATUS, S.TICKET_TYPE,
    S.PASSENGER_STATUS, S.SOURCE_FILE_NAME, S.LOADED_AT
  )
"""

STG_COLUMNS = (
    "PASSENGER_ID, FIRST_NAME, LAST_NAME, GENDER, AGE, AGE_GROUP, NATIONALITY, "
    "AIRPORT_NAME, AIRPORT_COUNTRY_CODE, COUNTRY_NAME, AIRPORT_CONTINENT, CONTINENTS, "
    "DEPARTURE_DATE, ARRIVAL_AIRPORT, PILOT_NAME, FLIGHT_STATUS, TICKET_TYPE, "
    "PASSENGER_STATUS, SOURCE_FILE_NAME, LOADED_AT, TRANSFORMED_AT"
)

BATCH = "AIRLINE_DWH.GOLD.TMP_SILVER_BATCH"
SOURCE = "AIRLINE_DWH.SILVER.STG_FLIGHTS_STREAM"
PROCEDURE = "AIRLINE_DWH.GOLD.SP_SILVER_TO_GOLD"


def rollback_quietly(session):
    try:
        session.sql("ROLLBACK").collect()
    except Exception:
        pass


def main(session, p_run_id):
    try:
        # DDL, outside the transaction (it would commit an open transaction).
        session.sql(
            f"CREATE OR REPLACE TRANSIENT TABLE {BATCH} LIKE AIRLINE_DWH.SILVER.STG_FLIGHTS"
        ).collect()

        session.sql("BEGIN TRANSACTION").collect()

        # DML: this is what consumes the silver stream. A rollback restores it.
        session.sql(
            f"INSERT INTO {BATCH} ({STG_COLUMNS}) SELECT {STG_COLUMNS} FROM {SOURCE}"
        ).collect()

        total = 0
        total += run_merge_and_log(session, p_run_id, DIM_PASSENGER_SQL, PROCEDURE,
                                   "AIRLINE_DWH.GOLD.DIM_PASSENGER", SOURCE,
                                   "Upserted passenger dimension rows.")
        total += run_merge_and_log(session, p_run_id, DIM_AIRPORT_SQL, PROCEDURE,
                                   "AIRLINE_DWH.GOLD.DIM_AIRPORT", SOURCE,
                                   "Upserted airport dimension rows.")
        total += run_merge_and_log(session, p_run_id, DIM_PILOT_SQL, PROCEDURE,
                                   "AIRLINE_DWH.GOLD.DIM_PILOT", SOURCE,
                                   "Upserted pilot dimension rows.")
        total += run_merge_and_log(session, p_run_id, DIM_DATE_SQL, PROCEDURE,
                                   "AIRLINE_DWH.GOLD.DIM_DATE", SOURCE,
                                   "Upserted date dimension rows.")
        total += run_merge_and_log(session, p_run_id, FACT_SQL, PROCEDURE,
                                   "AIRLINE_DWH.GOLD.FACT_FLIGHTS", SOURCE,
                                   "Built the fact table from dimension keys.")

        session.sql("COMMIT").collect()
        session.sql(f"DROP TABLE IF EXISTS {BATCH}").collect()
        return f"Merged silver into gold with {total} affected rows across dimensions and fact."
    except Exception as err:
        # The batch table is left in place after a failure, for inspection.
        rollback_quietly(session)
        log_audit(session, p_run_id, PROCEDURE, "AIRLINE_DWH.GOLD.FACT_FLIGHTS", SOURCE,
                  0, 0, 0, "FAILED", str(err))
        raise
$$
'''.replace("__UTIL_STAGE__", UTIL_STAGE_FQN)

# Earlier versions created these procedures with other argument lists. A new
# argument list creates an overload next to the old procedure instead of
# replacing it, so the old overloads are dropped before the new ones are created.
OBSOLETE_PROCEDURE_STATEMENTS = [
    "DROP PROCEDURE IF EXISTS AIRLINE_DWH.BRONZE.SP_LOAD_RAW_FROM_STAGE(STRING)",
    "DROP PROCEDURE IF EXISTS AIRLINE_DWH.SILVER.SP_RAW_TO_SILVER()",
    "DROP PROCEDURE IF EXISTS AIRLINE_DWH.GOLD.SP_SILVER_TO_GOLD()",
]
