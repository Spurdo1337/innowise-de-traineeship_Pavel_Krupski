-- Post-run verification for the airline DWH. Run each numbered block
-- separately (select it, then Ctrl+Enter): Snowsight shows the result of
-- one statement at a time. Expected values below were computed from
-- data/airline_dataset.csv and assume ONE clean run of snowflake_bootstrap
-- followed by ONE run of airline_dwh_pipeline.

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE AIRLINE_DWH_WH;

-- 1. Row counts per layer (every "ok" must be TRUE)
SELECT object_name, actual, expected, actual = expected AS ok
FROM (
  SELECT 'BRONZE.RAW_FLIGHTS' AS object_name, COUNT(*) AS actual, 98619 AS expected FROM AIRLINE_DWH.BRONZE.RAW_FLIGHTS
  UNION ALL SELECT 'SILVER.STG_FLIGHTS',  COUNT(*), 98619 FROM AIRLINE_DWH.SILVER.STG_FLIGHTS
  UNION ALL SELECT 'GOLD.FACT_FLIGHTS',   COUNT(*), 98619 FROM AIRLINE_DWH.GOLD.FACT_FLIGHTS
  UNION ALL SELECT 'GOLD.DIM_PASSENGER',  COUNT(*), 98619 FROM AIRLINE_DWH.GOLD.DIM_PASSENGER
  UNION ALL SELECT 'GOLD.DIM_AIRPORT',    COUNT(*), 9107  FROM AIRLINE_DWH.GOLD.DIM_AIRPORT
  UNION ALL SELECT 'GOLD.DIM_PILOT',      COUNT(*), 98610 FROM AIRLINE_DWH.GOLD.DIM_PILOT
  UNION ALL SELECT 'GOLD.DIM_DATE',       COUNT(*), 364   FROM AIRLINE_DWH.GOLD.DIM_DATE
)
ORDER BY object_name;

-- 2. Was Bronze loaded more than once? Expect duplicate_rows = 0 and load_runs = 1
--    (PASSENGER_ID is unique in the source file).
SELECT COUNT(*) AS bronze_rows,
       COUNT(DISTINCT PASSENGER_ID) AS distinct_passengers,
       COUNT(*) - COUNT(DISTINCT PASSENGER_ID) AS duplicate_rows,
       COUNT(DISTINCT INGESTION_RUN_ID) AS load_runs
FROM AIRLINE_DWH.BRONZE.RAW_FLIGHTS;

-- 3. Audit log. After one clean run: 7 rows, all SUCCESS.
--    Bronze 98619 inserted, Silver 98619 inserted, Gold: DIM_PASSENGER 98619,
--    DIM_AIRPORT 9107, DIM_PILOT 98610, DIM_DATE 364, FACT_FLIGHTS 98619.
SELECT AUDIT_ID, PROCEDURE_NAME, TARGET_OBJECT, STATUS,
       ROWS_INSERTED, ROWS_UPDATED, ROWS_AFFECTED, LEFT(DETAILS, 80) AS details, EXECUTED_AT
FROM AIRLINE_DWH.AUDIT.ETL_AUDIT_LOG
ORDER BY AUDIT_ID;

-- 4. Fact rows whose dimension key is missing or points nowhere. Expect 0.
SELECT COUNT(*) AS broken_fact_rows
FROM AIRLINE_DWH.GOLD.FACT_FLIGHTS f
LEFT JOIN AIRLINE_DWH.GOLD.DIM_PASSENGER p   ON f.PASSENGER_SK = p.PASSENGER_SK
LEFT JOIN AIRLINE_DWH.GOLD.DIM_AIRPORT   a   ON f.AIRPORT_SK   = a.AIRPORT_SK
LEFT JOIN AIRLINE_DWH.GOLD.DIM_PILOT     pil ON f.PILOT_SK     = pil.PILOT_SK
LEFT JOIN AIRLINE_DWH.GOLD.DIM_DATE      d   ON f.DATE_SK      = d.DATE_SK
WHERE p.PASSENGER_SK IS NULL OR a.AIRPORT_SK IS NULL
   OR pil.PILOT_SK IS NULL OR d.DATE_SK IS NULL;

-- 5. Streams are consumed after a successful run: both must be FALSE.
SELECT SYSTEM$STREAM_HAS_DATA('AIRLINE_DWH.BRONZE.RAW_FLIGHTS_STREAM') AS bronze_stream_has_data,
       SYSTEM$STREAM_HAS_DATA('AIRLINE_DWH.SILVER.STG_FLIGHTS_STREAM')  AS silver_stream_has_data;

-- 6. The gold snapshot table must be gone after a successful run: expect 0 rows.
SHOW TABLES LIKE 'TMP_SILVER_BATCH' IN SCHEMA AIRLINE_DWH.GOLD;

-- 7. The view is secure: is_secure = true.
SHOW VIEWS LIKE 'SECURE_FACT_FLIGHTS_V' IN SCHEMA AIRLINE_DWH.ANALYTICS;

-- 8. Row-level security: as AIRLINE_BI only three countries are visible.
--    Expected: Canada 5424, United Kingdom 1371, United States 22104.
--    Run the three statements together, the last one switches the role back.
USE ROLE AIRLINE_BI;
USE WAREHOUSE AIRLINE_DWH_WH;
SELECT COUNTRY_NAME, COUNT(*) AS flights
FROM AIRLINE_DWH.ANALYTICS.SECURE_FACT_FLIGHTS_V
GROUP BY COUNTRY_NAME
ORDER BY COUNTRY_NAME;
-- then run:  USE ROLE ACCOUNTADMIN;

-- 9. Snowflake's own view of what COPY INTO loaded into Bronze (last 24 h).
USE ROLE ACCOUNTADMIN;
USE SCHEMA AIRLINE_DWH.BRONZE;
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'RAW_FLIGHTS',
  START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
));
