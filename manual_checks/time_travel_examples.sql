-- Time Travel examples for Snowflake.
-- Run this whole script in one go (select everything and "Run All" /
-- Ctrl+A then Ctrl+Enter in Snowsight), after the pipeline has already
-- loaded data into AIRLINE_DWH.GOLD.FACT_FLIGHTS.
--
-- If you tested row-level security earlier in this same worksheet with
-- "USE ROLE AIRLINE_BI;", that role stays active for the rest of the session
-- until you switch it back - the line below makes sure this script always
-- runs with a role that actually has rights to create objects in
-- AIRLINE_DWH.ANALYTICS, regardless of what ran before it.
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE AIRLINE_DWH_WH;

-- Captured so the cleanup step at the end of this script can restore
-- FACT_FLIGHT_SK 1-10 to whatever they were before this script ran (see
-- the "Cleanup" section at the bottom) - otherwise every run of this demo
-- permanently leaves 'DDL1_DEMO_CHANGE' / 'DML1_DEMO_CHANGE' sitting in
-- GOLD.FACT_FLIGHTS, since a normal pipeline re-run won't touch them
-- again (bronze load is idempotent and won't reprocess the same file).
SET TS_SCRIPT_START = (SELECT CURRENT_TIMESTAMP());

-- Each section below captures its own "before" timestamp with SET right
-- before making a change, then travels back to that captured moment. The
-- SET statements create session variables ($TS_BEFORE_...) that later
-- statements in this same script depend on - so if you run statements one
-- at a time instead of the whole script together, make sure you run each
-- SET line first, or the AT (TIMESTAMP => $VAR) references below it will
-- fail with "variable not found".
--
-- Each variable stores the actual CURRENT_TIMESTAMP() value itself
-- (a TIMESTAMP_LTZ), not a string built from it. A TIMESTAMP_LTZ is an
-- unambiguous absolute instant - it carries its own UTC offset internally,
-- so it's used directly in the AT clause below with no further casting.
-- (An earlier version of this script built a STRING from the timestamp and
-- cast that string to TIMESTAMP_NTZ in the AT clause; casting a timestamp
-- string to TIMESTAMP_NTZ drops whatever offset is in the string and takes
-- the wall-clock digits as-is, which silently shifted the captured "before"
-- moment by the session's UTC offset - enough, in a non-UTC session, to
-- land before the table's creation time or outside the retention window.
-- Storing the TIMESTAMP_LTZ value directly avoids that string round-trip
-- entirely.)

-- ============================================================
-- DDL 1: Clone a table as it looked right before a change.
-- ============================================================
SET TS_BEFORE_DDL1 = (SELECT CURRENT_TIMESTAMP());

-- Make a small, deliberate change to a few rows so there is something
-- meaningful to see the "before" version of.
UPDATE AIRLINE_DWH.GOLD.FACT_FLIGHTS
SET PASSENGER_STATUS = 'DDL1_DEMO_CHANGE'
WHERE FACT_FLIGHT_SK BETWEEN 1 AND 5;

-- Clone the table as it looked at the captured timestamp (i.e. before the
-- UPDATE above). Compare FACT_FLIGHTS_CLONE vs FACT_FLIGHTS for
-- FACT_FLIGHT_SK 1-5 to see the difference.
CREATE OR REPLACE TABLE AIRLINE_DWH.ANALYTICS.FACT_FLIGHTS_CLONE
CLONE AIRLINE_DWH.GOLD.FACT_FLIGHTS
AT (TIMESTAMP => $TS_BEFORE_DDL1);

-- ============================================================
-- DDL 2: Clone a whole schema as it looked before a change.
-- ============================================================
SET TS_BEFORE_DDL2 = (SELECT CURRENT_TIMESTAMP());

-- A schema-level change: add a scratch table to ANALYTICS.
CREATE OR REPLACE TABLE AIRLINE_DWH.ANALYTICS.SCRATCH_MARKER (NOTE STRING);

-- Snapshot the ANALYTICS schema as it looked before SCRATCH_MARKER existed.
-- (Check: SHOW TABLES IN SCHEMA AIRLINE_DWH.ANALYTICS_BACKUP; should not
-- list SCRATCH_MARKER, while AIRLINE_DWH.ANALYTICS does.)
CREATE OR REPLACE SCHEMA AIRLINE_DWH.ANALYTICS_BACKUP
CLONE AIRLINE_DWH.ANALYTICS
AT (TIMESTAMP => $TS_BEFORE_DDL2);

-- ============================================================
-- DML 1: Copy a historical (pre-change) sample into a recovery table.
-- ============================================================
SET TS_BEFORE_DML1 = (SELECT CURRENT_TIMESTAMP());

UPDATE AIRLINE_DWH.GOLD.FACT_FLIGHTS
SET PASSENGER_STATUS = 'DML1_DEMO_CHANGE'
WHERE FACT_FLIGHT_SK BETWEEN 6 AND 10;

CREATE OR REPLACE TABLE AIRLINE_DWH.ANALYTICS.FACT_FLIGHTS_RECOVERY LIKE AIRLINE_DWH.GOLD.FACT_FLIGHTS;

INSERT INTO AIRLINE_DWH.ANALYTICS.FACT_FLIGHTS_RECOVERY
SELECT *
FROM AIRLINE_DWH.GOLD.FACT_FLIGHTS
AT (TIMESTAMP => $TS_BEFORE_DML1)
WHERE FACT_FLIGHT_SK BETWEEN 6 AND 10;

-- ============================================================
-- DML 2: Restore corrupted values straight from a historical snapshot.
-- ============================================================
SET TS_BEFORE_DML2 = (SELECT CURRENT_TIMESTAMP());

UPDATE AIRLINE_DWH.GOLD.FACT_FLIGHTS
SET PASSENGER_STATUS = 'CORRUPTED_BY_MISTAKE'
WHERE FACT_FLIGHT_SK BETWEEN 11 AND 15;

-- "Someone" broke PASSENGER_STATUS above. Fix it by pulling the value back
-- from Time Travel, using the timestamp captured right before the mistake.
UPDATE AIRLINE_DWH.GOLD.FACT_FLIGHTS
SET PASSENGER_STATUS = HIST.PASSENGER_STATUS
FROM (
  SELECT FACT_FLIGHT_SK, PASSENGER_STATUS
  FROM AIRLINE_DWH.GOLD.FACT_FLIGHTS
  AT (TIMESTAMP => $TS_BEFORE_DML2)
  WHERE FACT_FLIGHT_SK BETWEEN 11 AND 15
) AS HIST
WHERE AIRLINE_DWH.GOLD.FACT_FLIGHTS.FACT_FLIGHT_SK = HIST.FACT_FLIGHT_SK;

-- Sanity check: PASSENGER_STATUS for FACT_FLIGHT_SK 11-15 should be back to
-- whatever it was before this script ran (NOT 'CORRUPTED_BY_MISTAKE').
SELECT FACT_FLIGHT_SK, PASSENGER_STATUS
FROM AIRLINE_DWH.GOLD.FACT_FLIGHTS
WHERE FACT_FLIGHT_SK BETWEEN 11 AND 15
ORDER BY FACT_FLIGHT_SK;

-- ============================================================
-- Cleanup: restore FACT_FLIGHT_SK 1-10 to their pre-script values, so
-- running this demo doesn't leave permanent 'DDL1_DEMO_CHANGE' /
-- 'DML1_DEMO_CHANGE' residue in GOLD.FACT_FLIGHTS. FACT_FLIGHT_SK 11-15
-- are already restored by DML 2 above.
-- ============================================================
UPDATE AIRLINE_DWH.GOLD.FACT_FLIGHTS
SET PASSENGER_STATUS = HIST.PASSENGER_STATUS
FROM (
  SELECT FACT_FLIGHT_SK, PASSENGER_STATUS
  FROM AIRLINE_DWH.GOLD.FACT_FLIGHTS
  AT (TIMESTAMP => $TS_SCRIPT_START)
  WHERE FACT_FLIGHT_SK BETWEEN 1 AND 10
) AS HIST
WHERE AIRLINE_DWH.GOLD.FACT_FLIGHTS.FACT_FLIGHT_SK = HIST.FACT_FLIGHT_SK;

-- Final check: none of FACT_FLIGHT_SK 1-15 should show a *_DEMO_CHANGE or
-- CORRUPTED_BY_MISTAKE value anymore.
SELECT FACT_FLIGHT_SK, PASSENGER_STATUS
FROM AIRLINE_DWH.GOLD.FACT_FLIGHTS
WHERE FACT_FLIGHT_SK BETWEEN 1 AND 15
ORDER BY FACT_FLIGHT_SK;
