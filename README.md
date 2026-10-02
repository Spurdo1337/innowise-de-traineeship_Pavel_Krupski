# Airline Snowflake DWH + Airflow

This project demonstrates a small Snowflake-based data warehouse for airline passenger data, orchestrated by Airflow and deployed with Docker Compose.

## Layers

- Bronze: raw landing table loaded from a Snowflake internal stage.
- Silver: cleaned and deduplicated staging table.
- Gold: dimensional model with fact and dimension tables.
- Audit: procedure-level load logging.
- Security: secure view protected by a row access policy.

## Project structure

Folders are split by who runs what:

**Run by Airflow** (`dags/`)
- `dags/airline_bootstrap_dag.py` - creates Snowflake objects and stored procedures (split into several Airflow tasks: infrastructure, tables, streams, shared-module upload, security objects, procedures).
- `dags/airline_pipeline_dag.py` - uploads data and runs the ETL pipeline.
- `dags/snowflake_sql.py` - SQL/DDL statement groups and the three stored procedure definitions, imported by the bootstrap DAG.

**Runs inside Snowflake** (`snowflake/`, mounted into the Airflow containers and uploaded by the bootstrap DAG)
- `snowflake/python_procs/audit_utils.py` - shared Python module (uploaded to a stage and imported by all three stored procedures) with the common audit-logging / MERGE-row-count logic.

**Run by you, by hand, in a Snowsight worksheet** (`manual_checks/`, not mounted, not part of any DAG)
- `manual_checks/verify_load.sql` - post-run verification queries with expected values.
- `manual_checks/time_travel_examples.sql` - sample Snowflake Time Travel statements.

**Data, reports, infrastructure**
- `data/airline_dataset.csv` - source file for the demo.
- `powerBI_screenshots/` (project root, next to `dags/` and `data/`) - `flights_by_country.pbix`, the Power BI report (flight count by country, built on the Gold star schema), and `flights_by_country.PNG`, a screenshot of it.
- `docker-compose.yml` - Airflow stack.
- `Dockerfile` - custom Airflow image with Snowflake libraries.
- `requirements.txt` - pinned Python dependencies of the image.
- `.env.example` - template for the `.env` file with Snowflake and Airflow credentials.
- `.gitignore` - keeps `.env` and Airflow logs out of version control.

## How to run

1. Create a Snowflake trial account.
2. Copy `.env.example` to `.env` and fill in your Snowflake credentials. Change `AIRFLOW_ADMIN_PASSWORD` too - the placeholder is intentionally not a working default. (If your Snowflake password contains a literal `"` character, escape it as `\"` - it's substituted into a JSON string in `docker-compose.yml`.)
3. Start the stack:

```bash
docker compose up --build -d
```

4. Open Airflow and run the DAGs in this order:
   - `snowflake_bootstrap`
   - `airline_dwh_pipeline`

## Environment variables

The pipeline uses these values from `.env`:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_DATABASE`
- `AIRFLOW_UID`
- `AIRFLOW_ADMIN_USERNAME`
- `AIRFLOW_ADMIN_PASSWORD`
- `AIRFLOW_ADMIN_EMAIL`

## What the DAGs do

### `snowflake_bootstrap`
Six tasks, in dependency order: `create_infrastructure` (warehouse, database, schemas, file format, stage, sequences) → `create_tables` → [`create_streams`, `create_security_objects`] (row access policy, secure view, grants) and, in parallel with those two, `upload_shared_python_module` (uploads `snowflake/python_procs/audit_utils.py` to a stage) → `create_procedures` (the three Bronze/Silver/Gold stored procedures, which import that shared module).

### `airline_dwh_pipeline`
Uploads the CSV file to the internal stage, runs the bronze load procedure, then runs the silver and gold transformation procedures, and finally validates the result: it fails the task (and the DAG run) if Bronze/Silver/Gold/dimension tables are empty, if `GOLD.FACT_FLIGHTS` row count doesn't match `SILVER.STG_FLIGHTS`, or if any row in `GOLD.FACT_FLIGHTS` has a NULL foreign key.

## Verifying the load

`manual_checks/verify_load.sql` has nine numbered checks (row counts per layer, Bronze duplicates, audit log, fact-to-dimension integrity, streams, secure view, RLS as `PUBLIC`, `COPY_HISTORY`) with the expected values for one clean run: Bronze/Silver/Fact 98619 rows, `DIM_PASSENGER` 98619, `DIM_AIRPORT` 9107, `DIM_PILOT` 98610, `DIM_DATE` 364, 7 audit rows.

## Testing row-level security

By default you connect as `ACCOUNTADMIN` (see `.env.example`), and the
policy always lets `ACCOUNTADMIN` see every row — so nothing looks
filtered. To see the policy actually filter rows, run this in a Snowflake
worksheet after the pipeline has loaded data:

```sql
USE ROLE PUBLIC;
USE WAREHOUSE AIRLINE_DWH_WH;
SELECT COUNTRY_NAME, COUNT(*)
FROM AIRLINE_DWH.ANALYTICS.SECURE_FACT_FLIGHTS_V
GROUP BY COUNTRY_NAME
ORDER BY COUNTRY_NAME;
```

You should only see `United States`, `Canada`, and `United Kingdom` (the
countries listed for `PUBLIC` in `SECURITY.ALLOWED_COUNTRY_ROLES`). Switch
back with `USE ROLE ACCOUNTADMIN;` to see every country again.

## BI reporting

Power BI Desktop is connected directly to Snowflake with the native Snowflake connector (Get Data → Database → Snowflake), reading straight from the Gold star schema rather than through the secure view, so Power BI resolves the fact/dimension relationships itself.

Connection settings used:
- **Server**: `<account_locator>.snowflakecomputing.com` - use the full account identifier from `SNOWFLAKE_ACCOUNT` (including the organization prefix, e.g. `ORG-ACCOUNT.snowflakecomputing.com`), not just the account name.
- **Warehouse**: `AIRLINE_DWH_WH`
- **Role** (Advanced options): `ACCOUNTADMIN`
- **Authentication**: Database (username/password from `.env`)
- **Data connectivity mode**: Import

Tables imported: `GOLD.FACT_FLIGHTS`, `GOLD.DIM_PASSENGER`, `GOLD.DIM_AIRPORT`, `GOLD.DIM_PILOT`, `GOLD.DIM_DATE`. Relationships between them are built (or auto-detected) on the `*_SK` surrogate keys, reproducing the star schema in Power BI's Model view.

Report: `PowerBI_screenshots/flights_by_country.pbix` - a bar chart of flight count by country (`DIM_AIRPORT.COUNTRY_NAME` vs. count of `FACT_FLIGHTS` rows). Screenshot: `PowerBI_screenshots/flights_by_country.PNG`.

## Notes

- The secure view is built on `GOLD.FACT_FLIGHTS`.
- The row access policy filters rows by country, using `SECURITY.ALLOWED_COUNTRY_ROLES` as a role → allowed-country mapping table.
- The three stored procedures are Python, and share one implementation of the audit-logging/MERGE-row-count logic via `snowflake/python_procs/audit_utils.py` (imported from a stage) instead of each re-implementing its own copy.
- `manual_checks/time_travel_examples.sql` contains two DDL and two DML examples that use Snowflake Time Travel. It is self-contained (captures its own "before" timestamps) and self-cleaning (restores `FACT_FLIGHT_SK` 1-15 in `GOLD.FACT_FLIGHTS` to their pre-script values at the end), so it works, and leaves no residue, regardless of when or how many times you run it.
