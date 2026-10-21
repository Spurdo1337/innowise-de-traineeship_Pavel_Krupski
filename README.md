# Databricks medallion pipeline (bronze / silver / gold, DLT, Jobs)

Project: taxi trips (parquet) + users (avro) + a Faker-generated drivers stream are moved through
a medallion architecture on Databricks / Unity Catalog, refactored to DLT and orchestrated
with a Databricks Job.

## 1. Architecture

| Layer | Objects (catalog `workspace`) |
|---|---|
| raw_files | Managed Volume `landing`: `source/taxi`, `source/users`, `source/drivers_stream` (driver files for the bronze stream), `source/drivers` (JSON for DLT), `_checkpoints`, `_state`, `reports` |
| bronze | `taxi`, `users` (as-is from files), `drivers` (Faker stream, schema evolution, time travel, restore) |
| silver | `taxi`, `drivers`, `users` |
| gold | `taxi_metrics` (Liquid Clustering), `driver_metrics` (Z-Order), `user_metrics`, `enriched_trips` |
| DLT | Same logic in schemas `dlt_bronze`, `dlt_silver`, `dlt_gold` |

## 2. Repository layout

```
databricks.yml                    DAB bundle: name, variables (catalog, volume path), target dev
resources/
  schemas.yml                     7 schemas and the source Volume `landing`
  medallion_job.yml               the Job: 13 tasks and their dependencies
  medallion_pipeline.yml          the DLT pipeline (serverless)
deploy.bat                        validates, binds existing objects, deploys the bundle and uploads the source files
datasets/source/taxi/            tripdata.parquet
datasets/source/users/           users.avro
notebooks/
  _config.py                      shared constants (catalog, schemas, paths, driver pool size)
  _faker_helpers.py               Faker driver generator + registration_date UDF
  _transforms.py                  silver / enrichment logic (pure DataFrame functions)
  00_setup.py                     schemas, Volume, folders, source files check
  01_bronze_batch_ingest.py       parquet/avro -> bronze.taxi / bronze.users (as-is)
  02a_generate_driver_files.py    Faker -> driver JSON files, phase 1 (no phone) and phase 2 (with phone)
  02_bronze_drivers_stream.py     driver files -> bronze.drivers (AvailableNow, schema evolution in phase 2)
  03_time_travel_and_restore.py   VERSION AS OF + RESTORE
  04_generate_drivers_landing.py  raw JSON drivers for the DLT pipeline
  05_silver.py                    bronze -> silver
  06_gold_metrics.py              gold aggregates + OPTIMIZE (+ Z-Order)
  07_gold_enriched_trips.py       synthetic driver_id + LEFT JOIN
  08_report.py                    JSON report (row counts, task results)
pipelines/dlt_medallion.py       DLT pipeline source
```

The repository contains no workspace paths, user names or IDs. The bundle derives them at deploy time.

## 3. Prerequisites

* A Databricks workspace with Unity Catalog and a catalog named **`workspace`** (check in Catalog Explorer; if it
  does not exist, create it or change `CATALOG` in `notebooks/_config.py`).
* Source files: `tripdata.parquet` and `users.avro`.
* Compute: **serverless** 

## 4. Deployment step by step

1. **Authenticate the CLI** once per machine with the default profile:
   ```
   databricks auth login --host https://<workspace-url>
   ```
2. **Deploy from the project root:**
   ```
   .\deploy.bat
   ```
   The script checks the two files and runs `databricks bundle validate`. Then it binds the objects that already exist
   in the workspace with the project names (the 7 schemas, the Volume `landing`, the pipeline `medallion_dlt`, the job
   `medallion-pipeline`), so the deploy updates them instead of creating them again. For each bind the CLI shows the
   planned changes and asks `[y/N]`: answer `y` to confirm. Then it runs `databricks bundle deploy`, reads the Volume path
   from `databricks bundle summary` and copies `datasets\source` into the Volume as `source/taxi` and `source/users`.
3. **Run the job once** from *Workflows -> Jobs -> medallion-pipeline* (the only run; nothing is restarted by hand).
4. **Check the result** (section 5).
5. **Governance (manual, in the UI).** Create the user `innowise@innowise.com` and the group `innowise` in the account console,
   assign the group to the workspace and add the user to the group. Then run the grant, check it, run the revoke, check it:
   ```sql
   GRANT USE CATALOG ON CATALOG workspace TO `innowise`;
   GRANT USE SCHEMA ON SCHEMA workspace.gold TO `innowise`;
   GRANT SELECT ON SCHEMA workspace.gold TO `innowise`;
   SHOW GRANTS ON SCHEMA workspace.gold;      -- check: SELECT and USE SCHEMA present
   REVOKE SELECT ON SCHEMA workspace.gold FROM `innowise`;
   SHOW GRANTS ON SCHEMA workspace.gold;      -- check: only USE SCHEMA left
   ```

To run the notebooks manually instead of the job, use this order: `00` -> `01` -> `04` -> `02a` with `phase=1` ->
`02` with `phase=1` -> `02a` with `phase=2` -> `02` with `phase=2` -> `03` -> `05` -> `06` -> `07` -> run the DLT pipeline -> `08`.

## 5. How to verify

| Step | What to check |
|---|---|
| 00 | `SHOW SCHEMAS IN workspace` lists raw_files, bronze, silver, gold, dlt_*; Volume `landing` exists |
| 01 | `bronze.taxi` has 2,964,624 rows (for the provided file), `bronze.users` has 2 rows |
| 02a | Folder `source/drivers_stream` has 3 files `phase1_*.json` (no `phone`), then 3 files `phase2_*.json` (with `phone`) |
| 02 (phase 1) | Printed log shows `added: 6,000`; table has 6,000 rows and no `phone` |
| 02 (phase 2) | Printed log shows `added: 6,000`; table gets a `phone` column; `DESCRIBE HISTORY` shows the schema change; the checkpoint folder in the Volume is reused |
| 03 | Output shows old version without `phone` and current with `phone`; after `RESTORE` the assertions pass and history has a `RESTORE` entry |
| 05 | silver counts <= bronze counts; no `passenger_count/trip_distance/fare_amount <= 0`; `silver.drivers` has one row per `driver_id` |
| 06 | `DESCRIBE DETAIL workspace.gold.taxi_metrics` shows `clusteringColumns`; history of `driver_metrics` shows `OPTIMIZE` with Z-Order |
| 07 | `gold.enriched_trips` row count equals `silver.taxi`; no rows with NULL `driver_id` |
| DLT | Pipeline update succeeds; tables exist in `dlt_bronze/silver/gold`; expectations metrics in the pipeline UI show dropped records |
| 08 | `workflow_report_*.json` in Volume folder `reports` lists tables with row counts and task results |

## 6. Assumptions and limitations

**Data**
1. `users.avro` is the standard Apache Avro sample: **2 rows** (`name`, `favorite_color`, `favorite_numbers`). It has no registration date, so `gold.user_metrics` contains only 1-2 rows.
2. `tripdata.parquet` has no driver or trip id. Pickup and dropoff are `TIMESTAMP_NTZ`; bronze keeps them as-is. Bronze, silver and `gold.enriched_trips` set `delta.feature.timestampNtz = supported` in their table properties, because DLT cannot enable this table feature on an existing table.
3. **Faker generates everything missing from the files**: the driver id (a UUID per driver index), the driver profile (name, car number, phone) and `registration_date` (deterministic per name, 2020-01-01 .. 2024-01-31). The DLT version derives `registration_date` from a hash of the name instead, to keep pipeline dependencies minimal.
4. **Driver pool of 2,000** (`DRIVER_POOL_SIZE`): driver ids repeat, so deduplication is meaningful, and each driver gets about 1,500 trips. Each event is a fresh snapshot: name, car and phone are stable per driver id, while `experience` (0-30) and `rating` (0.5-5.0) are drawn per event. `experience = 0` and `rating < 1.0` are intentional bad data.
5. **Synthetic `driver_id` for trips**: each trip gets an id from `silver.drivers`, chosen by hashing the whole trip row. The choice is deterministic and every trip matches a profile; the join is still a `LEFT JOIN` as required.

**Schema and naming**
6. The stream schema has an extra `driver_id` column (needed for deduplication and the join). `phone` is added later by schema evolution.
7. Names differ from the task: the payment description column is `payment_description`, the count column in `user_metrics` is `users_count`, and the DLT pipeline writes to `dlt_bronze`, `dlt_silver`, `dlt_gold`, because pipeline-owned tables cannot be written by notebooks.

**Logic**
8. Duplicates are removed before `ingest_time` is added (otherwise no two rows would be equal). Silver drivers are filtered first (experience > 0, rating 1.0-5.0), then the latest record per `driver_id` by `timestamp` is kept.
9. Silver and gold notebook tables are fully overwritten on every run. Silver taxi deduplication compares whole rows, because the data has no natural key.
10. `OPTIMIZE ... ZORDER BY (experience)` runs on `gold.driver_metrics` as required, although the table is tiny. Liquid Clustering is used on `gold.taxi_metrics` instead, because Z-Order and Liquid Clustering cannot be combined on one table. The DLT version sets Z-Order with the table property `pipelines.autoOptimize.zOrderCols` and Liquid Clustering with `cluster_by`.

**Streaming**
11. The driver stream is **not continuous**. Serverless compute supports only `Trigger.AvailableNow`. Faker writes driver events as JSON files (3 files without `phone`, 3 with it, 2,000 events each). The stream reads them with `maxFilesPerTrigger = 1`, so each file is one micro-batch, and stops by itself. The checkpoint in the Volume remembers processed files. Phase 2 files are written only after phase 1 has finished, otherwise phase 1 would read them too.
12. Schema evolution: phase 1 reads the schema without `phone`, phase 2 reads the schema with `phone` from the same checkpoint and writes with `mergeSchema = true`. The schema is passed explicitly in each phase. Before phase 2, notebook 02 saves the table version without `phone` to `_state/drivers_pre_evolution_version.json`, which `VERSION AS OF` and `RESTORE` use. Phase 1 always starts from scratch (drops the table, the checkpoint, the saved version and old driver files), so the job can be repeated.

**DLT**
13. The pipeline uses the classic `import dlt` / `@dlt.table` API, as the task says; the newer `pyspark.pipelines` API is mentioned in a code comment. The bundle sets the pipeline `libraries` to `pipelines/dlt_medallion.py`.
14. DLT bronze drivers come from JSON files written by notebook 04, so they differ from the `bronze.drivers` stream. Every run of notebook 04 appends 6,000 events (24,000 after four runs); silver removes the duplicates.
15. `silver.taxi` (`ingest_time = current_timestamp()`, full-row deduplication) and `silver.users` are materialized views that are recomputed in full. During verification `dlt_gold.driver_metrics` and `dlt_gold.user_metrics` were empty after an incremental update and correct after a Full refresh of the pipeline; the cause was not confirmed. After changes to pipeline code, check the row counts and run Full refresh if they are empty.

**Operations**
16. The job uses no cluster. Its tasks use the `default` environment (client 1, `faker`), as in the successful run. The notebooks also run `%pip install faker`, so each notebook task installs it in its own session (six notebook task runs per job run).
17. Deployment uses Databricks Asset Bundles. `deploy.bat` binds the existing schemas, the Volume, the pipeline and the job (found by their project names) to the bundle before the deploy, so repeated runs and runs in a new workspace update the objects instead of failing with "already exists". Each bind asks for confirmation. The pipeline id is passed to the job by reference, so the repository holds no IDs.
18. Governance (user, group, grants) is done manually in the UI. Unity Catalog has no `READ` privilege on a schema; the equivalent is `USE CATALOG` + `USE SCHEMA` + `SELECT`.
