# Databricks notebook source
# MAGIC %md
# MAGIC # `_config` - shared constants (no side effects)
# MAGIC Included from other notebooks with `%run ./_config`. Change names here only.

# COMMAND ----------

CATALOG = "workspace"

# Medallion schemas required by the task.
RAW_SCHEMA = "raw_files"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Extra schemas used only by the DLT pipeline (a DLT-managed table cannot be written
# by notebooks and vice versa, so the pipeline gets its own schemas).
DLT_BRONZE_SCHEMA = "dlt_bronze"
DLT_SILVER_SCHEMA = "dlt_silver"
DLT_GOLD_SCHEMA = "dlt_gold"

ALL_SCHEMAS = [
    RAW_SCHEMA, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA,
    DLT_BRONZE_SCHEMA, DLT_SILVER_SCHEMA, DLT_GOLD_SCHEMA,
]

# Unity Catalog managed volume that holds source files, checkpoints, state and reports.
VOLUME_NAME = "landing"
VOLUME_PATH = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{VOLUME_NAME}"

SOURCE_TAXI_DIR = f"{VOLUME_PATH}/source/taxi"          # tripdata.parquet goes here
SOURCE_USERS_DIR = f"{VOLUME_PATH}/source/users"        # users.avro goes here
DRIVERS_LANDING_DIR = f"{VOLUME_PATH}/source/drivers"   # JSON files for the DLT pipeline
# Driver files read by the bronze.drivers stream (separate from DLT landing, see README "Streaming").
STREAM_DRIVERS_DIR = f"{VOLUME_PATH}/source/drivers_stream"
# Phase 1 (no phone) and phase 2 (with phone): this many files per phase, this many events per file.
DRIVERS_FILES_PER_PHASE = 3
DRIVERS_ROWS_PER_FILE = 2000
CHECKPOINT_DRIVERS = f"{VOLUME_PATH}/_checkpoints/bronze_drivers"
STATE_DIR = f"{VOLUME_PATH}/_state"
REPORT_DIR = f"{VOLUME_PATH}/reports"

# Number of distinct synthetic drivers (see README, "Assumptions").
DRIVER_POOL_SIZE = 2000


def fqn(schema: str, table: str) -> str:
    """Return the fully qualified Unity Catalog name `catalog.schema.table`."""
    return f"{CATALOG}.{schema}.{table}"
