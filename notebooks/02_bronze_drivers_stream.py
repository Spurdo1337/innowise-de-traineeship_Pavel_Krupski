# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Driver file stream -> `bronze.drivers` (schema evolution)
# MAGIC Reads the JSON files written by notebook `02a` with Structured Streaming and appends them to the Delta table
# MAGIC `bronze.drivers`.
# MAGIC
# MAGIC * `phase=1` - starts from scratch (drops the table and the checkpoint), reads the schema **without** `phone`.
# MAGIC * `phase=2` - uses the **same checkpoint**, reads the schema **with** `phone`, and writes with
# MAGIC   `mergeSchema` so the table accepts the new column.
# MAGIC
# MAGIC `Trigger.AvailableNow` with `maxFilesPerTrigger=1` processes the files that exist at the start of the run,
# MAGIC one file per micro-batch, then stops by itself. It is the only time-free trigger supported on serverless compute.

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %run ./_faker_helpers

# COMMAND ----------

import json
import os

from delta.tables import DeltaTable

dbutils.widgets.dropdown("phase", "1", ["1", "2"], "Phase (1 = no phone, 2 = with phone)")
PHASE = int(dbutils.widgets.get("phase"))

TARGET = fqn(BRONZE_SCHEMA, "drivers")
VERSION_FILE = f"{STATE_DIR}/drivers_pre_evolution_version.json"
SCHEMA_DDL = DRIVER_DDL_BASE if PHASE == 1 else DRIVER_DDL_WITH_PHONE

# COMMAND ----------

if PHASE == 1:
    spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
    dbutils.fs.rm(CHECKPOINT_DRIVERS, True)
    if os.path.exists(VERSION_FILE):
        os.remove(VERSION_FILE)
    print("Phase 1: table, checkpoint and saved version were reset.")
else:
    assert spark.catalog.tableExists(TARGET), "Phase 2 needs the table from phase 1. Run phase 1 first."
    assert "phone" not in spark.table(TARGET).columns, "Table already has phone - phase 1 must start from scratch."
    # The last version before phase 2 is the state without `phone`; notebook 03 restores it.
    version_before = DeltaTable.forName(spark, TARGET).history(1).collect()[0]["version"]
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(VERSION_FILE, "w") as f:
        json.dump({"table": TARGET, "version_before_phone": int(version_before)}, f)
    print(f"Saved version before schema evolution: {version_before}")

# COMMAND ----------

files = [f.name for f in dbutils.fs.ls(STREAM_DRIVERS_DIR) if f.name.startswith(f"phase{PHASE}_")]
assert len(files) == DRIVERS_FILES_PER_PHASE, f"Expected {DRIVERS_FILES_PER_PHASE} files for phase {PHASE}, found {files}"

rows_before = spark.table(TARGET).count() if spark.catalog.tableExists(TARGET) else 0

source = (
    spark.readStream.format("json")
    .schema(SCHEMA_DDL)
    .option("pathGlobFilter", "*.json")
    .option("maxFilesPerTrigger", 1)          # one file = one micro-batch
    .load(STREAM_DRIVERS_DIR)
)

writer = (
    source.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_DRIVERS)
    .trigger(availableNow=True)
)
if PHASE == 2:
    writer = writer.option("mergeSchema", "true")   # schema evolution: accept the new column

query = writer.toTable(TARGET)
query.awaitTermination()                             # AvailableNow stops by itself
if query.exception() is not None:
    raise RuntimeError(f"Streaming query failed: {query.exception()}")

# COMMAND ----------

# Verify the ingestion by row count.
rows_after = spark.table(TARGET).count()
added = rows_after - rows_before
print(f"rows before: {rows_before:,}; after: {rows_after:,}; added: {added:,}")
assert added == DRIVERS_FILES_PER_PHASE * DRIVERS_ROWS_PER_FILE, "Row count does not match the generated files."
print("Columns:", spark.table(TARGET).columns)
display(spark.sql(f"DESCRIBE HISTORY {TARGET}").select("version", "timestamp", "operation", "operationParameters"))
