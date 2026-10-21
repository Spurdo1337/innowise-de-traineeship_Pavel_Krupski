# Databricks notebook source
# MAGIC %md
# MAGIC # 02a - Generate driver event files for the `bronze.drivers` stream
# MAGIC Faker writes JSON files (one JSON object per line) into `source/drivers_stream`:
# MAGIC * `phase=1` - clears the folder and writes files **without** `phone`
# MAGIC * `phase=2` - adds files **with** `phone` (read by the second stream run, which evolves the schema)
# MAGIC
# MAGIC Run order inside the Job: `02a phase=1` -> `02 phase=1` -> `02a phase=2` -> `02 phase=2`.
# MAGIC Phase 2 files must not exist during the phase 1 stream run, because the checkpoint would
# MAGIC otherwise process them in run 1.

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %run ./_faker_helpers

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.dropdown("phase", "1", ["1", "2"], "Phase (1 = no phone, 2 = with phone)")
PHASE = int(dbutils.widgets.get("phase"))
INCLUDE_PHONE = PHASE == 2

# COMMAND ----------

if PHASE == 1:
    # A fresh start: run 1 must see only phase-1 files.
    dbutils.fs.rm(STREAM_DRIVERS_DIR, True)
dbutils.fs.mkdirs(STREAM_DRIVERS_DIR)

# COMMAND ----------

def write_driver_file(index: int) -> str:
    """Write one JSON-lines file with DRIVERS_ROWS_PER_FILE Faker driver events; return its path."""
    tmp_dir = f"{VOLUME_PATH}/_tmp/drivers_phase{PHASE}_{index}"
    base = spark.range(DRIVERS_ROWS_PER_FILE).select(
        (F.unix_timestamp(F.current_timestamp()) - (F.rand() * 3600).cast("long")).cast("timestamp").alias("timestamp")
    )
    events = generate_driver_events(base, include_phone=INCLUDE_PHONE, pool_size=DRIVER_POOL_SIZE)

    # Spark writes a folder; move its single part file to a predictable name, then remove the folder.
    events.coalesce(1).write.mode("overwrite").json(tmp_dir)
    part = [f.path for f in dbutils.fs.ls(tmp_dir) if f.name.startswith("part-")][0]
    target = f"{STREAM_DRIVERS_DIR}/phase{PHASE}_{index:03d}.json"
    dbutils.fs.mv(part, target)
    dbutils.fs.rm(tmp_dir, True)
    return target


written = [write_driver_file(i) for i in range(DRIVERS_FILES_PER_PHASE)]
print(f"Phase {PHASE}: wrote {len(written)} files x {DRIVERS_ROWS_PER_FILE} events, include_phone={INCLUDE_PHONE}")
for path in written:
    print(" ", path)
