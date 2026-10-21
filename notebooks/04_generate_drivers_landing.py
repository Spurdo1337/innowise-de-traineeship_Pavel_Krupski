# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Generate raw drivers JSON files for the DLT pipeline
# MAGIC The DLT pipeline reads *raw files* from the Volume with Auto Loader. This notebook writes a batch of Faker
# MAGIC driver events (final schema, including `phone`) as JSON files to the landing folder.
# MAGIC Every run **appends** a new batch (new files), like a real landing zone; duplicates are removed in silver.

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %run ./_faker_helpers

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("num_events", "6000", "Number of driver events to generate")
NUM_EVENTS = int(dbutils.widgets.get("num_events"))

# Spread event timestamps over the last hour so that "latest record per driver" is meaningful.
base = spark.range(NUM_EVENTS).select(
    (F.unix_timestamp(F.current_timestamp()) - (F.rand() * 3600).cast("long")).cast("timestamp").alias("timestamp")
)
events = generate_driver_events(base, include_phone=True, pool_size=DRIVER_POOL_SIZE)

events.coalesce(1).write.mode("append").json(DRIVERS_LANDING_DIR)

landed = [f.name for f in dbutils.fs.ls(DRIVERS_LANDING_DIR) if f.name.endswith(".json")]
print(f"{NUM_EVENTS:,} events written. JSON files now in {DRIVERS_LANDING_DIR}: {len(landed)}")
