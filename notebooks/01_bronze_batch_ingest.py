# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze batch ingestion (taxi trips + users)
# MAGIC Reads the files from the Volume and writes them **as-is** (no transformations) to Delta tables.
# MAGIC Full overwrite, so the notebook is idempotent.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

def ingest_as_is(read_format: str, source_dir: str, glob: str, target: str) -> None:
    """Read all files matching `glob` in `source_dir` and overwrite the Delta table `target`."""
    df = spark.read.format(read_format).option("pathGlobFilter", glob).load(source_dir)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target)
    )
    print(f"{target}: {spark.table(target).count():,} rows")


ingest_as_is("parquet", SOURCE_TAXI_DIR, "*.parquet", fqn(BRONZE_SCHEMA, "taxi"))
ingest_as_is("avro", SOURCE_USERS_DIR, "*.avro", fqn(BRONZE_SCHEMA, "users"))
