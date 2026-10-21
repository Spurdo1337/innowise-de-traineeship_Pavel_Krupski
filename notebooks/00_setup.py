# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: schemas, Unity Catalog Volume, source files check
# MAGIC Idempotent: safe to run repeatedly.
# MAGIC
# MAGIC **Order of work the first time:** run this notebook once (it creates the Volume and the folders),
# MAGIC upload `tripdata.parquet` to `.../source/taxi/` and `users.avro` to `.../source/users/`,
# MAGIC then run the whole Job. The last cell fails with a clear message when a file is missing.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# 1. The catalog must already exist (this notebook does not create it on purpose).
catalogs = [row[0] for row in spark.sql("SHOW CATALOGS").collect()]
if CATALOG not in catalogs:
    raise RuntimeError(
        f"Catalog '{CATALOG}' not found. Available catalogs: {catalogs}. "
        "Create it in Catalog Explorer or change CATALOG in the _config notebook."
    )

# 2. Schemas (the four required by the task + three used by the DLT pipeline).
for schema in ALL_SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

# 3. Managed volume inside raw_files.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{RAW_SCHEMA}.{VOLUME_NAME}")

# 4. Folder layout inside the volume.
for path in (SOURCE_TAXI_DIR, SOURCE_USERS_DIR, DRIVERS_LANDING_DIR, STREAM_DRIVERS_DIR, STATE_DIR, REPORT_DIR):
    dbutils.fs.mkdirs(path)

print(f"Volume ready: {VOLUME_PATH}")

# COMMAND ----------

# 5. Fail fast if the mock source files were not uploaded yet.
expected = {SOURCE_TAXI_DIR: ".parquet", SOURCE_USERS_DIR: ".avro"}
missing = [
    f"{folder}/*{ext}"
    for folder, ext in expected.items()
    if not any(f.name.endswith(ext) for f in dbutils.fs.ls(folder))
]
if missing:
    raise FileNotFoundError(
        "Upload the source files to the Volume (Catalog Explorer -> workspace -> raw_files -> landing). "
        f"Missing: {missing}"
    )
print("Source files found.")
