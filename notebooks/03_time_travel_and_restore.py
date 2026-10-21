# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Time travel and RESTORE of `bronze.drivers`
# MAGIC Shows the table as it was before the `phone` column was added (`VERSION AS OF`) and then rolls
# MAGIC the table back to that version with `RESTORE`. The version number is saved by notebook 02.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import json
import os

TARGET = fqn(BRONZE_SCHEMA, "drivers")
VERSION_FILE = f"{STATE_DIR}/drivers_pre_evolution_version.json"

if not os.path.exists(VERSION_FILE):
    raise FileNotFoundError(
        f"{VERSION_FILE} not found. Run notebook 02 with include_phone=false and then include_phone=true first."
    )
with open(VERSION_FILE) as f:
    VERSION = json.load(f)["version_before_phone"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table history (find the versions)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {TARGET}").select("version", "timestamp", "operation", "operationParameters"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The table exactly as it was before `phone` was added

# COMMAND ----------

old = spark.sql(f"SELECT * FROM {TARGET} VERSION AS OF {VERSION}")
current = spark.table(TARGET)
print(f"VERSION AS OF {VERSION}: {old.count():,} rows, columns = {old.columns}")
print(f"CURRENT:               {current.count():,} rows, columns = {current.columns}")
assert "phone" not in old.columns, "Version is not 'before phone' - check the saved version."
assert "phone" in current.columns, "Current table has no phone column - nothing to roll back."
display(old.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### RESTORE: permanently roll the table back

# COMMAND ----------

expected_rows = old.count()
spark.sql(f"RESTORE TABLE {TARGET} TO VERSION AS OF {VERSION}")

restored = spark.table(TARGET)
print(f"After RESTORE: {restored.count():,} rows, columns = {restored.columns}")
assert restored.count() == expected_rows
assert "phone" not in restored.columns
display(spark.sql(f"DESCRIBE HISTORY {TARGET}").select("version", "timestamp", "operation", "operationParameters").limit(3))
