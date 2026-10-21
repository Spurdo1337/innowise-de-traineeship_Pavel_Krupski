# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Silver layer (cleansing)
# MAGIC Reads the bronze tables, applies the transformations from `_transforms` and overwrites the silver tables.

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %run ./_faker_helpers

# COMMAND ----------

# MAGIC %run ./_transforms

# COMMAND ----------

def write_silver(df, table: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )


def run(name: str, builder) -> None:
    bronze_table, silver_table = fqn(BRONZE_SCHEMA, name), fqn(SILVER_SCHEMA, name)
    bronze = spark.table(bronze_table)
    write_silver(builder(bronze), silver_table)
    print(f"{bronze_table}: {bronze.count():,} rows -> {silver_table}: {spark.table(silver_table).count():,} rows")


run("taxi", build_silver_taxi)
run("drivers", build_silver_drivers)
run("users", build_silver_users)
