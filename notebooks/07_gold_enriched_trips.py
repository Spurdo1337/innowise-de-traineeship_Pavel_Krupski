# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - `gold.enriched_trips`
# MAGIC Assigns every trip a synthetic `driver_id` (picked from the driver IDs in `silver.drivers`) and LEFT JOINs
# MAGIC the trips with the driver profiles.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %run ./_transforms

# COMMAND ----------

silver_taxi = spark.table(fqn(SILVER_SCHEMA, "taxi"))
silver_drivers = spark.table(fqn(SILVER_SCHEMA, "drivers"))

if silver_drivers.limit(1).count() == 0:
    raise RuntimeError("silver.drivers is empty - run the stream and silver notebooks first.")

enriched = build_enriched_trips(silver_taxi, silver_drivers)

TARGET = fqn(GOLD_SCHEMA, "enriched_trips")
(
    enriched.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET)
)

# COMMAND ----------

result = spark.table(TARGET)
total = result.count()
without_profile = result.filter("driver_name IS NULL").count()
print(f"{TARGET}: {total:,} rows (silver.taxi: {silver_taxi.count():,}); trips without a driver profile: {without_profile:,}")
assert total == silver_taxi.count(), "LEFT JOIN must not change the number of trips."
display(result.limit(10))
