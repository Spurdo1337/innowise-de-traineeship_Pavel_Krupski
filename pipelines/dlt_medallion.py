# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion pipeline on Delta Live Tables (DLT)
# MAGIC Recreates the notebook workflow declaratively:
# MAGIC * **Bronze** - streaming tables (Auto Loader) over the raw files in the Volume
# MAGIC * **Silver** - data quality expectations (`expect_or_drop`) + enrichment
# MAGIC * **Gold** - aggregates and the trips <-> drivers join (materialized views)
# MAGIC
# MAGIC Target schemas: `dlt_bronze`, `dlt_silver`, `dlt_gold` (separate from the notebook tables).
# MAGIC Pipeline configuration keys (optional): `medallion.catalog`, `medallion.volume_path`.

# COMMAND ----------

# NOTE: Delta Live Tables was renamed to Lakeflow Spark Declarative Pipelines. The newer Python API is
# `from pyspark import pipelines as dp` (`@dp.table`, `@dp.materialized_view`, `@dp.expect_or_drop`, ...).
# This file follows the task statement and uses the classic `dlt` module (`@dlt.table`, `@dlt.expect_or_drop`),
# which still works. Migrating later is mostly a mechanical rename of the decorators/imports.
import dlt
from pyspark.sql import Window, functions as F

CATALOG = spark.conf.get("medallion.catalog", "workspace")
LANDING_ROOT = spark.conf.get("medallion.volume_path", f"/Volumes/{CATALOG}/raw_files/landing")

BRONZE = f"{CATALOG}.dlt_bronze"
SILVER = f"{CATALOG}.dlt_silver"
GOLD = f"{CATALOG}.dlt_gold"

DRIVERS_SCHEMA_HINTS = (
    "driver_id string, name string, car_number string, experience int, "
    "rating double, timestamp timestamp, phone string"
)

# The taxi parquet file stores pickup/dropoff as TIMESTAMP_NTZ (timestamps without a time zone).
# Delta requires the `timestampNtz` table feature to be enabled explicitly for such tables,
# and a pipeline cannot enable it on an existing table by itself, so it is set in the table definition.
NTZ_TABLE_PROPERTIES = {"delta.feature.timestampNtz": "supported"}


def autoloader(file_format: str, path: str, glob: str, **options):
    """Auto Loader stream over a Volume folder (schema location/checkpoints are managed by the pipeline)."""
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("pathGlobFilter", glob)
    )
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader.load(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze - streaming tables over raw files

# COMMAND ----------

@dlt.table(
    name=f"{BRONZE}.taxi",
    comment="Raw taxi trips (parquet), ingested as-is.",
    table_properties=NTZ_TABLE_PROPERTIES,
)
def bronze_taxi():
    return autoloader("parquet", f"{LANDING_ROOT}/source/taxi", "*.parquet")


@dlt.table(name=f"{BRONZE}.users", comment="Raw users (avro), ingested as-is.")
def bronze_users():
    return autoloader("avro", f"{LANDING_ROOT}/source/users", "*.avro")


@dlt.table(name=f"{BRONZE}.drivers", comment="Raw driver events (JSON files produced by Faker).")
def bronze_drivers():
    return autoloader(
        "json", f"{LANDING_ROOT}/source/drivers", "*.json",
        **{"cloudFiles.schemaHints": DRIVERS_SCHEMA_HINTS},
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver - expectations + enrichment

# COMMAND ----------

@dlt.table(
    name=f"{SILVER}.taxi",
    comment="Valid trips with a payment description; duplicates removed.",
    table_properties=NTZ_TABLE_PROPERTIES,
)
@dlt.expect_or_drop("passenger_count_positive", "passenger_count > 0")
@dlt.expect_or_drop("trip_distance_positive", "trip_distance > 0")
@dlt.expect_or_drop("fare_amount_positive", "fare_amount > 0")
def silver_taxi():
    return (
        spark.read.table(f"{BRONZE}.taxi")
        .drop("_rescued_data")
        .withColumn(
            "payment_description",
            F.when(F.col("payment_type") == 1, "Credit Card")
             .when(F.col("payment_type") == 2, "Cash")
             .otherwise("Other"),
        )
        .dropDuplicates()                                   # before ingest_time, otherwise nothing is a duplicate
        .withColumn("ingest_time", F.current_timestamp())
    )


# Intermediate (unpublished) view: the quality rules must be applied BEFORE picking the latest record
# per driver, as in the notebook version (filter first, then deduplicate).
@dlt.view(name="drivers_valid", comment="Driver events that pass the quality rules.")
@dlt.expect_or_drop("experience_positive", "experience > 0")
@dlt.expect_or_drop("rating_in_range", "rating BETWEEN 1.0 AND 5.0")
def drivers_valid():
    return spark.read.table(f"{BRONZE}.drivers").drop("_rescued_data")


@dlt.table(name=f"{SILVER}.drivers", comment="Latest valid record per driver_id.")
def silver_drivers():
    latest_first = Window.partitionBy("driver_id").orderBy(F.col("timestamp").desc())
    return (
        dlt.read("drivers_valid")
        .withColumn("_rn", F.row_number().over(latest_first))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("ingest_time", F.current_timestamp())
    )


@dlt.table(name=f"{SILVER}.users", comment="Users with a name, plus registration_date.")
@dlt.expect_or_drop("name_not_null", "name IS NOT NULL")
def silver_users():
    # Deterministic pseudo registration date in [2020-01-01, 2024-01-31] derived from the name.
    # Unlike the notebook version this does not use Faker: Databricks recommends keeping external
    # dependencies of pipelines to a minimum (see README, "Assumptions").
    days_span = 1492
    registration_date = F.date_add(
        F.lit("2020-01-01").cast("date"),
        F.pmod(F.xxhash64("name"), F.lit(days_span)).cast("int"),
    )
    return (
        spark.read.table(f"{BRONZE}.users")
        .drop("_rescued_data")
        .withColumn("registration_date", registration_date)
        .dropDuplicates()
        .withColumn("ingest_time", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold - aggregates and enrichment (materialized views)

# COMMAND ----------

@dlt.table(
    name=f"{GOLD}.taxi_metrics",
    comment="Trips and revenue per pickup zone and payment type.",
    cluster_by=["PULocationID", "payment_description"],      # Liquid Clustering
)
def gold_taxi_metrics():
    return (
        spark.read.table(f"{SILVER}.taxi")
        .groupBy("PULocationID", "payment_description")
        .agg(
            F.count(F.lit(1)).alias("total_trips"),
            F.round(F.sum("fare_amount"), 2).alias("total_revenue"),
        )
    )


@dlt.table(
    name=f"{GOLD}.driver_metrics",
    comment="Drivers and average rating per experience level.",
    table_properties={"pipelines.autoOptimize.zOrderCols": "experience"},   # Z-Order
)
def gold_driver_metrics():
    return (
        spark.read.table(f"{SILVER}.drivers")
        .groupBy("experience")
        .agg(
            F.count(F.lit(1)).alias("drivers_count"),
            F.round(F.avg("rating"), 2).alias("avg_rating"),
        )
    )


@dlt.table(name=f"{GOLD}.user_metrics", comment="Users per registration date, most popular first.")
def gold_user_metrics():
    return (
        spark.read.table(f"{SILVER}.users")
        .groupBy("registration_date")
        .agg(F.count(F.lit(1)).alias("users_count"))
        .orderBy(F.col("users_count").desc())
    )


@dlt.table(
    name=f"{GOLD}.enriched_trips",
    comment="Trips with synthetic driver_id LEFT JOINed to driver profiles.",
    table_properties=NTZ_TABLE_PROPERTIES,
)
def gold_enriched_trips():
    trips = spark.read.table(f"{SILVER}.taxi")
    drivers = spark.read.table(f"{SILVER}.drivers")

    # Same logic as `assign_driver_ids` in notebooks/_transforms.py (a pipeline cannot %run notebooks):
    # pick a driver_id from the pool of silver drivers by hashing the whole trip row (deterministic).
    pool = (
        drivers.select("driver_id").distinct()
        .withColumn("pool_idx", F.row_number().over(Window.orderBy("driver_id")) - 1)
    )
    pool_size = pool.agg(F.count(F.lit(1)).alias("pool_n"))
    trip_hash = F.xxhash64(*[F.col(c) for c in trips.columns])
    with_driver_id = (
        trips.crossJoin(F.broadcast(pool_size))
        .withColumn("pool_idx", F.pmod(trip_hash, F.nullif(F.col("pool_n"), F.lit(0))))
        .join(F.broadcast(pool), on="pool_idx", how="left")
        .drop("pool_idx", "pool_n")
    )
    profiles = drivers.select(
        "driver_id",
        F.col("name").alias("driver_name"),
        "car_number",
        F.col("experience").alias("driver_experience"),
        F.col("rating").alias("driver_rating"),
    )
    return with_driver_id.join(F.broadcast(profiles), on="driver_id", how="left")
