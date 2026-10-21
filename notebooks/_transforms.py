# Databricks notebook source
# MAGIC %md
# MAGIC # `_transforms` - pure DataFrame transformations (no I/O)
# MAGIC Silver cleansing and enrichment logic. Needs `registration_date_udf` from `_faker_helpers`
# MAGIC (run that notebook first).

# COMMAND ----------

from pyspark.sql import DataFrame, Window, functions as F


def build_silver_taxi(bronze: DataFrame) -> DataFrame:
    """Filter bad trips, map payment_type to a description, drop duplicates, stamp ingest_time."""
    return (
        bronze
        .filter((F.col("passenger_count") > 0)
                & (F.col("trip_distance") > 0)
                & (F.col("fare_amount") > 0))
        .withColumn(
            "payment_description",
            F.when(F.col("payment_type") == 1, "Credit Card")
             .when(F.col("payment_type") == 2, "Cash")
             .otherwise("Other"),
        )
        # Deduplicate BEFORE adding ingest_time: otherwise every row would be unique.
        .dropDuplicates()
        .withColumn("ingest_time", F.current_timestamp())
    )


def build_silver_drivers(bronze: DataFrame) -> DataFrame:
    """Keep valid drivers and the latest record per driver_id."""
    latest_first = Window.partitionBy("driver_id").orderBy(F.col("timestamp").desc())
    return (
        bronze
        .filter((F.col("experience") > 0) & F.col("rating").between(1.0, 5.0))
        .withColumn("_rn", F.row_number().over(latest_first))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("ingest_time", F.current_timestamp())
    )


def build_silver_users(bronze: DataFrame) -> DataFrame:
    """Drop users without a name, add a (Faker based) registration_date and ingest_time."""
    return (
        bronze
        .filter(F.col("name").isNotNull())
        .withColumn("registration_date", registration_date_udf(F.col("name")))
        .dropDuplicates()
        .withColumn("ingest_time", F.current_timestamp())
    )


def assign_driver_ids(trips: DataFrame, drivers: DataFrame) -> DataFrame:
    """Assign every trip a synthetic driver_id taken from the pool of drivers in `drivers`.

    Deterministic: the id is picked by hashing the whole trip row, so reruns give the same result
    and no global window over millions of rows is needed.
    """
    pool = (
        drivers.select("driver_id").distinct()
        .withColumn("pool_idx", F.row_number().over(Window.orderBy("driver_id")) - 1)
    )
    pool_size = pool.agg(F.count(F.lit(1)).alias("pool_n"))
    trip_hash = F.xxhash64(*[F.col(c) for c in trips.columns])
    return (
        trips
        .crossJoin(F.broadcast(pool_size))
        # nullif(...) turns an empty pool into NULL instead of a division-by-zero error (ANSI mode)
        .withColumn("pool_idx", F.pmod(trip_hash, F.nullif(F.col("pool_n"), F.lit(0))))
        .join(F.broadcast(pool), on="pool_idx", how="left")
        .drop("pool_idx", "pool_n")
    )


def build_enriched_trips(silver_taxi: DataFrame, silver_drivers: DataFrame) -> DataFrame:
    """Trips with synthetic driver_id LEFT JOINed to driver profiles."""
    trips = assign_driver_ids(silver_taxi, silver_drivers)
    profiles = silver_drivers.select(
        "driver_id",
        F.col("name").alias("driver_name"),
        "car_number",
        F.col("experience").alias("driver_experience"),
        F.col("rating").alias("driver_rating"),
    )
    return trips.join(F.broadcast(profiles), on="driver_id", how="left")
