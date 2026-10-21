# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Gold aggregates + OPTIMIZE
# MAGIC * `gold.taxi_metrics` - Liquid Clustering (`CLUSTER BY`)
# MAGIC * `gold.driver_metrics` - Z-Order optimization
# MAGIC * `gold.user_metrics` - plain table sorted by count (descending)
# MAGIC
# MAGIC Liquid Clustering and Z-Order cannot be combined on one table, which is why they are on different tables.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

SILVER_TAXI = fqn(SILVER_SCHEMA, "taxi")
SILVER_DRIVERS = fqn(SILVER_SCHEMA, "drivers")
SILVER_USERS = fqn(SILVER_SCHEMA, "users")
TAXI_METRICS = fqn(GOLD_SCHEMA, "taxi_metrics")
DRIVER_METRICS = fqn(GOLD_SCHEMA, "driver_metrics")
USER_METRICS = fqn(GOLD_SCHEMA, "user_metrics")

# COMMAND ----------

# Trips and revenue per pickup zone and payment type (Liquid Clustering on the grouping keys).
spark.sql(f"""
    CREATE OR REPLACE TABLE {TAXI_METRICS}
    CLUSTER BY (PULocationID, payment_description)
    AS
    SELECT
        PULocationID,
        payment_description,
        COUNT(*)                    AS total_trips,
        ROUND(SUM(fare_amount), 2)  AS total_revenue
    FROM {SILVER_TAXI}
    GROUP BY PULocationID, payment_description
""")

# Drivers and average rating per experience level.
spark.sql(f"""
    CREATE OR REPLACE TABLE {DRIVER_METRICS} AS
    SELECT
        experience,
        COUNT(*)              AS drivers_count,
        ROUND(AVG(rating), 2) AS avg_rating
    FROM {SILVER_DRIVERS}
    GROUP BY experience
""")

# Users per registration date, most popular dates first.
spark.sql(f"""
    CREATE OR REPLACE TABLE {USER_METRICS} AS
    SELECT
        registration_date,
        COUNT(*) AS users_count
    FROM {SILVER_USERS}
    GROUP BY registration_date
    ORDER BY users_count DESC
""")

# COMMAND ----------

# Maintenance: OPTIMIZE on all three gold tables.
spark.sql(f"OPTIMIZE {TAXI_METRICS}")                          # triggers clustering for the liquid table
spark.sql(f"OPTIMIZE {DRIVER_METRICS} ZORDER BY (experience)")  # Z-Order
spark.sql(f"OPTIMIZE {USER_METRICS}")

for table in (TAXI_METRICS, DRIVER_METRICS, USER_METRICS):
    print(f"{table}: {spark.table(table).count():,} rows")

# COMMAND ----------

display(spark.table(TAXI_METRICS).orderBy("total_revenue", ascending=False).limit(10))
display(spark.table(DRIVER_METRICS).orderBy("experience"))
display(spark.table(USER_METRICS))
