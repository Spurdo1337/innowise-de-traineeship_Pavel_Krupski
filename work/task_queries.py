import logging
import os
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("pagila_task5")

JDBC_URL = os.environ.get("JDBC_URL", "jdbc:postgresql://db:5432/pagila")
JDBC_PROPS = {
    "user": os.environ.get("JDBC_USER", "postgres"),
    "password": os.environ.get("JDBC_PASSWORD", ""),
    "driver": "org.postgresql.Driver",
}
JDBC_DRIVER_PATH = os.environ.get("JDBC_DRIVER_PATH", "/home/jovyan/jars/postgresql-42.7.13.jar")
JDBC_NUM_PARTITIONS = int(os.environ.get("JDBC_NUM_PARTITIONS", "4"))

OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", "/home/jovyan/work/output"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def build_spark() -> SparkSession:
    """Build a SparkSession that loads the PostgreSQL JDBC driver."""
    return (
        SparkSession.builder.appName("PagilaTask5")
        .config("spark.jars", JDBC_DRIVER_PATH)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_table(spark: SparkSession, table: str, partition_column: Optional[str] = None) -> DataFrame:
    """Read a table via JDBC."""
    if partition_column is None:
        return spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)

    bounds = (
        spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)
        .select(partition_column)
        .agg(F.min(partition_column).alias("lo"), F.max(partition_column).alias("hi"))
        .first()
    )
    if bounds is None or bounds["lo"] is None or bounds["hi"] is None:
        return spark.read.jdbc(url=JDBC_URL, table=table, properties=JDBC_PROPS)

    return spark.read.jdbc(
        url=JDBC_URL,
        table=table,
        column=partition_column,
        lowerBound=bounds["lo"],
        upperBound=bounds["hi"],
        numPartitions=JDBC_NUM_PARTITIONS,
        properties=JDBC_PROPS,
    )


def save_single_csv(df: DataFrame, target_file: Path) -> None:
    target_file.parent.mkdir(parents=True, exist_ok=True)
    df.toPandas().to_csv(target_file, index=False)


def print_and_save(title: str, df: DataFrame, filename: str) -> None:
    logger.info("=== %s ===", title)
    df = df.cache()
    df.show(truncate=False)
    save_single_csv(df, OUTPUT_DIR / filename)
    df.unpersist()


def q1_movies_by_category(film_category: DataFrame, category: DataFrame) -> DataFrame:
    return (
        film_category.join(category, "category_id")
        .groupBy(F.col("name").alias("category"))
        .agg(F.countDistinct("film_id").alias("movie_count"))
        .orderBy(F.desc("movie_count"), F.asc("category"))
    )


def q2_top_rented_actors(rental: DataFrame, inventory: DataFrame, film_actor: DataFrame, actor: DataFrame) -> DataFrame:
    return (
        rental.join(inventory, "inventory_id")
        .join(film_actor, "film_id")
        .join(actor, "actor_id")
        .groupBy("actor_id", "first_name", "last_name")
        .agg(F.count("rental_id").alias("rental_count"))
        .orderBy(F.desc("rental_count"), F.asc("last_name"), F.asc("first_name"), F.asc("actor_id"))
        .limit(10)
    )


def q3_most_expensive_category(payment: DataFrame, rental: DataFrame, inventory: DataFrame, film_category: DataFrame, category: DataFrame) -> DataFrame:
    totals = (
        payment.join(rental, "rental_id")
        .join(inventory, "inventory_id")
        .join(film_category, "film_id")
        .join(category, "category_id")
        .groupBy(F.col("name").alias("category"))
        .agg(F.sum("amount").alias("total_spent"))
    )

    window = Window.orderBy(F.desc("total_spent"))
    return totals.withColumn("rank", F.dense_rank().over(window)).filter(F.col("rank") == 1).drop("rank")


def q4_movies_not_in_inventory(film: DataFrame, inventory: DataFrame) -> DataFrame:
    return (
        film.join(inventory.select("film_id").distinct(), "film_id", "left_anti")
        .select("title")
        .distinct()
        .orderBy(F.asc("title"))
    )


def q5_top_children_actors(film_actor: DataFrame, film_category: DataFrame, category: DataFrame, actor: DataFrame) -> DataFrame:
    children_films = (
        film_category.join(category.filter(F.col("name") == "Children"), "category_id")
        .select("film_id")
        .distinct()
    )

    counts = (
        film_actor.join(children_films, "film_id")
        .join(actor, "actor_id")
        .groupBy("actor_id", "first_name", "last_name")
        .agg(F.countDistinct("film_id").alias("children_movie_count"))
    )

    window = Window.orderBy(F.desc("children_movie_count"))
    return (
        counts.withColumn("rank", F.dense_rank().over(window))
        .filter(F.col("rank") <= 3)
        .drop("rank")
        .orderBy(F.desc("children_movie_count"), F.asc("last_name"), F.asc("first_name"), F.asc("actor_id"))
    )


def q6_customers_by_city(customer: DataFrame, address: DataFrame, city: DataFrame, country: DataFrame) -> DataFrame:
    return (
        customer.join(address, "address_id")
        .join(city, "city_id")
        .join(country, "country_id")
        .filter(F.col("active").isNotNull())
        .groupBy("city_id", "city", F.col("country").alias("country"))
        .agg(
            F.sum(F.when(F.col("active") == 1, F.lit(1)).otherwise(F.lit(0))).alias("active_customers"),
            F.sum(F.when(F.col("active") == 1, F.lit(0)).otherwise(F.lit(1))).alias("inactive_customers"),
        )
        .drop("city_id")
        .orderBy(F.desc("inactive_customers"), F.asc("city"), F.asc("country"))
    )


def q7_top_category_for_city_filter(
    rental: DataFrame,
    customer: DataFrame,
    address: DataFrame,
    city: DataFrame,
    inventory: DataFrame,
    film_category: DataFrame,
    category: DataFrame,
    city_filter,
) -> DataFrame:
    rental_hours = (
        rental.filter(F.col("return_date").isNotNull())
        .withColumn("rental_hours", (F.unix_timestamp("return_date") - F.unix_timestamp("rental_date")) / F.lit(3600.0))
    )

    totals = (
        rental_hours.join(customer.select("customer_id", "address_id"), "customer_id")
        .join(address.select("address_id", "city_id"), "address_id")
        .join(city.select("city_id", "city"), "city_id")
        .join(inventory.select("inventory_id", "film_id"), "inventory_id")
        .join(film_category.select("film_id", "category_id"), "film_id")
        .join(category.select("category_id", F.col("name").alias("category")), "category_id")
        .filter(city_filter)
        .groupBy("category")
        .agg(F.sum("rental_hours").alias("total_rental_hours"))
    )

    # Assumption: ranked once across all matching cities together, not separately per city.
    window = Window.orderBy(F.desc("total_rental_hours"))
    return (
        totals.withColumn("rank", F.dense_rank().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank")
        .orderBy(F.asc("category"))
    )


def main() -> None:
    spark = build_spark()
    try:
        film = read_table(spark, "film")
        category = read_table(spark, "category")
        film_category = read_table(spark, "film_category")
        actor = read_table(spark, "actor")
        film_actor = read_table(spark, "film_actor")
        inventory = read_table(spark, "inventory")
        rental = read_table(spark, "rental", partition_column="rental_id")
        payment = read_table(spark, "payment", partition_column="payment_id")
        customer = read_table(spark, "customer")
        address = read_table(spark, "address")
        city = read_table(spark, "city")
        country = read_table(spark, "country")

        print_and_save("Movies by category", q1_movies_by_category(film_category, category), "01_movies_by_category.csv")
        print_and_save("Top 10 rented actors", q2_top_rented_actors(rental, inventory, film_actor, actor), "02_top_10_rented_actors.csv")
        print_and_save("Most expensive category", q3_most_expensive_category(payment, rental, inventory, film_category, category), "03_most_expensive_category.csv")
        print_and_save("Movies not in inventory", q4_movies_not_in_inventory(film, inventory), "04_movies_not_in_inventory.csv")
        print_and_save("Top Children actors", q5_top_children_actors(film_actor, film_category, category, actor), "05_top_children_actors.csv")
        print_and_save("Customers by city", q6_customers_by_city(customer, address, city, country), "06_customers_by_city.csv")
        print_and_save(
            "Top category for cities starting with a",
            q7_top_category_for_city_filter(
                rental, customer, address, city, inventory, film_category, category,
                F.lower(F.col("city")).startswith("a"),
            ),
            "07_top_category_cities_starting_with_a.csv",
        )
        print_and_save(
            'Top category for cities containing "-"',
            q7_top_category_for_city_filter(
                rental, customer, address, city, inventory, film_category, category,
                F.col("city").contains("-"),
            ),
            "08_top_category_cities_with_dash.csv",
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
