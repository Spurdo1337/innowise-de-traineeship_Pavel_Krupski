# Databricks notebook source
# MAGIC %md
# MAGIC # `_faker_helpers` - Faker based data generators
# MAGIC Requires the `faker` package (`%pip install faker` in the calling notebook, BEFORE `%run`).

# COMMAND ----------

from datetime import date

import pandas as pd
from pyspark.sql import DataFrame, functions as F

DRIVER_DDL_BASE = (
    "driver_id string, name string, car_number string, "
    "experience int, rating double, timestamp timestamp"
)
DRIVER_DDL_WITH_PHONE = DRIVER_DDL_BASE + ", phone string"


def make_driver_events_fn(include_phone: bool, pool_size: int):
    """Build a function for `DataFrame.mapInPandas` that turns input rows into driver events.

    A driver is identified by an index in [0, pool_size). The Faker instance is seeded with that
    index, so the same index always yields the same driver_id / name / car_number / phone
    (a stable "driver profile"). Experience and rating are re-drawn for every event, i.e. every
    event is a new snapshot of the driver, which makes "keep the latest record per driver" meaningful.
    The input DataFrame must have a `timestamp` column (the `rate` source provides one).
    """
    columns = ["driver_id", "name", "car_number", "experience", "rating", "timestamp"]
    if include_phone:
        columns.append("phone")

    def _generate(batches):
        import random
        from faker import Faker  # imported on the executor

        fake = Faker("en_US")
        rng = random.Random()
        for pdf in batches:
            records = []
            for ts in pdf["timestamp"]:
                fake.seed_instance(rng.randrange(pool_size))
                record = {
                    "driver_id": str(fake.uuid4()),
                    "name": fake.name(),
                    "car_number": fake.license_plate(),
                }
                if include_phone:
                    record["phone"] = fake.phone_number()
                record["experience"] = rng.randint(0, 30)        # 0 is invalid -> cleaned in silver
                record["rating"] = round(rng.uniform(0.5, 5.0), 1)  # < 1.0 is invalid -> cleaned in silver
                record["timestamp"] = ts
                records.append(record)
            out = pd.DataFrame.from_records(records, columns=columns)
            out["experience"] = out["experience"].astype("int32")
            yield out

    return _generate


def generate_driver_events(df: DataFrame, include_phone: bool, pool_size: int) -> DataFrame:
    """Apply the Faker generator to any DataFrame that has a `timestamp` column."""
    ddl = DRIVER_DDL_WITH_PHONE if include_phone else DRIVER_DDL_BASE
    return df.mapInPandas(make_driver_events_fn(include_phone, pool_size), schema=ddl)


@F.pandas_udf("date")
def registration_date_udf(names: pd.Series) -> pd.Series:
    """Deterministic fake registration date per user name (same name -> same date)."""
    import zlib
    from faker import Faker

    fake = Faker("en_US")
    dates = []
    for name in names:
        if name is None or pd.isna(name):
            dates.append(None)
            continue
        fake.seed_instance(zlib.crc32(str(name).encode("utf-8")))
        dates.append(fake.date_between(start_date=date(2020, 1, 1), end_date=date(2024, 1, 31)))
    return pd.Series(dates)
