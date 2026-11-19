"""Reference row counts and assertions shared by the setup DAGs.

The expected values come from the SQL dumps in ``sources/``. Both sample
databases ship the same demo data, so most counts match between Pagila and
Sakila.
"""

from collections.abc import Mapping

PAGILA_EXPECTED_COUNTS: dict[str, int] = {
    "actor": 200,
    "address": 603,
    "category": 16,
    "city": 600,
    "country": 109,
    "customer": 599,
    "film": 1000,
    "film_actor": 5462,
    "film_category": 1000,
    "inventory": 4581,
    "language": 6,
    "rental": 16044,
    "staff": 2,
    "store": 2,
    # The payment parent table returns the rows of its six inheritance
    # children (payment_p2015_01 ... payment_p2015_06).
    "payment": 16049,
}

SAKILA_EXPECTED_COUNTS: dict[str, int] = {
    "actor": 200,
    "address": 603,
    "category": 16,
    "city": 600,
    "country": 109,
    "customer": 599,
    "film": 1000,
    # Filled by triggers on film inserts; the dump has no direct INSERT.
    "film_text": 1000,
    "film_actor": 5462,
    "film_category": 1000,
    "inventory": 4581,
    "language": 6,
    # Sakila has one payment per rental; Pagila also has five without a rental.
    "payment": 16044,
    "rental": 16044,
    "staff": 2,
    "store": 2,
}


def assert_row_counts(
    actual: Mapping[str, int],
    expected: Mapping[str, int],
    database: str,
) -> None:
    """Raise a single, readable error when any table has an unexpected size."""
    mismatches = {
        table: (expected[table], actual.get(table))
        for table in expected
        if actual.get(table) != expected[table]
    }
    if not mismatches:
        return

    details = "; ".join(
        f"{table}: expected {expected_count}, got {actual_count}"
        for table, (expected_count, actual_count) in mismatches.items()
    )
    raise ValueError(f"{database} row count check failed -> {details}")
