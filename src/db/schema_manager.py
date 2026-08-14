"""Schema management: DDL operations only (no reporting logic here).

Kept separate from the repositories (which only INSERT data) and from
the query classes (which only SELECT data) so each class has a single
reason to change - Single Responsibility Principle.
"""

from pathlib import Path

from db.connection import Database


class SchemaManager:
    def __init__(self, database: Database) -> None:
        self._database = database

    def initialize_schema(self, schema_file: Path) -> None:
        """Create the tables if they do not exist yet."""
        sql_script = schema_file.read_text(encoding="utf-8")
        self._database.execute_script(sql_script)

    def reset_data(self) -> None:
        """Empty both tables so the script can be re-run idempotently."""
        self._database.execute_script(
            "TRUNCATE TABLE students, rooms RESTART IDENTITY CASCADE;"
        )

    def create_indexes(self, indexes_file: Path) -> None:
        """Create the reporting indexes (run AFTER the bulk data load)."""
        sql_script = indexes_file.read_text(encoding="utf-8")
        self._database.execute_script(sql_script)
