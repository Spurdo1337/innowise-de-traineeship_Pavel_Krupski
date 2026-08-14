"""Database connection abstraction.

The rest of the application talks to the ``Database`` interface only.
``PostgresDatabase`` is the concrete implementation used today. If the
project ever needed to switch to MySQL, a new ``MySQLDatabase`` class
implementing the same interface would be enough - no other file would
have to change (Open/Closed + Dependency Inversion principles).
"""

from abc import ABC, abstractmethod
from typing import Any, Iterable, Sequence

import psycopg2
import psycopg2.extras

from config import DatabaseConfig


class Database(ABC):
    """Abstract database interface used by the rest of the app."""

    @abstractmethod
    def connect(self) -> None:
        """Open the connection."""

    @abstractmethod
    def close(self) -> None:
        """Close the connection."""

    @abstractmethod
    def execute_script(self, sql_script: str) -> None:
        """Execute a multi-statement raw SQL script (DDL, index creation, ...)."""

    @abstractmethod
    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        """Execute a single statement that does not return rows."""

    @abstractmethod
    def execute_values(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        """Bulk-insert helper: run ``sql`` once for a batch of ``rows``."""

    @abstractmethod
    def fetch_all(self, sql: str, params: Sequence[Any] | None = None) -> list[dict]:
        """Run a SELECT and return the rows as a list of plain dicts."""

    def __enter__(self) -> "Database":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


class PostgresDatabase(Database):
    """PostgreSQL implementation of the ``Database`` interface."""

    def __init__(self, config: DatabaseConfig) -> None:
        self._config = config
        self._connection: psycopg2.extensions.connection | None = None

    def connect(self) -> None:
        self._connection = psycopg2.connect(
            host=self._config.host,
            port=self._config.port,
            dbname=self._config.name,
            user=self._config.user,
            password=self._config.password,
        )
        self._connection.autocommit = False

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def _require_connection(self) -> psycopg2.extensions.connection:
        if self._connection is None:
            raise RuntimeError("Database connection is not open. Call connect() first.")
        return self._connection

    def execute_script(self, sql_script: str) -> None:
        connection = self._require_connection()
        with connection.cursor() as cursor:
            cursor.execute(sql_script)
        connection.commit()

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        connection = self._require_connection()
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
        connection.commit()

    def execute_values(self, sql: str, rows: Iterable[Sequence[Any]]) -> None:
        connection = self._require_connection()
        rows = list(rows)
        if not rows:
            return
        with connection.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, sql, rows)
        connection.commit()

    def fetch_all(self, sql: str, params: Sequence[Any] | None = None) -> list[dict]:
        connection = self._require_connection()
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        return [dict(row) for row in rows]
