"""Query abstraction.

Every reporting query is its own class with a single SQL statement.
All aggregation/"math" (averages, min/max, counts) happens inside the
SQL itself, never in Python - as required by the task.

Adding a new report later just means adding a new ``Query`` subclass;
nothing else in the app needs to change (Open/Closed Principle).
"""

from abc import ABC, abstractmethod

from db.connection import Database


class Query(ABC):
    #: key used for this query's results in the exported report
    name: str = ""

    @abstractmethod
    def execute(self, database: Database) -> list[dict]:
        raise NotImplementedError
