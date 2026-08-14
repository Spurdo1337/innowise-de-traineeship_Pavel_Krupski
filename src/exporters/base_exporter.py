"""Exporter abstraction.

An ``Exporter`` writes the result of a single query (a list of row
dicts) to a single file. Each query is exported to its own file, so
the result stays readable and independent per report.
"""

from abc import ABC, abstractmethod
from pathlib import Path


class Exporter(ABC):
    @abstractmethod
    def export(self, rows: list[dict], output_path: Path) -> None:
        raise NotImplementedError
