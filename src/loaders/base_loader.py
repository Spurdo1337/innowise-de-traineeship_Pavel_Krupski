"""Loader abstractions.

``Loader`` is the interface the rest of the app depends on. ``JSONLoader``
factors out the "open a file, parse JSON, map each record to a model"
logic that both concrete loaders (rooms/students) share, so a new file
format (e.g. CSV) could be supported later by adding a new subclass
without touching existing code (Open/Closed Principle).
"""

import json
from abc import ABC, abstractmethod
from typing import Any


class Loader(ABC):
    """Reads a source file and returns a list of model instances."""

    @abstractmethod
    def load(self, filepath: str) -> list[Any]:
        raise NotImplementedError


class JSONLoader(Loader):
    """Base class for loaders backed by a JSON array of objects."""

    def load(self, filepath: str) -> list[Any]:
        raw_records = self._read_json(filepath)
        return [self._to_model(record) for record in raw_records]

    @staticmethod
    def _read_json(filepath: str) -> list[dict]:
        with open(filepath, "r", encoding="utf-8") as file:
            return json.load(file)

    @abstractmethod
    def _to_model(self, record: dict) -> Any:
        """Convert a single raw JSON record into a model instance."""
        raise NotImplementedError
