from datetime import datetime

from loaders.base_loader import JSONLoader
from models import Student


class StudentsLoader(JSONLoader):
    """Loads students.json into a list of ``Student`` objects."""

    def _to_model(self, record: dict) -> Student:
        return Student(
            id=record["id"],
            name=record["name"],
            birthday=datetime.fromisoformat(record["birthday"]).date(),
            sex=record["sex"],
            room_id=record["room"],
        )
