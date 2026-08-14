from loaders.base_loader import JSONLoader
from models import Student


class StudentsLoader(JSONLoader):
    """Loads students.json into a list of ``Student`` objects."""

    def _to_model(self, record: dict) -> Student:
        return Student(
            id=record["id"],
            name=record["name"],
            # Source data looks like "2011-08-22T00:00:00.000000";
            # only the date part is needed for a DATE column.
            birthday=record["birthday"][:10],
            sex=record["sex"],
            room_id=record["room"],
        )
