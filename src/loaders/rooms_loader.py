from loaders.base_loader import JSONLoader
from models import Room


class RoomsLoader(JSONLoader):
    """Loads rooms.json into a list of ``Room`` objects."""

    def _to_model(self, record: dict) -> Room:
        return Room(id=record["id"], name=record["name"])
