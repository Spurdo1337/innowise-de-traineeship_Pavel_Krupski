from db.connection import Database
from queries.base_query import Query


class MixedSexRoomsQuery(Query):
    """Rooms where students of different sex live together."""

    name = "mixed_sex_rooms"

    _SQL = """
        SELECT
            r.id   AS room_id,
            r.name AS room_name
        FROM rooms r
        JOIN students s ON s.room_id = r.id
        GROUP BY r.id, r.name
        HAVING COUNT(DISTINCT s.sex) > 1
        ORDER BY r.id;
    """

    def execute(self, database: Database) -> list[dict]:
        return database.fetch_all(self._SQL)
