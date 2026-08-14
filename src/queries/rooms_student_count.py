from db.connection import Database
from queries.base_query import Query


class RoomsStudentCountQuery(Query):
    """List of rooms and the number of students living in each of them."""

    name = "rooms_with_student_count"

    _SQL = """
        SELECT
            r.id   AS room_id,
            r.name AS room_name,
            COUNT(s.id) AS student_count
        FROM rooms r
        LEFT JOIN students s ON s.room_id = r.id
        GROUP BY r.id, r.name
        ORDER BY r.id;
    """

    def execute(self, database: Database) -> list[dict]:
        return database.fetch_all(self._SQL)
