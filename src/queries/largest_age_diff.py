from db.connection import Database
from queries.base_query import Query


class LargestAgeDiffQuery(Query):
    """5 rooms with the largest difference in the age of students."""

    name = "top5_rooms_largest_age_diff"

    _SQL = """
        SELECT
            r.id   AS room_id,
            r.name AS room_name,
            (
                MAX(EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birthday))) -
                MIN(EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birthday)))
            )::int AS age_diff
        FROM rooms r
        JOIN students s ON s.room_id = r.id
        GROUP BY r.id, r.name
        ORDER BY age_diff DESC
        LIMIT 5;
    """

    def execute(self, database: Database) -> list[dict]:
        return database.fetch_all(self._SQL)
