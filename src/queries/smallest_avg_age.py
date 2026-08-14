from db.connection import Database
from queries.base_query import Query


class SmallestAverageAgeQuery(Query):
    """5 rooms with the smallest average age of students."""

    name = "top5_rooms_smallest_avg_age"

    _SQL = """
        SELECT
            r.id   AS room_id,
            r.name AS room_name,
            ROUND(
                AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.birthday)))::numeric,
                2
            ) AS avg_age
        FROM rooms r
        JOIN students s ON s.room_id = r.id
        GROUP BY r.id, r.name
        ORDER BY avg_age ASC
        LIMIT 5;
    """

    def execute(self, database: Database) -> list[dict]:
        return database.fetch_all(self._SQL)
