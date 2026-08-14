"""Entry point.

Usage:
    python main.py --students /data/students.json --rooms /data/rooms.json

Pipeline:
    1. Connect to the database (with a short retry loop, useful when the
       DB container is still starting up).
    2. Create the schema (tables) if it does not exist yet, then wipe any
       previously loaded data so re-running the script is idempotent.
    3. Load rooms.json and students.json from disk into memory.
    4. Bulk-insert that data into the database.
    5. Create the reporting indexes (AFTER the data load - faster than
       maintaining indexes during the insert).
    6. Run the four reporting queries (all "math" happens in SQL) and
       export each query's result to its own JSON file.
"""

import argparse
import sys
import time
from pathlib import Path

from config import DatabaseConfig
from db.connection import Database, PostgresDatabase
from db.repository import RoomRepository, StudentRepository
from db.schema_manager import SchemaManager
from exporters.json_exporter import JSONExporter
from loaders.rooms_loader import RoomsLoader
from loaders.students_loader import StudentsLoader
from queries.base_query import Query
from queries.largest_age_diff import LargestAgeDiffQuery
from queries.mixed_sex_rooms import MixedSexRoomsQuery
from queries.rooms_student_count import RoomsStudentCountQuery
from queries.smallest_avg_age import SmallestAverageAgeQuery

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_FILE = PROJECT_ROOT / "sql" / "schema.sql"
INDEXES_FILE = PROJECT_ROOT / "sql" / "indexes.sql"
DEFAULT_OUTPUT_DIR = Path("/app/output")

# All reporting queries live here. Adding a new report = add one line.
REPORT_QUERIES: list[Query] = [
    RoomsStudentCountQuery(),
    SmallestAverageAgeQuery(),
    LargestAgeDiffQuery(),
    MixedSexRoomsQuery(),
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load rooms/students JSON files into a database and export reports."
    )
    parser.add_argument("--students", required=True, help="Path to the students JSON file")
    parser.add_argument("--rooms", required=True, help="Path to the rooms JSON file")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory where each query result is written as its own JSON file "
        "(default: /app/output)",
    )
    return parser.parse_args(argv)


def wait_for_database(database: Database, retries: int = 15, delay_seconds: float = 2.0) -> None:
    """Retry connecting for a while - handy right after `docker compose up`."""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            database.connect()
            return
        except Exception as error:  # noqa: BLE001 - we want to retry on any connect error
            last_error = error
            print(f"[{attempt}/{retries}] Database is not ready yet ({error}). Retrying...")
            time.sleep(delay_seconds)
    raise ConnectionError(f"Could not connect to the database: {last_error}")


def run(args: argparse.Namespace) -> list[Path]:
    config = DatabaseConfig.from_env()
    database: Database = PostgresDatabase(config)
    output_dir = Path(args.output_dir) if args.output_dir else DEFAULT_OUTPUT_DIR

    wait_for_database(database)
    try:
        schema_manager = SchemaManager(database)
        schema_manager.initialize_schema(SCHEMA_FILE)
        schema_manager.reset_data()

        rooms = RoomsLoader().load(args.rooms)
        students = StudentsLoader().load(args.students)
        print(f"Loaded {len(rooms)} rooms and {len(students)} students from disk.")

        RoomRepository(database).save_all(rooms)
        StudentRepository(database).save_all(students)
        print("Data written to the database.")

        schema_manager.create_indexes(INDEXES_FILE)
        print("Reporting indexes created.")

        exporter = JSONExporter()
        written_files: list[Path] = []
        for query in REPORT_QUERIES:
            rows = query.execute(database)
            output_path = output_dir / f"{query.name}.json"
            exporter.export(rows, output_path)
            written_files.append(output_path)
            print(f"{query.name}: {len(rows)} row(s) -> {output_path}")

        return written_files
    finally:
        database.close()


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        run(args)
    except Exception as error:  # noqa: BLE001 - top-level CLI error handler
        print(f"Error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
