"""Repositories: write already-loaded model objects into the database.

Each repository has one job - persist one kind of entity. They know
nothing about JSON files (that is the loaders' job) or about reporting
queries (that is the queries' job).
"""

from abc import ABC, abstractmethod
from typing import Sequence

from db.connection import Database
from models import Room, Student


class Repository(ABC):
    @abstractmethod
    def save_all(self, items: Sequence) -> None:
        raise NotImplementedError


class RoomRepository(Repository):
    def __init__(self, database: Database) -> None:
        self._database = database

    def save_all(self, items: Sequence[Room]) -> None:
        sql = "INSERT INTO rooms (id, name) VALUES %s"
        rows = [(room.id, room.name) for room in items]
        self._database.execute_values(sql, rows)


class StudentRepository(Repository):
    def __init__(self, database: Database) -> None:
        self._database = database

    def save_all(self, items: Sequence[Student]) -> None:
        sql = "INSERT INTO students (id, name, birthday, sex, room_id) VALUES %s"
        rows = [
            (student.id, student.name, student.birthday, student.sex, student.room_id)
            for student in items
        ]
        self._database.execute_values(sql, rows)
