"""Plain data structures shared between loaders and repositories."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Room:
    id: int
    name: str


@dataclass(frozen=True)
class Student:
    id: int
    name: str
    birthday: str  # ISO date string, e.g. "2011-08-22"
    sex: str       # "M" or "F"
    room_id: int
