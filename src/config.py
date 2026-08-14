"""Application configuration.

All configuration is read from environment variables so the same code
works both inside Docker Compose and when run locally. Sensible
defaults are provided so the app also works out of the box.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DatabaseConfig:
    """Holds everything needed to open a connection to the database."""

    host: str
    port: int
    name: str
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        return cls(
            host=os.environ.get("DB_HOST", "db"),
            port=int(os.environ.get("DB_PORT", "5432")),
            name=os.environ.get("DB_NAME", "dorm_db"),
            user=os.environ.get("DB_USER", "dorm_user"),
            password=os.environ.get("DB_PASSWORD", "dorm_password"),
        )
