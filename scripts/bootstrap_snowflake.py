"""Provision the Snowflake objects required by the SADA project.

The script reads credentials from the project .env file, renders the
placeholders in snowflake/00_admin_bootstrap.sql and executes the statements
with an ACCOUNTADMIN role. It is a one-time administrative step: Airflow and
Airbyte use the least-privilege service users created here.

Usage:
    python -m pip install snowflake-connector-python
    python scripts/bootstrap_snowflake.py
"""

import os
import sys
from pathlib import Path

import snowflake.connector

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"
SQL_FILE = PROJECT_ROOT / "snowflake" / "00_admin_bootstrap.sql"

PLACEHOLDERS = {
    "{{ADMIN_USER}}": "SNOWFLAKE_USER",
    "{{AIRFLOW_SVC_PASSWORD}}": "AIRFLOW_SF_PASSWORD",
    "{{AIRBYTE_SVC_PASSWORD}}": "AIRBYTE_SF_PASSWORD",
}


def load_env(path: Path) -> dict[str, str]:
    """Read a simple KEY=VALUE .env file without external dependencies."""
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw_line in path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def resolve_settings() -> dict[str, str]:
    settings = load_env(ENV_FILE)
    settings.update({key: value for key, value in os.environ.items() if key in settings})

    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [key for key in required if not settings.get(key)]
    if missing:
        raise SystemExit(f"Missing required settings in .env: {', '.join(missing)}")
    return settings


def render_sql(settings: dict[str, str]) -> str:
    sql = SQL_FILE.read_text(encoding="utf-8")
    for placeholder, setting in PLACEHOLDERS.items():
        value = settings.get(setting, "")
        if not value:
            raise SystemExit(f"Missing value for {setting} in .env")
        sql = sql.replace(placeholder, value)

    if "{{" in sql:
        raise SystemExit("Unresolved placeholders left in the bootstrap SQL")
    return sql


def split_statements(sql: str) -> list[str]:
    """Split the script into statements, dropping comment-only lines.

    The bootstrap file contains plain DDL without semicolons inside string
    literals or Snowflake Scripting blocks, so a simple split is sufficient.
    """
    lines = [
        line
        for line in sql.splitlines()
        if not line.strip().startswith("--")
    ]
    return [statement.strip() for statement in "\n".join(lines).split(";") if statement.strip()]


def main() -> int:
    settings = resolve_settings()
    sql = render_sql(settings)
    statements = split_statements(sql)

    connection = snowflake.connector.connect(
        account=settings["SNOWFLAKE_ACCOUNT"],
        user=settings["SNOWFLAKE_USER"],
        password=settings["SNOWFLAKE_PASSWORD"],
        role=settings.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        application="sada_bootstrap",
    )

    try:
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
            cursor.execute("SHOW SCHEMAS IN DATABASE SADA")
            schemas = sorted(row[1] for row in cursor.fetchall())
            cursor.execute("SHOW ROLES LIKE 'SADA%'")
            roles = sorted(row[1] for row in cursor.fetchall())
    finally:
        connection.close()

    print("Bootstrap completed")
    print(f"  database : {settings.get('SNOWFLAKE_DATABASE', 'SADA')}")
    print(f"  schemas  : {', '.join(schemas)}")
    print(f"  roles    : {', '.join(roles)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
