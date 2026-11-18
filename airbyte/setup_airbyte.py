"""Create the Airbyte sources, destination and connections used by SADA.

The script is idempotent: every object is looked up by name first, created
when missing and left alone when it already exists. It talks to the Airbyte
public API (Cloud or self-managed) and only needs the client id/secret of an
API application.

Usage:
    python airbyte/setup_airbyte.py
"""

import argparse
import json
import socket
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"

# Base URL of the Airbyte API. Self-managed instances use
# http://localhost:8000/api/public/v1, Airbyte Cloud uses
# https://api.airbyte.com/v1.
DEFAULT_API_BASE = "http://localhost:8000/api/public/v1"

# Connector definition identifiers of the installed Airbyte version.
POSTGRES_SOURCE_DEFINITION = "decd338e-5647-4c0b-adf4-da0e75f5a750"
MYSQL_SOURCE_DEFINITION = "435bb9a5-7887-4809-aa58-28c27df0d7ad"
SNOWFLAKE_DESTINATION_DEFINITION = "424892c4-daac-4491-b35d-c6688ba547ba"

PAGILA_TABLES = [
    "actor",
    "address",
    "category",
    "city",
    "country",
    "customer",
    "film",
    "film_actor",
    "film_category",
    "inventory",
    "language",
    "payment",
    "rental",
    "staff",
    "store",
]

# payment_p2015_* tables are inheritance children of payment: the parent
# already returns their rows, so replicating them would duplicate every
# payment in the raw layer.
SAKILA_TABLES = [
    "actor",
    "address",
    "category",
    "city",
    "country",
    "customer",
    "film",
    "film_actor",
    "film_category",
    "film_text",
    "inventory",
    "language",
    "payment",
    "rental",
    "staff",
    "store",
]


def load_env(path: Path) -> dict[str, str]:
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


class AirbyteApi:
    """Minimal client for the Airbyte public API."""

    def __init__(self, base_url: str, client_id: str, client_secret: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.client_id = client_id
        self.client_secret = client_secret
        self._token: str | None = None

    def _authenticate(self) -> str:
        payload = json.dumps(
            {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            }
        ).encode()
        request = urllib.request.Request(
            f"{self.base_url}/applications/token",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            self._token = json.load(response)["access_token"]
        return self._token

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        timeout: int = 600,
    ) -> dict[str, Any]:
        if self._token is None:
            self._authenticate()

        body = json.dumps(payload).encode() if payload is not None else None
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }
        request = urllib.request.Request(
            f"{self.base_url}{path}", data=body, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as error:
            if error.code == 401:
                self._authenticate()
                return self.request(method, path, payload, timeout)
            detail = error.read().decode("utf-8", "replace")
            raise RuntimeError(f"{method} {path} failed: {error.code} {detail}") from error

    def list(self, path: str) -> list[dict[str, Any]]:
        return self.request("GET", path).get("data", [])


def find_by_name(items: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    return next((item for item in items if item.get("name") == name), None)


def build_streams(tables: list[str], namespace: str) -> list[dict[str, Any]]:
    return [
        {
            "name": table,
            "namespace": namespace,
            "syncMode": "full_refresh_overwrite",
            "selected": True,
        }
        for table in tables
    ]


def ensure_source(
    api: AirbyteApi,
    workspace_id: str,
    name: str,
    definition_id: str,
    configuration: dict[str, Any],
) -> str:
    existing = find_by_name(api.list("/sources"), name)
    if existing:
        # Tunnel addresses change between runs, so the stored configuration is
        # refreshed from .env on every execution.
        api.request(
            "PATCH",
            f"/sources/{existing['sourceId']}",
            {"name": name, "workspaceId": workspace_id, "configuration": configuration},
        )
        print(f"source      {name}: updated ({existing['sourceId']})")
        return existing["sourceId"]

    created = api.request(
        "POST",
        "/sources",
        {
            "name": name,
            "workspaceId": workspace_id,
            "definitionId": definition_id,
            "configuration": configuration,
        },
    )
    print(f"source      {name}: created ({created['sourceId']})")
    return created["sourceId"]


def ensure_destination(
    api: AirbyteApi,
    workspace_id: str,
    name: str,
    definition_id: str,
    configuration: dict[str, Any],
) -> str:
    existing = find_by_name(api.list("/destinations"), name)
    if existing:
        api.request(
            "PATCH",
            f"/destinations/{existing['destinationId']}",
            {"name": name, "workspaceId": workspace_id, "configuration": configuration},
        )
        print(f"destination {name}: updated ({existing['destinationId']})")
        return existing["destinationId"]

    created = api.request(
        "POST",
        "/destinations",
        {
            "name": name,
            "workspaceId": workspace_id,
            "definitionId": definition_id,
            "configuration": configuration,
        },
    )
    print(f"destination {name}: created ({created['destinationId']})")
    return created["destinationId"]


def ensure_connection(
    api: AirbyteApi,
    name: str,
    source_id: str,
    destination_id: str,
    namespace: str,
    streams: list[dict[str, Any]],
) -> str:
    payload = {
        "name": name,
        "sourceId": source_id,
        "destinationId": destination_id,
        "namespaceDefinition": "custom_format",
        "namespaceFormat": namespace,
        "prefix": "",
        "configurations": {"streams": streams},
    }

    existing = find_by_name(api.list("/connections"), name)
    if existing:
        # Updating a connection triggers a new schema discovery on Airbyte 2.x,
        # which is expensive on a local installation. The existing definition
        # is left untouched; recreate it in the UI if the table selection
        # really has to change.
        print(f"connection  {name}: exists ({existing['connectionId']})")
        return existing["connectionId"]

    # Airbyte runs a schema discovery while creating the connection, which can
    # exceed the gateway timeout on slow machines. Retry and accept the object
    # if the server created it despite the timeout.
    for attempt in range(1, 4):
        try:
            created = api.request("POST", "/connections", payload, timeout=600)
            connection_id = created["connectionId"]
            print(f"connection  {name}: created ({connection_id})")
            return connection_id
        except (RuntimeError, TimeoutError, urllib.error.URLError) as error:
            print(f"connection  {name}: attempt {attempt} failed: {error}")
            time.sleep(15)
            existing = find_by_name(api.list("/connections"), name)
            if existing:
                print(f"connection  {name}: created on retry ({existing['connectionId']})")
                return existing["connectionId"]

    raise SystemExit(f"Could not create connection {name}")


def check_tunnel(prefix: str, env: dict[str, str]) -> None:
    """Fail early when the public tunnel Airbyte Cloud needs is not reachable."""
    host = env.get(f"{prefix}_SOURCE_HOST", "").strip()
    port_text = env.get(f"{prefix}_SOURCE_PORT", "").strip()
    if not host or not port_text:
        raise SystemExit(
            f"{prefix}_SOURCE_HOST and {prefix}_SOURCE_PORT are not set in .env. "
            "Airbyte Cloud cannot reach a local database: open a TCP tunnel and "
            "put its public host and port into .env (see README)."
        )
    try:
        port = int(port_text)
    except ValueError as error:
        raise SystemExit(f"{prefix}_SOURCE_PORT must be a number, got '{port_text}'") from error

    try:
        with socket.create_connection((host, port), timeout=5):
            pass
    except OSError as error:
        raise SystemExit(
            f"Cannot reach the {prefix} source at {host}:{port} ({error}). "
            "Start the tunnel, check the address in .env and run the script again."
        ) from error
    print(f"tunnel      {prefix}: {host}:{port} reachable")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="print the plan only")
    args = parser.parse_args()

    env = load_env(ENV_FILE)
    required = [
        "AIRBYTE_CLIENT_ID",
        "AIRBYTE_CLIENT_SECRET",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_WAREHOUSE",
        "AIRBYTE_SF_USER",
        "AIRBYTE_SF_PASSWORD",
        "PAGILA_READER_PASSWORD",
        "SAKILA_READER_PASSWORD",
    ]
    missing = [key for key in required if not env.get(key)]
    if missing:
        raise SystemExit(f"Missing values in .env: {', '.join(missing)}")

    workspace_name = env.get("AIRBYTE_WORKSPACE_NAME", "Default Workspace")
    api_base = env.get("AIRBYTE_API_BASE", DEFAULT_API_BASE)
    api = AirbyteApi(api_base, env["AIRBYTE_CLIENT_ID"], env["AIRBYTE_CLIENT_SECRET"])

    if args.dry_run:
        print("Plan: two sources, one destination, two connections")
        print(f"  Pagila tables : {len(PAGILA_TABLES)}")
        print(f"  Sakila tables : {len(SAKILA_TABLES)}")
        return 0

    if "api.airbyte.com" in api_base:
        # Airbyte Cloud cannot reach localhost: the public tunnel endpoints
        # must be set and reachable before the sources are created.
        check_tunnel("PAGILA", env)
        check_tunnel("SAKILA", env)

    workspaces = api.list("/workspaces")
    workspace = find_by_name(workspaces, workspace_name)
    if not workspace and len(workspaces) == 1:
        workspace = workspaces[0]
        print(f"workspace   '{workspace_name}' not found, using '{workspace['name']}'")
    if not workspace:
        raise SystemExit(f"Airbyte workspace '{workspace_name}' not found")
    workspace_id = workspace["workspaceId"]

    pagila_source = ensure_source(
        api,
        workspace_id,
        "pagila-postgres",
        POSTGRES_SOURCE_DEFINITION,
        {
            "host": env.get("PAGILA_SOURCE_HOST", "host.docker.internal"),
            "port": int(env.get("PAGILA_SOURCE_PORT", "5433")),
            "database": "pagila",
            "username": "airbyte_reader",
            "password": env["PAGILA_READER_PASSWORD"],
            "schemas": ["public"],
            "ssl_mode": {"mode": "disable"},
            "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
            "replication_method": {"method": "Standard"},
        },
    )

    sakila_source = ensure_source(
        api,
        workspace_id,
        "sakila-mysql",
        MYSQL_SOURCE_DEFINITION,
        {
            "host": env.get("SAKILA_SOURCE_HOST", "host.docker.internal"),
            "port": int(env.get("SAKILA_SOURCE_PORT", "3307")),
            "database": "sakila",
            "username": "airbyte_reader",
            "password": env["SAKILA_READER_PASSWORD"],
            "ssl": False,
            "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
            "replication_method": {"method": "STANDARD"},
        },
    )

    destination = ensure_destination(
        api,
        workspace_id,
        "snowflake-sada",
        SNOWFLAKE_DESTINATION_DEFINITION,
        {
            "host": f"{env['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com",
            "role": "SADA_INGEST",
            "warehouse": env["SNOWFLAKE_WAREHOUSE"],
            "database": env["SNOWFLAKE_DATABASE"],
            "schema": "PAGILA_RAW",
            "username": env["AIRBYTE_SF_USER"],
            # The Snowflake destination reads the secret from a credentials
            # object; without it the connector defaults to key-pair auth.
            "credentials": {
                "auth_type": "Username and Password",
                "password": env["AIRBYTE_SF_PASSWORD"],
            },
            "loading_method": {"method": "Internal Staging"},
        },
    )

    ensure_connection(
        api,
        "pagila_to_snowflake",
        pagila_source,
        destination,
        "PAGILA_RAW",
        build_streams(PAGILA_TABLES, "public"),
    )
    ensure_connection(
        api,
        "sakila_to_snowflake",
        sakila_source,
        destination,
        "SAKILA_RAW",
        build_streams(SAKILA_TABLES, "sakila"),
    )

    print("Airbyte configuration completed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
