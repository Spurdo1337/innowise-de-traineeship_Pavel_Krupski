"""Minimal Airbyte API client used by the replication DAG.

Only the operations needed by the pipeline are implemented: authentication,
connection lookup, sync trigger and job polling.
"""

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any

# Full API base URL. Airbyte Cloud: https://api.airbyte.com/v1;
# self-managed: http://host.docker.internal:8000/api/public/v1
API_BASE = os.environ.get(
    "AIRBYTE_API_BASE", "http://host.docker.internal:8000/api/public/v1"
).rstrip("/")


class AirbyteError(RuntimeError):
    """Raised when the Airbyte API returns an error."""


def _token() -> str:
    payload = json.dumps(
        {
            "client_id": os.environ["AIRBYTE_CLIENT_ID"],
            "client_secret": os.environ["AIRBYTE_CLIENT_SECRET"],
            "grant_type": "client_credentials",
        }
    ).encode()
    request = urllib.request.Request(
        f"{API_BASE}/applications/token",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)["access_token"]


def _request(
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    timeout: int = 60,
) -> dict[str, Any]:
    token = _token()
    body = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(
        f"{API_BASE}{path}",
        data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", "replace")
        raise AirbyteError(f"{method} {path} failed: {error.code} {detail}") from error


def find_connection(name: str) -> dict[str, Any]:
    connections = _request("GET", "/connections").get("data", [])
    for connection in connections:
        if connection.get("name") == name:
            return connection
    raise AirbyteError(f"Airbyte connection '{name}' not found")


def trigger_sync(connection_id: str) -> str:
    """Start a sync job and return its Airbyte identifier.

    Airbyte 2.x rejects the legacy ``/connections/{id}/sync`` endpoint for
    public API clients; sync jobs are created through ``POST /jobs``.
    """
    response = _request("POST", "/jobs", {"jobType": "sync", "connectionId": connection_id})
    job_id = response.get("jobId") or response.get("id")
    if not job_id:
        raise AirbyteError(f"No job id returned by Airbyte: {response}")
    return str(job_id)


def job_status(job_id: str) -> str:
    response = _request("GET", f"/jobs/{job_id}")
    return str(response.get("status", "unknown")).lower()


def wait_for_job(job_id: str, timeout_seconds: int = 1800, poll_seconds: int = 15) -> None:
    """Block until the Airbyte job succeeds, fails or times out."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        status = job_status(job_id)
        if status in {"succeeded", "success", "completed"}:
            return
        if status in {"failed", "incomplete", "cancelled", "canceled", "error"}:
            raise AirbyteError(f"Airbyte job {job_id} finished with status '{status}'")
        time.sleep(poll_seconds)
    raise AirbyteError(f"Airbyte job {job_id} did not finish within {timeout_seconds}s")
