# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - JSON report of the workflow
# MAGIC Collects row counts of all tables and (when run from a Job) the result of every task of the current
# MAGIC Job run, and writes `workflow_report_<timestamp>.json` to the Volume.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

import json
import os
from datetime import datetime, timezone

dbutils.widgets.text("job_run_id", "", "Job run id (filled by the Job)")
JOB_RUN_ID = dbutils.widgets.get("job_run_id").strip()

TABLES = (
    [fqn(BRONZE_SCHEMA, t) for t in ("taxi", "users", "drivers")]
    + [fqn(SILVER_SCHEMA, t) for t in ("taxi", "drivers", "users")]
    + [fqn(GOLD_SCHEMA, t) for t in ("taxi_metrics", "driver_metrics", "user_metrics", "enriched_trips")]
    + [fqn(DLT_BRONZE_SCHEMA, t) for t in ("taxi", "drivers", "users")]
    + [fqn(DLT_SILVER_SCHEMA, t) for t in ("taxi", "drivers", "users")]
    + [fqn(DLT_GOLD_SCHEMA, t) for t in ("taxi_metrics", "driver_metrics", "user_metrics", "enriched_trips")]
)


def table_summary(name: str) -> dict:
    if not spark.catalog.tableExists(name):
        return {"exists": False, "row_count": None}
    return {"exists": True, "row_count": spark.table(name).count()}


def enum_value(value):
    return None if value is None else getattr(value, "value", str(value))


def millis_to_iso(ms):
    return None if ms is None else datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def job_run_summary(run_id: str) -> dict:
    """Task results of the current Job run (the report task itself is still running at this point)."""
    from databricks.sdk import WorkspaceClient

    run = WorkspaceClient().jobs.get_run(run_id=int(run_id))
    tasks = []
    for task in run.tasks or []:
        state = task.state
        tasks.append({
            "task_key": task.task_key,
            "life_cycle_state": enum_value(state.life_cycle_state) if state else None,
            "result_state": enum_value(state.result_state) if state else None,
            "start_time": millis_to_iso(task.start_time),
            "end_time": millis_to_iso(task.end_time),
        })
    return {"run_id": int(run_id), "run_page_url": run.run_page_url, "tasks": tasks}

# COMMAND ----------

tables = {name: table_summary(name) for name in TABLES}

job_run = None
if JOB_RUN_ID:
    try:
        job_run = job_run_summary(JOB_RUN_ID)
    except Exception as exc:  # the report must not fail because of the Jobs API
        job_run = {"run_id": JOB_RUN_ID, "error": str(exc)}

report = {
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "catalog": CATALOG,
    "job_run": job_run,
    "tables": tables,
    "summary": {
        "tables_expected": len(TABLES),
        "tables_found": sum(1 for t in tables.values() if t["exists"]),
        "tables_missing": [n for n, t in tables.items() if not t["exists"]],
    },
}

os.makedirs(REPORT_DIR, exist_ok=True)
path = f"{REPORT_DIR}/workflow_report_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
with open(path, "w") as f:
    json.dump(report, f, indent=2)

print(json.dumps(report, indent=2))
print(f"\nReport saved to {path}")
