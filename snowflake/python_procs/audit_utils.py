"""
Shared helpers for the Bronze/Silver/Gold Python stored procedures.

This module is uploaded once (by the bootstrap DAG) to
AIRLINE_DWH.AUDIT.UTIL_STAGE and referenced by every procedure via
IMPORTS = ('@AIRLINE_DWH.AUDIT.UTIL_STAGE/audit_utils.py'), so all three
procedures share one implementation instead of each re-implementing its
own copy.
"""


def log_audit(session, procedure_name, target_object, source_object,
              inserted, updated, deleted, status, details):
    """Insert one row into AIRLINE_DWH.AUDIT.ETL_AUDIT_LOG. Returns the affected-row total."""
    affected = inserted + updated + deleted
    session.sql(
        """
        INSERT INTO AIRLINE_DWH.AUDIT.ETL_AUDIT_LOG
          (RUN_ID, PROCEDURE_NAME, TARGET_OBJECT, SOURCE_OBJECT,
           ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED, ROWS_AFFECTED,
           STATUS, DETAILS)
        SELECT UUID_STRING(), ?, ?, ?, ?, ?, ?, ?, ?, ?
        """,
        params=[
            procedure_name, target_object, source_object,
            inserted, updated, deleted, affected, status, details,
        ],
    ).collect()
    return affected


def parse_dml_counts(rows):
    """
    Turn the result rows of an executed MERGE into row counts. A MERGE
    returns one row whose columns are, in order: rows inserted, rows
    updated, and (only if the MERGE has a DELETE branch) rows deleted -
    so the counts can be read straight from what .collect() returned,
    with no RESULT_SCAN / query-id lookup needed.
    """
    values = list(rows[0]) if rows else []

    inserted = int(values[0]) if len(values) >= 1 and values[0] is not None else 0
    updated = int(values[1]) if len(values) >= 2 and values[1] is not None else 0
    deleted = int(values[2]) if len(values) >= 3 and values[2] is not None else 0

    return {
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "affected": inserted + updated + deleted,
    }


def run_merge_and_log(session, sql_text, procedure_name, target_object, source_object, details):
    """Run a MERGE, read its row counts from the result, and log them."""
    counts = parse_dml_counts(session.sql(sql_text).collect())
    log_audit(
        session, procedure_name, target_object, source_object,
        counts["inserted"], counts["updated"], counts["deleted"],
        "SUCCESS", details,
    )
    return counts["affected"]
