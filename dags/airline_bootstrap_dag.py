import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from snowflake_sql import (
    INFRA_STATEMENTS,
    TABLE_STATEMENTS,
    STREAM_STATEMENTS,
    SECURITY_STATEMENTS,
    OBSOLETE_PROCEDURE_STATEMENTS,
    BRONZE_LOAD_PROC,
    SILVER_PROC,
    GOLD_PROC,
    ROW_ACCESS_POLICY_FQN,
    UTIL_STAGE_FQN,
)

UTIL_MODULE_LOCAL_PATH = "/opt/airflow/snowflake/python_procs/audit_utils.py"
DATABASE_NAME = "AIRLINE_DWH"


def _detach_row_access_policy_everywhere(session, policy_fqn: str) -> None:
    """
    Detach `policy_fqn` from every table/view that currently has it
    attached, wherever in the account that is. Looks up all current
    attachments via INFORMATION_SCHEMA.POLICY_REFERENCES and issues an
    ALTER TABLE/VIEW ... DROP ALL ROW ACCESS POLICIES for each one. This
    lets SECURITY_STATEMENTS freely CREATE OR REPLACE the policy
    afterwards, regardless of what it's currently attached to (a plain
    table replace does not detach an attached policy, and cloning an
    object carries its policy attachment along too).
    """
    try:
        refs = session.sql(
            "SELECT REF_DATABASE_NAME, REF_SCHEMA_NAME, REF_ENTITY_NAME, REF_ENTITY_DOMAIN "
            "FROM TABLE(AIRLINE_DWH.INFORMATION_SCHEMA.POLICY_REFERENCES("
            f"POLICY_NAME => '{policy_fqn}'))"
        ).collect()
    except Exception:
        # The policy (or even the database) doesn't exist yet - e.g. a
        # brand-new account running this DAG for the very first time.
        return

    for row in refs:
        ref_db = row["REF_DATABASE_NAME"]
        ref_schema = row["REF_SCHEMA_NAME"]
        ref_entity = row["REF_ENTITY_NAME"]
        ref_domain = (row["REF_ENTITY_DOMAIN"] or "").upper()
        object_kind = "VIEW" if "VIEW" in ref_domain else "TABLE"
        fq_entity = f"{ref_db}.{ref_schema}.{ref_entity}"
        session.sql(f"ALTER {object_kind} IF EXISTS {fq_entity} DROP ALL ROW ACCESS POLICIES").collect()


def _database_exists(session, name: str) -> bool:
    """True only for an exact name match: SHOW ... LIKE treats '_' as a wildcard."""
    rows = session.sql(f"SHOW DATABASES LIKE '{name}'").collect()
    return any(row["name"].upper() == name for row in rows)


@dag(
    dag_id="snowflake_bootstrap",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["snowflake", "bootstrap"],
    doc_md="Creates all Snowflake objects used by the demo.",
    params={
        "confirm_reset": Param(
            False,
            type="boolean",
            description="Rebuild the database even though it already exists. All data is deleted.",
        ),
    },
)
def snowflake_bootstrap():
    @task.snowpark
    def create_infrastructure(session) -> str:
        confirm_reset = bool(get_current_context()["params"].get("confirm_reset", False))
        if _database_exists(session, DATABASE_NAME) and not confirm_reset:
            raise ValueError(
                "Bootstrap has already run: database AIRLINE_DWH exists. "
                "Re-run with confirm_reset=true to rebuild it. "
                "All existing data will be deleted."
            )

        for statement in INFRA_STATEMENTS:
            session.sql(statement).collect()
        return "Warehouse, database, schemas, file format, stage and sequences ready."

    @task.snowpark
    def create_tables(session) -> str:
        for statement in TABLE_STATEMENTS:
            session.sql(statement).collect()
        return "Tables created."

    @task.snowpark
    def create_streams(session) -> str:
        for statement in STREAM_STATEMENTS:
            session.sql(statement).collect()
        return "Streams created."

    @task.snowpark
    def upload_shared_python_module(session) -> str:
        session.file.put(
            UTIL_MODULE_LOCAL_PATH,
            f"@{UTIL_STAGE_FQN}",
            overwrite=True,
            auto_compress=False,
        )
        return f"Uploaded audit_utils.py to @{UTIL_STAGE_FQN}"

    @task.snowpark
    def create_security_objects(session) -> str:
        _detach_row_access_policy_everywhere(session, ROW_ACCESS_POLICY_FQN)
        for statement in SECURITY_STATEMENTS:
            session.sql(statement).collect()

        # Grant the Power BI role to the user this bootstrap runs as.
        # Verified manually: GRANT ROLE ... TO USER IDENTIFIER($me) after SET me.
        session.sql("SET me = (SELECT CURRENT_USER())").collect()
        session.sql("GRANT ROLE AIRLINE_BI TO USER IDENTIFIER($me)").collect()
        return "Row access policy, secure view, roles and grants ready."

    @task.snowpark
    def create_procedures(session) -> str:
        for statement in OBSOLETE_PROCEDURE_STATEMENTS:
            session.sql(statement).collect()
        session.sql(BRONZE_LOAD_PROC).collect()
        session.sql(SILVER_PROC).collect()
        session.sql(GOLD_PROC).collect()
        return "Bronze/Silver/Gold procedures created."

    infra = create_infrastructure()
    tables = create_tables()
    streams = create_streams()
    upload_module = upload_shared_python_module()
    security = create_security_objects()
    procedures = create_procedures()

    # These tasks don't pass data to each other (each just returns a status
    # string), so - unlike the pipeline DAG - the dependency here has to be
    # declared explicitly; TaskFlow has no argument-passing to infer it from.
    infra >> tables >> [streams, security]
    infra >> upload_module
    [streams, security, upload_module] >> procedures


snowflake_bootstrap()
