import re

import streamlit as st
from snowflake.snowpark.context import get_active_session


st.set_page_config(
    page_title="Snowflake DB Clone Tool",
    page_icon="🧩",
    layout="centered",
)

session = get_active_session()


# The only roles allowed to own a clone. The app role inherits DEVELOPMENT
# (see setup.sql), so it keeps the privileges needed to drop a clone that
# DEVELOPMENT owns. Any other owner could own a database the app cannot drop.
ALLOWED_OWNER_ROLES = {"DEVELOPMENT"}

# Roles that must never own a development copy or receive read access to it.
# PUBLIC would make the database accessible to every account user.
FORBIDDEN_ROLES = {
    "PUBLIC",
    "ACCOUNTADMIN",
    "SECURITYADMIN",
    "SYSADMIN",
    "USERADMIN",
    "ORGADMIN",
    "DB_CLONER_ROLE",
}

# Known databases in this account that must never be selected as a target.
# This is an account-specific safety list, not a universal Snowflake list.
PROTECTED_DATABASES = {
    "SNOWFLAKE",
    "SNOWFLAKE_SAMPLE_DATA",
    "SNOWFLAKE_LEARNING_DB",
    "ADMIN_TOOLS",
    "AIRLINE_DWH",
    "DEV",
    "USER$SPURDO6769",
    "PROD",
    "PRODUCTION",
}

APP_COMMENT_PREFIX = "Created by Snowflake DB Clone Tool"
APP_ROLE = "DB_CLONER_ROLE"


def normalize_identifier(value: str, field_name: str) -> str:
    """Validate and normalize a Snowflake unquoted identifier."""
    value = value.strip().upper()

    if not re.fullmatch(r"[A-Z_][A-Z0-9_$]*", value):
        raise ValueError(
            f"{field_name} must contain only letters, digits, _, or $ "
            "and must not start with a digit."
        )

    return value


def find_database(database_name: str):
    """Return database name, owner, and comment, or None if it does not exist."""
    rows = session.sql(
        f"SHOW DATABASES LIKE '{database_name}'"
    ).collect()

    # In LIKE, '_' is a wildcard, so compare returned names exactly.
    for row in rows:
        if row["name"].upper() == database_name:
            return {
                "name": row["name"],
                "owner": row["owner"],
                "comment": row["comment"] or "",
            }

    return None



def load_roles():
    """Return visible role names, or None if SHOW ROLES is unavailable."""
    try:
        rows = session.sql("SHOW ROLES").collect()
        return {row["name"].upper() for row in rows}
    except Exception:
        return None


def build_plan(
    source_db: str,
    target_db: str,
    owner_role: str,
    readonly_role: str,
    include_drop: bool,
):
    """Build SQL statements and explanations for the UI."""
    comment = f"{APP_COMMENT_PREFIX} from {source_db}"
    plan = []

    if include_drop:
        plan.append(
            (
                f"DROP DATABASE IF EXISTS {target_db};",
                "Remove the existing managed clone before recreating it. This "
                "runs only after the owner check and the user's confirmation.",
            )
        )

    plan += [
        (
            # COMMENT is part of the same statement, so there is no moment when
            # the clone exists without its marker.
            f"CREATE DATABASE {target_db} CLONE {source_db} COMMENT = '{comment}';",
            "Create a zero-copy clone and mark it as managed in the same "
            "statement, so the marker is present as soon as the clone exists.",
        ),
        (
            f"GRANT OWNERSHIP ON ALL SCHEMAS IN DATABASE {target_db} "
            f"TO ROLE {owner_role} REVOKE CURRENT GRANTS;",
            "The development role becomes the owner of all cloned schemas.",
        ),
        (
            f"GRANT OWNERSHIP ON ALL TABLES IN DATABASE {target_db} "
            f"TO ROLE {owner_role} REVOKE CURRENT GRANTS;",
            "The development role gets full control over cloned tables.",
        ),
        (
            f"GRANT OWNERSHIP ON ALL VIEWS IN DATABASE {target_db} "
            f"TO ROLE {owner_role} REVOKE CURRENT GRANTS;",
            "The development role gets full control over cloned views.",
        ),
        (
            f"GRANT OWNERSHIP ON DATABASE {target_db} "
            f"TO ROLE {owner_role} REVOKE CURRENT GRANTS;",
            "The development role becomes the owner of the cloned database. "
            "This is done last, after its contents.",
        ),
        (
            f"GRANT USAGE ON DATABASE {target_db} "
            f"TO ROLE {readonly_role};",
            "Allows the read-only role to access the database.",
        ),
        (
            f"GRANT USAGE ON ALL SCHEMAS IN DATABASE {target_db} "
            f"TO ROLE {readonly_role};",
            "Allows the read-only role to access existing schemas.",
        ),
        (
            f"GRANT SELECT ON ALL TABLES IN DATABASE {target_db} "
            f"TO ROLE {readonly_role};",
            "Allows the read-only role to read existing tables.",
        ),
        (
            f"GRANT SELECT ON ALL VIEWS IN DATABASE {target_db} "
            f"TO ROLE {readonly_role};",
            "Allows the read-only role to read existing views.",
        ),
        (
            f"GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {target_db} "
            f"TO ROLE {readonly_role};",
            "Provides USAGE on schemas created later in this database.",
        ),
        (
            f"GRANT SELECT ON FUTURE TABLES IN DATABASE {target_db} "
            f"TO ROLE {readonly_role};",
            "Provides SELECT on tables created later in this database.",
        ),
        (
            f"GRANT SELECT ON FUTURE VIEWS IN DATABASE {target_db} "
            f"TO ROLE {readonly_role};",
            "Provides SELECT on views created later in this database.",
        ),
    ]

    return plan


def refresh_roles():
    """Forget cached role results so the next rerun re-reads SHOW ROLES."""
    st.session_state.pop("all_roles", None)


st.title("🧩 Snowflake DB Clone Tool")
st.write(
    "Clone a Snowflake database and assign development "
    "and read-only permissions."
)

if "all_roles" not in st.session_state:
    st.session_state["all_roles"] = load_roles()

all_roles = st.session_state["all_roles"]

target_db_input = st.text_input(
    "Target DB name",
    placeholder="AIRLINE_DWH_SPURDO6769_V2",
)
source_db_input = st.text_input(
    "Clone from DB",
    value="AIRLINE_DWH",
)

# Only ALLOWED_OWNER_ROLES can be chosen. Without SHOW ROLES the field is text,
# and the value is still checked against ALLOWED_OWNER_ROLES below.
if all_roles is None:
    owner_role_input = st.text_input(
        "Owner role",
        value="DEVELOPMENT",
    )
else:
    owner_role_input = st.selectbox(
        "Owner role",
        sorted(ALLOWED_OWNER_ROLES),
    )

readonly_role_input = st.text_input(
    "Read-only role",
    value="READ_ONLY_ROLE",
)

st.button("🔄 Refresh roles", on_click=refresh_roles)

if not target_db_input.strip():
    st.info("Enter a target DB name to generate the SQL.")
    st.stop()

try:
    source_db = normalize_identifier(source_db_input, "Source DB")
    target_db = normalize_identifier(target_db_input, "Target DB")
    owner_role = normalize_identifier(owner_role_input, "Owner role")
    readonly_role = normalize_identifier(readonly_role_input, "Read-only role")

    if owner_role not in ALLOWED_OWNER_ROLES:
        raise ValueError(
            f"Owner role {owner_role} is not allowed. Only "
            f"{', '.join(sorted(ALLOWED_OWNER_ROLES))} can own a clone."
        )

    if owner_role == readonly_role:
        raise ValueError("Owner role and read-only role must be different.")

    if target_db == source_db:
        raise ValueError("Target database must be different from source database.")

    if target_db in PROTECTED_DATABASES:
        raise ValueError(
            f"{target_db} is a protected database and cannot be dropped or overwritten."
        )

    current_user = str(
        session.sql("SELECT CURRENT_USER() AS USER_NAME").collect()[0]["USER_NAME"]
    ).upper()
    user_token = re.sub(r"[^A-Z0-9_$]", "_", current_user)

    if user_token and user_token not in target_db:
        raise ValueError(
            f"Target DB name must contain your Snowflake username token: {user_token}."
        )

    for role, label in (
        (owner_role, "Owner role"),
        (readonly_role, "Read-only role"),
    ):
        if role in FORBIDDEN_ROLES:
            raise ValueError(
                f"{label} {role} is not allowed: use a dedicated role."
            )

        if all_roles is not None and role not in all_roles:
            raise ValueError(
                f"{label} {role} does not exist or is not visible to this app."
            )

    # Check the source before any DROP can run.
    if find_database(source_db) is None:
        raise ValueError(
            f"Source database {source_db} was not found "
            "(or this app has no access to it)."
        )

except ValueError as exc:
    st.error(str(exc))
    st.stop()

existing = find_database(target_db)

if existing:
    comment = existing["comment"].strip()
    existing_owner = str(existing["owner"]).upper()

    # Only databases marked by this app are ever dropped automatically.
    if not comment.startswith(APP_COMMENT_PREFIX):
        st.error(
            f"Refusing to drop {target_db}: it is not marked as a clone "
            "created by this tool. Choose a new target name, or verify the "
            "database manually before marking it as managed."
        )
        st.stop()

    # After a failed run the database may still be owned by the app role,
    # because ownership is transferred at the end. A retry is allowed for
    # the app role and the selected owner role; any other owner means the
    # database was handed to someone else and must not be dropped here.
    app_role = APP_ROLE
    allowed_owners = {owner_role, app_role}

    if existing_owner not in allowed_owners:
        st.error(
            f"Refusing to drop {target_db}: its owner is {existing_owner}, "
            f"which is neither the selected owner role ({owner_role}) nor "
            f"the app role ({app_role}). Inspect the database manually."
        )
        st.stop()

    if existing_owner == app_role and existing_owner != owner_role:
        st.warning(
            f"⚠️ {target_db} exists and is still owned by the app role "
            f"{app_role}. This usually means a previous run stopped before "
            "ownership was transferred. It will be dropped and recreated "
            "if you confirm below."
        )
    else:
        st.warning(
            f"⚠️ {target_db} already exists (owner: {existing_owner}). "
            "It is marked as managed by this tool and will be dropped and "
            "recreated if you proceed. Any changes made in it since it was "
            "cloned will be lost."
        )

plan = build_plan(
    source_db,
    target_db,
    owner_role,
    readonly_role,
    include_drop=existing is not None,
)

st.header("Generated SQL")
st.subheader("Please, always check the database names you set up!")

for sql, reason in plan:
    st.code(sql, language="sql")
    with st.expander("Why is this statement needed?"):
        st.write(reason)

if existing:
    confirmed = st.checkbox(f"Yes, drop {target_db} and recreate it")
    typed_name = st.text_input(f"Type {target_db} to confirm deletion")

    if not confirmed or typed_name.strip().upper() != target_db:
        st.info("Tick the checkbox and type the exact database name to enable cloning.")
        st.stop()

if st.button("Clone database", type="primary"):
    errors = []
    progress = st.empty()

    for number, (sql, _reason) in enumerate(plan, start=1):
        progress.write(f"Running step {number}/{len(plan)}...")

        try:
            session.sql(sql).collect()
            st.success(f"Step {number} completed:\n\n{sql}")
        except Exception as exc:
            errors.append((sql, str(exc)))
            st.error(f"Step {number} failed:\n\n{sql}\n\nError: {exc}")
            st.warning(
                "Execution stopped. Snowflake DDL statements are not wrapped "
                "in one transaction, so inspect the target database before retrying."
            )
            break

    if not errors:
        progress.success("Database clone completed successfully.")
        st.balloons()