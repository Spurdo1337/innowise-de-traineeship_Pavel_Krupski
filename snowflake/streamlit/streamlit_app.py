"""DB Clone Tool - Streamlit in Snowflake (container runtime).

Clones a database that the app can see into a new database under a temporary
name, grants the owner role ownership and the read-only role read access to the
schemas you select, then swaps the clone with the target (or renames it when
the target does not exist yet). The previous target is removed only after the swap.

Nothing here names a particular database or role. Design choices are in README.md.
"""
import re

import streamlit as st

DEFAULT_OWNER_ROLE = "DEVELOPMENT"
DEFAULT_READONLY_ROLE = "READ_ONLY_ROLE"
DEFAULT_READONLY_SCHEMAS = ("ANALYTICS",)

APP_COMMENT_PREFIX = "Created by Snowflake DB Clone Tool"

# Account-level system roles. The app's own role is added at runtime.
SYSTEM_ROLES = {
    "PUBLIC",
    "ACCOUNTADMIN",
    "SECURITYADMIN",
    "SYSADMIN",
    "USERADMIN",
    "ORGADMIN",
}

IDENTIFIER_PATTERN = re.compile(r"[A-Z_][A-Z0-9_$]*")

st.set_page_config(page_title="Snowflake DB Clone Tool", page_icon="🧩", layout="centered")

session = st.connection("snowflake").session()


def normalize_identifier(raw, label):
    """Upper-case a plain Snowflake identifier, or raise ValueError."""
    value = (raw or "").strip().upper()
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(
            f"{label} must contain only letters, digits, _ or $ "
            "and must not start with a digit."
        )
    return value


def current_role():
    """The role the app runs as. Its owner role is never accepted as owner or read-only role."""
    return str(session.sql("SELECT CURRENT_ROLE()").collect()[0][0]).upper()


def list_source_databases():
    """Standard databases visible to the app: not shared, not imported, not applications."""
    rows = session.sql("SHOW DATABASES").collect()
    return sorted(
        str(row["name"]).upper()
        for row in rows
        if str(row["kind"]).upper() == "STANDARD" and not row["origin"]
    )


def find_database(name):
    """Return owner and comment of an existing database, or None."""
    for row in session.sql(f"SHOW DATABASES LIKE '{name}'").collect():
        # LIKE treats '_' as a wildcard, so compare names exactly.
        if str(row["name"]).upper() == name:
            return {"owner": str(row["owner"]).upper(), "comment": row["comment"] or ""}
    return None


def list_schemas(database):
    rows = session.sql(f"SHOW SCHEMAS IN DATABASE {database}").collect()
    return sorted(
        str(row["name"]).upper() for row in rows if str(row["name"]).upper() != "INFORMATION_SCHEMA"
    )


def list_all_roles():
    """Every role name visible to the app, or None if SHOW ROLES is not allowed."""
    try:
        return {str(row["name"]).upper() for row in session.sql("SHOW ROLES").collect()}
    except Exception:
        return None


def build_plan(source, target, tmp, schemas, owner, readonly, replace_existing):
    """Return the statements as (sql, reason, phase) tuples.

    phase "build": the target is untouched; a failure here removes the temporary clone.
    phase "swap": the switch itself, one statement.
    phase "cleanup": removes the previous version after the switch.
    """
    comment = f"{APP_COMMENT_PREFIX} from {source}"
    plan = [
        (
            f"DROP DATABASE IF EXISTS {tmp};",
            "Remove a temporary clone left by an earlier failed run. The app only reaches "
            "this when that database carries its marker.",
            "build",
        ),
        (
            f"CREATE DATABASE {tmp} CLONE {source} COMMENT = '{comment}';",
            f"Create a zero-copy clone of {source} under a temporary name. "
            f"{target} is not touched yet.",
            "build",
        ),
    ]

    # Children first, the database last, so the owner role controls the contents before
    # the container. REVOKE CURRENT GRANTS drops privileges the clone inherited.
    for kind in ("SCHEMAS", "TABLES", "VIEWS"):
        plan.append((
            f"GRANT OWNERSHIP ON ALL {kind} IN DATABASE {tmp} TO ROLE {owner} REVOKE CURRENT GRANTS;",
            f"The {owner} role owns every cloned {kind.lower()}.",
            "build",
        ))
    plan.append((
        f"GRANT OWNERSHIP ON DATABASE {tmp} TO ROLE {owner} REVOKE CURRENT GRANTS;",
        f"The {owner} role owns the cloned database. Done last, after its contents.",
        "build",
    ))

    plan.append((
        f"GRANT USAGE ON DATABASE {tmp} TO ROLE {readonly};",
        f"The {readonly} role can see the database. Without this nothing inside is visible.",
        "build",
    ))
    for schema in schemas:
        qualified = f"{tmp}.{schema}"
        plan += [
            (f"GRANT USAGE ON SCHEMA {qualified} TO ROLE {readonly};",
             f"The {readonly} role can see schema {schema}.", "build"),
            (f"GRANT SELECT ON ALL TABLES IN SCHEMA {qualified} TO ROLE {readonly};",
             f"Read access to the tables that exist in {schema} now.", "build"),
            (f"GRANT SELECT ON ALL VIEWS IN SCHEMA {qualified} TO ROLE {readonly};",
             f"Read access to the views that exist in {schema} now.", "build"),
            (f"GRANT SELECT ON FUTURE TABLES IN SCHEMA {qualified} TO ROLE {readonly};",
             f"Read access to tables created later in {schema}.", "build"),
            (f"GRANT SELECT ON FUTURE VIEWS IN SCHEMA {qualified} TO ROLE {readonly};",
             f"Read access to views created later in {schema}.", "build"),
        ]

    if replace_existing:
        plan.append((
            f"ALTER DATABASE {target} SWAP WITH {tmp};",
            "Exchange the contents, identifiers and privileges of the two databases in one "
            "statement. Until this runs, the target still holds the previous version.",
            "swap",
        ))
        plan.append((
            f"DROP DATABASE {tmp};",
            "Remove the previous version, which now sits under the temporary name.",
            "cleanup",
        ))
    else:
        plan.append((
            f"ALTER DATABASE {tmp} RENAME TO {target};",
            "The target does not exist yet, so the finished clone takes its name.",
            "swap",
        ))
    return plan


def run_plan(plan, target, tmp):
    """Run the statements in order. Returns True when every statement succeeded."""
    for number, (sql, _reason, phase) in enumerate(plan, start=1):
        try:
            session.sql(sql).collect()
            st.success(f"Step {number} completed:\n\n{sql}")
        except Exception as exc:
            st.error(f"Step {number} failed:\n\n{sql}\n\nError: {exc}")
            if phase == "cleanup":
                st.warning(
                    f"{target} is the new clone. The previous version is still in {tmp}; "
                    f"drop it manually with: DROP DATABASE {tmp};"
                )
            else:
                try:
                    session.sql(f"DROP DATABASE IF EXISTS {tmp};").collect()
                    st.warning(f"{target} was not changed. The temporary clone {tmp} was removed.")
                except Exception as cleanup_exc:
                    st.warning(
                        f"{target} was not changed, but removing {tmp} failed: {cleanup_exc}. "
                        f"Drop it manually with: DROP DATABASE {tmp};"
                    )
            return False
    return True


def verify(target, schemas, owner, readonly):
    """Read back what the app promised. Returns a list of problems (empty = fine)."""
    problems = []
    info = find_database(target)
    if info is None:
        return [f"{target} was not found after the run."]
    if info["owner"] != owner:
        problems.append(f"{target} is owned by {info['owner']}, expected {owner}.")
    try:
        grants = session.sql(f"SHOW GRANTS TO ROLE {readonly}").collect()
    except Exception as exc:
        return problems + [f"Could not read the grants of {readonly}: {exc}"]
    granted = {
        (str(g["granted_on"]).upper(), str(g["name"]).upper(), str(g["privilege"]).upper())
        for g in grants
    }
    if ("DATABASE", target, "USAGE") not in granted:
        problems.append(f"{readonly} has no USAGE on {target}.")
    for schema in schemas:
        if ("SCHEMA", f"{target}.{schema}", "USAGE") not in granted:
            problems.append(f"{readonly} has no USAGE on {target}.{schema}.")
    return problems


# ---------------------------------------------------------------- UI

st.title("🧩 Snowflake DB Clone Tool")

target_input = st.text_input("Target DB name", placeholder="YOUR_DB_NAME_WITH_YOUR_NAME")

sources = list_source_databases()
if not sources:
    st.error("No source databases are visible to this app.")
    st.stop()
source_input = st.selectbox("Clone from DB", sources)

app_role = current_role()
all_roles = list_all_roles()
if all_roles is None:
    owner_input = st.text_input("Owner role", value=DEFAULT_OWNER_ROLE)
else:
    owner_options = sorted(all_roles - SYSTEM_ROLES - {app_role})
    if not owner_options:
        st.error("No role is available to be the owner role.")
        st.stop()
    owner_input = st.selectbox(
        "Owner role",
        owner_options,
        index=owner_options.index(DEFAULT_OWNER_ROLE) if DEFAULT_OWNER_ROLE in owner_options else 0,
    )

readonly_input = st.text_input("Read-only role", value=DEFAULT_READONLY_ROLE)

schemas_available = list_schemas(source_input)
schemas = st.multiselect(
    "Schemas the read-only role can read",
    schemas_available,
    default=[s for s in DEFAULT_READONLY_SCHEMAS if s in schemas_available],
)

if not target_input.strip():
    st.info("Enter a target DB name to generate the SQL.")
    st.stop()

try:
    source = normalize_identifier(source_input, "Source DB")
    target = normalize_identifier(target_input, "Target DB")
    owner = normalize_identifier(owner_input, "Owner role")
    readonly = normalize_identifier(readonly_input, "Read-only role")
    tmp = f"{target}_NEW"

    if target == source:
        raise ValueError("Target database must be different from the source database.")
    if owner == readonly:
        raise ValueError("Owner role and read-only role must be different.")
    for role, label in ((owner, "Owner role"), (readonly, "Read-only role")):
        if role in SYSTEM_ROLES or role == app_role:
            raise ValueError(f"{label} {role} is not allowed: use a dedicated role.")
        if all_roles is not None and role not in all_roles:
            raise ValueError(f"{label} {role} does not exist or is not visible to this app.")

    tmp_info = find_database(tmp)
    if tmp_info is not None and not tmp_info["comment"].startswith(APP_COMMENT_PREFIX):
        raise ValueError(f"{tmp} exists and was not created by this tool. Choose another target name.")

    existing = find_database(target)
    if existing is not None:
        if not existing["comment"].startswith(APP_COMMENT_PREFIX):
            raise ValueError(
                f"{target} exists but was not created by this tool, so it will not be replaced."
            )
        if existing["owner"] != owner:
            raise ValueError(
                f"{target} is owned by {existing['owner']}, not {owner}. Inspect it manually."
            )
except ValueError as exc:
    st.error(str(exc))
    st.stop()

if existing is not None:
    st.warning(
        f"⚠️ {target} already exists (owner: {existing['owner']}). "
        "It will be dropped and recreated if you proceed."
    )

st.button("🔄 Refresh SQL")  # rerunning the script re-reads databases, roles and schemas

plan = build_plan(source, target, tmp, schemas, owner, readonly, replace_existing=existing is not None)

st.header("Generated SQL")
st.subheader("Please, always check the database names you set up!")
for sql, reason, _phase in plan:
    st.code(sql, language="sql")
    with st.expander("Why is this statement needed?"):
        st.write(reason)

if existing is not None:
    confirmed = st.checkbox(f"Yes, replace {target} with a new clone")
    typed_name = st.text_input(f"Type {target} to confirm")
    if not confirmed or typed_name.strip().upper() != target:
        st.info("Tick the box and type the exact database name to enable cloning.")
        st.stop()

if st.button("Clone database", type="primary"):
    with st.spinner("Cloning..."):
        ok = run_plan(plan, target, tmp)
    if ok:
        problems = verify(target, schemas, owner, readonly)
        if problems:
            for problem in problems:
                st.warning(problem)
        else:
            st.success(f"{target} is ready: owner {owner}, read access for {readonly}.")
            st.balloons()