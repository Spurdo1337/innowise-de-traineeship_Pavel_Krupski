# Snowflake DB Clone Tool

A small Streamlit app that clones a Snowflake database and assigns separate development and read-only permissions. It shows the SQL plan and explains each grant before execution.

## How it works

- Creates a zero-copy clone of the selected source database and adds a comment marker in the same `CREATE DATABASE ... CLONE` statement.
- Transfers ownership of existing schemas, tables, views, and the database to `DEVELOPMENT`.
- Grants `READ_ONLY_ROLE` `USAGE` on the database and existing schemas, plus `SELECT` on existing tables and views.
- Adds future grants so schemas, tables, and views created later receive the corresponding read access.
- If the target already exists, the app only allows recreation when the database has the app's comment marker, its owner is `DEVELOPMENT` or `DB_CLONER_ROLE`, and the user confirms by ticking the checkbox and typing the exact database name.

Recreating a target drops the existing clone first. Any changes made to that clone after creation will be lost.

## Requirements

This project is designed for **Streamlit in Snowflake Workspaces using Container Runtime**. It is not intended to be run locally with `streamlit run`.

The current account configuration uses:

| Setting | Value |
|---|---|
| App execution role | `DB_CLONER_ROLE` |
| Development owner role | `DEVELOPMENT` |
| Read-only role | `READ_ONLY_ROLE` |
| Source database | `AIRLINE_DWH` |
| App database/schema | `ADMIN_TOOLS.CLONE_TOOL` |
| Compute pool | `SYSTEM_COMPUTE_POOL_CPU` |
| Query warehouse | `AIRLINE_DWH_WH` |

The roles must already exist. `DB_CLONER_ROLE` must inherit `DEVELOPMENT`, have `CREATE DATABASE` and `MANAGE GRANTS` on the account, have access to the source database and its objects, and have `USAGE` on the configured warehouse and compute pool. It must own, or otherwise have the required privileges on, the app's database and schema. These are powerful privileges: restrict app access to trusted users only.

## Install and run

1. In Snowsight, switch to `DB_CLONER_ROLE`.
2. Open **Workspaces** and create or open the workspace for this app.
3. Select **+ Add new → Streamlit app**. Keep the generated `pyproject.toml`, `snowflake.yml`, and `.streamlit/config.toml` files.
4. Replace the contents of `streamlit_app.py` with this project's code and save it.
5. Configure the workspace to use compute pool `SYSTEM_COMPUTE_POOL_CPU` and query warehouse `AIRLINE_DWH_WH` if prompted.
6. Click **Run**. The app should start, and Snowsight should show that it is running as `DB_CLONER_ROLE`.
7. Enter a target database name containing the Snowflake username token `SPURDO6769`, for example `AIRLINE_DWH_CLONE_SPURDO6769_3`. Check that the name does not already belong to a database you need to keep.
8. Set the source to `AIRLINE_DWH`, owner role to `DEVELOPMENT`, and read-only role to `READ_ONLY_ROLE`. Review the generated SQL before clicking **Clone database**.
9. If the target already exists, proceed only if it is a managed clone you intend to replace. Confirm the checkbox and type the exact target database name.

**Run vs. Deploy:** **Run** starts a private development preview in the workspace. To publish a Snowflake Streamlit object, click **Deploy**, choose `ADMIN_TOOLS.CLONE_TOOL` as the target location, select the compute pool and query warehouse above, and share it only with trusted roles. After editing code, run it for a preview and deploy again to publish the changes.

## Verification

Replace `<TARGET_DB>` with the clone name you created:

```sql
SHOW DATABASES LIKE '<TARGET_DB>';
SHOW GRANTS ON DATABASE <TARGET_DB>;
SHOW FUTURE GRANTS IN DATABASE <TARGET_DB>;
SHOW GRANTS ON TABLE <TARGET_DB>.GOLD.DIM_DATE;
```

Expected at database level: `DEVELOPMENT` has `OWNERSHIP`, and `READ_ONLY_ROLE` has `USAGE`. Table grants should show `OWNERSHIP` for `DEVELOPMENT` and `SELECT` for `READ_ONLY_ROLE`. Future grants should include `USAGE` on future schemas and `SELECT` on future tables and views.

Test read-only access with secondary roles disabled:

```sql
USE ROLE READ_ONLY_ROLE;
USE SECONDARY ROLES NONE;
USE WAREHOUSE AIRLINE_DWH_WH;

SELECT COUNT(*)
FROM <TARGET_DB>.GOLD.DIM_DATE;

-- This should fail with insufficient privileges and should not modify data:
DELETE FROM <TARGET_DB>.GOLD.DIM_DATE WHERE 1 = 0;
```

## Notes and limitations

- Snowflake DDL statements are not run as one transaction. If a step fails, inspect the target database before retrying; the app stops at the failed step.
- Ownership transfer is implemented for schemas, tables, views, and the database. Other object types (for example procedures, tasks, stages, streams, and policies) are not comprehensively handled by this minimal version.
- Existing row access policies from the source can still filter query results in the clone. For example, `FACT_FLIGHTS` may return zero rows for `READ_ONLY_ROLE` if the role is not allowed by the source project's row access policy. This does not necessarily mean the `SELECT` grant is missing.
- The app owner role has powerful account-level privileges. Anyone allowed to use the app may be able to invoke operations using those owner privileges, so keep sharing restricted.
