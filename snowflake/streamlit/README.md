# DB Clone Tool

A Streamlit in Snowflake app using the **container runtime**. It clones a standard database visible to the app's execution role, assigns ownership of the cloned database, schemas, tables and ordinary views to a selected owner role, and grants a selected read-only role access to the schemas chosen in the form.

The default roles are `DEVELOPMENT` and `READ_ONLY_ROLE`. The app does not create roles or assign them to users; the selected roles must already exist and be visible to the app.

## Security and limitations

- **The app runs with `DB_CLONER_ROLE` privileges.** This role has account-level `CREATE DATABASE` and `MANAGE GRANTS` privileges, so only trusted users should be allowed to use the app. Do not grant this execution role to every app viewer.
- **The source selector is not an allowlist.** The current code lists all standard databases visible to the app, excluding imported databases and databases with an origin. The source grants in `setup.sql` provide privileges needed for cloning; they do not restrict which databases appear in the selector. The code currently has no explicit source allowlist.
- The selected read-only role receives `USAGE` on the clone and selected schemas, plus `SELECT` on existing and future tables and ordinary views in those schemas. The app does not remove any other privileges that role may already have through direct grants or role inheritance. Use a dedicated role without unrelated write privileges when read-only access is required.
- The app transfers ownership of the cloned database, schemas, tables and ordinary views. It does not transfer ownership of every Snowflake object type. Cloning other object types depends on Snowflake's clone rules and the privileges granted to the app role.
- **Temporary-name warning:** the app uses `<TARGET_NAME>_NEW` as its temporary database. If that database exists and its comment starts with `Created by Snowflake DB Clone Tool`, the app may drop it before cloning. The marker does not prove that the database belongs to the current operation. Do not use a target/source combination that could put a database you need under this temporary name.
- An existing target is replaced only if its comment has the app's marker and its owner matches the selected owner role. The app asks for a checkbox confirmation and the exact target name. After a successful swap, the previous target is deleted; its changes are lost. Databases not marked as created by this tool are not replaced.
- After cloning, `verify()` performs a **basic access check**: it checks that the target exists, that its reported owner matches the selected owner, and that the read-only role has `USAGE` on the database and selected schemas. It is a smoke check, not a complete grant audit; it does not verify every `SELECT` grant, future grant, inherited privilege or cloned object.

## Deployment

### 1. Configure and run `setup.sql` once

1. Review the example settings near the top of `setup.sql` and make them match your Snowflake environment:
   - `QUERY_WAREHOUSE_NAME` — the query warehouse configured for the Streamlit app. `AIRLINE_DWH_WH` is a demonstration value, not a requirement and not a source-database restriction.
   - `STREAMLIT_COMPUTE_POOL_NAME` — the compute pool configured for the container runtime. The example is `SYSTEM_COMPUTE_POOL_CPU`.
   - `SOURCE_DATABASE_NAME` in section E — the example source database, `AIRLINE_DWH`. Change it to a database that exists in your account, or comment out section E and configure source privileges separately.
2. Run the script in a Snowsight SQL worksheet as `ACCOUNTADMIN`. It creates the roles, grants the app's required privileges, creates `ADMIN_TOOLS.CLONE_TOOL`, and grants the example source privileges.
3. If the app needs to clone another source database, repeat the grants in section E with that database's name. **These grants do not form a security allowlist:** the app currently displays every standard database visible to its execution role. Because `DB_CLONER_ROLE` has account-level `MANAGE GRANTS`, treat it as a highly privileged role.
4. Grant `DB_CLONER_ROLE` to the person who deploys and manages the app, if they do not already have it. Replace the placeholder before running:

   ```sql
   GRANT ROLE DB_CLONER_ROLE TO USER <DEPLOYER_USER>;
   ```

   Do not grant `DB_CLONER_ROLE` to every app viewer.

`setup.sql` is a demonstration bootstrap, not a production security policy. Review the role privileges and role inheritance before using it outside a controlled training environment.

### 2. Create and deploy the Streamlit app

1. In Snowsight, open **Workspaces** and choose **+ Add new → Streamlit app**.
2. Create the app in database `ADMIN_TOOLS` and schema `CLONE_TOOL`.
3. Replace the generated `streamlit_app.py` with this repository's `streamlit_app.py`.
4. Keep the generated `pyproject.toml` if it already includes `streamlit[snowflake]`.
5. Set the app's settings to match `setup.sql`:

   | Setting | Demonstration value |
   |---|---|
   | App name | `DB_CLONE_TOOL` |
   | Database | `ADMIN_TOOLS` |
   | Schema | `CLONE_TOOL` |
   | Runtime | Container |
   | Runtime name | `SYSTEM$ST_CONTAINER_RUNTIME_PY3_11` |
   | Compute pool | `SYSTEM_COMPUTE_POOL_CPU` |
   | Query warehouse | `AIRLINE_DWH_WH` |
   | App execution/owner role | `DB_CLONER_ROLE` |

   If you changed the warehouse or compute pool in `setup.sql`, use the same values in the app settings. `AIRLINE_DWH_WH` is only an example query warehouse; it is not hardcoded as the source database in the app.

6. Deploy the app and open it. Confirm that it loads without `ModuleNotFoundError` and that the expected source databases appear in **Clone from DB**.
