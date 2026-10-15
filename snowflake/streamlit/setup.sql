-- =====================================================================
-- DB Clone Tool - minimal one-time bootstrap
-- Run as ACCOUNTADMIN in a Snowsight SQL worksheet.
--
-- This is a demonstration configuration. Replace the example warehouse,
-- compute pool and source database with resources available in your account.
-- The source database name below is an example, not a restriction in the app.
-- The app lists all visible standard databases; this script does not create
-- a source allowlist or a security boundary around database selection.
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- ---------------------------------------------------------------------
-- A. Configuration: keep these values aligned with the Streamlit settings
-- ---------------------------------------------------------------------
SET QUERY_WAREHOUSE_NAME = 'AIRLINE_DWH_WH';
SET STREAMLIT_COMPUTE_POOL_NAME = 'SYSTEM_COMPUTE_POOL_CPU';

-- ---------------------------------------------------------------------
-- B. Roles
-- ---------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS DEVELOPMENT
    COMMENT = 'Default owner role for cloned development databases';
CREATE ROLE IF NOT EXISTS READ_ONLY_ROLE
    COMMENT = 'Read access to selected schemas in cloned databases';
CREATE ROLE IF NOT EXISTS DB_CLONER_ROLE
    COMMENT = 'Execution role for the DB Clone Tool Streamlit app';

-- The current app defaults to DEVELOPMENT for clone ownership. This role grant
-- also allows the app role to access objects owned by DEVELOPMENT. This is a
-- deliberate privilege trade-off; do not grant unrelated or sensitive objects
-- to DEVELOPMENT if the app should not be able to see or manage them.
GRANT ROLE DEVELOPMENT TO ROLE DB_CLONER_ROLE;

-- ---------------------------------------------------------------------
-- C. Privileges and runtime resources required by the app
-- ---------------------------------------------------------------------
GRANT CREATE DATABASE ON ACCOUNT TO ROLE DB_CLONER_ROLE;
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE DB_CLONER_ROLE;

-- AIRLINE_DWH_WH is only the example query warehouse. Container Runtime uses
-- the compute pool for Python and the query warehouse for SQL statements.
GRANT USAGE ON WAREHOUSE IDENTIFIER($QUERY_WAREHOUSE_NAME)
    TO ROLE DB_CLONER_ROLE;
GRANT USAGE ON COMPUTE POOL IDENTIFIER($STREAMLIT_COMPUTE_POOL_NAME)
    TO ROLE DB_CLONER_ROLE;

-- ---------------------------------------------------------------------
-- D. Location and deployment permissions for the Streamlit app
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS ADMIN_TOOLS;
CREATE SCHEMA IF NOT EXISTS ADMIN_TOOLS.CLONE_TOOL;

GRANT USAGE ON DATABASE ADMIN_TOOLS TO ROLE DB_CLONER_ROLE;
GRANT USAGE ON SCHEMA ADMIN_TOOLS.CLONE_TOOL TO ROLE DB_CLONER_ROLE;
GRANT CREATE STREAMLIT ON SCHEMA ADMIN_TOOLS.CLONE_TOOL
    TO ROLE DB_CLONER_ROLE;

-- Grant DB_CLONER_ROLE to the person who deploys/manages the app only.
-- Do not give this powerful role to every app viewer.
-- GRANT ROLE DB_CLONER_ROLE TO USER <DEPLOYER_USER>;

-- The read-only role must exist before it can be assigned to a user.
-- Assign it only to people who should read the schemas selected in the form.
-- GRANT ROLE READ_ONLY_ROLE TO USER <READ_ONLY_USER>;

-- ---------------------------------------------------------------------
-- E. Example source-database access: AIRLINE_DWH
-- ---------------------------------------------------------------------
-- This section is an example for one standard database. If AIRLINE_DWH does not
-- exist in your account, change the value below or comment out this section.
-- Repeat these GRANT statements when the app needs privileges on another source.
--
-- IMPORTANT: these grants do NOT restrict the app's source selector. The current
-- code lists all standard databases visible to DB_CLONER_ROLE; it has no explicit
-- source allowlist. The app does not automatically grant itself missing privileges,
-- but DB_CLONER_ROLE has account-level MANAGE GRANTS and is highly privileged.
-- Treat this section as example operational grants, not a security boundary.
--
-- These grants cover existing and future schemas, tables and ordinary views.
-- Other source object types may require additional object-specific privileges;
-- see README.md before granting broad privileges on source objects.

SET SOURCE_DATABASE_NAME = 'AIRLINE_DWH';

GRANT USAGE ON DATABASE IDENTIFIER($SOURCE_DATABASE_NAME)
    TO ROLE DB_CLONER_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE IDENTIFIER($SOURCE_DATABASE_NAME)
    TO ROLE DB_CLONER_ROLE;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE IDENTIFIER($SOURCE_DATABASE_NAME)
    TO ROLE DB_CLONER_ROLE;
GRANT SELECT ON ALL TABLES IN DATABASE IDENTIFIER($SOURCE_DATABASE_NAME)
    TO ROLE DB_CLONER_ROLE;
GRANT SELECT ON FUTURE TABLES IN DATABASE IDENTIFIER($SOURCE_DATABASE_NAME)
    TO ROLE DB_CLONER_ROLE;
GRANT SELECT ON ALL VIEWS IN DATABASE IDENTIFIER($SOURCE_DATABASE_NAME)
    TO ROLE DB_CLONER_ROLE;
GRANT SELECT ON FUTURE VIEWS IN DATABASE IDENTIFIER($SOURCE_DATABASE_NAME)
    TO ROLE DB_CLONER_ROLE;
