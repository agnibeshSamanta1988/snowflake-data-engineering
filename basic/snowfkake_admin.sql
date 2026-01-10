--https://www.youtube.com/watch?v=2t-ls6ekA8E&t=2872s

/* ============================================================
   SNOWFLAKE CONTEXT & ENVIRONMENT INFORMATION
   ============================================================ */

/* ------------------------------------------------------------
   Account Identifiers
   ------------------------------------------------------------ */

-- Full Account Identifier (used internally & UI)
-- Format: <ORGANIZATION_NAME>-<ACCOUNT_NAME>
SELECT
    CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME()
        AS ACCOUNT_IDENTIFIER;

-- Data Sharing Account Identifier
-- Format: <ORGANIZATION_NAME>.<ACCOUNT_NAME>
SELECT
    CURRENT_ORGANIZATION_NAME() || '.' || CURRENT_ACCOUNT_NAME()
        AS DATA_SHARING_ACCOUNT_IDENTIFIER;


/* ------------------------------------------------------------
   Organization & Account Details
   ------------------------------------------------------------ */

-- Organization name
SELECT
    CURRENT_ORGANIZATION_NAME() AS ORGANIZATION_NAME;

-- Account name (logical account name)
SELECT
    CURRENT_ACCOUNT_NAME() AS ACCOUNT_NAME;

-- Account locator (cloud-region specific, immutable)
SELECT
    CURRENT_ACCOUNT_LOCATOR() AS ACCOUNT_LOCATOR;


/* ------------------------------------------------------------
   Session Context
   ------------------------------------------------------------ */

-- Current logged-in user
SELECT
    CURRENT_USER() AS CURRENT_USER;

-- Active role
SELECT
    CURRENT_ROLE() AS CURRENT_ROLE;

-- Active warehouse
SELECT
    CURRENT_WAREHOUSE() AS CURRENT_WAREHOUSE;

-- Active database
SELECT
    CURRENT_DATABASE() AS CURRENT_DATABASE;

-- Active schema
SELECT
    CURRENT_SCHEMA() AS CURRENT_SCHEMA;


/* ============================================================
   METADATA & OBJECT LISTING
   ============================================================ */

-- List all databases in the account
SHOW DATABASES;

-- List all warehouses in the account
SHOW WAREHOUSES;


/* ============================================================
   WAREHOUSE MANAGEMENT
   ============================================================ */

-- Create a warehouse
-- NOTE:
--  - Warehouse is created in SUSPENDED state by default
--  - It becomes the current warehouse if AUTO_RESUME = TRUE
CREATE WAREHOUSE IF NOT EXISTS WAREHOUSE_DEMO
    WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE WAREHOUSE IF NOT EXISTS WAREHOUSE_DEMO
    -- Sets the compute size of each cluster (smallest, lowest cost)
    WAREHOUSE_SIZE = 'XSMALL'
    -- Standard warehouse supports all workloads (ETL, BI, ad-hoc)
    WAREHOUSE_TYPE = 'STANDARD'
    -- Minimum number of clusters always available
    -- Ensures at least 1 cluster for query execution
    MIN_CLUSTER_COUNT = 1
    -- Maximum number of clusters allowed
    -- Enables automatic scaling up to handle high concurrency
    MAX_CLUSTER_COUNT = 3
    -- Automatically suspends the warehouse after 60 seconds of inactivity
    -- Helps reduce compute cost
    AUTO_SUSPEND = 60
    -- Automatically resumes the warehouse when a query is submitted
    AUTO_RESUME = TRUE
    -- Warehouse starts in suspended state when created (no cost until used)
    INITIALLY_SUSPENDED = TRUE;


-- Use a specific warehouse for the current session
USE WAREHOUSE WAREHOUSE_DEMO;

-- Suspend (stop) a warehouse to save credits
ALTER WAREHOUSE WAREHOUSE_DEMO SUSPEND;

-- Resume a suspended warehouse
ALTER WAREHOUSE WAREHOUSE_DEMO RESUME;

-- Drop (delete) a warehouse
-- IMPORTANT: Warehouse must be SUSPENDED before dropping
DROP WAREHOUSE IF EXISTS WAREHOUSE_DEMO;


--set warehouse size to medium
ALTER WAREHOUSE WAREHOUSE_DEMO SET WAREHOUSE_SIZE = MEDIUM;

--set warehouse size to xsmall
ALTER WAREHOUSE WAREHOUSE_DEMO SET WAREHOUSE_SIZE = XSMALL;


--Resume a warehouse
ALTER WAREHOUSE WAREHOUSE_DEMO RESUME;

--Suspend a warehouse
ALTER WAREHOUSE WAREHOUSE_DEMO SUSPEND;

--Check warehouse state
SHOW WAREHOUSES LIKE 'WAREHOUSE_DEMO';



/* ============================================================
   INFORMATION SCHEMA QUERIES
   ============================================================ */


USE WAREHOUSE WIREHOUSE_DE;

USE DATABASE SNOWFLAKE;

-- List all tables visible in the current database & schema
SELECT
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE,
    CREATED,
    LAST_ALTERED
FROM
    INFORMATION_SCHEMA.TABLES
ORDER BY
    TABLE_SCHEMA,
    TABLE_NAME;

USE ROLE ACCOUNTADMIN;

USE WAREHOUSE WIREHOUSE_DE;

CREATE DATABASE IF NOT EXISTS DE_DATABASE;


CREATE SCHEMA IF NOT EXISTS DE_PROJECT;

USE SCHEMA DE_DATABASE.DE_PROJECT;


SELECT CURRENT_ACCOUNT(), CURRENT_REGION();
LIST @azure_stage;


SHOW STORAGE INTEGRATIONS;


SELECT 
  CURRENT_ACCOUNT() as account_locator,  -- Account ID
  CURRENT_USER() as username,
  CURRENT_WAREHOUSE() as warehouse,
  CURRENT_DATABASE() as database,
  CURRENT_SCHEMA() as schema;

SELECT SYSTEM$ALLOWLIST();

SHOW NETWORK POLICIES;
SELECT SYSTEM$ALLOWLIST();

SELECT
  CURRENT_ORGANIZATION_NAME(),
  CURRENT_ACCOUNT_NAME(),
  CURRENT_ACCOUNT();
