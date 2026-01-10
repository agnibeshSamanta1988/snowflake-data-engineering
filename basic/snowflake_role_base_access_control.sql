-- =====================================================
-- Snowflake Default Account Roles
-- Platform: Snowflake
-- Purpose : Explain built-in roles in simple terms
-- =====================================================

-- ACCOUNTADMIN
-- Full control of the account.
-- Can manage users, roles, billing, security, databases, and all account settings.
-- Highest level role.

-- ORGADMIN
-- Manages the Snowflake organization.
-- Can create and manage accounts inside the organization.
-- Used mainly at the organization level, not daily operations.

-- SECURITYADMIN
-- Manages security-related tasks.
-- Can create and manage users, roles, grants, and security policies.
-- Does NOT manage warehouses or data directly.

-- USERADMIN
-- Manages users and roles only.
-- Can create users and assign roles.
-- Cannot manage data or warehouses.

-- SYSADMIN
-- Manages system objects.
-- Can create and manage databases, schemas, tables, and warehouses.
-- Main role for day-to-day development and operations.

-- PUBLIC
-- Default role automatically granted to every user.
-- Contains minimal privileges.
-- Used as a base role that other roles build on.

-- SNOWFLAKE_LEARNING_ROLE
-- Automatically created by Snowflake during account setup.
-- Used for Snowflake tutorials, demos, and learning content.
-- Typically not used in production workloads.


-- ============================================================
-- Snowflake Role & Privilege Setup
-- Pattern: Assign role to user, grant permissions to role
-- ============================================================

-- ------------------------------------------------------------
-- 1. Use ACCOUNTADMIN to perform security administration
-- ------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- ------------------------------------------------------------
-- 2. Create a custom role
-- ------------------------------------------------------------
CREATE ROLE IF NOT EXISTS tasty_de;

-- ------------------------------------------------------------
-- 3. Inspect privileges of the new role (should be empty/minimal)
-- ------------------------------------------------------------
SHOW GRANTS TO ROLE tasty_de;

-- ------------------------------------------------------------
-- 4. Inspect privileges of ACCOUNTADMIN (auto-generated role)
-- ------------------------------------------------------------
SHOW GRANTS TO ROLE ACCOUNTADMIN;

-- ------------------------------------------------------------
-- 5. Grant the custom role to a user
-- ------------------------------------------------------------
GRANT ROLE tasty_de TO USER asamanta;

-- ------------------------------------------------------------
-- 6. Switch to the new role
-- ------------------------------------------------------------
USE ROLE tasty_de;

-- ------------------------------------------------------------
-- 7. Attempt to create a warehouse (expected to fail initially)
-- ------------------------------------------------------------
CREATE WAREHOUSE tasty_de_test;

-- ------------------------------------------------------------
-- 8. Switch back to ACCOUNTADMIN to grant required privileges
-- ------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- ------------------------------------------------------------
-- 9. Grant CREATE WAREHOUSE privilege at the account level
-- ------------------------------------------------------------
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE tasty_de;

-- ------------------------------------------------------------
-- 10. Verify all privileges granted to the role
-- ------------------------------------------------------------
SHOW GRANTS TO ROLE tasty_de;

-- ------------------------------------------------------------
-- 11. Switch back to the custom role
-- ------------------------------------------------------------
USE ROLE tasty_de;

-- ------------------------------------------------------------
-- 12. (Optional) Retry warehouse creation - should now succeed
-- ------------------------------------------------------------
CREATE WAREHOUSE tasty_de_test;


---> test to see whether tasty_de can create a warehouse
CREATE WAREHOUSE tasty_de_test;

---> learn more about the privileges each of the following auto-generated roles has

SHOW GRANTS TO ROLE securityadmin;

SHOW GRANTS TO ROLE useradmin;

SHOW GRANTS TO ROLE sysadmin;

SHOW GRANTS TO ROLE public;


-- ============================================================
-- Custom Role with PUBLIC baseline + Table/View creation
-- ============================================================

-- 1. Use a high-privilege role
USE ROLE SECURITYADMIN;

-- 2. Create the custom role
CREATE ROLE IF NOT EXISTS tasty_object_creator;

-- ------------------------------------------------------------
-- NOTE:
-- PUBLIC is automatically inherited by all roles.
-- No need to grant PUBLIC explicitly.
-- ------------------------------------------------------------

-- 3. Grant schema usage (required before object creation)
GRANT USAGE
ON DATABASE TASTY_BYTES
TO ROLE tasty_object_creator;

GRANT USAGE
ON SCHEMA TASTY_BYTES.RAW_POS
TO ROLE tasty_object_creator;

-- 4. Grant table & view creation privileges
GRANT CREATE TABLE
ON SCHEMA TASTY_BYTES.RAW_POS
TO ROLE tasty_object_creator;

GRANT CREATE VIEW
ON SCHEMA TASTY_BYTES.RAW_POS
TO ROLE tasty_object_creator;

-- 5. (Optional) Allow managing owned objects
GRANT MODIFY
ON SCHEMA TASTY_BYTES.RAW_POS
TO ROLE tasty_object_creator;

-- 6. Verify privileges
SHOW GRANTS TO ROLE tasty_object_creator;
