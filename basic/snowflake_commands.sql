---===============================================================
---Commands : Database, Schema, Table, View, and Materialized View
---===============================================================

USE WAREHOUSE COMPUTE_WH;

/* =====================================================
   DATABASE COMMANDS
   ===================================================== */

-- CREATE DATABASE
CREATE DATABASE db_name;
CREATE DATABASE IF NOT EXISTS db_name;

-- SHOW DATABASES
SHOW DATABASES;
SHOW DATABASES LIKE 'DB_%';

-- DESCRIBE DATABASE
DESCRIBE DATABASE db_name;

-- ALTER DATABASE
ALTER DATABASE db_name RENAME TO new_db_name;
ALTER DATABASE db_name SET DATA_RETENTION_TIME_IN_DAYS = 7;
ALTER DATABASE db_name UNSET DATA_RETENTION_TIME_IN_DAYS;

-- DROP DATABASE
DROP DATABASE db_name;
DROP DATABASE IF EXISTS db_name;

-- UNDROP DATABASE
UNDROP DATABASE db_name;


/* =====================================================
   SCHEMA COMMANDS
   ===================================================== */

-- CREATE SCHEMA
CREATE SCHEMA schema_name;
CREATE SCHEMA IF NOT EXISTS db_name.schema_name;

-- SHOW SCHEMAS
SHOW SCHEMAS;
SHOW SCHEMAS IN DATABASE db_name;

-- DESCRIBE SCHEMA
DESCRIBE SCHEMA db_name.schema_name;

-- ALTER SCHEMA
ALTER SCHEMA schema_name RENAME TO new_schema_name;
ALTER SCHEMA schema_name SET DATA_RETENTION_TIME_IN_DAYS = 3;

-- DROP SCHEMA
DROP SCHEMA schema_name;
DROP SCHEMA IF EXISTS schema_name;

-- UNDROP SCHEMA
UNDROP SCHEMA schema_name;


/* =====================================================
   TABLE COMMANDS
   ===================================================== */

-- CREATE TABLE
CREATE TABLE table_name (
    id INT,
    name STRING,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS table_name (
    id INT,
    name STRING
);

-- CREATE TABLE AS SELECT
CREATE TABLE table_name 
    AS
SELECT * 
FROM source_table;

-- SHOW TABLES
SHOW TABLES;
SHOW TABLES IN SCHEMA db_name.schema_name;

-- DESCRIBE TABLE
DESCRIBE TABLE table_name;

-- ALTER TABLE
ALTER TABLE table_name ADD COLUMN age INT;
ALTER TABLE table_name DROP COLUMN age;
ALTER TABLE table_name RENAME TO new_table_name;
ALTER TABLE table_name SET DATA_RETENTION_TIME_IN_DAYS = 1;

-- TRUNCATE TABLE
TRUNCATE TABLE table_name;

-- DROP TABLE
DROP TABLE table_name;
DROP TABLE IF EXISTS table_name;

-- UNDROP TABLE
UNDROP TABLE table_name;


/* =====================================================
   VIEW COMMANDS
   ===================================================== */

-- CREATE VIEW
CREATE VIEW view_name AS
SELECT * FROM table_name;

CREATE OR REPLACE VIEW view_name AS
SELECT id, name FROM table_name;

-- SHOW VIEWS
SHOW VIEWS;
SHOW VIEWS IN SCHEMA db_name.schema_name;

-- DESCRIBE VIEW
DESCRIBE VIEW view_name;

-- ALTER VIEW
ALTER VIEW view_name RENAME TO new_view_name;

-- DROP VIEW
DROP VIEW view_name;
DROP VIEW IF EXISTS view_name;

-- UNDROP VIEW
UNDROP VIEW view_name;


/* =====================================================
   MATERIALIZED VIEW COMMANDS
   ===================================================== */

-- CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW mv_name AS
SELECT id, COUNT(*) cnt
FROM table_name
GROUP BY id;

CREATE OR REPLACE MATERIALIZED VIEW mv_name AS
SELECT * FROM table_name WHERE active = true;

-- SHOW MATERIALIZED VIEWS
SHOW MATERIALIZED VIEWS;
SHOW MATERIALIZED VIEWS IN SCHEMA db_name.schema_name;

-- DESCRIBE MATERIALIZED VIEW
DESCRIBE MATERIALIZED VIEW mv_name;

-- ALTER MATERIALIZED VIEW
ALTER MATERIALIZED VIEW mv_name RENAME TO new_mv_name;
ALTER MATERIALIZED VIEW mv_name SUSPEND;
ALTER MATERIALIZED VIEW mv_name RESUME;

-- DROP MATERIALIZED VIEW
DROP MATERIALIZED VIEW mv_name;
DROP MATERIALIZED VIEW IF EXISTS mv_name;

-- UNDROP MATERIALIZED VIEW
UNDROP MATERIALIZED VIEW mv_name;


/* =====================================================
   GRANT COMMANDS
   ===================================================== */

-- DATABASE GRANTS
GRANT USAGE ON DATABASE db_name TO ROLE role_name;

-- SCHEMA GRANTS
GRANT USAGE ON SCHEMA schema_name TO ROLE role_name;

-- TABLE GRANTS
GRANT SELECT, INSERT ON TABLE table_name TO ROLE role_name;

-- VIEW GRANTS
GRANT SELECT ON VIEW view_name TO ROLE role_name;

-- MATERIALIZED VIEW GRANTS
GRANT SELECT ON MATERIALIZED VIEW mv_name TO ROLE role_name;

/* ===================== END OF SCRIPT ===================== */
```
