-- =====================================================
-- FROSTBYTE TASTY BYTES – SNOWFLAKE TRUCK DATA
-- =====================================================
-- This script demonstrates:
-- 1. Database & schema creation
-- 2. Loading data from S3
-- 3. Permanent vs Transient vs Temporary tables
-- 4. Zero-copy cloning
-- 5. Time Travel (DATA_RETENTION_TIME_IN_DAYS)
-- =====================================================

-- -----------------------------------------------------
-- 1. SECURITY CONTEXT
-- -----------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------
-- 2. DATABASE & SCHEMA SETUP
-- -----------------------------------------------------
CREATE OR REPLACE DATABASE FROSTBYTE_TASTY_BYTES;

CREATE OR REPLACE SCHEMA FROSTBYTE_TASTY_BYTES.RAW_POS;

-- -----------------------------------------------------
-- 3. BASE (PERMANENT) TRUCK TABLE
-- -----------------------------------------------------
CREATE OR REPLACE TABLE FROSTBYTE_TASTY_BYTES.RAW_POS.TRUCK
(
   truck_id NUMBER(38,0),
   menu_type_id NUMBER(38,0),
   primary_city VARCHAR,
   region VARCHAR,
   iso_region VARCHAR,
   country VARCHAR,
   iso_country_code VARCHAR,
   franchise_flag NUMBER(38,0),
   year NUMBER(38,0),
   make VARCHAR,
   model VARCHAR,
   ev_flag NUMBER(38,0),
   franchise_id NUMBER(38,0),
   truck_opening_date DATE
);

-- -----------------------------------------------------
-- 4. FILE FORMAT & STAGE (S3)
-- -----------------------------------------------------
CREATE OR REPLACE FILE FORMAT FROSTBYTE_TASTY_BYTES.PUBLIC.CSV_FF
  TYPE = 'CSV';

CREATE OR REPLACE STAGE FROSTBYTE_TASTY_BYTES.PUBLIC.S3LOAD
  URL = 's3://sfquickstarts/frostbyte_tastybytes/'
  FILE_FORMAT = FROSTBYTE_TASTY_BYTES.PUBLIC.CSV_FF;

-- View files available in the stage
LIST @FROSTBYTE_TASTY_BYTES.PUBLIC.S3LOAD;

-- -----------------------------------------------------
-- 5. LOAD DATA INTO TRUCK TABLE
-- -----------------------------------------------------
COPY INTO FROSTBYTE_TASTY_BYTES.RAW_POS.TRUCK
FROM @FROSTBYTE_TASTY_BYTES.PUBLIC.S3LOAD/raw_pos/truck/;

-- -----------------------------------------------------
-- 6. CLONE TABLES
-- -----------------------------------------------------

-- PARMANENT TABLE (Fail-safe, TIME TRAVEL)
CREATE OR REPLACE TABLE FROSTBYTE_TASTY_BYTES.RAW_POS.TRUCK_PARMANENT
  CLONE FROSTBYTE_TASTY_BYTES.RAW_POS.TRUCK;

-- TRANSIENT TABLE (no Fail-safe, lower cost)
CREATE OR REPLACE TRANSIENT TABLE FROSTBYTE_TASTY_BYTES.RAW_POS.TRUCK_TRANSIENT
  CLONE FROSTBYTE_TASTY_BYTES.RAW_POS.TRUCK;

-- TEMPORARY TABLE (session scoped, auto-dropped)
CREATE OR REPLACE TEMPORARY TABLE FROSTBYTE_TASTY_BYTES.RAW_POS.TRUCK_TEMPORARY
  CLONE FROSTBYTE_TASTY_BYTES.RAW_POS.TRUCK;

-- -----------------------------------------------------
-- 7. INSPECT TABLE TYPES
-- -----------------------------------------------------
SHOW TABLES LIKE 'TRUCK%';

-- -----------------------------------------------------
-- 8. SET TIME TRAVEL RETENTION (90 DAYS)
-- -----------------------------------------------------
-- Permanent table
ALTER TABLE FROSTBYTE_TASTY_BYTES.RAW_POS.TRUCK_PARMANENT
  SET DATA_RETENTION_TIME_IN_DAYS = 90;

-- Transient table : it will give error
ALTER TABLE FROSTBYTE_TASTY_BYTES.RAW_POS.TRUCK_TRANSIENT
  SET DATA_RETENTION_TIME_IN_DAYS = 90;

-- Temporary table : it will give error
ALTER TABLE FROSTBYTE_TASTY_BYTES.RAW_POS.TRUCK_TEMPORARY
  SET DATA_RETENTION_TIME_IN_DAYS = 90;

SHOW TABLES LIKE 'TRUCK%';

-- -----------------------------------------------------
-- 9. REDUCE RETENTION TO 0 DAYS
-- -----------------------------------------------------
-- Common for transient / temp tables to reduce cost
ALTER TABLE FROSTBYTE_TASTY_BYTES.RAW_POS.TRUCK_TRANSIENT
  SET DATA_RETENTION_TIME_IN_DAYS = 0;

ALTER TABLE FROSTBYTE_TASTY_BYTES.RAW_POS.TRUCK_TEMPORARY
  SET DATA_RETENTION_TIME_IN_DAYS = 0;

SHOW TABLES LIKE 'TRUCK%';



-- =====================================================
-- TABLE CLONING
-- =====================================================

-- -----------------------------------------------------
-- Create a clone of the TRUCK table
-- -----------------------------------------------------

CREATE OR REPLACE TABLE TASTY_BYTES.RAW_POS.TRUCK_CLONE
  CLONE TASTY_BYTES.RAW_POS.TRUCK;


-- -----------------------------------------------------
-- Inspect storage metadata (before insert)
-- -----------------------------------------------------

-- Storage metrics view
SELECT *
FROM TASTY_BYTES.INFORMATION_SCHEMA.TABLE_STORAGE_METRICS
WHERE TABLE_NAME IN ('TRUCK', 'TRUCK_CLONE');

-- Tables metadata view
SELECT *
FROM TASTY_BYTES.INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME IN ('TRUCK', 'TRUCK_CLONE');


-- -----------------------------------------------------
-- Insert data into the clone (increases storage)
-- -----------------------------------------------------

INSERT INTO TASTY_BYTES.RAW_POS.TRUCK_CLONE
SELECT *
FROM TASTY_BYTES.RAW_POS.TRUCK;


-- -----------------------------------------------------
-- Inspect metadata again (after insert)
-- -----------------------------------------------------

SELECT *
FROM TASTY_BYTES.INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME IN ('TRUCK', 'TRUCK_CLONE');


-- =====================================================
-- SCHEMA CLONING
-- =====================================================

CREATE OR REPLACE SCHEMA TASTY_BYTES.RAW_POS_CLONE
  CLONE TASTY_BYTES.RAW_POS;


-- =====================================================
-- DATABASE CLONING
-- =====================================================

CREATE OR REPLACE DATABASE TASTY_BYTES_CLONE
  CLONE TASTY_BYTES;


-- =====================================================
-- TIME-TRAVEL BASED TABLE CLONE
-- =====================================================
-- Clone the TRUCK table as it existed 10 minutes ago
-- -----------------------------------------------------

CREATE OR REPLACE TABLE TASTY_BYTES.RAW_POS.TRUCK_CLONE_TIME_TRAVEL
  CLONE TASTY_BYTES.RAW_POS.TRUCK
  AT (OFFSET => -60 * 10);

-- Validate time-travel clone
SELECT *
FROM TASTY_BYTES.RAW_POS.TRUCK_CLONE_TIME_TRAVEL;



