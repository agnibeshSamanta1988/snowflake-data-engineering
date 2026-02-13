/*=====================================================
  1️⃣  SESSION SETUP
=====================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE LA_DB;
USE SCHEMA LA_SCHEMA;


/*=====================================================
  2️⃣  CREATE BASE CUSTOMERS TABLE
=====================================================*/

CREATE OR REPLACE TABLE customers (
    customer INT NOT NULL,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    region VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    gender VARCHAR(255) NOT NULL,
    "order" INT NOT NULL
);


/*=====================================================
  3️⃣  CREATE STAGE
=====================================================*/

CREATE OR REPLACE STAGE cust_stage
    DIRECTORY = (ENABLE = TRUE);


/*=====================================================
  4️⃣  LOAD DATA FROM STAGE INTO CUSTOMERS
=====================================================*/

COPY INTO customers
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7
    FROM @cust_stage
)
FILES = ('MOCK.csv')
FILE_FORMAT = (
    TYPE = CSV,
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    TRIM_SPACE = TRUE,
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    REPLACE_INVALID_CHARACTERS = TRUE,
    DATE_FORMAT = AUTO,
    TIME_FORMAT = AUTO,
    TIMESTAMP_FORMAT = AUTO
)
ON_ERROR = ABORT_STATEMENT;


/*=====================================================
  5️⃣  STAGE & FILE OPERATIONS
=====================================================*/

-- List user stage
LIST @~;

-- Show all stages
SHOW STAGES;

-- Export table to stage as CSV
COPY INTO @~/customers/staged/MOCK_latest
FROM customers
FILE_FORMAT = (
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    COMPRESSION = NONE
)
HEADER = TRUE
SINGLE = FALSE
MAX_FILE_SIZE = 5368709120;


/*=====================================================
  6️⃣  FILE FORMAT & SCHEMA INFERENCE
=====================================================*/

CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = CSV;

-- Infer schema from file
SELECT *
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@~/customers/staged/MOCK1.csv',
        FILE_FORMAT => 'my_csv_format'
    )
);

-- Query file directly
SELECT $1, $2, $3, $4, $5, $6, $7
FROM @~/customers/staged/MOCK1.csv
(FILE_FORMAT => 'my_csv_format');


/*=====================================================
  7️⃣  CREATE TRANSFORMED TABLE (AUTOINCREMENT)
=====================================================*/

CREATE OR REPLACE TABLE new_table2 (
    index NUMBER AUTOINCREMENT START 2 INCREMENT 10,
    customer VARCHAR,
    full_name VARCHAR,
    region VARCHAR,
    email VARCHAR,
    gender VARCHAR,
    "ORDER" DECIMAL(15,3),
    extra_col BINARY
);


/*=====================================================
  8️⃣  LOAD WITH TRANSFORMATIONS
=====================================================*/

COPY INTO new_table2 (
    customer,
    full_name,
    region,
    email,
    gender,
    "ORDER",
    extra_col
)
FROM (
    SELECT
        t.$1,
        CONCAT(t.$2, ' ', t.$3),
        t.$4,
        t.$5,
        t.$6,
        CAST(t.$7 AS INTEGER),
        TO_BINARY(t.$4, 'utf-8')
    FROM @~/customers/staged/MOCK1.csv t
)
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
)
TRUNCATECOLUMNS = TRUE
FORCE = TRUE;


/*=====================================================
  9️⃣  VALIDATION
=====================================================*/

SELECT * FROM new_table2;


/*=====================================================
  🔟  ALTER STRUCTURE FOR TRUNCATION TEST
=====================================================*/

TRUNCATE TABLE new_table2;

CREATE OR REPLACE TABLE new_table2 (
    index NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    customer VARCHAR,
    full_name VARCHAR(8),
    region VARCHAR,
    email VARCHAR,
    gender VARCHAR,
    "ORDER" DECIMAL(15,3),
    extra_col BINARY
);


/*=====================================================
  1️⃣1️⃣  LOAD WITH COLUMN TRUNCATION ENABLED (full_name)
=====================================================*/

COPY INTO new_table2 (
    customer,
    full_name,
    region,
    email,
    gender,
    "ORDER",
    extra_col
)
FROM (
    SELECT
        t.$1,
        CONCAT(t.$2, ' ', t.$3),
        t.$4,
        t.$5,
        t.$6,
        CAST(t.$7 AS INTEGER),
        TO_BINARY(t.$4, 'utf-8')
    FROM @~/customers/staged/MOCK1.csv t
)
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
)
TRUNCATECOLUMNS = TRUE
FORCE = TRUE;
