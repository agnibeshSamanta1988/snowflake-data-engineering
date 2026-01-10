/*===================================================================================
Snowpipe:
Snowpipe automates micro-batch data loading from external/internal stages into Snowflake tables using event notifications (S3, GCS, Azure) or REST API calls, enabling near-real-time analytics without managing a dedicated warehouse.

Basic CREATE PIPE example (requires stage and file format created first):
CREATE OR REPLACE PIPE mypipe 
  AUTO_INGEST = TRUE 
  AS 
  COPY INTO mytable 
  FROM @mystage 
  FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1);


==================================================================================*/





--In Snowflake, Azure auto-ingest requires TWO different integrations
--  1.STORAGE INTEGRATION
--  2.NOTIFICATION INTEGRATION
--https://www.youtube.com/watch?v=oGN6P6f_QUQ
-- =====================================================
-- Snowflake Azure Blob Storage Data Loading Pipeline
-- =====================================================

-- Step 1: Create Storage Integration for secure Azure access
USE ROLE ACCOUNTADMIN;

--1.STORAGE INTEGRATION
CREATE OR REPLACE STORAGE INTEGRATION snowflake_azure_blob_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '5748c9w6-8734-4d52-v6f9-gba16z09991a'
  STORAGE_ALLOWED_LOCATIONS = (
    'azure://azurestoragedata.blob.core.windows.net/project-snowflex-de/'
  );

-- Retrieve consent URL and App Name (AZURE_MULTI_TENANT_APP_NAME) for Azure Admin
DESC STORAGE INTEGRATION snowflake_azure_blob_integration;

--Grant "Storage Blob Data Contributor" access in azure to  AZURE_MULTI_TENANT
DESC INTEGRATION snowflake_azure_blob_integration;

-- =====================================================
-- Step 2: Switch to Project Context and Create File Format
-- =====================================================

USE DATABASE DE_DATABASE;

USE SCHEMA DE_PROJECT;

-- CSV File Format: Handles standard sales CSV with header, comma-delimited
CREATE OR REPLACE FILE FORMAT DE_DATABASE.DE_PROJECT.csv_fileformat
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  EMPTY_FIELD_AS_NULL = TRUE;

-- =====================================================
-- Step 3: Create External Stage referencing Integration
-- =====================================================

CREATE OR REPLACE STAGE DE_DATABASE.DE_PROJECT.stg_azure_container
  URL = 'azure://azurestoragedata.blob.core.windows.net/project-snowflex-de/'
  STORAGE_INTEGRATION = snowflake_azure_blob_integration
  FILE_FORMAT = DE_DATABASE.DE_PROJECT.csv_fileformat;

-- =====================================================
-- Step 4: Verify Stage Access and Preview Data
-- =====================================================
LIST @DE_DATABASE.DE_PROJECT.stg_azure_container;

SELECT
  $1::STRING AS row_sample,
  $2::STRING AS row_sample
FROM @stg_azure_container/sales_data.csv
  (FILE_FORMAT => 'csv_fileformat')
LIMIT 5;

SELECT
  $1 AS order_id,
  $2 AS order_date,
  $3 AS customer_name,
  $4 AS product,
  $5 AS quantity,
  $6 AS unit_price,
  $7 AS total_amount
FROM @DE_DATABASE.DE_PROJECT.stg_azure_container/sales_data.csv
  (FILE_FORMAT => 'DE_DATABASE.DE_PROJECT.csv_fileformat');


-- =====================================================
-- Step 5: Create Target Table (Superstore Sales Schema)
-- =====================================================

Create or replace table DE_DATABASE.DE_PROJECT.sales_data
AS SELECT
  $1 AS order_id,
  $2 AS order_date,
  $3 AS customer_name,
  $4 AS product,
  $5 AS quantity,
  $6 AS unit_price,
  $7 AS total_amount
FROM @DE_DATABASE.DE_PROJECT.stg_azure_container/sales_data.csv
  (FILE_FORMAT => 'DE_DATABASE.DE_PROJECT.csv_fileformat');

CREATE OR REPLACE TABLE DE_DATABASE.DE_PROJECT.sales_data
(
	order_id STRING,
	region STRING,
	country STRING,
	item_type STRING,
	sales_channel STRING,
	order_priority STRING,
	order_date DATE,
	ship_date DATE,
	units_sold NUMBER,
	unit_price NUMBER(10,2),
	unit_cost NUMBER(10,2),
	total_revenue NUMBER(14,2),
	total_cost NUMBER(14,2),
	total_profit NUMBER(14,2)
);

-- =====================================================
-- Step 6: Load Data using COPY INTO (Best Practice
-- =====================================================
COPY INTO DE_DATABASE.DE_PROJECT.sales_data
FROM @DE_DATABASE.DE_PROJECT.stg_azure_container
FILE_FORMAT = (FORMAT_NAME = DE_DATABASE.DE_PROJECT.csv_fileformat)
ON_ERROR = 'CONTINUE';

COPY INTO DE_DATABASE.DE_PROJECT.sales_data
FROM @DE_DATABASE.DE_PROJECT.stg_azure_container/sales_data.csv
FILE_FORMAT = (FORMAT_NAME = DE_DATABASE.DE_PROJECT.csv_fileformat);

SELECT * FROM DE_DATABASE.DE_PROJECT.sales_data LIMIT 100;


-- =====================================================
-- Step 7: Validate Load and Copy Results
-- =====================================================

SELECT * FROM DE_DATABASE.DE_PROJECT.sales_data LIMIT 100;

-- Check load history
SELECT 
  file_name,
  status,
  row_parsed AS rows_parsed,  -- Actual column is ROW_PARSED
  row_count AS rows_loaded,   -- ROW_COUNT for loaded rows
  error_count AS errors_seen  -- ERROR_COUNT for errors
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'DE_DATABASE.DE_PROJECT.sales_data', 
  START_TIME => DATEADD(HOURS, -1, CURRENT_TIMESTAMP())
));

-- Summary stats
SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT order_id) AS unique_orders,
  MIN(order_date) AS earliest_order,
  MAX(order_date) AS latest_order,
  SUM(total_revenue) AS total_revenue
FROM DE_DATABASE.DE_PROJECT.sales_data;

-- =====================================================
-- SNOWPIPE AUTO-INGEST SETUP (New Addition)
-- =====================================================

--2.NOTIFICATION INTEGRATION

CREATE OR REPLACE NOTIFICATION INTEGRATION snowpipe_notification_integration
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = 'AZURE_STORAGE_QUEUE'
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://azurestoragedata.queue.core.windows.net/salesdataqueue/'
  AZURE_TENANT_ID = '5748c9w6-8734-4d52-v6f9-gba16z09991a';

--Grant "Storage Queue Data Contributor" access in azure to  AZURE_MULTI_TENANT
DESC INTEGRATION snowpipe_notification_integration;


CREATE OR REPLACE PIPE DE_DATABASE.DE_PROJECT.sales_pipe
  AUTO_INGEST = TRUE
  INTEGRATION = snowpipe_notification_integration
  AS
  COPY INTO DE_DATABASE.DE_PROJECT.sales_data
  FROM @DE_DATABASE.DE_PROJECT.stg_azure_container
  FILE_FORMAT = (FORMAT_NAME = DE_DATABASE.DE_PROJECT.csv_fileformat);


DELETE FROM DE_DATABASE.DE_PROJECT.sales_data;

SELECT * FROM DE_DATABASE.DE_PROJECT.sales_data LIMIT 100;

SELECT COUNT(*) FROM DE_DATABASE.DE_PROJECT.sales_data;


SHOW PIPES IN DE_DATABASE.DE_PROJECT;

DESC PIPE DE_DATABASE.DE_PROJECT.sales_pipe;

ALTER PIPE DE_DATABASE.DE_PROJECT.sales_pipe REFRESH;  -- Catch existing files
ALTER PIPE DE_DATABASE.DE_PROJECT.sales_pipe SET PIPE_EXECUTION_PAUSED = FALSE;

DROP PIPE DE_DATABASE.DE_PROJECT.sales_pipe;