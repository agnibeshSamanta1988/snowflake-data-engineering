# Snowflake SnowSQL Cheat Sheet

## Connection & Authentication

```bash
# Basic connection
snowsql -a <account_name> -u <username>

# Connection with database and schema
snowsql -a <account_name> -u <username> -d <database> -s <schema>

# Connection with warehouse
snowsql -a <account_name> -u <username> -w <warehouse>

# Connection with role
snowsql -a <account_name> -u <username> -r <role>

# Check SnowSQL location
where snowsql  # Windows
which snowsql  # Linux/Mac
```

## Database & Schema Operations

```sql
-- Use database
USE DATABASE LA_DB;

-- Use schema
USE SCHEMA LA_SCHEMA;

-- Create database
CREATE DATABASE my_database;

-- Create schema
CREATE SCHEMA my_schema;

-- Show databases
SHOW DATABASES;

-- Show schemas
SHOW SCHEMAS IN DATABASE LA_DB;

-- Show current context
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();
```

## Warehouse Management

```sql
-- Use warehouse
USE WAREHOUSE COMPUTE_WH;

-- Create warehouse
CREATE WAREHOUSE my_warehouse 
    WITH WAREHOUSE_SIZE = 'SMALL' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE;

-- Show warehouses
SHOW WAREHOUSES;

-- Suspend warehouse
ALTER WAREHOUSE COMPUTE_WH SUSPEND;

-- Resume warehouse
ALTER WAREHOUSE COMPUTE_WH RESUME;

-- Resize warehouse
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'MEDIUM';
```

## Table Operations

```sql
-- Query table with limit
SELECT * FROM customers LIMIT 10;

-- Count records
SELECT COUNT(*) FROM CUSTOMERS;

-- Truncate table (delete all data)
TRUNCATE TABLE CUSTOMERS;

-- Drop table
DROP TABLE IF EXISTS customers;

-- Create table
CREATE TABLE customers (
    customer INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    region VARCHAR(50),
    email VARCHAR(100),
    gender VARCHAR(10),
    order INT
);

-- Show tables
SHOW TABLES;

-- Describe table structure
DESC TABLE customers;

-- Show table details
SHOW TABLES LIKE 'customers';
```

## Stage Operations (Internal Stages)

```sql
-- List files in user stage
LIST @~;

-- List files in named stage
LIST @my_stage;

-- List files in table stage
LIST @%customers;

-- List files with pattern
LIST @~/customers/staged/;

-- Create named stage
CREATE STAGE my_stage;

-- Create stage with file format
CREATE STAGE my_stage 
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

-- Show stages
SHOW STAGES;

-- Remove file from stage
REMOVE @~/customers/staged/MOCK.csv;

-- Remove all files from stage
REMOVE @~/customers/staged/;
```

## File Upload (PUT Command)

```sql
-- Upload single file (no compression)
PUT file://F:\temp\MOCK.csv @~/customers/staged AUTO_COMPRESS = FALSE;

-- Upload with compression
PUT file://F:\temp\data.csv @~/staged;

-- Upload multiple files using wildcard
PUT file://F:\temp\*.csv @~/staged AUTO_COMPRESS = FALSE;

-- Upload to table stage
PUT file://F:\temp\data.csv @%customers;

-- Upload to named stage
PUT file://F:\temp\data.csv @my_stage;

-- Overwrite existing file
PUT file://F:\temp\data.csv @~/staged OVERWRITE = TRUE;
```

## Data Loading (COPY INTO)

```sql
-- Load from user stage (all files)
COPY INTO customers
    FROM @~/customers/staged
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

-- Load specific file
COPY INTO customers
    FROM @~/customers/staged/MOCK1.csv
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

-- Load with PURGE (deletes staged files after loading)
COPY INTO customers
    FROM @~/customers/staged/
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1) 
    PURGE = TRUE;

-- Load with pattern matching
COPY INTO customers
    FROM @~/customers/staged/
    PATTERN = '.*MOCK.*\.csv'
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);

-- Load with error handling
COPY INTO customers
    FROM @~/staged/
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    ON_ERROR = 'CONTINUE'
    ERROR_LIMIT = 10;

-- Load from external stage (S3)
COPY INTO customers
    FROM s3://mybucket/data/
    CREDENTIALS = (AWS_KEY_ID='xxx' AWS_SECRET_KEY='xxx')
    FILE_FORMAT = (TYPE = 'CSV');

-- Load with column mapping
COPY INTO customers (customer, first_name, last_name)
    FROM @~/staged/
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);
```

## File Formats

```sql
-- Create CSV file format
CREATE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE;

-- Create JSON file format
CREATE FILE FORMAT my_json_format
    TYPE = 'JSON'
    COMPRESSION = 'AUTO';

-- Create Parquet file format
CREATE FILE FORMAT my_parquet_format
    TYPE = 'PARQUET';

-- Use file format in COPY
COPY INTO customers
    FROM @~/staged/
    FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');

-- Show file formats
SHOW FILE FORMATS;
```

## Data Unloading (GET & COPY INTO Location)

```sql
-- Unload data to stage
COPY INTO @~/customers/staged/MOCK_latest
    FROM customers
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION = NONE)
    HEADER = TRUE
    SINGLE = FALSE
    MAX_FILE_SIZE = 5368709120;

-- Download file from stage to local (SnowSQL)
GET @~/customers/staged/MOCK_latest_0_0_0.csv file://F:\temp\;

-- Unload to S3
COPY INTO s3://mybucket/unload/
    FROM customers
    CREDENTIALS = (AWS_KEY_ID='xxx' AWS_SECRET_KEY='xxx')
    FILE_FORMAT = (TYPE = 'CSV' HEADER = TRUE);
```

## Query History & Monitoring

```sql
-- View query history
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
    ORDER BY start_time DESC
    LIMIT 10;

-- View running queries
SHOW QUERIES;

-- Cancel query
SELECT SYSTEM$CANCEL_QUERY('<query_id>');

-- View warehouse usage
SELECT * FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
    DATE_RANGE_START => DATEADD('days', -7, CURRENT_DATE()),
    DATE_RANGE_END => CURRENT_DATE()
));
```

## User & Role Management

```sql
-- Show current role
SELECT CURRENT_ROLE();

-- Use role
USE ROLE ACCOUNTADMIN;

-- Show roles
SHOW ROLES;

-- Grant role to user
GRANT ROLE data_analyst TO USER john_doe;

-- Create user
CREATE USER john_doe 
    PASSWORD = 'StrongPassword123'
    DEFAULT_ROLE = data_analyst;

-- Show users
SHOW USERS;
```

## Time Travel & Data Recovery

```sql
-- Query data from 5 minutes ago
SELECT * FROM customers AT(OFFSET => -60*5);

-- Query data as of specific timestamp
SELECT * FROM customers AT(TIMESTAMP => '2026-02-11 09:00:00'::TIMESTAMP);

-- Restore table from history
CREATE TABLE customers_restored CLONE customers 
    AT(OFFSET => -60*30);

-- Undrop table
UNDROP TABLE customers;

-- Undrop schema
UNDROP SCHEMA my_schema;
```

## Useful SnowSQL Commands

```sql
-- Help
!help

-- Exit SnowSQL
!exit
!quit

-- Execute SQL from file
!source /path/to/script.sql

-- Set output format
!set output_format=csv
!set output_format=json
!set output_format=psql

-- Enable query timing
!set timing=on

-- Set row limit for results
!set rowset_size=1000

-- Save output to file
!spool /path/to/output.txt
-- Run queries
!spool off
```

## Performance & Optimization

```sql
-- Show table statistics
SHOW TABLES LIKE 'customers';

-- Analyze query plan
EXPLAIN SELECT * FROM customers WHERE region = 'Spain';

-- Create clustering key
ALTER TABLE customers CLUSTER BY (region, customer);

-- Show clustering information
SELECT SYSTEM$CLUSTERING_INFORMATION('customers');

-- Automatic clustering
ALTER TABLE customers SUSPEND RECLUSTER;
ALTER TABLE customers RESUME RECLUSTER;
```

## Common Patterns from Your Session

```sql
-- 1. Setup environment
USE DATABASE LA_DB;
USE SCHEMA LA_SCHEMA;

-- 2. Check existing staged files
LIST @~;

-- 3. Upload files
PUT file://F:\temp\MOCK.csv @~/customers/staged AUTO_COMPRESS = FALSE;
PUT file://F:\temp\MOCK1.csv @~/customers/staged AUTO_COMPRESS = FALSE;

-- 4. Verify upload
LIST @~;

-- 5. Prepare table (if needed)
TRUNCATE TABLE CUSTOMERS;
SELECT COUNT(*) FROM CUSTOMERS;

-- 6. Load data with cleanup
COPY INTO customers
    FROM @~/customers/staged/
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1) 
    PURGE = TRUE;

-- 7. Verify load
SELECT COUNT(*) FROM CUSTOMERS;
SELECT * FROM CUSTOMERS LIMIT 10;
```

## Error Handling

```sql
-- View copy errors
SELECT * FROM TABLE(VALIDATE(customers, JOB_ID => '_last'));

-- Skip error rows during load
COPY INTO customers
    FROM @~/staged/
    FILE_FORMAT = (TYPE = 'CSV')
    ON_ERROR = 'SKIP_FILE'
    ERROR_LIMIT = 100;

-- Continue on errors
ON_ERROR = 'CONTINUE'

-- Abort on first error
ON_ERROR = 'ABORT_STATEMENT'
```

## Tips & Best Practices

1. **Always use PURGE = TRUE** when you don't need to keep staged files
2. **Set appropriate ERROR_LIMIT** to avoid loading bad data
3. **Use SKIP_HEADER = 1** for CSV files with headers
4. **Monitor warehouse usage** to optimize costs
5. **Use AUTO_COMPRESS = FALSE** only when compression causes issues
6. **List staged files** before and after operations
7. **Verify row counts** after COPY operations
8. **Use transactions** for critical operations
9. **Enable AUTO_RESUME** on warehouses to save costs
10. **Use Time Travel** for data recovery instead of manual backups
