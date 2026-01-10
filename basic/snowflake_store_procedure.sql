
USE ROLE accountadmin;

---> create tasty_bytes database
CREATE OR REPLACE DATABASE tasty_bytes;

---> create raw_pos schema
CREATE OR REPLACE SCHEMA tasty_bytes.raw_pos;


---> file format creation
CREATE OR REPLACE FILE FORMAT tasty_bytes.public.csv_ff
type = 'csv';

---> stage creation
CREATE OR REPLACE STAGE tasty_bytes.public.s3load
url = 's3://sfquickstarts/frostbyte_tastybytes/'
file_format = tasty_bytes.public.csv_ff;
---> example of creating an internal stage
-- CREATE OR REPLACE STAGE tasty_bytes.public.internal_stage_test;

---> list files in stage
ls @tasty_bytes.public.s3load;

---> order_header table build
CREATE OR REPLACE TABLE tasty_bytes.raw_pos.order_header
(
   order_id NUMBER(38,0),
   truck_id NUMBER(38,0),
   location_id FLOAT,
   customer_id NUMBER(38,0),
   discount_id VARCHAR(16777216),
   shift_id NUMBER(38,0),
   shift_start_time TIME(9),
   shift_end_time TIME(9),
   order_channel VARCHAR(16777216),
   order_ts TIMESTAMP_NTZ(9),
   served_ts VARCHAR(16777216),
   order_currency VARCHAR(3),
   order_amount NUMBER(38,4),
   order_tax_amount VARCHAR(16777216),
   order_discount_amount VARCHAR(16777216),
   order_total NUMBER(38,4)
);

---> order_header table load
COPY INTO tasty_bytes.raw_pos.order_header
FROM @tasty_bytes.public.s3load/raw_pos/order_header/;

SELECT
  CASE
    WHEN cnt >= 1000000000 THEN TO_VARCHAR(ROUND(cnt / 1000000000.0, 1)) || 'B'
    WHEN cnt >= 1000000    THEN TO_VARCHAR(ROUND(cnt / 1000000.0, 1)) || 'M'
    WHEN cnt >= 1000       THEN TO_VARCHAR(ROUND(cnt / 1000.0, 1)) || 'K'
    ELSE TO_VARCHAR(cnt)
  END AS formatted_count
FROM (
  SELECT COUNT(*) AS cnt
  FROM tasty_bytes.raw_pos.order_header
);


-- ============================================================
-- STEP 1: Review existing procedures
-- ============================================================
SHOW PROCEDURES;


-- ============================================================
-- STEP 2: Inspect sample data from ORDER_HEADER
-- ============================================================
SELECT *
FROM TASTY_BYTES.RAW_POS.ORDER_HEADER
LIMIT 100;


-- ============================================================
-- STEP 3: Identify earliest and latest order timestamps
-- ============================================================
SELECT
    MAX(ORDER_TS) AS MAX_ORDER_TS,
    MIN(ORDER_TS) AS MIN_ORDER_TS
FROM TASTY_BYTES.RAW_POS.ORDER_HEADER;


-- ============================================================
-- STEP 4: Store the maximum order timestamp
-- ============================================================
SET max_ts = (
    SELECT MAX(ORDER_TS)
    FROM TASTY_BYTES.RAW_POS.ORDER_HEADER
);

SELECT $max_ts;


-- ============================================================
-- STEP 5: Calculate timestamp 180 days before max timestamp
-- ============================================================
SELECT DATEADD('DAY', -180, $max_ts);


-- ============================================================
-- STEP 6: Store the cutoff timestamp (180 days back)
-- ============================================================
SET cutoff_ts = (
    SELECT DATEADD('DAY', -180, $max_ts)
);


-- ============================================================
-- STEP 7: Validate cutoff by checking max timestamp before it
-- ============================================================
SELECT MAX(ORDER_TS)
FROM TASTY_BYTES.RAW_POS.ORDER_HEADER
WHERE ORDER_TS < $cutoff_ts;


-- ============================================================
-- STEP 8: Switch to target database
-- ============================================================
USE DATABASE TASTY_BYTES;


-- ============================================================
-- STEP 9: Create stored procedure to delete old orders
-- ============================================================
CREATE OR REPLACE PROCEDURE delete_old()
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
DECLARE
    max_ts TIMESTAMP;
    cutoff_ts TIMESTAMP;
BEGIN
    max_ts := (
        SELECT MAX(ORDER_TS)
        FROM TASTY_BYTES.RAW_POS.ORDER_HEADER
    );

    cutoff_ts := (
        SELECT DATEADD('DAY', -180, :max_ts)
    );

    DELETE FROM TASTY_BYTES.RAW_POS.ORDER_HEADER
    WHERE ORDER_TS < :cutoff_ts;
END;
$$
;


-- ============================================================
-- STEP 10: Verify procedure creation
-- ============================================================
SHOW PROCEDURES;


-- ============================================================
-- STEP 11: Review procedure definition
-- ============================================================
DESCRIBE PROCEDURE delete_old();


-- ============================================================
-- STEP 12: Execute the stored procedure
-- ============================================================
CALL DELETE_OLD();


-- ============================================================
-- STEP 13: Validate deletion by checking new minimum timestamp
-- ============================================================
SELECT MIN(ORDER_TS)
FROM TASTY_BYTES.RAW_POS.ORDER_HEADER;


-- ============================================================
-- STEP 14: Display the cutoff timestamp used
-- ============================================================
SELECT $cutoff_ts;
