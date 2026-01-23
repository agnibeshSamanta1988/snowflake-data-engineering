

USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE tasty_bytes;

-- Query to explore sales in the city of Hamburg, Germany
WITH _feb_date_dim AS 
    (
    SELECT DATEADD(DAY, SEQ4(), '2022-02-01') AS date FROM TABLE(GENERATOR(ROWCOUNT => 28))
    )
SELECT
    fdd.date,
    ZEROIFNULL(SUM(o.price)) AS daily_sales
FROM _feb_date_dim fdd
LEFT JOIN analytics.orders_v o
    ON fdd.date = DATE(o.order_ts)
    AND o.country = 'Germany'
    AND o.primary_city = 'Hamburg'
WHERE fdd.date BETWEEN '2022-02-01' AND '2022-02-28'
GROUP BY fdd.date
ORDER BY fdd.date ASC;

-- Create view that adds weather data for cities where Tasty Bytes operates
CREATE OR REPLACE VIEW tasty_bytes.harmonized.daily_weather_v
COMMENT = 'Weather Source Daily History filtered to Tasty Bytes supported Cities'
    AS
SELECT
    hd.*,
    TO_VARCHAR(hd.date_valid_std, 'YYYY-MM') AS yyyy_mm,
    pc.city_name AS city,
    c.country AS country_desc
FROM FROSTBYTE_WEATHERSOURCE.onpoint_id.history_day hd
JOIN FROSTBYTE_WEATHERSOURCE.onpoint_id.postal_codes pc
    ON pc.postal_code = hd.postal_code
    AND pc.country = hd.country
JOIN TASTY_BYTES.raw_pos.country c
    ON c.iso_country = hd.country
    AND c.city = hd.city_name;


-- Query the view to explore daily temperatures in Hamburg, Germany for anomalies
SELECT
    dw.country_desc,
    dw.city_name,
    dw.date_valid_std,
    AVG(dw.avg_temperature_air_2m_f) AS avg_temperature_air_2m_f
FROM harmonized.daily_weather_v dw
WHERE 1=1
    AND dw.country_desc = 'Germany'
    AND dw.city_name = 'Hamburg'
    AND YEAR(date_valid_std) = '2022'
    AND MONTH(date_valid_std) = '2' -- February
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;

-- Query the view to explore wind speeds in Hamburg, Germany for anomalies
SELECT
    dw.country_desc,
    dw.city_name,
    dw.date_valid_std,
    MAX(dw.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM tasty_bytes.harmonized.daily_weather_v dw
WHERE 1=1
    AND dw.country_desc IN ('Germany')
    AND dw.city_name = 'Hamburg'
    AND YEAR(date_valid_std) = '2022'
    AND MONTH(date_valid_std) = '2' -- February
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;

-- Create a view that tracks windspeed for Hamburg, Germany
CREATE OR REPLACE VIEW tasty_bytes.harmonized.windspeed_hamburg
    AS
SELECT
    dw.country_desc,
    dw.city_name,
    dw.date_valid_std,
    MAX(dw.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM harmonized.daily_weather_v dw
WHERE 1=1
    AND dw.country_desc IN ('Germany')
    AND dw.city_name = 'Hamburg'
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;


--============================================================================
--============================================================================
--============================================================================

USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE tasty_bytes;


--Function to convert fahrenheit to celsius
CREATE OR REPLACE FUNCTION tasty_bytes.analytics.fahrenheit_to_celsius(temp_f NUMBER(35,4))
  RETURNS NUMBER(35,4)
  AS
  $$
    (temp_f - 32) * (5/9)
  $$
;

--Function to convert inch to millimeter
CREATE OR REPLACE FUNCTION tasty_bytes.analytics.inch_to_millimeter(inch NUMBER(35,4))
  RETURNS NUMBER(35,4)
  AS
  $$
    inch * 25.4
  $$
;


-- Apply UDFs and confirm successful execution
CREATE OR REPLACE VIEW tasty_bytes.harmonized.weather_hamburg
AS
SELECT
    fd.date_valid_std AS date,
    fd.city_name,
    fd.country_desc,
    ZEROIFNULL(SUM(odv.price)) AS daily_sales,
    ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,
    ROUND(AVG(analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
    ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
    ROUND(AVG(analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
    MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM tasty_bytes.harmonized.daily_weather_v fd
LEFT JOIN tasty_bytes.harmonized.orders_v odv
    ON fd.date_valid_std = DATE(odv.order_ts)
    AND fd.city_name = odv.primary_city
    AND fd.country_desc = odv.country
WHERE 1=1
    AND fd.country_desc = 'Germany'
    AND fd.city = 'Hamburg'
    AND fd.yyyy_mm = '2022-02'
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc
ORDER BY fd.date_valid_std ASC;


SELECT * FROM tasty_bytes.harmonized.weather_hamburg LIMIT 10;

-- Expand tracking to all cities and deploy view with this new information
CREATE OR REPLACE VIEW tasty_bytes.analytics.daily_city_metrics_v
COMMENT = 'Daily Weather Metrics and Orders Data'
AS
SELECT
    fd.date_valid_std AS date,
    fd.city_name,
    fd.country_desc,
    ZEROIFNULL(SUM(odv.price)) AS daily_sales,
    ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,
    ROUND(AVG(tasty_bytes.analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
    ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
    ROUND(AVG(tasty_bytes.analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
    MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM tasty_bytes.harmonized.daily_weather_v fd
LEFT JOIN tasty_bytes.harmonized.orders_v odv
    ON fd.date_valid_std = DATE(odv.order_ts)
    AND fd.city_name = odv.primary_city
    AND fd.country_desc = odv.country
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc;

select * from tasty_bytes.analytics.daily_city_metrics_v limit 10;


--============================================================================
--====Snowflake standard table STREAM used for Change Data Capture (CDC)======
--============================================================================

USE SCHEMA raw_pos;

CREATE OR REPLACE STREAM tasty_bytes.raw_pos.order_header_stream 
    ON TABLE tasty_bytes.raw_pos.order_header;

INSERT INTO tasty_bytes.raw_pos.order_header (
    ORDER_ID, 
    TRUCK_ID, 
    LOCATION_ID, 
    CUSTOMER_ID, 
    DISCOUNT_ID, 
    SHIFT_ID, 
    SHIFT_START_TIME, 
    SHIFT_END_TIME, 
    ORDER_CHANNEL, 
    ORDER_TS, 
    SERVED_TS, 
    ORDER_CURRENCY, 
    ORDER_AMOUNT, 
    ORDER_TAX_AMOUNT, 
    ORDER_DISCOUNT_AMOUNT, 
    ORDER_TOTAL
) VALUES (
    123456789,                     -- ORDER_ID
    101,                           -- TRUCK_ID
    1234,                          -- LOCATION_ID
    null,                          -- CUSTOMER_ID
    null,                          -- DISCOUNT_ID
    123456789,                     -- SHIFT_ID
    '08:00:00',                    -- SHIFT_START_TIME
    '16:00:00',                    -- SHIFT_END_TIME
    null,                          -- ORDER_CHANNEL
    '2023-07-01 12:30:45',         -- ORDER_TS
    null,                          -- SERVED_TS
    'USD',                         -- ORDER_CURRENCY
    50.00,                         -- ORDER_AMOUNT
    null,                          -- ORDER_TAX_AMOUNT
    null,                          -- ORDER_DISCOUNT_AMOUNT
    52.50                          -- ORDER_TOTAL
);

SELECT * FROM tasty_bytes.raw_pos.order_header_stream;

DELETE FROM tasty_bytes.raw_pos.order_header WHERE order_id=123456789;

-- This won't return the deleted action in the stream because of how standard streams work
-- See: https://docs.snowflake.com/en/user-guide/streams-intro#types-of-streams
SELECT * FROM tasty_bytes.raw_pos.order_header_stream;



--============================================================================
--====Change Data Capture (CDC) with streams, incremental ELT pipelines======
--============================================================================

-- Create the stored procedure, define its logic with Snowpark for Python, write sales to raw_pos.daily_sales_hamburg_t

/*
It reads only newly inserted orders from order_header_stream, joins them with the location table, and filters orders for Hamburg, Germany. It then aggregates total sales per day and appends the results to daily_sales_hamburg_t, consuming the stream so the same orders are not processed again.
*/

CREATE OR REPLACE PROCEDURE tasty_bytes.raw_pos.process_order_headers_stream()
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.10'
  HANDLER ='process_order_headers_stream'
  PACKAGES = ('snowflake-snowpark-python')
AS
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session

def process_order_headers_stream(session: Session) -> float:
    # Query the stream
    recent_orders = session.table("tasty_bytes.raw_pos.order_header_stream").filter(F.col("METADATA$ACTION") == "INSERT")
    
    # Look up location of the orders in the stream using the LOCATIONS table
    locations = session.table("tasty_bytes.raw_pos.location")
    hamburg_orders = recent_orders.join(
        locations,
        recent_orders["LOCATION_ID"] == locations["LOCATION_ID"]
    ).filter(
        (locations["CITY"] == "Hamburg") &
        (locations["COUNTRY"] == "Germany")
    )
    
    # Calculate the sum of sales in Hamburg
    total_sales = hamburg_orders.group_by(F.date_trunc('DAY', F.col("ORDER_TS"))).agg(
        F.coalesce(F.sum("ORDER_TOTAL"), F.lit(0)).alias("total_sales")
    )
    
    # Select the columns with proper aliases and convert to date type
    daily_sales = total_sales.select(
        F.col("DATE_TRUNC('DAY', ORDER_TS)").cast("DATE").alias("DATE"),
        F.col("total_sales")
    )
    
    # Write the results to the DAILY_SALES_HAMBURG_T table
    total_sales.write.mode("append").save_as_table("tasty_bytes.raw_pos.daily_sales_hamburg_t")
    
    # Return a message indicating the operation was successful
    return "Daily sales for Hamburg, Germany have been successfully written to raw_pos.daily_sales_hamburg_t"
$$;


-- Insert dummy data for a sale occurring at a location in Hamburg
INSERT INTO tasty_bytes.raw_pos.order_header (
    ORDER_ID, 
    TRUCK_ID, 
    LOCATION_ID, 
    CUSTOMER_ID, 
    DISCOUNT_ID, 
    SHIFT_ID, 
    SHIFT_START_TIME, 
    SHIFT_END_TIME, 
    ORDER_CHANNEL, 
    ORDER_TS, 
    SERVED_TS, 
    ORDER_CURRENCY, 
    ORDER_AMOUNT, 
    ORDER_TAX_AMOUNT, 
    ORDER_DISCOUNT_AMOUNT, 
    ORDER_TOTAL
) VALUES (
    123456789,                     -- ORDER_ID
    101,                           -- TRUCK_ID
    4493,                          -- LOCATION_ID
    null,                          -- CUSTOMER_ID
    null,                          -- DISCOUNT_ID
    123456789,                     -- SHIFT_ID
    '08:00:00',                    -- SHIFT_START_TIME
    '16:00:00',                    -- SHIFT_END_TIME
    null,                          -- ORDER_CHANNEL
    '2023-07-01 12:30:45',         -- ORDER_TS
    null,                          -- SERVED_TS
    'USD',                         -- ORDER_CURRENCY
    41.30,                         -- ORDER_AMOUNT
    null,                          -- ORDER_TAX_AMOUNT
    null,                          -- ORDER_DISCOUNT_AMOUNT
    45.80                          -- ORDER_TOTAL
);

-- Confirm the insert
SELECT * FROM tasty_bytes.raw_pos.order_header WHERE location_id = 4493;

-- Call the stored procedure
CALL tasty_bytes.raw_pos.process_order_headers_stream();

-- Confirm the insert to the daily_sales_hamburg_t table
SELECT * FROM tasty_bytes.raw_pos.daily_sales_hamburg_t;


--============================================================================
--========================Snowflake:Dynamic Table=============================
--============================================================================



/*
This script creates a Dynamic Table in Snowflake that automatically maintains daily total sales for Hamburg, Germany, based on data in the order_header table

In Snowflake Dynamic Tables, TARGET_LAG controls how fresh the data should be compared to the source tables
TARGET_LAG = '5 minutes'
TARGET_LAG = '10 minutes'
TARGET_LAG = '30 minutes'
TARGET_LAG = '1 hour'
TARGET_LAG = '2 hours'
TARGET_LAG = '1 day'

*/
CREATE OR REPLACE DYNAMIC TABLE tasty_bytes.raw_pos.daily_sales_hamburg
WAREHOUSE = 'COMPUTE_WH'
TARGET_LAG = '1 minute'
AS
SELECT
    CAST(oh.ORDER_TS AS DATE) AS date,
    COALESCE(SUM(oh.ORDER_TOTAL), 0) AS total_sales
FROM
    tasty_bytes.raw_pos.order_header oh
JOIN
    tasty_bytes.raw_pos.location loc
ON
    oh.LOCATION_ID = loc.LOCATION_ID
WHERE
    loc.CITY = 'Hamburg'
    AND loc.COUNTRY = 'Germany'
GROUP BY
    CAST(oh.ORDER_TS AS DATE);

SELECT * FROM tasty_bytes.raw_pos.daily_sales_hamburg;

-- Insert dummy data for a sale occurring at a location in Hamburg
INSERT INTO tasty_bytes.raw_pos.order_header (
    ORDER_ID, 
    TRUCK_ID, 
    LOCATION_ID, 
    CUSTOMER_ID, 
    DISCOUNT_ID, 
    SHIFT_ID, 
    SHIFT_START_TIME, 
    SHIFT_END_TIME, 
    ORDER_CHANNEL, 
    ORDER_TS, 
    SERVED_TS, 
    ORDER_CURRENCY, 
    ORDER_AMOUNT, 
    ORDER_TAX_AMOUNT, 
    ORDER_DISCOUNT_AMOUNT, 
    ORDER_TOTAL
) VALUES (
    123456789,                     -- ORDER_ID
    101,                           -- TRUCK_ID
    4493,                          -- LOCATION_ID
    null,                          -- CUSTOMER_ID
    null,                          -- DISCOUNT_ID
    123456789,                     -- SHIFT_ID
    '08:00:00',                    -- SHIFT_START_TIME
    '16:00:00',                    -- SHIFT_END_TIME
    null,                          -- ORDER_CHANNEL
    '2024-03-09 12:30:45',         -- ORDER_TS
    null,                          -- SERVED_TS
    'USD',                         -- ORDER_CURRENCY
    12.00,                         -- ORDER_AMOUNT
    null,                          -- ORDER_TAX_AMOUNT
    null,                          -- ORDER_DISCOUNT_AMOUNT
    12.35                          -- ORDER_TOTAL
);

SELECT * FROM tasty_bytes.raw_pos.daily_sales_hamburg
order by date desc;

drop table tasty_bytes.raw_pos.daily_sales_hamburg;

select * from information_schema.columns;


--============================================================================
--========================Snowflake:TASK======================================
--============================================================================
/*
A Snowflake TASK is a serverless scheduling and orchestration object in Snowflake that automatically runs SQL statements based on time schedules, data availability, or both.

A Snowflake TASK is used to automate data pipelines by executing SQL such as:
1.INSERT
2.MERGE
3.UPDATE
4.DELETE
5.Stored procedures
6. Transformations that read from streams or tables

*/

-- Task that runs executes every minute
CREATE OR REPLACE TASK tasty_bytes.raw_pos.process_orders_header_sproc
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = 'USING CRON * * * * * UTC'
AS
CALL tasty_bytes.raw_pos.process_order_headers_stream();

-- Activate the task to run
ALTER TASK tasty_bytes.raw_pos.process_orders_header_sproc RESUME;

-- Query the table
SELECT * FROM tasty_bytes.raw_pos.daily_sales_hamburg_t;

-- Insert some dummy data into ORDER_HEADER
INSERT INTO tasty_bytes.raw_pos.order_header (
    ORDER_ID, 
    TRUCK_ID, 
    LOCATION_ID, 
    CUSTOMER_ID, 
    DISCOUNT_ID, 
    SHIFT_ID, 
    SHIFT_START_TIME, 
    SHIFT_END_TIME, 
    ORDER_CHANNEL, 
    ORDER_TS, 
    SERVED_TS, 
    ORDER_CURRENCY, 
    ORDER_AMOUNT, 
    ORDER_TAX_AMOUNT, 
    ORDER_DISCOUNT_AMOUNT, 
    ORDER_TOTAL
) VALUES (
    123456789,                     -- ORDER_ID
    101,                           -- TRUCK_ID
    4494,                          -- LOCATION_ID
    null,                          -- CUSTOMER_ID
    null,                          -- DISCOUNT_ID
    123456789,                     -- SHIFT_ID
    '08:00:00',                    -- SHIFT_START_TIME
    '16:00:00',                    -- SHIFT_END_TIME
    null,                          -- ORDER_CHANNEL
    '2024-01-12 12:30:45',         -- ORDER_TS
    null,                          -- SERVED_TS
    'USD',                         -- ORDER_CURRENCY
    22.00,                         -- ORDER_AMOUNT
    null,                          -- ORDER_TAX_AMOUNT
    null,                          -- ORDER_DISCOUNT_AMOUNT
    24.50                          -- ORDER_TOTAL
);

-- Wait 1 minute before running this, query the table once more
SELECT * FROM tasty_bytes.raw_pos.daily_sales_hamburg_t;

-- Suspend the task
ALTER TASK tasty_bytes.raw_pos.process_orders_header_sproc SUSPEND;


-- Optional: recreate the task such that it executes every 24 hours
-- CREATE OR REPLACE TASK tasty_bytes.raw_pos.process_orders_header_sproc
-- SCHEDULE = 'USING CRON 0 0 * * * UTC'
-- AS
-- CALL tasty_bytes.raw_pos.process_order_headers_stream();

-- Optional: Start the task
-- ALTER TASK tasty_bytes.raw_pos.process_orders_header_sproc RESUME;

-- Required: Stop the task if you started it using the command directly above this one
-- ALTER TASK tasty_bytes.raw_pos.process_orders_header_sproc SUSPEND;