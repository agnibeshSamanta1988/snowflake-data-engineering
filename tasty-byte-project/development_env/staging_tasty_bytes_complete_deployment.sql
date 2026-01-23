/*******************************************************************************
 * TASTY BYTES - COMPLETE DEPLOYMENT SCRIPT
 * 
 * Description: Unified deployment script for the Tasty Bytes data platform
 * Pattern: Environment-based deployment with templated database objects
 * 
 * Usage: Replace staging with your environment prefix (e.g., DEV, PROD, TEST)
 *        before executing this script
 * 
 * Components:
 *   1. Database and Schema Setup
 *   2. Warehouse Configuration
 *   3. File Format and Stage Setup
 *   4. Raw Zone Tables
 *   5. Data Loading from S3
 *   6. User-Defined Functions
 *   7. Streams for CDC
 *   8. Harmonized Layer Views
 *   9. Analytics Layer Views
 *  10. Dynamic Tables
 *  11. Stored Procedures
 ******************************************************************************/

USE ROLE accountadmin;


/*******************************************************************************
 * SECTION 1: DATABASE AND SCHEMA CREATION
 ******************************************************************************/

-- Create main database
CREATE OR ALTER DATABASE staging_tasty_bytes;

-- Create schemas for data layers
CREATE OR ALTER SCHEMA staging_tasty_bytes.raw_pos;
CREATE OR ALTER SCHEMA staging_tasty_bytes.raw_customer;
CREATE OR ALTER SCHEMA staging_tasty_bytes.harmonized;
CREATE OR ALTER SCHEMA staging_tasty_bytes.analytics;


/*******************************************************************************
 * SECTION 2: WAREHOUSE CONFIGURATION
 ******************************************************************************/

CREATE OR REPLACE WAREHOUSE demo_build_wh
   WAREHOUSE_SIZE = 'xlarge'
   WAREHOUSE_TYPE = 'standard'
   AUTO_SUSPEND = 60
   AUTO_RESUME = TRUE
   INITIALLY_SUSPENDED = TRUE;


/*******************************************************************************
 * SECTION 3: FILE FORMAT AND STAGE SETUP
 ******************************************************************************/

CREATE OR ALTER FILE FORMAT staging_tasty_bytes.public.csv_ff
   type = 'csv';

CREATE OR REPLACE STAGE staging_tasty_bytes.public.s3load
   url = 's3://sfquickstarts/tasty-bytes-builder-education/'
   file_format = staging_tasty_bytes.public.csv_ff;


/*******************************************************************************
 * SECTION 4: RAW ZONE TABLE DEFINITIONS
 ******************************************************************************/

-- Country table
CREATE OR REPLACE TABLE staging_tasty_bytes.raw_pos.country
(
   country_id NUMBER(18,0),
   country VARCHAR(16777216),
   iso_currency VARCHAR(3),
   iso_country VARCHAR(2),
   city VARCHAR(16777216),
   city_population VARCHAR(16777216),
   city_id number(19,0)
);

-- Franchise table
CREATE OR ALTER TABLE staging_tasty_bytes.raw_pos.franchise
(
   franchise_id NUMBER(38,0),
   first_name VARCHAR(16777216),
   last_name VARCHAR(16777216),
   city VARCHAR(16777216),
   country VARCHAR(16777216),
   e_mail VARCHAR(16777216),
   phone_number VARCHAR(16777216)
);

-- Location table
CREATE OR ALTER TABLE staging_tasty_bytes.raw_pos.location
(
   location_id NUMBER(19,0),
   placekey VARCHAR(16777216),
   location VARCHAR(16777216),
   city VARCHAR(16777216),
   region VARCHAR(16777216),
   iso_country_code VARCHAR(16777216),
   country VARCHAR(16777216)
);

-- Menu table
CREATE OR ALTER TABLE staging_tasty_bytes.raw_pos.menu
(
   menu_id NUMBER(19,0),
   menu_type_id NUMBER(38,0),
   menu_type VARCHAR(16777216),
   truck_brand_name VARCHAR(16777216),
   menu_item_id NUMBER(38,0),
   menu_item_name VARCHAR(16777216),
   item_category VARCHAR(16777216),
   item_subcategory VARCHAR(16777216),
   cost_of_goods_usd NUMBER(38,4),
   sale_price_usd NUMBER(38,4),
   menu_item_health_metrics_obj VARIANT
);

-- Truck table
CREATE OR ALTER TABLE staging_tasty_bytes.raw_pos.truck
(
   truck_id NUMBER(38,0),
   menu_type_id NUMBER(38,0),
   primary_city VARCHAR(16777216),
   region VARCHAR(16777216),
   iso_region VARCHAR(16777216),
   country VARCHAR(16777216),
   iso_country_code VARCHAR(16777216),
   franchise_flag NUMBER(38,0),
   year NUMBER(38,0),
   make VARCHAR(16777216),
   model VARCHAR(16777216),
   ev_flag NUMBER(38,0),
   franchise_id NUMBER(38,0),
   truck_opening_date DATE
);

-- Order header table
CREATE OR ALTER TABLE staging_tasty_bytes.raw_pos.order_header
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

-- Order detail table
CREATE OR ALTER TABLE staging_tasty_bytes.raw_pos.order_detail
(
   order_detail_id NUMBER(38,0),
   order_id NUMBER(38,0),
   menu_item_id NUMBER(38,0),
   discount_id VARCHAR(16777216),
   line_number NUMBER(38,0),
   quantity NUMBER(5,0),
   unit_price NUMBER(38,4),
   price NUMBER(38,4),
   order_item_discount_amount VARCHAR(16777216)
);

-- Customer loyalty table
CREATE OR ALTER TABLE staging_tasty_bytes.raw_customer.customer_loyalty
(
   customer_id NUMBER(38,0),
   first_name VARCHAR(16777216),
   last_name VARCHAR(16777216),
   city VARCHAR(16777216),
   country VARCHAR(16777216),
   postal_code VARCHAR(16777216),
   preferred_language VARCHAR(16777216),
   gender VARCHAR(16777216),
   favourite_brand VARCHAR(16777216),
   marital_status VARCHAR(16777216),
   children_count VARCHAR(16777216),
   sign_up_date DATE,
   birthday_date DATE,
   e_mail VARCHAR(16777216),
   phone_number VARCHAR(16777216)
);


/*******************************************************************************
 * SECTION 5: DATA LOADING FROM S3
 ******************************************************************************/

USE WAREHOUSE demo_build_wh;

use warehouse COMPUTE_WH;

-- country table load
COPY INTO staging_tasty_bytes.raw_pos.country
(
    country_id,
    country,
    iso_currency,
    iso_country,
    city_id,
    city,
    city_population
 )
FROM @staging_tasty_bytes.public.s3load/raw_pos/country/;

-- Load franchise data
COPY INTO staging_tasty_bytes.raw_pos.franchise
FROM @staging_tasty_bytes.public.s3load/raw_pos/franchise/;

-- Load location data
COPY INTO staging_tasty_bytes.raw_pos.location
FROM @staging_tasty_bytes.public.s3load/raw_pos/location/;

-- Load menu data
COPY INTO staging_tasty_bytes.raw_pos.menu
FROM @staging_tasty_bytes.public.s3load/raw_pos/menu/;

-- Load truck data
COPY INTO staging_tasty_bytes.raw_pos.truck
FROM @staging_tasty_bytes.public.s3load/raw_pos/truck/;

-- Load customer loyalty data
COPY INTO staging_tasty_bytes.raw_customer.customer_loyalty
FROM @staging_tasty_bytes.public.s3load/raw_customer/customer_loyalty/;

-- Load order header data
COPY INTO staging_tasty_bytes.raw_pos.order_header
FROM @staging_tasty_bytes.public.s3load/raw_pos/subset_order_header/;

-- Load order detail data
COPY INTO staging_tasty_bytes.raw_pos.order_detail
FROM @staging_tasty_bytes.public.s3load/raw_pos/subset_order_detail/;


/*******************************************************************************
 * SECTION 6: USER-DEFINED FUNCTIONS
 ******************************************************************************/

-- Temperature conversion: Fahrenheit to Celsius
CREATE OR ALTER FUNCTION staging_tasty_bytes.analytics.fahrenheit_to_celsius(temp_f NUMBER(35,4))
   RETURNS NUMBER(35,4)
   AS
   $$
      (temp_f - 32) * (5/9)
   $$
;

-- Measurement conversion: Inches to Millimeters
CREATE OR ALTER FUNCTION staging_tasty_bytes.analytics.inch_to_millimeter(inch NUMBER(35,4))
   RETURNS NUMBER(35,4)
   AS
   $$
      inch * 25.4
   $$
;


/*******************************************************************************
 * SECTION 7: STREAMS FOR CHANGE DATA CAPTURE
 ******************************************************************************/

-- Stream on order_header table for CDC
CREATE OR REPLACE STREAM staging_tasty_bytes.raw_pos.order_header_stream 
   ON TABLE staging_tasty_bytes.raw_pos.order_header;


/*******************************************************************************
 * SECTION 8: HARMONIZED LAYER VIEWS
 ******************************************************************************/

-- Orders view - joins all relevant order information
CREATE OR REPLACE VIEW staging_tasty_bytes.harmonized.orders_v
   AS
SELECT
   oh.order_id,
   oh.truck_id,
   oh.order_ts,
   od.order_detail_id,
   od.line_number,
   m.truck_brand_name,
   m.menu_type,
   t.primary_city,
   t.region,
   t.country,
   t.franchise_flag,
   t.franchise_id,
   f.first_name AS franchisee_first_name,
   f.last_name AS franchisee_last_name,
   l.location_id,
   cl.customer_id,
   cl.first_name,
   cl.last_name,
   cl.e_mail,
   cl.phone_number,
   cl.children_count,
   cl.gender,
   cl.marital_status,
   od.menu_item_id,
   m.menu_item_name,
   od.quantity,
   od.unit_price,
   od.price,
   oh.order_amount,
   oh.order_tax_amount,
   oh.order_discount_amount,
   oh.order_total
FROM staging_tasty_bytes.raw_pos.order_detail od
JOIN staging_tasty_bytes.raw_pos.order_header oh
   ON od.order_id = oh.order_id
JOIN staging_tasty_bytes.raw_pos.truck t
   ON oh.truck_id = t.truck_id
JOIN staging_tasty_bytes.raw_pos.menu m
   ON od.menu_item_id = m.menu_item_id
JOIN staging_tasty_bytes.raw_pos.franchise f
   ON t.franchise_id = f.franchise_id
JOIN staging_tasty_bytes.raw_pos.location l
   ON oh.location_id = l.location_id
LEFT JOIN staging_tasty_bytes.raw_customer.customer_loyalty cl
   ON oh.customer_id = cl.customer_id;

-- Customer loyalty metrics view
CREATE OR REPLACE VIEW staging_tasty_bytes.harmonized.customer_loyalty_metrics_v
   AS
SELECT
   cl.customer_id,
   cl.city,
   cl.country,
   cl.first_name,
   cl.last_name,
   cl.phone_number,
   cl.e_mail,
   SUM(oh.order_total) AS total_sales,
   ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
FROM staging_tasty_bytes.raw_customer.customer_loyalty cl
JOIN staging_tasty_bytes.raw_pos.order_header oh
   ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
   cl.last_name, cl.phone_number, cl.e_mail;

-- Daily weather view (requires Weather Source data from Snowflake Marketplace)
CREATE OR REPLACE VIEW staging_tasty_bytes.harmonized.daily_weather_v
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
JOIN staging_tasty_bytes.raw_pos.country c
   ON c.iso_country = hd.country
   AND c.city = hd.city_name;

-- Weather data for Hamburg specifically
CREATE OR REPLACE VIEW staging_tasty_bytes.harmonized.weather_hamburg
   AS
SELECT
   fd.date_valid_std AS date,
   fd.city_name,
   fd.country_desc,
   ZEROIFNULL(SUM(odv.price)) AS daily_sales,
   ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,
   ROUND(AVG(staging_tasty_bytes.analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
   ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
   ROUND(AVG(staging_tasty_bytes.analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
   MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM staging_tasty_bytes.harmonized.daily_weather_v fd
LEFT JOIN staging_tasty_bytes.harmonized.orders_v odv
   ON fd.date_valid_std = DATE(odv.order_ts)
   AND fd.city_name = odv.primary_city
   AND fd.country_desc = odv.country
WHERE 1=1
   AND fd.country_desc = 'Germany'
   AND fd.city_name = 'Hamburg'
   AND fd.yyyy_mm = '2022-02'
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc
ORDER BY fd.date_valid_std ASC;

-- Windspeed tracking for Hamburg
CREATE OR REPLACE VIEW staging_tasty_bytes.harmonized.windspeed_hamburg
   AS
SELECT
   dw.country_desc,
   dw.city_name,
   dw.date_valid_std,
   MAX(dw.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM staging_tasty_bytes.harmonized.daily_weather_v dw
WHERE 1=1
   AND dw.country_desc IN ('Germany')
   AND dw.city_name = 'Hamburg'
GROUP BY dw.country_desc, dw.city_name, dw.date_valid_std
ORDER BY dw.date_valid_std DESC;


/*******************************************************************************
 * SECTION 9: ANALYTICS LAYER VIEWS
 ******************************************************************************/

-- Analytics orders view with date dimension
CREATE OR REPLACE VIEW staging_tasty_bytes.analytics.orders_v
   COMMENT = 'Tasty Bytes Order Detail View'
   AS
SELECT DATE(o.order_ts) AS date, * 
FROM staging_tasty_bytes.harmonized.orders_v o;

-- Analytics customer loyalty metrics view
CREATE OR REPLACE VIEW staging_tasty_bytes.analytics.customer_loyalty_metrics_v
   COMMENT = 'Tasty Bytes Customer Loyalty Member Metrics View'
   AS
SELECT * FROM staging_tasty_bytes.harmonized.customer_loyalty_metrics_v;

-- Daily city metrics with weather data
CREATE OR REPLACE VIEW staging_tasty_bytes.analytics.daily_city_metrics_v
   COMMENT = 'Daily Weather Metrics and Orders Data'
   AS
SELECT
   fd.date_valid_std AS date,
   fd.city_name,
   fd.country_desc,
   ZEROIFNULL(SUM(odv.price)) AS daily_sales,
   ROUND(AVG(fd.avg_temperature_air_2m_f),2) AS avg_temperature_fahrenheit,
   ROUND(AVG(staging_tasty_bytes.analytics.fahrenheit_to_celsius(fd.avg_temperature_air_2m_f)),2) AS avg_temperature_celsius,
   ROUND(AVG(fd.tot_precipitation_in),2) AS avg_precipitation_inches,
   ROUND(AVG(staging_tasty_bytes.analytics.inch_to_millimeter(fd.tot_precipitation_in)),2) AS avg_precipitation_millimeters,
   MAX(fd.max_wind_speed_100m_mph) AS max_wind_speed_100m_mph
FROM staging_tasty_bytes.harmonized.daily_weather_v fd
LEFT JOIN staging_tasty_bytes.harmonized.orders_v odv
   ON fd.date_valid_std = DATE(odv.order_ts)
   AND fd.city_name = odv.primary_city
   AND fd.country_desc = odv.country
GROUP BY fd.date_valid_std, fd.city_name, fd.country_desc;


/*******************************************************************************
 * SECTION 10: DYNAMIC TABLES
 ******************************************************************************/

-- Dynamic table for Hamburg daily sales (near real-time aggregation)
CREATE OR REPLACE DYNAMIC TABLE staging_tasty_bytes.raw_pos.daily_sales_hamburg
   WAREHOUSE = 'COMPUTE_WH'
   TARGET_LAG = '1 minute'
   AS
SELECT
   CAST(oh.ORDER_TS AS DATE) AS date,
   COALESCE(SUM(oh.ORDER_TOTAL), 0) AS total_sales
FROM
   staging_tasty_bytes.raw_pos.order_header oh
JOIN
   staging_tasty_bytes.raw_pos.location loc
ON
   oh.LOCATION_ID = loc.LOCATION_ID
WHERE
   loc.CITY = 'Hamburg'
   AND loc.COUNTRY = 'Germany'
GROUP BY
   CAST(oh.ORDER_TS AS DATE);


/*******************************************************************************
 * SECTION 11: STORED PROCEDURES
 ******************************************************************************/

-- Stored procedure to process order header stream
CREATE OR REPLACE PROCEDURE staging_tasty_bytes.raw_pos.process_order_headers_stream()
   RETURNS STRING
   LANGUAGE PYTHON
   RUNTIME_VERSION = '3.10'
   HANDLER = 'process_order_headers_stream'
   PACKAGES = ('snowflake-snowpark-python')
AS
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session

def process_order_headers_stream(session: Session) -> str:
    # Query the stream
    recent_orders = session.table("order_header_stream").filter(F.col("METADATA$ACTION") == "INSERT")
    
    # Look up location of the orders in the stream using the LOCATIONS table
    locations = session.table("location")
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
    daily_sales.write.mode("append").save_as_table("raw_pos.daily_sales_hamburg_t")
    
    # Return a message indicating the operation was successful
    return "Daily sales for Hamburg, Germany have been successfully written to raw_pos.daily_sales_hamburg_t"
$$;


/*******************************************************************************
 * DEPLOYMENT COMPLETE
 * 
 * Next Steps:
 *   1. Replace staging with your environment prefix (e.g., DEV, PROD)
 *   2. Ensure Weather Source data is available from Snowflake Marketplace
 *   3. Execute this script in your Snowflake environment
 *   4. Verify data loads completed successfully
 *   5. Test views and procedures
 ******************************************************************************/

