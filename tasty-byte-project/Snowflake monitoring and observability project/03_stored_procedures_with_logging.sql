/*==============================================================================
  Tasty Bytes Stored Procedures with Logging (FIXED VERSION)
  
  This script creates stored procedures with comprehensive logging for:
  - Daily sales aggregation
  - Regional sales processing
  
  FIXES:
  - Simplified table references
  - Better error handling
  - Explicit schema qualification
  - Type casting fixes
==============================================================================*/

USE ROLE accountadmin;
USE DATABASE tasty_bytes;
USE SCHEMA harmonized;

-- Set account-level logging
ALTER ACCOUNT SET LOG_LEVEL = 'INFO';

/*--
  Procedure 1: Process Daily Sales from Order Stream (FIXED)
--*/

CREATE OR REPLACE PROCEDURE tasty_bytes.harmonized.process_daily_sales()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'process_daily_sales'
PACKAGES = ('snowflake-snowpark-python')
AS
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
import logging
from datetime import datetime

def process_daily_sales(session: Session) -> str:
    # Set up logging
    logger = logging.getLogger('process_daily_sales')
    
    try:
        start_time = datetime.now()
        logger.info("Starting process_daily_sales procedure")
        
        # Query the order_header stream for new records
        logger.info("Querying order_header_stream for new orders")
        
        # Use fully qualified table name
        stream_df = session.table("TASTY_BYTES.RAW_POS.ORDER_HEADER_STREAM")
        new_orders = stream_df.filter(F.col("METADATA$ACTION") == "INSERT")
        
        order_count = new_orders.count()
        logger.info(f"Found {order_count} new orders in stream")
        
        if order_count == 0:
            logger.info("No new orders to process")
            return "No new orders to process"
        
        # Get base order data with proper casting
        logger.info("Selecting and casting order fields")
        orders_base = new_orders.select(
            F.to_date(F.col("ORDER_TS")).alias("SALES_DATE"),
            F.col("ORDER_ID"),
            F.col("TRUCK_ID"),
            F.col("CUSTOMER_ID"),
            F.coalesce(F.col("ORDER_TOTAL"), F.lit(0)).cast("DECIMAL(38,4)").alias("ORDER_TOTAL"),
            F.coalesce(F.col("ORDER_TAX_AMOUNT"), F.lit(0)).cast("DECIMAL(38,4)").alias("ORDER_TAX_AMOUNT"),
            F.coalesce(F.col("ORDER_DISCOUNT_AMOUNT"), F.lit(0)).cast("DECIMAL(38,4)").alias("ORDER_DISCOUNT_AMOUNT")
        )
        
        # Join with truck table
        logger.info("Enriching with truck data")
        trucks = session.table("TASTY_BYTES.RAW_POS.TRUCK")
        
        enriched_orders = orders_base.join(
            trucks,
            orders_base["TRUCK_ID"] == trucks["TRUCK_ID"],
            "inner"
        ).select(
            orders_base["SALES_DATE"],
            orders_base["ORDER_ID"],
            orders_base["CUSTOMER_ID"],
            orders_base["ORDER_TOTAL"],
            orders_base["ORDER_TAX_AMOUNT"],
            orders_base["ORDER_DISCOUNT_AMOUNT"],
            trucks["PRIMARY_CITY"],
            trucks["REGION"],
            trucks["COUNTRY"],
            trucks["MENU_TYPE_ID"]
        )
        
        # Join with menu to get brand name
        logger.info("Enriching with menu/brand data")
        menu = session.table("TASTY_BYTES.RAW_POS.MENU")
        
        final_orders = enriched_orders.join(
            menu,
            enriched_orders["MENU_TYPE_ID"] == menu["MENU_TYPE_ID"],
            "inner"
        ).select(
            enriched_orders["SALES_DATE"],
            enriched_orders["ORDER_ID"],
            enriched_orders["CUSTOMER_ID"],
            enriched_orders["ORDER_TOTAL"],
            enriched_orders["ORDER_TAX_AMOUNT"],
            enriched_orders["ORDER_DISCOUNT_AMOUNT"],
            enriched_orders["PRIMARY_CITY"],
            enriched_orders["REGION"],
            enriched_orders["COUNTRY"],
            menu["TRUCK_BRAND_NAME"]
        ).distinct()
        
        # Aggregate by date, brand, and city
        logger.info("Aggregating sales data")
        daily_sales = final_orders.group_by(
            F.col("SALES_DATE"),
            F.col("TRUCK_BRAND_NAME"),
            F.col("PRIMARY_CITY"),
            F.col("REGION"),
            F.col("COUNTRY")
        ).agg(
            F.count(F.col("ORDER_ID")).alias("TOTAL_ORDERS"),
            F.sum(F.col("ORDER_TOTAL")).alias("TOTAL_REVENUE"),
            F.sum(F.col("ORDER_TAX_AMOUNT")).alias("TOTAL_TAX"),
            F.sum(F.col("ORDER_DISCOUNT_AMOUNT")).alias("TOTAL_DISCOUNT"),
            F.count_distinct(F.col("CUSTOMER_ID")).alias("UNIQUE_CUSTOMERS"),
            F.avg(F.col("ORDER_TOTAL")).alias("AVG_ORDER_VALUE")
        ).select(
            F.col("SALES_DATE"),
            F.col("TRUCK_BRAND_NAME"),
            F.col("PRIMARY_CITY"),
            F.col("REGION"),
            F.col("COUNTRY"),
            F.col("TOTAL_ORDERS"),
            F.col("TOTAL_REVENUE"),
            F.col("TOTAL_TAX"),
            F.col("TOTAL_DISCOUNT"),
            F.col("UNIQUE_CUSTOMERS"),
            F.col("AVG_ORDER_VALUE"),
            F.current_timestamp().alias("LAST_UPDATED")
        )
        
        # Write to daily_sales_summary table
        logger.info("Writing aggregated sales to daily_sales_summary table")
        daily_sales.write.mode("append").save_as_table("TASTY_BYTES.HARMONIZED.DAILY_SALES_SUMMARY")
        
        rows_processed = daily_sales.count()
        
        # Calculate execution time
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds() * 1000
        
        # Log to pipeline_events table using SQL
        logger.info("Logging execution metrics to pipeline_events")
        
        log_sql = f"""
        INSERT INTO TASTY_BYTES.TELEMETRY.PIPELINE_EVENTS 
        (record_type, severity_level, procedure_name, process_step, message, 
         error_flag, execution_time_ms, records_processed)
        VALUES 
        ('LOG', 'INFO', 'process_daily_sales', 'complete',
         'Successfully processed {order_count} orders into {rows_processed} daily summaries',
         FALSE, {execution_time}, {order_count})
        """
        
        session.sql(log_sql).collect()
        
        logger.info("Procedure completed successfully")
        return f"Successfully processed {order_count} orders into {rows_processed} daily summaries. Execution time: {execution_time:.2f}ms"
    
    except Exception as e:
        error_msg = str(e).replace("'", "''")  # Escape single quotes for SQL
        logger.error(f"Error in process_daily_sales: {error_msg}")
        
        # Log error using SQL
        try:
            error_sql = f"""
            INSERT INTO TASTY_BYTES.TELEMETRY.PIPELINE_EVENTS 
            (record_type, severity_level, procedure_name, process_step, message, 
             error_flag, error_message)
            VALUES 
            ('LOG', 'ERROR', 'process_daily_sales', 'error',
             'Procedure failed with error', TRUE, '{error_msg[:500]}')
            """
            session.sql(error_sql).collect()
        except:
            pass
        
        raise
$$;

/*--
  Procedure 2: Process Regional Sales with Logging (FIXED)
--*/

CREATE OR REPLACE PROCEDURE tasty_bytes.harmonized.process_regional_sales()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'process_regional_sales'
PACKAGES = ('snowflake-snowpark-python')
AS
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
import logging
from datetime import datetime

def process_regional_sales(session: Session) -> str:
    logger = logging.getLogger('process_regional_sales')
    
    try:
        start_time = datetime.now()
        logger.info("Starting process_regional_sales procedure")
        
        # Query order stream with explicit casting
        logger.info("Querying order_header_stream")
        stream_df = session.table("TASTY_BYTES.RAW_POS.ORDER_HEADER_STREAM")
        new_orders = stream_df.filter(F.col("METADATA$ACTION") == "INSERT")
        
        order_count = new_orders.count()
        logger.info(f"Found {order_count} new orders")
        
        if order_count == 0:
            return "No new orders to process"
        
        # Select and cast order fields
        orders_base = new_orders.select(
            F.to_date(F.col("ORDER_TS")).alias("SALES_DATE"),
            F.col("ORDER_ID"),
            F.col("LOCATION_ID").cast("INTEGER"),
            F.col("CUSTOMER_ID"),
            F.col("TRUCK_ID"),
            F.coalesce(F.col("ORDER_TOTAL"), F.lit(0)).cast("DECIMAL(38,4)").alias("ORDER_TOTAL")
        )
        
        # Join with location table
        logger.info("Joining with location data")
        locations = session.table("TASTY_BYTES.RAW_POS.LOCATION")
        
        orders_with_location = orders_base.join(
            locations,
            orders_base["LOCATION_ID"] == locations["LOCATION_ID"],
            "inner"
        ).select(
            orders_base["SALES_DATE"],
            orders_base["ORDER_ID"],
            orders_base["CUSTOMER_ID"],
            orders_base["TRUCK_ID"],
            orders_base["ORDER_TOTAL"],
            locations["CITY"],
            locations["COUNTRY"]
        )
        
        # Join with truck table to get region
        logger.info("Joining with truck data for region")
        trucks = session.table("TASTY_BYTES.RAW_POS.TRUCK")
        
        regional_orders = orders_with_location.join(
            trucks,
            orders_with_location["TRUCK_ID"] == trucks["TRUCK_ID"],
            "inner"
        ).select(
            orders_with_location["SALES_DATE"],
            orders_with_location["ORDER_ID"],
            orders_with_location["CUSTOMER_ID"],
            orders_with_location["TRUCK_ID"],
            orders_with_location["ORDER_TOTAL"],
            orders_with_location["CITY"],
            orders_with_location["COUNTRY"],
            trucks["REGION"]
        )
        
        # Aggregate by region
        logger.info("Aggregating by region")
        regional_summary = regional_orders.group_by(
            F.col("SALES_DATE"),
            F.col("CITY"),
            F.col("COUNTRY"),
            F.col("REGION")
        ).agg(
            F.count(F.col("ORDER_ID")).alias("ORDER_COUNT"),
            F.sum(F.col("ORDER_TOTAL")).alias("TOTAL_REVENUE"),
            F.count_distinct(F.col("CUSTOMER_ID")).alias("UNIQUE_CUSTOMERS"),
            F.count_distinct(F.col("TRUCK_ID")).alias("UNIQUE_TRUCKS"),
            F.avg(F.col("ORDER_TOTAL")).alias("AVG_ORDER_VALUE")
        ).select(
            F.col("SALES_DATE"),
            F.col("CITY"),
            F.col("COUNTRY"),
            F.col("REGION"),
            F.col("ORDER_COUNT"),
            F.col("TOTAL_REVENUE"),
            F.col("UNIQUE_CUSTOMERS"),
            F.col("UNIQUE_TRUCKS"),
            F.col("AVG_ORDER_VALUE"),
            F.current_timestamp().alias("LAST_UPDATED")
        )
        
        # Write to table
        logger.info("Writing to regional_sales_summary table")
        regional_summary.write.mode("append").save_as_table("TASTY_BYTES.HARMONIZED.REGIONAL_SALES_SUMMARY")
        
        rows_processed = regional_summary.count()
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds() * 1000
        
        # Log execution using SQL
        log_sql = f"""
        INSERT INTO TASTY_BYTES.TELEMETRY.PIPELINE_EVENTS 
        (record_type, severity_level, procedure_name, process_step, message, 
         error_flag, execution_time_ms, records_processed)
        VALUES 
        ('LOG', 'INFO', 'process_regional_sales', 'complete',
         'Processed {order_count} orders into {rows_processed} regional summaries',
         FALSE, {execution_time}, {order_count})
        """
        
        session.sql(log_sql).collect()
        
        logger.info("Procedure completed successfully")
        return f"Processed {order_count} orders into {rows_processed} regional summaries"
    
    except Exception as e:
        error_msg = str(e).replace("'", "''")
        logger.error(f"Error: {error_msg}")
        
        try:
            error_sql = f"""
            INSERT INTO TASTY_BYTES.TELEMETRY.PIPELINE_EVENTS 
            (record_type, severity_level, procedure_name, process_step, message, 
             error_flag, error_message)
            VALUES 
            ('LOG', 'ERROR', 'process_regional_sales', 'error',
             'Procedure failed', TRUE, '{error_msg[:500]}')
            """
            session.sql(error_sql).collect()
        except:
            pass
        
        raise
$$;

/*--
  Simplified Test Data Insert
--*/

-- Make sure the stream exists and tables are ready
USE DATABASE tasty_bytes;
USE SCHEMA raw_pos;

-- Insert test data (ensure LOCATION_ID matches existing locations)
INSERT INTO TASTY_BYTES.RAW_POS.ORDER_HEADER (
    ORDER_ID, TRUCK_ID, LOCATION_ID, CUSTOMER_ID, DISCOUNT_ID, SHIFT_ID,
    SHIFT_START_TIME, SHIFT_END_TIME, ORDER_CHANNEL, ORDER_TS, SERVED_TS,
    ORDER_CURRENCY, ORDER_AMOUNT, ORDER_TAX_AMOUNT, ORDER_DISCOUNT_AMOUNT, ORDER_TOTAL
) 
SELECT 
    9001, 1, MIN(LOCATION_ID), 1, NULL, 1, 
    '08:00:00', '16:00:00', 'POS', 
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 
    'USD', 45.50, '3.64', '0.00', 49.14
FROM TASTY_BYTES.RAW_POS.LOCATION
LIMIT 1;

INSERT INTO TASTY_BYTES.RAW_POS.ORDER_HEADER (
    ORDER_ID, TRUCK_ID, LOCATION_ID, CUSTOMER_ID, DISCOUNT_ID, SHIFT_ID,
    SHIFT_START_TIME, SHIFT_END_TIME, ORDER_CHANNEL, ORDER_TS, SERVED_TS,
    ORDER_CURRENCY, ORDER_AMOUNT, ORDER_TAX_AMOUNT, ORDER_DISCOUNT_AMOUNT, ORDER_TOTAL
) 
SELECT 
    9002, 2, MIN(LOCATION_ID), 2, NULL, 2,
    '09:00:00', '17:00:00', 'MOBILE', 
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 
    'USD', 32.75, '2.62', '5.00', 30.37
FROM TASTY_BYTES.RAW_POS.LOCATION
LIMIT 1;

-- Call procedures
CALL tasty_bytes.harmonized.process_daily_sales();
CALL tasty_bytes.harmonized.process_regional_sales();

-- Check logs
SELECT * FROM tasty_bytes.telemetry.pipeline_events 
WHERE procedure_name IN ('process_daily_sales', 'process_regional_sales')
ORDER BY event_timestamp DESC 
LIMIT 10;

-- Check results
SELECT * FROM tasty_bytes.harmonized.daily_sales_summary ORDER BY last_updated DESC LIMIT 5;
SELECT * FROM tasty_bytes.harmonized.regional_sales_summary ORDER BY last_updated DESC LIMIT 5;

SELECT 'Stored procedures with logging created successfully!' AS status;
