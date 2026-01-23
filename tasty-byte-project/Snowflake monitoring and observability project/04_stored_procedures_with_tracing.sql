/*==============================================================================
  Tasty Bytes Stored Procedures with Distributed Tracing
  
  This script creates stored procedures with comprehensive distributed tracing
  using OpenTelemetry concepts including:
  - Trace IDs and Span IDs
  - Span attributes and events
  - Performance metrics
  - Error tracking
==============================================================================*/

USE ROLE accountadmin;
USE DATABASE tasty_bytes;
USE SCHEMA harmonized;

-- Enable tracing
ALTER SESSION SET TRACE_LEVEL = ALWAYS;

/*--
  Procedure with Full Tracing: Customer Activity Analysis
--*/

CREATE OR REPLACE PROCEDURE tasty_bytes.harmonized.analyze_customer_activity()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'analyze_customer_activity'
PACKAGES = ('snowflake-snowpark-python', 'snowflake-telemetry-python')
AS
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
import logging
from snowflake import telemetry
import uuid
from datetime import datetime

def analyze_customer_activity(session: Session) -> str:
    logger = logging.getLogger('analyze_customer_activity')
    
    # Generate trace ID
    trace_id = str(uuid.uuid4())
    
    # Set root span attributes
    telemetry.set_span_attribute("procedure", "analyze_customer_activity")
    telemetry.set_span_attribute("trace_id", trace_id)
    telemetry.set_span_attribute("database", "tasty_bytes")
    
    start_time = datetime.now()
    
    try:
        # SPAN 1: Query stream
        telemetry.set_span_attribute("process_step", "query_stream")
        telemetry.add_event("stream_query_begin", {
            "description": "Starting to query order_header_stream",
            "stream_name": "order_header_stream"
        })
        
        logger.info("Querying order_header_stream")
        new_orders = session.table("tasty_bytes.raw_pos.order_header_stream") \
                           .filter(F.col("METADATA$ACTION") == "INSERT")
        
        order_count = new_orders.count()
        
        telemetry.set_span_attribute("orders_in_stream", order_count)
        telemetry.add_event("stream_query_complete", {
            "description": "Completed stream query",
            "record_count": order_count
        })
        
        logger.info(f"Found {order_count} new orders")
        
        if order_count == 0:
            telemetry.add_event("early_exit", {
                "reason": "No new orders to process"
            })
            return "No new orders to process"
        
        # SPAN 2: Enrich with order details
        telemetry.set_span_attribute("process_step", "enrich_orders")
        telemetry.add_event("enrichment_begin", {
            "description": "Joining orders with order details and menu"
        })
        
        logger.info("Enriching orders with order details and menu items")
        order_details = session.table("tasty_bytes.raw_pos.order_detail")
        menu = session.table("tasty_bytes.raw_pos.menu")
        
        enriched_orders = new_orders.join(
            order_details,
            new_orders["ORDER_ID"] == order_details["ORDER_ID"]
        ).join(
            menu,
            order_details["MENU_ITEM_ID"] == menu["MENU_ITEM_ID"]
        ).select(
            new_orders["CUSTOMER_ID"],
            F.date_trunc('DAY', new_orders["ORDER_TS"]).cast("DATE").alias("ACTIVITY_DATE"),
            new_orders["ORDER_ID"],
            F.coalesce(new_orders["ORDER_TOTAL"], F.lit(0)).alias("ORDER_TOTAL"),
            menu["MENU_ITEM_NAME"],
            new_orders["LOCATION_ID"],
            new_orders["ORDER_TS"]
        )
        
        enriched_count = enriched_orders.count()
        telemetry.set_span_attribute("enriched_records", enriched_count)
        telemetry.add_event("enrichment_complete", {
            "description": "Completed order enrichment",
            "enriched_records": enriched_count
        })
        
        # SPAN 3: Aggregate customer metrics
        telemetry.set_span_attribute("process_step", "aggregate_metrics")
        telemetry.add_event("aggregation_begin", {
            "description": "Aggregating customer activity metrics"
        })
        
        logger.info("Aggregating customer activity metrics")
        customer_activity = enriched_orders.group_by(
            "CUSTOMER_ID", "ACTIVITY_DATE"
        ).agg(
            F.count("ORDER_ID").alias("ORDER_COUNT"),
            F.sum("ORDER_TOTAL").alias("TOTAL_SPENT"),
            F.array_agg(F.distinct("MENU_ITEM_NAME")).alias("FAVORITE_MENU_ITEMS"),
            F.array_agg(F.distinct("LOCATION_ID")).alias("VISITED_LOCATIONS"),
            F.max("ORDER_TS").alias("LAST_ORDER_TIMESTAMP")
        ).select(
            "CUSTOMER_ID",
            "ACTIVITY_DATE",
            "ORDER_COUNT",
            "TOTAL_SPENT",
            "FAVORITE_MENU_ITEMS",
            "VISITED_LOCATIONS",
            "LAST_ORDER_TIMESTAMP",
            F.current_timestamp().alias("LAST_UPDATED")
        )
        
        aggregated_count = customer_activity.count()
        telemetry.set_span_attribute("aggregated_customers", aggregated_count)
        telemetry.add_event("aggregation_complete", {
            "description": "Completed customer aggregation",
            "customer_count": aggregated_count
        })
        
        # SPAN 4: Write to table
        telemetry.set_span_attribute("process_step", "write_table")
        telemetry.add_event("write_begin", {
            "description": "Writing to customer_activity_summary table",
            "target_table": "customer_activity_summary"
        })
        
        logger.info("Writing to customer_activity_summary table")
        customer_activity.write.mode("append").save_as_table(
            "tasty_bytes.harmonized.customer_activity_summary"
        )
        
        telemetry.add_event("write_complete", {
            "description": "Successfully wrote customer activity data",
            "rows_written": aggregated_count
        })
        
        # Calculate execution metrics
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds() * 1000
        
        telemetry.set_span_attribute("execution_time_ms", execution_time)
        telemetry.set_span_attribute("records_processed", order_count)
        
        # SPAN 5: Log to telemetry
        telemetry.set_span_attribute("process_step", "log_telemetry")
        telemetry.add_event("logging_begin", {
            "description": "Writing trace data to pipeline_events"
        })
        
        logger.info("Logging execution to pipeline_events")
        
        # Create detailed trace entry
        trace_entry = session.create_dataframe([[
            'SPAN',
            'INFO',
            'analyze_customer_activity',
            trace_id,
            str(uuid.uuid4()),
            'complete',
            f'Analyzed {order_count} orders for {aggregated_count} customers',
            {
                "orders_processed": order_count,
                "customers_analyzed": aggregated_count,
                "enriched_records": enriched_count,
                "execution_time_ms": execution_time
            },
            False,
            None,
            execution_time,
            order_count
        ]], schema=[
            "record_type", "severity_level", "procedure_name", "trace_id", 
            "span_id", "process_step", "message", "attributes", "error_flag", 
            "error_message", "execution_time_ms", "records_processed"
        ])
        
        trace_entry.write.mode("append").save_as_table("tasty_bytes.telemetry.pipeline_events")
        
        telemetry.add_event("procedure_complete", {
            "description": "Procedure completed successfully",
            "orders_processed": order_count,
            "customers_analyzed": aggregated_count,
            "execution_time_ms": execution_time
        })
        
        logger.info("Procedure completed successfully")
        return f"Analyzed {order_count} orders for {aggregated_count} customers. Execution time: {execution_time:.2f}ms"
    
    except Exception as e:
        error_msg = str(e)
        
        # Record error in trace
        telemetry.set_span_attribute("error", True)
        telemetry.set_span_attribute("error_message", error_msg)
        telemetry.add_event("procedure_error", {
            "description": "Error during procedure execution",
            "error_message": error_msg,
            "error_type": type(e).__name__
        })
        
        logger.error(f"Error: {error_msg}")
        
        # Log error to telemetry
        try:
            error_entry = session.create_dataframe([[
                'SPAN',
                'ERROR',
                'analyze_customer_activity',
                trace_id,
                str(uuid.uuid4()),
                'error',
                'Procedure failed with exception',
                {"error_type": type(e).__name__, "error_message": error_msg},
                True,
                error_msg,
                None,
                None
            ]], schema=[
                "record_type", "severity_level", "procedure_name", "trace_id", 
                "span_id", "process_step", "message", "attributes", "error_flag", 
                "error_message", "execution_time_ms", "records_processed"
            ])
            error_entry.write.mode("append").save_as_table("tasty_bytes.telemetry.pipeline_events")
        except:
            pass
        
        raise
$$;

/*--
  Procedure with Tracing: Menu Item Performance
--*/

CREATE OR REPLACE PROCEDURE tasty_bytes.harmonized.analyze_menu_performance()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'analyze_menu_performance'
PACKAGES = ('snowflake-snowpark-python', 'snowflake-telemetry-python')
AS
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
import logging
from snowflake import telemetry
import uuid
from datetime import datetime

def analyze_menu_performance(session: Session) -> str:
    logger = logging.getLogger('analyze_menu_performance')
    
    trace_id = str(uuid.uuid4())
    
    telemetry.set_span_attribute("procedure", "analyze_menu_performance")
    telemetry.set_span_attribute("trace_id", trace_id)
    
    start_time = datetime.now()
    
    try:
        # Query order detail stream
        telemetry.set_span_attribute("process_step", "query_stream")
        telemetry.add_event("stream_query_begin", {
            "stream": "order_detail_stream"
        })
        
        logger.info("Querying order_detail_stream")
        new_details = session.table("tasty_bytes.raw_pos.order_detail_stream") \
                            .filter(F.col("METADATA$ACTION") == "INSERT")
        
        detail_count = new_details.count()
        telemetry.set_span_attribute("order_details_count", detail_count)
        
        if detail_count == 0:
            return "No new order details to process"
        
        # Enrich with menu and order header data
        telemetry.set_span_attribute("process_step", "enrich_data")
        telemetry.add_event("enrichment_begin", {})
        
        logger.info("Enriching with menu and order data")
        menu = session.table("tasty_bytes.raw_pos.menu")
        order_header = session.table("tasty_bytes.raw_pos.order_header")
        
        enriched = new_details.join(
            menu,
            new_details["MENU_ITEM_ID"] == menu["MENU_ITEM_ID"]
        ).join(
            order_header,
            new_details["ORDER_ID"] == order_header["ORDER_ID"]
        ).select(
            F.date_trunc('DAY', order_header["ORDER_TS"]).cast("DATE").alias("PERFORMANCE_DATE"),
            menu["MENU_ITEM_ID"],
            menu["MENU_ITEM_NAME"],
            menu["TRUCK_BRAND_NAME"],
            new_details["QUANTITY"],
            new_details["PRICE"]
        )
        
        # Aggregate menu performance
        telemetry.set_span_attribute("process_step", "aggregate")
        telemetry.add_event("aggregation_begin", {})
        
        logger.info("Aggregating menu item performance")
        menu_performance = enriched.group_by(
            "PERFORMANCE_DATE", "MENU_ITEM_ID", "MENU_ITEM_NAME", "TRUCK_BRAND_NAME"
        ).agg(
            F.count("*").alias("TIMES_ORDERED"),
            F.sum("QUANTITY").alias("TOTAL_QUANTITY"),
            F.sum("PRICE").alias("TOTAL_REVENUE"),
            F.avg("PRICE").alias("AVG_PRICE")
        ).select(
            "PERFORMANCE_DATE",
            "MENU_ITEM_ID",
            "MENU_ITEM_NAME",
            "TRUCK_BRAND_NAME",
            "TIMES_ORDERED",
            "TOTAL_QUANTITY",
            "TOTAL_REVENUE",
            "AVG_PRICE",
            F.current_timestamp().alias("LAST_UPDATED")
        )
        
        # Write to table
        telemetry.set_span_attribute("process_step", "write_table")
        
        logger.info("Writing to menu_item_performance table")
        menu_performance.write.mode("append").save_as_table(
            "tasty_bytes.harmonized.menu_item_performance"
        )
        
        rows_written = menu_performance.count()
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds() * 1000
        
        telemetry.set_span_attribute("execution_time_ms", execution_time)
        telemetry.add_event("procedure_complete", {
            "detail_records_processed": detail_count,
            "menu_items_analyzed": rows_written
        })
        
        # Log trace
        trace_entry = session.create_dataframe([[
            'SPAN', 'INFO', 'analyze_menu_performance', trace_id, str(uuid.uuid4()),
            'complete', f'Analyzed {detail_count} order details for {rows_written} menu items',
            {"details_processed": detail_count, "menu_items": rows_written},
            False, None, execution_time, detail_count
        ]], schema=[
            "record_type", "severity_level", "procedure_name", "trace_id", 
            "span_id", "process_step", "message", "attributes", "error_flag", 
            "error_message", "execution_time_ms", "records_processed"
        ])
        trace_entry.write.mode("append").save_as_table("tasty_bytes.telemetry.pipeline_events")
        
        return f"Analyzed {detail_count} order details for {rows_written} menu items"
    
    except Exception as e:
        error_msg = str(e)
        telemetry.set_span_attribute("error", True)
        telemetry.set_span_attribute("error_message", error_msg)
        logger.error(f"Error: {error_msg}")
        raise
$$;

/*--
  Test the Traced Procedures
--*/

-- Insert test data into order_detail
INSERT INTO tasty_bytes.raw_pos.order_detail (
    ORDER_DETAIL_ID, ORDER_ID, MENU_ITEM_ID, DISCOUNT_ID, 
    LINE_NUMBER, QUANTITY, UNIT_PRICE, PRICE, ORDER_ITEM_DISCOUNT_AMOUNT
) VALUES 
(90001, 9001, 1, NULL, 1, 2, 12.50, 25.00, '0.00'),
(90002, 9001, 5, NULL, 2, 1, 8.75, 8.75, '0.00'),
(90003, 9002, 3, NULL, 1, 3, 10.25, 30.75, '0.00');

-- Call traced procedures
CALL tasty_bytes.harmonized.analyze_customer_activity();
CALL tasty_bytes.harmonized.analyze_menu_performance();

-- Query trace data
SELECT * FROM tasty_bytes.telemetry.pipeline_events 
WHERE record_type = 'SPAN'
ORDER BY event_timestamp DESC 
LIMIT 20;

-- View trace details with attributes
SELECT 
    event_timestamp,
    procedure_name,
    trace_id,
    process_step,
    message,
    attributes,
    execution_time_ms,
    records_processed
FROM tasty_bytes.telemetry.pipeline_events 
WHERE record_type = 'SPAN'
ORDER BY event_timestamp DESC;

SELECT 'Stored procedures with distributed tracing created successfully!' AS status;
