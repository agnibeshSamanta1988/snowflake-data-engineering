-- =====================================================
-- TESTING & MONITORING SCRIPT
-- Monitor and test Snowflake Data Engineering Project
-- =====================================================

USE DATABASE ECOMMERCE_DB;
USE WAREHOUSE DE_PROJECT_WH;

-- =====================================================
-- SECTION 1: RESUME TASKS (Run these first)
-- =====================================================

-- Resume all tasks
ALTER TASK ECOMMERCE_DB.STREAMS_TASKS.process_customer_changes RESUME;
ALTER TASK ECOMMERCE_DB.STREAMS_TASKS.load_fact_sales RESUME;
ALTER TASK ECOMMERCE_DB.STREAMS_TASKS.update_daily_summary RESUME;

-- =====================================================
-- SECTION 2: MONITOR TASKS
-- =====================================================

-- Check task status
SHOW TASKS IN SCHEMA STREAMS_TASKS;

-- View task history
SELECT 
    name,
    state,
    scheduled_time,
    completed_time,
    DATEDIFF('second', scheduled_time, completed_time) as duration_seconds,
    error_code,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 100
))
WHERE DATABASE_NAME = 'ECOMMERCE_DB'
ORDER BY scheduled_time DESC;

-- =====================================================
-- SECTION 3: MONITOR STREAMS
-- =====================================================

-- Check stream status
SHOW STREAMS IN SCHEMA STREAMS_TASKS;


-- Check if streams have data
SELECT 
    'orders_stream' as stream_name,
    SYSTEM$STREAM_HAS_DATA('STREAMS_TASKS.orders_stream') as has_data,
    (SELECT COUNT(*) FROM STREAMS_TASKS.orders_stream) as record_count
UNION ALL
SELECT 
    'order_items_stream',
    SYSTEM$STREAM_HAS_DATA('STREAMS_TASKS.order_items_stream'),
    (SELECT COUNT(*) FROM STREAMS_TASKS.order_items_stream)
UNION ALL
SELECT 
    'customers_stream',
    SYSTEM$STREAM_HAS_DATA('STREAMS_TASKS.customers_stream'),
    (SELECT COUNT(*) FROM STREAMS_TASKS.customers_stream)
UNION ALL
SELECT 
    'inventory_stream',
    SYSTEM$STREAM_HAS_DATA('STREAMS_TASKS.inventory_stream'),
    (SELECT COUNT(*) FROM STREAMS_TASKS.inventory_stream);

-- View stream contents (before task processes them)
SELECT 
    customer_id,
    customer_name,
    email,
    METADATA$ACTION as action_type,
    METADATA$ISUPDATE as is_update,
    METADATA$ROW_ID as row_id
FROM STREAMS_TASKS.customers_stream
LIMIT 20;

-- =====================================================
-- SECTION 4: MONITOR DYNAMIC TABLES
-- =====================================================

-- Show all dynamic tables
SHOW DYNAMIC TABLES IN SCHEMA STAGING;

-- Check dynamic table refresh history
SELECT 
    name,
    database_name,
    schema_name,
    scheduling_state,
    target_lag,
    data_timestamp,
    refresh_action,
    refresh_action_start_time,
    refresh_action_completed_time,
    DATEDIFF('second', refresh_action_start_time, refresh_action_completed_time) as refresh_duration_seconds
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    'ECOMMERCE_DB',
    'STAGING'
))
ORDER BY refresh_action_start_time DESC
LIMIT 50;

-- Check row counts in dynamic tables
SELECT 'enriched_orders' as table_name, COUNT(*) as row_count FROM STAGING.enriched_orders
UNION ALL
SELECT 'product_performance', COUNT(*) FROM STAGING.product_performance
UNION ALL
SELECT 'current_inventory', COUNT(*) FROM STAGING.current_inventory;

-- =====================================================
-- SECTION 5: DATA QUALITY CHECKS
-- =====================================================

-- Check for duplicate orders
SELECT 
    order_id,
    COUNT(*) as duplicate_count
FROM RAW_DATA.orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Check for orphaned order items (items without valid orders)
SELECT COUNT(*) as orphaned_items
FROM RAW_DATA.order_items oi
LEFT JOIN RAW_DATA.orders o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL;

-- Check for products with negative stock
SELECT 
    product_id,
    product_name,
    current_stock
FROM STAGING.current_inventory
WHERE current_stock < 0;

-- Check for orders with no items
SELECT 
    o.order_id,
    o.customer_id,
    o.order_date
FROM RAW_DATA.orders o
LEFT JOIN RAW_DATA.order_items oi ON o.order_id = oi.order_id
WHERE oi.order_item_id IS NULL;

-- Validate date dimension completeness
SELECT 
    MIN(date_actual) as min_date,
    MAX(date_actual) as max_date,
    COUNT(*) as total_days,
    COUNT(DISTINCT year) as distinct_years
FROM ANALYTICS.dim_date;

-- =====================================================
-- SECTION 6: BUSINESS ANALYTICS QUERIES
-- =====================================================

-- Top 10 customers by revenue (Last 30 days)
SELECT 
    customer_name,
    customer_segment,
    country,
    COUNT(DISTINCT order_id) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value
FROM STAGING.enriched_orders
WHERE order_date >= DATEADD('day', -30, CURRENT_DATE())
    AND order_status = 'COMPLETED'
GROUP BY customer_name, customer_segment, country
ORDER BY total_revenue DESC
LIMIT 10;

-- Top 10 products by profit margin
SELECT 
    product_name,
    category,
    brand,
    order_count,
    net_revenue,
    profit,
    profit_margin_percent,
    CASE 
        WHEN profit_margin_percent >= 50 THEN 'Excellent'
        WHEN profit_margin_percent >= 30 THEN 'Good'
        WHEN profit_margin_percent >= 15 THEN 'Fair'
        ELSE 'Low'
    END as margin_category
FROM STAGING.product_performance
WHERE order_count > 0
ORDER BY profit_margin_percent DESC
LIMIT 10;

-- Sales by category and month
SELECT 
    YEAR(eo.order_date) as year,
    MONTH(eo.order_date) as month,
    pp.category,
    COUNT(DISTINCT eo.order_id) as orders,
    SUM(oi.quantity) as units_sold,
    SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) as revenue
FROM STAGING.enriched_orders eo
JOIN RAW_DATA.order_items oi ON eo.order_id = oi.order_id
JOIN STAGING.product_performance pp ON oi.product_id = pp.product_id
WHERE eo.order_status = 'COMPLETED'
GROUP BY YEAR(eo.order_date), MONTH(eo.order_date), pp.category
ORDER BY year DESC, month DESC, revenue DESC;

-- Customer segment analysis
SELECT 
    customer_segment,
    COUNT(DISTINCT customer_id) as customer_count,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    SUM(total_amount) / COUNT(DISTINCT customer_id) as revenue_per_customer
FROM STAGING.enriched_orders
WHERE order_status = 'COMPLETED'
GROUP BY customer_segment
ORDER BY total_revenue DESC;

-- Inventory alerts - Products needing restock
SELECT 
    product_name,
    category,
    brand,
    current_stock,
    stock_status,
    total_quantity_sold,
    last_movement_date,
    DATEDIFF('day', last_movement_date, CURRENT_DATE()) as days_since_last_movement
FROM STAGING.current_inventory ci
JOIN STAGING.product_performance pp ON ci.product_id = pp.product_id
WHERE stock_status IN ('LOW', 'MEDIUM')
ORDER BY current_stock ASC, total_quantity_sold DESC
LIMIT 20;

-- Daily sales trend (Last 30 days)
SELECT 
    summary_date,
    total_orders,
    total_revenue,
    total_profit,
    unique_customers,
    avg_order_value,
    ROUND((total_profit / NULLIF(total_revenue, 0)) * 100, 2) as profit_margin_percent
FROM ANALYTICS.daily_sales_summary
WHERE summary_date >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY summary_date DESC;

-- Order status distribution
SELECT 
    order_status,
    COUNT(*) as order_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
    SUM(total_amount) as total_value
FROM STAGING.enriched_orders
GROUP BY order_status
ORDER BY order_count DESC;

-- Payment method preference
SELECT 
    payment_method,
    COUNT(*) as transaction_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as usage_percent,
    SUM(total_amount) as total_processed,
    AVG(total_amount) as avg_transaction_value
FROM STAGING.enriched_orders
WHERE order_status = 'COMPLETED'
GROUP BY payment_method
ORDER BY transaction_count DESC;

-- Geographic sales distribution
SELECT 
    country,
    COUNT(DISTINCT customer_id) as customers,
    COUNT(DISTINCT order_id) as orders,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_order_value
FROM STAGING.enriched_orders
WHERE order_status = 'COMPLETED'
GROUP BY country
ORDER BY revenue DESC;

-- =====================================================
-- SECTION 7: PERFORMANCE MONITORING
-- =====================================================

-- Query history (Last 24 hours)
SELECT 
    query_id,
    query_text,
    user_name,
    warehouse_name,
    execution_status,
    total_elapsed_time / 1000 as elapsed_seconds,
    bytes_scanned,
    rows_produced,
    start_time
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    CURRENT_TIMESTAMP()
))
WHERE warehouse_name = 'DE_PROJECT_WH'
ORDER BY start_time DESC
LIMIT 50;

-- Warehouse credit usage
SELECT 
    warehouse_name,
    SUM(credits_used) as total_credits,
    SUM(credits_used_compute) as compute_credits,
    SUM(credits_used_cloud_services) as cloud_services_credits
FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
    DATEADD('day', -7, CURRENT_TIMESTAMP())
))
WHERE warehouse_name = 'DE_PROJECT_WH'
GROUP BY warehouse_name;

-- Storage usage
SELECT 
    table_catalog as database_name,
    table_schema as schema_name,
    table_name,
    active_bytes / (1024*1024*1024) as active_gb,
    time_travel_bytes / (1024*1024*1024) as time_travel_gb,
    failsafe_bytes / (1024*1024*1024) as failsafe_gb,
    (active_bytes + time_travel_bytes + failsafe_bytes) / (1024*1024*1024) as total_gb
FROM TABLE(INFORMATION_SCHEMA.TABLE_STORAGE_METRICS(
    DATEADD('day', -1, CURRENT_TIMESTAMP())
))
WHERE table_catalog = 'ECOMMERCE_DB'
ORDER BY total_gb DESC;


SELECT 
    table_catalog AS database_name,
    table_schema  AS schema_name,
    table_name,

    /* Total size in bytes */
    (active_bytes + time_travel_bytes + failsafe_bytes) AS total_bytes,

    /* Dynamic size value */
    CASE 
        WHEN (active_bytes + time_travel_bytes + failsafe_bytes) > 512 * 1024 * 1024
            THEN ROUND((active_bytes + time_travel_bytes + failsafe_bytes) / POWER(1024,3), 2)
        ELSE ROUND((active_bytes + time_travel_bytes + failsafe_bytes) / POWER(1024,2), 2)
    END AS size_value,

    /* Dynamic unit */
    CASE 
        WHEN (active_bytes + time_travel_bytes + failsafe_bytes) > 512 * 1024 * 1024
            THEN 'GB'
        ELSE 'MB'
    END AS size_unit

FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE table_catalog = 'ECOMMERCE_DB'
ORDER BY total_bytes DESC;



-- =====================================================
-- SECTION 8: TEST INCREMENTAL LOADS
-- =====================================================

-- Insert new test order
INSERT INTO RAW_DATA.orders (order_id, customer_id, order_date, order_status, payment_method, shipping_cost, discount_amount)
VALUES (99999, 1, CURRENT_DATE(), 'PROCESSING', 'Credit Card', 15.00, 5.00);

-- Insert order items for test order
INSERT INTO RAW_DATA.order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_percent)
SELECT 
    99900 + ROW_NUMBER() OVER (ORDER BY product_id),
    99999,
    product_id,
    2,
    unit_price,
    10
FROM RAW_DATA.products
LIMIT 3;

-- Check if new data appears in streams
SELECT COUNT(*) as new_orders FROM STREAMS_TASKS.orders_stream WHERE order_id = 99999;
SELECT COUNT(*) as new_items FROM STREAMS_TASKS.order_items_stream WHERE order_id = 99999;

-- Wait for tasks to process, then verify
-- SELECT * FROM ANALYTICS.fact_sales WHERE order_id = 99999;

-- =====================================================
-- SECTION 9: CLEANUP TEST DATA
-- =====================================================

-- Delete test data (run after verification)
-- DELETE FROM RAW_DATA.order_items WHERE order_id = 99999;
-- DELETE FROM RAW_DATA.orders WHERE order_id = 99999;

-- =====================================================
-- SECTION 10: ADMINISTRATIVE COMMANDS
-- =====================================================

-- Suspend tasks (when maintenance needed)
-- ALTER TASK STREAMS_TASKS.process_customer_changes SUSPEND;
-- ALTER TASK STREAMS_TASKS.load_fact_sales SUSPEND;
-- ALTER TASK STREAMS_TASKS.update_daily_summary SUSPEND;

-- Manually refresh a dynamic table
-- ALTER DYNAMIC TABLE STAGING.enriched_orders REFRESH;

-- Check object dependencies
-- SHOW DEPENDENCIES ON TABLE RAW_DATA.orders;

-- View table DDL
-- SELECT GET_DDL('table', 'RAW_DATA.orders');

-- =====================================================
-- MONITORING DASHBOARD QUERY
-- (Run this for quick status overview)
-- =====================================================

SELECT 'RAW_CUSTOMERS' AS object_name,
       'TABLE'         AS source_type,
       COUNT(*)        AS record_count
FROM ECOMMERCE_DB.RAW_DATA.customers

UNION ALL
SELECT 'RAW_PRODUCTS',
       'TABLE',
       COUNT(*)
FROM ECOMMERCE_DB.RAW_DATA.products

UNION ALL
SELECT 'RAW_ORDERS',
       'TABLE',
       COUNT(*)
FROM ECOMMERCE_DB.RAW_DATA.orders

UNION ALL
SELECT 'RAW_ORDER_ITEMS',
       'TABLE',
       COUNT(*)
FROM ECOMMERCE_DB.RAW_DATA.order_items

UNION ALL
SELECT 'RAW_INVENTORY_MOVEMENTS',
       'TABLE',
       COUNT(*)
FROM ECOMMERCE_DB.RAW_DATA.inventory_movements

UNION ALL
SELECT 'CUSTOMERS_STREAM',
       'STREAM',
       COUNT(*)
FROM ECOMMERCE_DB.STREAMS_TASKS.customers_stream

UNION ALL
SELECT 'ORDER_ITEMS_STREAM',
       'STREAM',
       COUNT(*)
FROM ECOMMERCE_DB.STREAMS_TASKS.order_items_stream

UNION ALL
SELECT 'DIM_CUSTOMER',
       'TABLE',
       COUNT(*)
FROM ECOMMERCE_DB.ANALYTICS.dim_customer

UNION ALL
SELECT 'DIM_PRODUCT',
       'TABLE',
       COUNT(*)
FROM ECOMMERCE_DB.ANALYTICS.dim_product

UNION ALL
SELECT 'DIM_DATE',
       'TABLE',
       COUNT(*)
FROM ECOMMERCE_DB.ANALYTICS.dim_date

UNION ALL
SELECT 'FACT_SALES',
       'TABLE',
       COUNT(*)
FROM ECOMMERCE_DB.ANALYTICS.fact_sales

UNION ALL
SELECT 'ENRICHED_ORDERS',
       'DYNAMIC_TABLE',
       COUNT(*)
FROM ECOMMERCE_DB.STAGING.enriched_orders

UNION ALL
SELECT 'PRODUCT_PERFORMANCE',
       'DYNAMIC_TABLE',
       COUNT(*)
FROM ECOMMERCE_DB.STAGING.product_performance

UNION ALL
SELECT 'CURRENT_INVENTORY',
       'DYNAMIC_TABLE',
       COUNT(*)
FROM ECOMMERCE_DB.STAGING.current_inventory

ORDER BY source_type, object_name;


-- =====================================================
-- END OF MONITORING SCRIPT
-- =====================================================