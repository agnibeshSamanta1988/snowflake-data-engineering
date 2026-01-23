/*==============================================================================
  Tasty Bytes Streams for Change Data Capture
  
  This script creates streams on key tables to capture INSERT, UPDATE, 
  and DELETE operations for:
  - Order processing
  - Customer loyalty tracking
  - Menu changes
  - Inventory management
==============================================================================*/

USE ROLE accountadmin;
USE DATABASE tasty_bytes;
USE SCHEMA raw_pos;

/*--
  Order Processing Streams
--*/

-- Stream on order_header table to capture new orders
CREATE OR REPLACE STREAM tasty_bytes.raw_pos.order_header_stream 
ON TABLE tasty_bytes.raw_pos.order_header
COMMENT = 'Captures all changes to order_header table for downstream processing';

-- Stream on order_detail table to capture order line items
CREATE OR REPLACE STREAM tasty_bytes.raw_pos.order_detail_stream 
ON TABLE tasty_bytes.raw_pos.order_detail
COMMENT = 'Captures all changes to order_detail table for inventory and analytics';

/*--
  Customer Loyalty Streams
--*/

-- Stream on customer_loyalty table
CREATE OR REPLACE STREAM tasty_bytes.raw_customer.customer_loyalty_stream 
ON TABLE tasty_bytes.raw_customer.customer_loyalty
COMMENT = 'Captures customer loyalty program changes for marketing analytics';

/*--
  Menu and Inventory Streams
--*/

-- Stream on menu table to track menu changes
CREATE OR REPLACE STREAM tasty_bytes.raw_pos.menu_stream 
ON TABLE tasty_bytes.raw_pos.menu
COMMENT = 'Captures menu item changes including pricing and availability updates';

-- Stream on truck table to track fleet changes
CREATE OR REPLACE STREAM tasty_bytes.raw_pos.truck_stream 
ON TABLE tasty_bytes.raw_pos.truck
COMMENT = 'Captures truck fleet changes including new trucks and retirements';

/*--
  Harmonized Schema Tables for Aggregations
--*/

USE SCHEMA harmonized;

-- Daily sales summary table (target for aggregations)
CREATE OR REPLACE TABLE tasty_bytes.harmonized.daily_sales_summary (
    sales_date DATE,
    truck_brand_name VARCHAR(200),
    primary_city VARCHAR(200),
    region VARCHAR(200),
    country VARCHAR(200),
    total_orders INTEGER,
    total_revenue NUMBER(38,4),
    total_tax NUMBER(38,4),
    total_discount NUMBER(38,4),
    unique_customers INTEGER,
    avg_order_value NUMBER(38,4),
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_daily_sales PRIMARY KEY (sales_date, truck_brand_name, primary_city)
)
COMMENT = 'Daily aggregated sales metrics by brand and location';

-- Customer activity summary table
CREATE OR REPLACE TABLE tasty_bytes.harmonized.customer_activity_summary (
    customer_id NUMBER(38,0),
    activity_date DATE,
    order_count INTEGER,
    total_spent NUMBER(38,4),
    favorite_menu_items ARRAY,
    visited_locations ARRAY,
    last_order_timestamp TIMESTAMP_NTZ,
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_customer_activity PRIMARY KEY (customer_id, activity_date)
)
COMMENT = 'Daily customer activity and purchase behavior summary';

-- Menu item performance table
CREATE OR REPLACE TABLE tasty_bytes.harmonized.menu_item_performance (
    performance_date DATE,
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(200),
    truck_brand_name VARCHAR(200),
    times_ordered INTEGER,
    total_quantity INTEGER,
    total_revenue NUMBER(38,4),
    avg_price NUMBER(38,4),
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_menu_performance PRIMARY KEY (performance_date, menu_item_id, truck_brand_name)
)
COMMENT = 'Daily menu item sales performance metrics';

-- Regional sales performance table (for Hamburg focus)
CREATE OR REPLACE TABLE tasty_bytes.harmonized.regional_sales_summary (
    sales_date DATE,
    city VARCHAR(200),
    country VARCHAR(200),
    region VARCHAR(200),
    order_count INTEGER,
    total_revenue NUMBER(38,4),
    unique_customers INTEGER,
    unique_trucks INTEGER,
    avg_order_value NUMBER(38,4),
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_regional_sales PRIMARY KEY (sales_date, city, country)
)
COMMENT = 'Daily regional sales performance across all markets';

/*--
  Analytics Schema Views on Stream Data
--*/

USE SCHEMA analytics;

-- View to see current stream contents for order_header
CREATE OR REPLACE VIEW tasty_bytes.analytics.order_header_changes_v AS
SELECT 
    METADATA$ACTION AS change_type,
    METADATA$ISUPDATE AS is_update,
    METADATA$ROW_ID AS row_id,
    order_id,
    truck_id,
    location_id,
    customer_id,
    order_ts,
    order_total,
    order_amount
FROM tasty_bytes.raw_pos.order_header_stream;

-- View to monitor pending changes across all streams
CREATE OR REPLACE VIEW tasty_bytes.analytics.stream_monitoring_v AS
SELECT 
    'ORDER_HEADER' AS stream_name,
    COUNT(*) AS pending_changes,
    COUNT_IF(METADATA$ACTION = 'INSERT') AS inserts,
    COUNT_IF(METADATA$ACTION = 'DELETE') AS deletes,
    MIN(order_ts) AS earliest_change_timestamp,
    MAX(order_ts) AS latest_change_timestamp
FROM tasty_bytes.raw_pos.order_header_stream
UNION ALL
SELECT 
    'ORDER_DETAIL' AS stream_name,
    COUNT(*) AS pending_changes,
    COUNT_IF(METADATA$ACTION = 'INSERT') AS inserts,
    COUNT_IF(METADATA$ACTION = 'DELETE') AS deletes,
    NULL AS earliest_change_timestamp,
    NULL AS latest_change_timestamp
FROM tasty_bytes.raw_pos.order_detail_stream
UNION ALL
SELECT 
    'CUSTOMER_LOYALTY' AS stream_name,
    COUNT(*) AS pending_changes,
    COUNT_IF(METADATA$ACTION = 'INSERT') AS inserts,
    COUNT_IF(METADATA$ACTION = 'DELETE') AS deletes,
    NULL AS earliest_change_timestamp,
    NULL AS latest_change_timestamp
FROM tasty_bytes.raw_customer.customer_loyalty_stream
UNION ALL
SELECT 
    'MENU' AS stream_name,
    COUNT(*) AS pending_changes,
    COUNT_IF(METADATA$ACTION = 'INSERT') AS inserts,
    COUNT_IF(METADATA$ACTION = 'DELETE') AS deletes,
    NULL AS earliest_change_timestamp,
    NULL AS latest_change_timestamp
FROM tasty_bytes.raw_pos.menu_stream;

/*--
  Verification
--*/

-- Show all streams created
SHOW STREAMS IN SCHEMA tasty_bytes.raw_pos;
SHOW STREAMS IN SCHEMA tasty_bytes.raw_customer;

-- Show tables created in harmonized schema
USE SCHEMA harmonized;
SHOW TABLES IN SCHEMA tasty_bytes.harmonized;

-- Check stream monitoring view
SELECT * FROM tasty_bytes.analytics.stream_monitoring_v;

SELECT 'Stream infrastructure created successfully!' AS status;
