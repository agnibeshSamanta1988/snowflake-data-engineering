-- =====================================================
-- SNOWFLAKE DATA ENGINEERING PROJECT
-- E-Commerce Sales Data Pipeline with Dynamic Tables & Streams
-- =====================================================

-- =====================================================
-- PART 1: ENVIRONMENT SETUP
-- =====================================================

-- Create dedicated warehouse for the project
CREATE OR REPLACE WAREHOUSE DE_PROJECT_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Data Engineering Project';

-- Create database and schemas
CREATE OR REPLACE DATABASE ECOMMERCE_DB;

USE DATABASE ECOMMERCE_DB;

CREATE OR REPLACE SCHEMA RAW_DATA
    COMMENT = 'Schema for raw ingested data';

CREATE OR REPLACE SCHEMA STAGING
    COMMENT = 'Schema for staging transformations';

CREATE OR REPLACE SCHEMA ANALYTICS
    COMMENT = 'Schema for analytics-ready data';

CREATE OR REPLACE SCHEMA STREAMS_TASKS
    COMMENT = 'Schema for streams and task management';

USE WAREHOUSE DE_PROJECT_WH;

-- =====================================================
-- PART 2: RAW DATA LAYER - SOURCE TABLES
-- =====================================================

USE SCHEMA RAW_DATA;

-- Customers Table
CREATE OR REPLACE TABLE ECOMMERCE_DB.RAW_DATA.customers 
(
    customer_id INTEGER,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    country VARCHAR(50),
    city VARCHAR(50),
    signup_date DATE,
    customer_segment VARCHAR(20),
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Products Table
CREATE OR REPLACE TABLE ECOMMERCE_DB.RAW_DATA.products 
(
    product_id INTEGER,
    product_name VARCHAR(200),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    brand VARCHAR(50),
    unit_price DECIMAL(10,2),
    cost_price DECIMAL(10,2),
    stock_quantity INTEGER,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Orders Table (main transactional table)
CREATE OR REPLACE TABLE ECOMMERCE_DB.RAW_DATA.orders 
(
    order_id INTEGER,
    customer_id INTEGER,
    order_date DATE,
    order_status VARCHAR(20),
    payment_method VARCHAR(30),
    shipping_cost DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Order Items Table
CREATE OR REPLACE TABLE ECOMMERCE_DB.RAW_DATA.order_items 
(
    order_item_id INTEGER,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount_percent DECIMAL(5,2),
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Inventory Movements Table
CREATE OR REPLACE TABLE ECOMMERCE_DB.RAW_DATA.inventory_movements 
(
    movement_id INTEGER,
    product_id INTEGER,
    movement_type VARCHAR(20), -- 'PURCHASE', 'SALE', 'RETURN', 'ADJUSTMENT'
    quantity INTEGER,
    movement_date DATE,
    reference_id INTEGER,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =====================================================
-- PART 3: CREATE STREAMS FOR CHANGE DATA CAPTURE
-- =====================================================

USE SCHEMA STREAMS_TASKS;

-- Stream on orders to capture new orders
CREATE OR REPLACE STREAM orders_stream 
ON TABLE RAW_DATA.orders
APPEND_ONLY = FALSE
COMMENT = 'Captures all changes in orders table';

-- Stream on order items
CREATE OR REPLACE STREAM ECOMMERCE_DB.STREAMS_TASKS.order_items_stream
ON TABLE ECOMMERCE_DB.RAW_DATA.order_items
APPEND_ONLY = TRUE
COMMENT = 'Order items CDC stream (incremental only)';

-- Stream on inventory movements
CREATE OR REPLACE STREAM inventory_stream 
ON TABLE RAW_DATA.inventory_movements
APPEND_ONLY = TRUE
COMMENT = 'Captures new inventory movements only';

-- Stream on customers for SCD Type 2
CREATE OR REPLACE STREAM ECOMMERCE_DB.STREAMS_TASKS.customers_stream
ON TABLE ECOMMERCE_DB.RAW_DATA.customers
APPEND_ONLY = TRUE
COMMENT = 'Customer CDC stream (incremental only)';

-- Stream on products for SCD Type 2
CREATE OR REPLACE STREAM ECOMMERCE_DB.STREAMS_TASKS.products_stream
ON TABLE ECOMMERCE_DB.RAW_DATA.products
APPEND_ONLY = TRUE
COMMENT = 'Product CDC stream (incremental)';

-- =====================================================
-- PART 4: STAGING LAYER - DYNAMIC TABLES
-- =====================================================

USE SCHEMA STAGING;

-- Dynamic Table 1: Enriched Orders
-- Combines orders with customer and aggregated order items
CREATE OR REPLACE DYNAMIC TABLE ECOMMERCE_DB.STAGING.enriched_orders
    TARGET_LAG = '1 minutes'
    WAREHOUSE = DE_PROJECT_WH
    COMMENT = 'Orders enriched with customer and item details'
AS
SELECT 
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.email,
    c.country,
    c.city,
    c.customer_segment,
    o.order_date,
    o.order_status,
    o.payment_method,
    o.shipping_cost,
    o.discount_amount,
    COUNT(oi.order_item_id) as total_items,
    SUM(oi.quantity * oi.unit_price) as subtotal,
    SUM(oi.quantity * oi.unit_price * oi.discount_percent / 100) as item_discount,
    SUM(oi.quantity * oi.unit_price) - 
        SUM(oi.quantity * oi.unit_price * oi.discount_percent / 100) - 
        o.discount_amount + 
        o.shipping_cost as total_amount
FROM ECOMMERCE_DB.RAW_DATA.orders o
JOIN ECOMMERCE_DB.RAW_DATA.customers c 
    ON o.customer_id = c.customer_id
LEFT JOIN RAW_DATA.order_items oi 
    ON o.order_id = oi.order_id
GROUP BY 
    o.order_id, o.customer_id, c.customer_name, c.email, 
    c.country, c.city, c.customer_segment, o.order_date, 
    o.order_status, o.payment_method, o.shipping_cost, o.discount_amount;

-- Dynamic Table 2: Product Performance
-- Real-time product sales metrics
CREATE OR REPLACE DYNAMIC TABLE ECOMMERCE_DB.STAGING.product_performance
    TARGET_LAG = '1 minutes'
    WAREHOUSE = DE_PROJECT_WH
    COMMENT = 'Real-time product sales metrics'
AS
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.unit_price,
    p.cost_price,
    p.stock_quantity,
    COUNT(DISTINCT oi.order_id) as order_count,
    SUM(oi.quantity) as total_quantity_sold,
    SUM(oi.quantity * oi.unit_price) as gross_revenue,
    SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) as net_revenue,
    SUM(oi.quantity * p.cost_price) as total_cost,
    SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) - 
        SUM(oi.quantity * p.cost_price) as profit,
    CASE 
        WHEN SUM(oi.quantity * p.cost_price) > 0 
        THEN ((SUM(oi.quantity * oi.unit_price * (1 - oi.discount_percent/100)) - 
               SUM(oi.quantity * p.cost_price)) / 
              SUM(oi.quantity * p.cost_price)) * 100
        ELSE 0 
    END as profit_margin_percent
FROM ECOMMERCE_DB.RAW_DATA.products p
LEFT JOIN ECOMMERCE_DB.RAW_DATA.order_items oi 
    ON p.product_id = oi.product_id
GROUP BY 
    p.product_id, p.product_name, p.category, p.subcategory, 
    p.brand, p.unit_price, p.cost_price, p.stock_quantity;

-- Dynamic Table 3: Current Inventory Levels
-- Real-time inventory tracking
CREATE OR REPLACE DYNAMIC TABLE ECOMMERCE_DB.STAGING.current_inventory
    TARGET_LAG = '2 minutes'
    WAREHOUSE = DE_PROJECT_WH
    COMMENT = 'Real-time inventory levels by product'
AS
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.stock_quantity as initial_stock,
    COALESCE(SUM(CASE 
        WHEN im.movement_type IN ('PURCHASE', 'RETURN') THEN im.quantity
        WHEN im.movement_type IN ('SALE', 'ADJUSTMENT') THEN -im.quantity
        ELSE 0 
    END), 0) as net_movement,
    p.stock_quantity + COALESCE(SUM(CASE 
        WHEN im.movement_type IN ('PURCHASE', 'RETURN') THEN im.quantity
        WHEN im.movement_type IN ('SALE', 'ADJUSTMENT') THEN -im.quantity
        ELSE 0 
    END), 0) as current_stock,
    MAX(im.movement_date) as last_movement_date,
    CASE 
        WHEN p.stock_quantity + COALESCE(SUM(CASE 
            WHEN im.movement_type IN ('PURCHASE', 'RETURN') THEN im.quantity
            WHEN im.movement_type IN ('SALE', 'ADJUSTMENT') THEN -im.quantity
            ELSE 0 
        END), 0) < 10 THEN 'LOW'
        WHEN p.stock_quantity + COALESCE(SUM(CASE 
            WHEN im.movement_type IN ('PURCHASE', 'RETURN') THEN im.quantity
            WHEN im.movement_type IN ('SALE', 'ADJUSTMENT') THEN -im.quantity
            ELSE 0 
        END), 0) < 50 THEN 'MEDIUM'
        ELSE 'HEALTHY'
    END as stock_status
FROM ECOMMERCE_DB.RAW_DATA.products p
LEFT JOIN ECOMMERCE_DB.RAW_DATA.inventory_movements im 
    ON p.product_id = im.product_id
GROUP BY 
    p.product_id, p.product_name, p.category, p.brand, p.stock_quantity;



-- =====================================================
-- PART 5: ANALYTICS LAYER - FINAL TABLES
-- =====================================================

USE SCHEMA ANALYTICS;

-- Customer Dimension (SCD Type 2)
CREATE OR REPLACE TABLE ECOMMERCE_DB.ANALYTICS.dim_customer 
(
    customer_sk INTEGER AUTOINCREMENT,
    customer_id INTEGER,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(20),
    country VARCHAR(50),
    city VARCHAR(50),
    customer_segment VARCHAR(20),
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN,
    PRIMARY KEY (customer_sk)
);

-- Product Dimension
CREATE OR REPLACE TABLE ECOMMERCE_DB.ANALYTICS.dim_product 
(
    product_sk INTEGER AUTOINCREMENT,
    product_id INTEGER,
    product_name VARCHAR(200),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    brand VARCHAR(50),
    unit_price DECIMAL(10,2),
    cost_price DECIMAL(10,2),
    effective_date DATE,
    end_date DATE,
    is_current BOOLEAN,
    PRIMARY KEY (product_sk)
);

-- Date Dimension
CREATE OR REPLACE TABLE ECOMMERCE_DB.ANALYTICS.dim_date 
(
    date_sk INTEGER,
    date_actual DATE,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month_number INTEGER,
    month_name VARCHAR(10),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    PRIMARY KEY (date_sk)
);

-- Sales Fact Table
CREATE OR REPLACE TABLE ECOMMERCE_DB.ANALYTICS.fact_sales 
(
    sales_sk INTEGER AUTOINCREMENT,
    order_id INTEGER,
    customer_sk INTEGER,
    product_sk INTEGER,
    date_sk INTEGER,
    order_item_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount_percent DECIMAL(5,2),
    discount_amount DECIMAL(10,2),
    subtotal DECIMAL(10,2),
    net_amount DECIMAL(10,2),
    cost_amount DECIMAL(10,2),
    profit_amount DECIMAL(10,2),
    order_status VARCHAR(20),
    payment_method VARCHAR(30),
    PRIMARY KEY (sales_sk)
);

-- Daily Sales Summary (Aggregate Table)
CREATE OR REPLACE TABLE ECOMMERCE_DB.ANALYTICS.daily_sales_summary 
(
    summary_date DATE,
    total_orders INTEGER,
    total_revenue DECIMAL(15,2),
    total_profit DECIMAL(15,2),
    total_quantity INTEGER,
    unique_customers INTEGER,
    avg_order_value DECIMAL(10,2),
    PRIMARY KEY (summary_date)
);

-- =====================================================
-- PART 6: TASKS FOR AUTOMATED PROCESSING
-- =====================================================

USE SCHEMA STREAMS_TASKS;

-- TASK 1: Process Customer Changes (SCD Type 2)
CREATE OR REPLACE TASK ECOMMERCE_DB.STREAMS_TASKS.process_customer_changes
    WAREHOUSE = DE_PROJECT_WH
    SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('ECOMMERCE_DB.STREAMS_TASKS.customers_stream')
AS
BEGIN

    /* =====================================================
       1. Close current records for UPDATED customers
       ===================================================== */
    UPDATE ECOMMERCE_DB.ANALYTICS.dim_customer dc
    SET end_date = CURRENT_DATE() - 1,
        is_current = FALSE
    WHERE dc.is_current = TRUE
      AND dc.customer_id IN (
          SELECT customer_id
          FROM ECOMMERCE_DB.STREAMS_TASKS.customers_stream
          WHERE METADATA$ISUPDATE = TRUE
      );

    /* =====================================================
       2. Insert NEW version for UPDATED customers
       ===================================================== */
    INSERT INTO ECOMMERCE_DB.ANALYTICS.dim_customer (
        customer_id,
        customer_name,
        email,
        phone,
        country,
        city,
        customer_segment,
        effective_date,
        end_date,
        is_current
    )
    SELECT
        customer_id,
        customer_name,
        email,
        phone,
        country,
        city,
        customer_segment,
        CURRENT_DATE(),
        DATE '9999-12-31',
        TRUE
    FROM ECOMMERCE_DB.STREAMS_TASKS.customers_stream
    WHERE METADATA$ISUPDATE = TRUE;

    /* =====================================================
       3. Insert BRAND-NEW customers
       ===================================================== */
    INSERT INTO ECOMMERCE_DB.ANALYTICS.dim_customer (
        customer_id,
        customer_name,
        email,
        phone,
        country,
        city,
        customer_segment,
        effective_date,
        end_date,
        is_current
    )
    SELECT
        s.customer_id,
        s.customer_name,
        s.email,
        s.phone,
        s.country,
        s.city,
        s.customer_segment,
        CURRENT_DATE(),
        DATE '9999-12-31',
        TRUE
    FROM ECOMMERCE_DB.STREAMS_TASKS.customers_stream s
    WHERE METADATA$ISUPDATE = FALSE
      AND NOT EXISTS (
          SELECT 1
          FROM ECOMMERCE_DB.ANALYTICS.dim_customer dc
          WHERE dc.customer_id = s.customer_id
            AND dc.is_current = TRUE
      );
END;

-- TASK 2: Load Fact Sales from Order Items Stream
CREATE OR REPLACE TASK ECOMMERCE_DB.STREAMS_TASKS.load_fact_sales
    WAREHOUSE = DE_PROJECT_WH
    SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('ECOMMERCE_DB.STREAMS_TASKS.order_items_stream')
AS
BEGIN

    INSERT INTO ECOMMERCE_DB.ANALYTICS.fact_sales 
    (
        order_id, customer_sk, product_sk, date_sk,
        order_item_id, quantity, unit_price,
        discount_percent, discount_amount,
        subtotal, net_amount, cost_amount,
        profit_amount, order_status, payment_method
    )
    SELECT
        o.order_id,
        dc.customer_sk,
        dp.product_sk,
        TO_NUMBER(TO_CHAR(o.order_date, 'YYYYMMDD')),
        oi.order_item_id,
        oi.quantity,
        oi.unit_price,
        oi.discount_percent,
        oi.quantity * oi.unit_price * oi.discount_percent / 100,
        oi.quantity * oi.unit_price,
        oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100),
        oi.quantity * p.cost_price,
        (oi.quantity * oi.unit_price * (1 - oi.discount_percent / 100))
          - (oi.quantity * p.cost_price),
        o.order_status,
        o.payment_method
    FROM ECOMMERCE_DB.STREAMS_TASKS.order_items_stream oi
    JOIN ECOMMERCE_DB.RAW_DATA.orders o 
        ON oi.order_id = o.order_id
    JOIN ECOMMERCE_DB.RAW_DATA.products p 
        ON oi.product_id = p.product_id
    JOIN ECOMMERCE_DB.ANALYTICS.dim_customer dc
        ON o.customer_id = dc.customer_id 
        AND dc.is_current = TRUE
    JOIN ECOMMERCE_DB.ANALYTICS.dim_product dp
        ON oi.product_id = dp.product_id 
        AND dp.is_current = TRUE;
END;


--  TASK 3: Update Daily Sales Summary
CREATE OR REPLACE TASK ECOMMERCE_DB.STREAMS_TASKS.update_daily_summary
    WAREHOUSE = DE_PROJECT_WH
    SCHEDULE = '10 MINUTE'
AS
BEGIN

    MERGE INTO ECOMMERCE_DB.ANALYTICS.daily_sales_summary dst
    USING (
        SELECT
            dd.date_actual,
            COUNT(DISTINCT fs.order_id) total_orders,
            SUM(fs.net_amount) total_revenue,
            SUM(fs.profit_amount) total_profit,
            SUM(fs.quantity) total_quantity,
            COUNT(DISTINCT fs.customer_sk) unique_customers,
            AVG(fs.net_amount) avg_order_value
        FROM ECOMMERCE_DB.ANALYTICS.fact_sales fs
        JOIN ECOMMERCE_DB.ANALYTICS.dim_date dd
            ON fs.date_sk = dd.date_sk
        GROUP BY dd.date_actual
    ) src
    ON dst.summary_date = src.date_actual

    WHEN MATCHED THEN UPDATE SET
        total_orders = src.total_orders,
        total_revenue = src.total_revenue,
        total_profit = src.total_profit,
        total_quantity = src.total_quantity,
        unique_customers = src.unique_customers,
        avg_order_value = src.avg_order_value

    WHEN NOT MATCHED THEN INSERT VALUES (
        src.date_actual, src.total_orders, src.total_revenue,
        src.total_profit, src.total_quantity,
        src.unique_customers, src.avg_order_value
    );

END;

--  TASK 4: Process Product Changes (SCD Type 2)
CREATE OR REPLACE TASK ECOMMERCE_DB.STREAMS_TASKS.process_product_changes
    WAREHOUSE = DE_PROJECT_WH
    SCHEDULE = '1 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('ECOMMERCE_DB.STREAMS_TASKS.products_stream')
AS
BEGIN

    /* 1. Close current rows for updates */
    UPDATE ECOMMERCE_DB.ANALYTICS.dim_product dp
    SET end_date = CURRENT_DATE() - 1,
        is_current = FALSE
    WHERE dp.is_current = TRUE
      AND dp.product_id IN (
          SELECT product_id
          FROM ECOMMERCE_DB.STREAMS_TASKS.products_stream
          WHERE METADATA$ACTION = 'INSERT'
            AND METADATA$ISUPDATE = TRUE
      );

    /* 2. Insert new version ONLY if no current row exists */
    INSERT INTO ECOMMERCE_DB.ANALYTICS.dim_product (
        product_id, product_name, category,
        subcategory, brand, unit_price,
        cost_price, effective_date,
        end_date, is_current
    )
    SELECT
        s.product_id, s.product_name, s.category,
        s.subcategory, s.brand, s.unit_price,
        s.cost_price, CURRENT_DATE(),
        DATE '9999-12-31', TRUE
    FROM ECOMMERCE_DB.STREAMS_TASKS.products_stream s
    WHERE METADATA$ACTION = 'INSERT'
      AND NOT EXISTS (
          SELECT 1
          FROM ECOMMERCE_DB.ANALYTICS.dim_product dp
          WHERE dp.product_id = s.product_id
            AND dp.is_current = TRUE
      );

END;



-- =====================================================
-- PART 7: RESUME TASKS (Run these first)
-- =====================================================

-- Resume all tasks
ALTER TASK ECOMMERCE_DB.STREAMS_TASKS.process_customer_changes RESUME;
ALTER TASK ECOMMERCE_DB.STREAMS_TASKS.load_fact_sales RESUME;
ALTER TASK ECOMMERCE_DB.STREAMS_TASKS.update_daily_summary RESUME;
ALTER TASK ECOMMERCE_DB.STREAMS_TASKS.process_product_changes RESUME;


-- =====================================================
-- PART 8: SAMPLE QUERIES FOR ANALYSIS
-- =====================================================

-- Query 1: Top 10 products by revenue
-- SELECT 
--     product_name,
--     category,
--     net_revenue,
--     profit,
--     profit_margin_percent,
--     order_count
-- FROM STAGING.product_performance
-- ORDER BY net_revenue DESC
-- LIMIT 10;

-- Query 2: Customer segment performance
-- SELECT 
--     customer_segment,
--     COUNT(DISTINCT customer_id) as customer_count,
--     SUM(total_amount) as total_revenue,
--     AVG(total_amount) as avg_order_value,
--     COUNT(order_id) as order_count
-- FROM STAGING.enriched_orders
-- WHERE order_status = 'COMPLETED'
-- GROUP BY customer_segment
-- ORDER BY total_revenue DESC;

-- Query 3: Low stock alerts
-- SELECT 
--     product_name,
--     category,
--     current_stock,
--     stock_status,
--     last_movement_date
-- FROM STAGING.current_inventory
-- WHERE stock_status = 'LOW'
-- ORDER BY current_stock ASC;

-- Query 4: Daily sales trend
-- SELECT 
--     summary_date,
--     total_orders,
--     total_revenue,
--     total_profit,
--     unique_customers,
--     avg_order_value
-- FROM ANALYTICS.daily_sales_summary
-- ORDER BY summary_date DESC
-- LIMIT 30;

-- Query 5: Stream monitoring
-- SELECT 
--     'orders_stream' as stream_name,
--     SYSTEM$STREAM_HAS_DATA('STREAMS_TASKS.orders_stream') as has_data
-- UNION ALL
-- SELECT 
--     'order_items_stream',
--     SYSTEM$STREAM_HAS_DATA('STREAMS_TASKS.order_items_stream')
-- UNION ALL
-- SELECT 
--     'customers_stream',
--     SYSTEM$STREAM_HAS_DATA('STREAMS_TASKS.customers_stream');

-- =====================================================
-- EXECUTION NOTES:
-- 1. Run this script section by section in Snowflake
-- 2. Use the data loading script to populate tables
-- 3. Resume tasks: ALTER TASK <task_name> RESUME;
-- 4. Monitor dynamic tables: SHOW DYNAMIC TABLES;
-- 5. Check stream status: SELECT * FROM TABLE(INFORMATION_SCHEMA.STREAMS_IN_SCHEMA());
-- =====================================================
