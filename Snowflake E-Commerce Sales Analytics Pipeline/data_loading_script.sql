-- =====================================================
-- DATA LOADING SCRIPT
-- Sample data for E-Commerce Data Engineering Project
-- =====================================================

USE DATABASE ECOMMERCE_DB;
USE SCHEMA RAW_DATA;
USE WAREHOUSE DE_PROJECT_WH;

-- =====================================================
-- STEP 1: POPULATE DATE DIMENSION (2023-2025)
-- =====================================================

USE SCHEMA ANALYTICS;

INSERT INTO ECOMMERCE_DB.ANALYTICS.dim_date
WITH date_range AS (
    SELECT 
        DATEADD(day, SEQ4(), '2023-01-01'::DATE) AS date_actual
    FROM TABLE(GENERATOR(ROWCOUNT => 1095)) -- 3 years
)
SELECT 
    TO_NUMBER(TO_CHAR(date_actual, 'YYYYMMDD')) as date_sk,
    date_actual,
    DAYOFWEEK(date_actual) as day_of_week,
    DAYNAME(date_actual) as day_name,
    DAYOFMONTH(date_actual) as day_of_month,
    DAYOFYEAR(date_actual) as day_of_year,
    WEEKOFYEAR(date_actual) as week_of_year,
    MONTH(date_actual) as month_number,
    MONTHNAME(date_actual) as month_name,
    QUARTER(date_actual) as quarter,
    YEAR(date_actual) as year,
    CASE WHEN DAYOFWEEK(date_actual) IN (0, 6) THEN TRUE ELSE FALSE END as is_weekend,
    FALSE as is_holiday
FROM date_range;

SELECT * FROM ECOMMERCE_DB.ANALYTICS.dim_date LIMIT 10;

-- =====================================================
-- STEP 2: LOAD CUSTOMER DATA (100 customers)
-- =====================================================

USE SCHEMA ECOMMERCE_DB.RAW_DATA;

INSERT INTO ECOMMERCE_DB.RAW_DATA.customers (customer_id, customer_name, email, phone, country, city, signup_date, customer_segment)
SELECT 
    SEQ4() as customer_id,
    CASE (UNIFORM(1, 50, RANDOM()) % 50)
        WHEN 0 THEN 'John Smith'
        WHEN 1 THEN 'Emma Johnson'
        WHEN 2 THEN 'Michael Brown'
        WHEN 3 THEN 'Sophia Davis'
        WHEN 4 THEN 'James Wilson'
        WHEN 5 THEN 'Olivia Martinez'
        WHEN 6 THEN 'William Anderson'
        WHEN 7 THEN 'Ava Thomas'
        WHEN 8 THEN 'Robert Taylor'
        WHEN 9 THEN 'Isabella Moore'
        WHEN 10 THEN 'David Jackson'
        WHEN 11 THEN 'Mia White'
        WHEN 12 THEN 'Richard Harris'
        WHEN 13 THEN 'Charlotte Martin'
        WHEN 14 THEN 'Joseph Thompson'
        WHEN 15 THEN 'Amelia Garcia'
        WHEN 16 THEN 'Thomas Robinson'
        WHEN 17 THEN 'Harper Clark'
        WHEN 18 THEN 'Christopher Rodriguez'
        WHEN 19 THEN 'Evelyn Lewis'
        WHEN 20 THEN 'Daniel Lee'
        WHEN 21 THEN 'Abigail Walker'
        WHEN 22 THEN 'Matthew Hall'
        WHEN 23 THEN 'Emily Allen'
        WHEN 24 THEN 'Anthony Young'
        WHEN 25 THEN 'Elizabeth Hernandez'
        WHEN 26 THEN 'Mark King'
        WHEN 27 THEN 'Sofia Wright'
        WHEN 28 THEN 'Donald Lopez'
        WHEN 29 THEN 'Avery Hill'
        WHEN 30 THEN 'Steven Scott'
        WHEN 31 THEN 'Ella Green'
        WHEN 32 THEN 'Andrew Adams'
        WHEN 33 THEN 'Scarlett Baker'
        WHEN 34 THEN 'Joshua Nelson'
        WHEN 35 THEN 'Victoria Carter'
        WHEN 36 THEN 'Kenneth Mitchell'
        WHEN 37 THEN 'Madison Perez'
        WHEN 38 THEN 'Kevin Roberts'
        WHEN 39 THEN 'Luna Turner'
        WHEN 40 THEN 'Brian Phillips'
        WHEN 41 THEN 'Grace Campbell'
        WHEN 42 THEN 'George Parker'
        WHEN 43 THEN 'Chloe Evans'
        WHEN 44 THEN 'Timothy Edwards'
        WHEN 45 THEN 'Penelope Collins'
        WHEN 46 THEN 'Ronald Stewart'
        WHEN 47 THEN 'Layla Sanchez'
        WHEN 48 THEN 'Edward Morris'
        ELSE 'Riley Rogers'
    END || ' #' || SEQ4() as customer_name,
    'customer' || SEQ4() || '@email.com' as email,
    '+1-' || (200 + UNIFORM(0, 799, RANDOM())) || '-' || 
            (100 + UNIFORM(0, 899, RANDOM())) || '-' || 
            (1000 + UNIFORM(0, 8999, RANDOM())) as phone,
    CASE (UNIFORM(1, 10, RANDOM()) % 10)
        WHEN 0 THEN 'USA'
        WHEN 1 THEN 'USA'
        WHEN 2 THEN 'USA'
        WHEN 3 THEN 'USA'
        WHEN 4 THEN 'Canada'
        WHEN 5 THEN 'UK'
        WHEN 6 THEN 'Germany'
        WHEN 7 THEN 'France'
        WHEN 8 THEN 'Australia'
        ELSE 'Japan'
    END as country,
    CASE (UNIFORM(1, 15, RANDOM()) % 15)
        WHEN 0 THEN 'New York'
        WHEN 1 THEN 'Los Angeles'
        WHEN 2 THEN 'Chicago'
        WHEN 3 THEN 'Houston'
        WHEN 4 THEN 'Phoenix'
        WHEN 5 THEN 'Toronto'
        WHEN 6 THEN 'London'
        WHEN 7 THEN 'Berlin'
        WHEN 8 THEN 'Paris'
        WHEN 9 THEN 'Sydney'
        WHEN 10 THEN 'Tokyo'
        WHEN 11 THEN 'Seattle'
        WHEN 12 THEN 'Miami'
        WHEN 13 THEN 'Boston'
        ELSE 'San Francisco'
    END as city,
    DATEADD(day, -UNIFORM(30, 730, RANDOM()), CURRENT_DATE()) as signup_date,
    CASE (UNIFORM(1, 100, RANDOM()) % 3)
        WHEN 0 THEN 'PREMIUM'
        WHEN 1 THEN 'STANDARD'
        ELSE 'BASIC'
    END as customer_segment
FROM TABLE(GENERATOR(ROWCOUNT => 100));


SELECT * FROM ECOMMERCE_DB.RAW_DATA.customers LIMIT 10;

-- =====================================================
-- STEP 3: LOAD PRODUCT DATA (200 products)
-- =====================================================

INSERT INTO ECOMMERCE_DB.RAW_DATA.products (product_id, product_name, category, subcategory, brand, unit_price, cost_price, stock_quantity)
SELECT 
    SEQ4() as product_id,
    CASE (UNIFORM(1, 50, RANDOM()) % 50)
        WHEN 0 THEN 'Wireless Headphones'
        WHEN 1 THEN 'Smart Watch'
        WHEN 2 THEN 'Laptop Computer'
        WHEN 3 THEN 'Tablet Device'
        WHEN 4 THEN 'Smartphone'
        WHEN 5 THEN 'Gaming Console'
        WHEN 6 THEN 'Digital Camera'
        WHEN 7 THEN 'Bluetooth Speaker'
        WHEN 8 THEN 'Fitness Tracker'
        WHEN 9 THEN 'Wireless Mouse'
        WHEN 10 THEN 'Mechanical Keyboard'
        WHEN 11 THEN 'External Hard Drive'
        WHEN 12 THEN 'USB Flash Drive'
        WHEN 13 THEN 'Monitor 27 inch'
        WHEN 14 THEN 'Webcam HD'
        WHEN 15 THEN 'Microphone USB'
        WHEN 16 THEN 'Graphics Card'
        WHEN 17 THEN 'SSD Storage'
        WHEN 18 THEN 'RAM Memory'
        WHEN 19 THEN 'Motherboard'
        WHEN 20 THEN 'Power Supply'
        WHEN 21 THEN 'PC Case'
        WHEN 22 THEN 'Cooling Fan'
        WHEN 23 THEN 'Thermal Paste'
        WHEN 24 THEN 'Cable HDMI'
        WHEN 25 THEN 'USB-C Cable'
        WHEN 26 THEN 'Phone Charger'
        WHEN 27 THEN 'Power Bank'
        WHEN 28 THEN 'Screen Protector'
        WHEN 29 THEN 'Phone Case'
        WHEN 30 THEN 'Laptop Bag'
        WHEN 31 THEN 'Desk Lamp'
        WHEN 32 THEN 'Ergonomic Chair'
        WHEN 33 THEN 'Standing Desk'
        WHEN 34 THEN 'Desk Organizer'
        WHEN 35 THEN 'Notebook Set'
        WHEN 36 THEN 'Pen Collection'
        WHEN 37 THEN 'Desk Calendar'
        WHEN 38 THEN 'Whiteboard'
        WHEN 39 THEN 'Office Chair Mat'
        WHEN 40 THEN 'Monitor Stand'
        WHEN 41 THEN 'Keyboard Wrist Rest'
        WHEN 42 THEN 'Mouse Pad'
        WHEN 43 THEN 'Cable Management'
        WHEN 44 THEN 'Surge Protector'
        WHEN 45 THEN 'Desk Fan'
        WHEN 46 THEN 'Air Purifier'
        WHEN 47 THEN 'Smart Light Bulb'
        WHEN 48 THEN 'Security Camera'
        ELSE 'Smart Thermostat'
    END || ' Model-' || SEQ4() as product_name,
    CASE (UNIFORM(1, 100, RANDOM()) % 5)
        WHEN 0 THEN 'Electronics'
        WHEN 1 THEN 'Computers'
        WHEN 2 THEN 'Accessories'
        WHEN 3 THEN 'Office Supplies'
        ELSE 'Smart Home'
    END as category,
    CASE (UNIFORM(1, 100, RANDOM()) % 8)
        WHEN 0 THEN 'Audio'
        WHEN 1 THEN 'Wearables'
        WHEN 2 THEN 'Computing'
        WHEN 3 THEN 'Storage'
        WHEN 4 THEN 'Peripherals'
        WHEN 5 THEN 'Components'
        WHEN 6 THEN 'Cables'
        ELSE 'Furniture'
    END as subcategory,
    CASE (UNIFORM(1, 10, RANDOM()) % 10)
        WHEN 0 THEN 'TechPro'
        WHEN 1 THEN 'SmartLife'
        WHEN 2 THEN 'ProGear'
        WHEN 3 THEN 'EliteDevice'
        WHEN 4 THEN 'UltraTech'
        WHEN 5 THEN 'MegaBrand'
        WHEN 6 THEN 'PrimeTech'
        WHEN 7 THEN 'NovaSystems'
        WHEN 8 THEN 'ApexGear'
        ELSE 'ZenithTech'
    END as brand,
    ROUND(UNIFORM(10, 2000, RANDOM()), 2) as unit_price,
    ROUND(UNIFORM(5, 1500, RANDOM()), 2) as cost_price,
    UNIFORM(0, 500, RANDOM()) as stock_quantity
FROM TABLE(GENERATOR(ROWCOUNT => 200));

SELECT * FROM ECOMMERCE_DB.RAW_DATA.products LIMIT 10;

-- =====================================================
-- STEP 4: INITIAL DIMENSION LOADS
-- =====================================================

USE SCHEMA ANALYTICS;

-- Load initial customer dimension
INSERT INTO ECOMMERCE_DB.ANALYTICS.dim_customer (customer_id, customer_name, email, phone, country, city, customer_segment, effective_date, end_date, is_current)
SELECT 
    customer_id,
    customer_name,
    email,
    phone,
    country,
    city,
    customer_segment,
    signup_date as effective_date,
    DATE '9999-12-31' as end_date,
    TRUE as is_current
FROM ECOMMERCE_DB.RAW_DATA.customers;

SELECT * FROM ECOMMERCE_DB.ANALYTICS.dim_customer LIMIT 10;

-- Load initial product dimension
INSERT INTO ECOMMERCE_DB.ANALYTICS.dim_product (product_id, product_name, category, subcategory, brand, unit_price, cost_price, effective_date, end_date, is_current)
SELECT 
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    unit_price,
    cost_price,
    CURRENT_DATE() as effective_date,
    DATE '9999-12-31' as end_date,
    TRUE as is_current
FROM ECOMMERCE_DB.RAW_DATA.products;

SELECT * FROM ECOMMERCE_DB.ANALYTICS.dim_product LIMIT 10;

-- =====================================================
-- STEP 5: LOAD TRANSACTIONAL DATA (5000 orders)
-- =====================================================

USE SCHEMA RAW_DATA;

-- Load Orders
INSERT INTO ECOMMERCE_DB.RAW_DATA.orders (order_id, customer_id, order_date, order_status, payment_method, shipping_cost, discount_amount)
SELECT 
    SEQ4() as order_id,
    UNIFORM(1, 100, RANDOM()) as customer_id,
    DATEADD(day, -UNIFORM(0, 365, RANDOM()), CURRENT_DATE()) as order_date,
    CASE (UNIFORM(1, 100, RANDOM()) % 10)
        WHEN 0 THEN 'PENDING'
        WHEN 1 THEN 'PROCESSING'
        WHEN 2 THEN 'CANCELLED'
        ELSE 'COMPLETED'
    END as order_status,
    CASE (UNIFORM(1, 100, RANDOM()) % 5)
        WHEN 0 THEN 'Credit Card'
        WHEN 1 THEN 'PayPal'
        WHEN 2 THEN 'Debit Card'
        WHEN 3 THEN 'Bank Transfer'
        ELSE 'Cash on Delivery'
    END as payment_method,
    ROUND(UNIFORM(5, 50, RANDOM()), 2) as shipping_cost,
    ROUND(UNIFORM(0, 100, RANDOM()), 2) as discount_amount
FROM TABLE(GENERATOR(ROWCOUNT => 5000));

SELECT * FROM ECOMMERCE_DB.RAW_DATA.orders LIMIT 10;

-- Load Order Items (2-5 items per order)
INSERT INTO ECOMMERCE_DB.RAW_DATA.order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_percent)
WITH order_list AS (
    SELECT order_id 
    FROM ECOMMERCE_DB.RAW_DATA.orders
),
items_per_order AS (
    SELECT 
        o.order_id,
        UNIFORM(2, 5, RANDOM()) as num_items
    FROM order_list o
),
expanded_items AS (
    SELECT 
        order_id,
        UNIFORM(1, 200, RANDOM()) as product_id,
        SEQ4() as item_seq
    FROM items_per_order,
    LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(1, num_items + 1))
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY e.order_id, e.item_seq) as order_item_id,
    e.order_id,
    e.product_id,
    UNIFORM(1, 5, RANDOM()) as quantity,
    p.unit_price,
    CASE (UNIFORM(1, 100, RANDOM()) % 5)
        WHEN 0 THEN 5
        WHEN 1 THEN 10
        WHEN 2 THEN 15
        ELSE 0
    END as discount_percent
FROM expanded_items e
JOIN ECOMMERCE_DB.RAW_DATA.products p 
    ON e.product_id = p.product_id
LIMIT 15000;


SELECT * FROM ECOMMERCE_DB.RAW_DATA.order_items LIMIT 10;
-- =====================================================
-- STEP 6: LOAD INVENTORY MOVEMENTS (10000 records)
-- =====================================================

INSERT INTO ECOMMERCE_DB.RAW_DATA.inventory_movements (movement_id, product_id, movement_type, quantity, movement_date, reference_id)
SELECT 
    SEQ4() as movement_id,
    UNIFORM(1, 200, RANDOM()) as product_id,
    CASE (UNIFORM(1, 100, RANDOM()) % 4)
        WHEN 0 THEN 'PURCHASE'
        WHEN 1 THEN 'SALE'
        WHEN 2 THEN 'RETURN'
        ELSE 'ADJUSTMENT'
    END as movement_type,
    UNIFORM(1, 100, RANDOM()) as quantity,
    DATEADD(day, -UNIFORM(0, 365, RANDOM()), CURRENT_DATE()) as movement_date,
    UNIFORM(1, 5000, RANDOM()) as reference_id
FROM TABLE(GENERATOR(ROWCOUNT => 10000));

SELECT * FROM ECOMMERCE_DB.RAW_DATA.inventory_movements LIMIT 10;


--Insert the first time when data loaded
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
    c.customer_id,
    c.customer_name,
    c.email,
    c.phone,
    c.country,
    c.city,
    c.customer_segment,
    CURRENT_DATE(),
    DATE '9999-12-31',
    TRUE
FROM ECOMMERCE_DB.RAW_DATA.customers c
WHERE NOT EXISTS (
    SELECT 1
    FROM ECOMMERCE_DB.ANALYTICS.dim_customer dc
    WHERE dc.customer_id = c.customer_id
      AND dc.is_current = TRUE
);



INSERT INTO ECOMMERCE_DB.ANALYTICS.dim_product (
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    unit_price,
    cost_price,
    effective_date,
    end_date,
    is_current
)
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    p.unit_price,
    p.cost_price,
    CURRENT_DATE(),
    DATE '9999-12-31',
    TRUE
FROM ECOMMERCE_DB.RAW_DATA.products p
WHERE NOT EXISTS (
    SELECT 1
    FROM ECOMMERCE_DB.ANALYTICS.dim_product dp
    WHERE dp.product_id = p.product_id
      AND dp.is_current = TRUE
);



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
FROM ECOMMERCE_DB.RAW_DATA.order_items oi
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





-- =====================================================
-- STEP 7: SIMULATE NEW DATA (for testing streams/tasks)
-- =====================================================

-- New customers
INSERT INTO customers (customer_id, customer_name, email, phone, country, city, signup_date, customer_segment)
VALUES 
    (101, 'Sarah Connor', 'sarah.connor@email.com', '+1-555-0101', 'USA', 'Los Angeles', CURRENT_DATE(), 'PREMIUM'),
    (102, 'John Connor', 'john.connor@email.com', '+1-555-0102', 'USA', 'Los Angeles', CURRENT_DATE(), 'STANDARD'),
    (103, 'Kyle Reese', 'kyle.reese@email.com', '+1-555-0103', 'USA', 'New York', CURRENT_DATE(), 'BASIC');

-- New orders
INSERT INTO orders (order_id, customer_id, order_date, order_status, payment_method, shipping_cost, discount_amount)
VALUES 
    (5001, 101, CURRENT_DATE(), 'PROCESSING', 'Credit Card', 15.00, 20.00),
    (5002, 102, CURRENT_DATE(), 'PENDING', 'PayPal', 10.00, 0.00),
    (5003, 103, CURRENT_DATE(), 'COMPLETED', 'Debit Card', 12.50, 10.00);

-- New order items
INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount_percent)
SELECT 
    15001 + (ROW_NUMBER() OVER (ORDER BY o.order_id, p.product_id)) as order_item_id,
    o.order_id,
    p.product_id,
    UNIFORM(1, 3, RANDOM()) as quantity,
    p.unit_price,
    0 as discount_percent
FROM (SELECT order_id FROM orders WHERE order_id >= 5001) o
CROSS JOIN (SELECT product_id, unit_price FROM products WHERE product_id <= 10) p
LIMIT 8;

-- New inventory movements
INSERT INTO inventory_movements (movement_id, product_id, movement_type, quantity, movement_date, reference_id)
VALUES 
    (10001, 1, 'SALE', 5, CURRENT_DATE(), 5001),
    (10002, 2, 'SALE', 3, CURRENT_DATE(), 5002),
    (10003, 3, 'PURCHASE', 100, CURRENT_DATE(), 0),
    (10004, 4, 'RETURN', 2, CURRENT_DATE(), 5003);

-- =====================================================
-- DATA LOADING COMPLETE
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
-- NEXT STEPS:
-- 1. Dynamic tables will auto-refresh based on TARGET_LAG
-- 2. Resume tasks to start processing streams:
--    ALTER TASK STREAMS_TASKS.process_customer_changes RESUME;
--    ALTER TASK STREAMS_TASKS.load_fact_sales RESUME;
--    ALTER TASK STREAMS_TASKS.update_daily_summary RESUME;
-- 3. Monitor with sample queries from main script
-- =====================================================
