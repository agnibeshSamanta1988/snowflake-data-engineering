# QUICK START GUIDE
## Get Your Snowflake Data Engineering Project Running in 30 Minutes

---

## Prerequisites
- Snowflake account with ACCOUNTADMIN or similar privileges
- Access to Snowflake Web UI or SnowSQL CLI
- Basic understanding of SQL

---

## Execution Order (Follow Exactly)

### ⏱️ STEP 1: Initial Setup (5 minutes)
Open `snowflake_complete_project.sql` and run sections 1-6 sequentially:

```sql
-- PART 1: ENVIRONMENT SETUP
-- Copy and run lines 1-50
-- ✅ Creates: Warehouse, Database, Schemas

-- PART 2: RAW DATA LAYER
-- Copy and run lines 52-130
-- ✅ Creates: 5 source tables

-- PART 3: STREAMS
-- Copy and run lines 132-175
-- ✅ Creates: 4 streams for CDC

-- PART 4: DYNAMIC TABLES
-- Copy and run lines 177-310
-- ✅ Creates: 3 dynamic tables

-- PART 5: ANALYTICS LAYER
-- Copy and run lines 312-420
-- ✅ Creates: Dimension and fact tables

-- PART 6: TASKS
-- Copy and run lines 422-540
-- ✅ Creates: 3 automated tasks (suspended state)
```

**Verification:**
```sql
SHOW TABLES IN SCHEMA RAW_DATA;        -- Should show 5 tables
SHOW STREAMS IN SCHEMA STREAMS_TASKS;  -- Should show 4 streams
SHOW DYNAMIC TABLES IN SCHEMA STAGING; -- Should show 3 dynamic tables
SHOW TASKS IN SCHEMA STREAMS_TASKS;    -- Should show 3 tasks
```

---

### ⏱️ STEP 2: Load Data (10 minutes)
Open `data_loading_script.sql` and run the ENTIRE file:

```sql
-- Just click "Run All" or execute the entire script
-- This populates:
-- - 1,095 dates (3 years)
-- - 103 customers
-- - 200 products
-- - 5,003 orders
-- - ~15,008 order items
-- - 10,004 inventory movements
```

**Verification:**
```sql
-- Run this at the end of the data loading script
SELECT 'Customers' as table_name, COUNT(*) FROM RAW_DATA.customers
UNION ALL SELECT 'Products', COUNT(*) FROM RAW_DATA.products
UNION ALL SELECT 'Orders', COUNT(*) FROM RAW_DATA.orders
UNION ALL SELECT 'Order Items', COUNT(*) FROM RAW_DATA.order_items;

-- Expected output:
-- Customers: 103
-- Products: 200
-- Orders: 5,003
-- Order Items: ~15,008
```

---

### ⏱️ STEP 3: Activate Automation (2 minutes)
Open `testing_monitoring_script.sql` and run Section 1:

```sql
-- SECTION 1: RESUME TASKS
ALTER TASK STREAMS_TASKS.process_customer_changes RESUME;
ALTER TASK STREAMS_TASKS.load_fact_sales RESUME;
ALTER TASK STREAMS_TASKS.update_daily_summary RESUME;
```

**Verification:**
```sql
SHOW TASKS IN SCHEMA STREAMS_TASKS;
-- All tasks should show state = 'started'
```

---

### ⏱️ STEP 4: Wait & Verify (5-10 minutes)
Dynamic tables and tasks need time to process initial data.

**While waiting, check progress:**

```sql
-- Check dynamic table refresh status
SELECT 
    name,
    scheduling_state,
    target_lag,
    data_timestamp
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
    'ECOMMERCE_DB', 'STAGING'
))
ORDER BY refresh_action_start_time DESC
LIMIT 10;

-- Check if streams have data
SELECT 
    'orders_stream' as stream_name,
    SYSTEM$STREAM_HAS_DATA('STREAMS_TASKS.orders_stream') as has_data,
    (SELECT COUNT(*) FROM STREAMS_TASKS.orders_stream) as records
UNION ALL
SELECT 
    'order_items_stream',
    SYSTEM$STREAM_HAS_DATA('STREAMS_TASKS.order_items_stream'),
    (SELECT COUNT(*) FROM STREAMS_TASKS.order_items_stream);

-- Check task execution history
SELECT 
    name,
    state,
    scheduled_time,
    completed_time,
    error_code
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE database_name = 'ECOMMERCE_DB'
ORDER BY scheduled_time DESC
LIMIT 10;
```

**Wait until:**
- Dynamic tables show recent `data_timestamp`
- Streams show 0 records (means tasks processed them)
- Tasks show successful executions

---

### ⏱️ STEP 5: Run Analytics (5 minutes)
Once data is processed, run these queries from `testing_monitoring_script.sql` Section 6:

```sql
-- 1. Top 10 Products by Revenue
SELECT 
    product_name,
    category,
    net_revenue,
    profit_margin_percent,
    order_count
FROM STAGING.product_performance
WHERE order_count > 0
ORDER BY net_revenue DESC
LIMIT 10;

-- 2. Customer Segment Performance
SELECT 
    customer_segment,
    COUNT(DISTINCT customer_id) as customers,
    COUNT(DISTINCT order_id) as orders,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_order_value
FROM STAGING.enriched_orders
WHERE order_status = 'COMPLETED'
GROUP BY customer_segment
ORDER BY revenue DESC;

-- 3. Low Stock Products
SELECT 
    product_name,
    category,
    current_stock,
    stock_status
FROM STAGING.current_inventory
WHERE stock_status = 'LOW'
ORDER BY current_stock ASC
LIMIT 10;

-- 4. Daily Sales Summary (Last 30 days)
SELECT 
    summary_date,
    total_orders,
    total_revenue,
    total_profit,
    avg_order_value
FROM ANALYTICS.daily_sales_summary
ORDER BY summary_date DESC
LIMIT 30;

-- 5. Sales by Country
SELECT 
    country,
    COUNT(DISTINCT customer_id) as customers,
    COUNT(DISTINCT order_id) as orders,
    SUM(total_amount) as revenue
FROM STAGING.enriched_orders
WHERE order_status = 'COMPLETED'
GROUP BY country
ORDER BY revenue DESC;
```

---

### ⏱️ STEP 6: Test Real-Time Features (5 minutes)

#### Add a new customer:
```sql
USE SCHEMA RAW_DATA;

INSERT INTO customers (
    customer_id, customer_name, email, phone, 
    country, city, signup_date, customer_segment
)
VALUES (
    999, 'Test User', 'test@example.com', '+1-555-9999',
    'USA', 'New York', CURRENT_DATE(), 'PREMIUM'
);
```

#### Check stream captures it:
```sql
SELECT * FROM STREAMS_TASKS.customers_stream 
WHERE customer_id = 999;
-- Should show the new customer with METADATA$ACTION = 'INSERT'
```

#### Wait 5-10 minutes, then verify in dimension:
```sql
SELECT * FROM ANALYTICS.dim_customer 
WHERE customer_id = 999 AND is_current = TRUE;
-- Should show the customer after task processes it
```

---

## Common Issues & Solutions

### Issue 1: Tasks not running
**Symptom:** Tasks show state = 'suspended'

**Solution:**
```sql
ALTER TASK STREAMS_TASKS.process_customer_changes RESUME;
ALTER TASK STREAMS_TASKS.load_fact_sales RESUME;
ALTER TASK STREAMS_TASKS.update_daily_summary RESUME;
```

---

### Issue 2: Dynamic tables not refreshing
**Symptom:** data_timestamp is old

**Solution:**
```sql
-- Force manual refresh
ALTER DYNAMIC TABLE STAGING.enriched_orders REFRESH;
ALTER DYNAMIC TABLE STAGING.product_performance REFRESH;
ALTER DYNAMIC TABLE STAGING.current_inventory REFRESH;
```

---

### Issue 3: No data in fact tables
**Symptom:** fact_sales is empty

**Possible causes:**
1. Tasks haven't run yet (wait 10-15 minutes after loading data)
2. Streams have no data (already processed or no changes)
3. Task failed (check error logs)

**Check:**
```sql
-- Check if streams have data
SELECT COUNT(*) FROM STREAMS_TASKS.order_items_stream;

-- Check task errors
SELECT 
    name, error_code, error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE error_code IS NOT NULL
ORDER BY scheduled_time DESC;

-- Manually execute task
EXECUTE TASK STREAMS_TASKS.load_fact_sales;
```

---

### Issue 4: Warehouse suspended
**Symptom:** Queries fail with "warehouse not active"

**Solution:**
```sql
ALTER WAREHOUSE DE_PROJECT_WH RESUME;
```

---

## Success Checklist

After completing all steps, verify:

- [ ] 5 tables in RAW_DATA schema populated
- [ ] 4 streams created and monitoring changes
- [ ] 3 dynamic tables showing recent refresh times
- [ ] 3 tasks in 'started' state
- [ ] Dimension tables (dim_customer, dim_product, dim_date) populated
- [ ] fact_sales table has records
- [ ] daily_sales_summary table has records
- [ ] All analytical queries return results
- [ ] New customer test passed (visible in dim_customer)

---

## What's Next?

### Explore Features:
1. **Modify a customer** to see SCD Type 2 in action:
```sql
UPDATE RAW_DATA.customers 
SET email = 'newemail@example.com' 
WHERE customer_id = 1;

-- Wait 5 minutes, then check:
SELECT * FROM ANALYTICS.dim_customer 
WHERE customer_id = 1 
ORDER BY effective_date DESC;
-- Should show 2 records: old (is_current=FALSE) and new (is_current=TRUE)
```

2. **Add more orders** to see real-time processing:
```sql
INSERT INTO RAW_DATA.orders VALUES 
(9999, 50, CURRENT_DATE(), 'COMPLETED', 'Credit Card', 10.00, 5.00);

INSERT INTO RAW_DATA.order_items VALUES 
(99999, 9999, 10, 2, 29.99, 0);

-- Check enriched_orders dynamic table (refreshes within 5 min)
SELECT * FROM STAGING.enriched_orders WHERE order_id = 9999;
```

3. **Monitor performance**:
```sql
-- Credit usage
SELECT 
    warehouse_name,
    SUM(credits_used) as total_credits
FROM TABLE(INFORMATION_SCHEMA.WAREHOUSE_METERING_HISTORY(
    DATEADD('day', -1, CURRENT_TIMESTAMP())
))
GROUP BY warehouse_name;

-- Query performance
SELECT 
    query_text,
    total_elapsed_time/1000 as seconds,
    bytes_scanned/(1024*1024*1024) as gb_scanned
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    CURRENT_TIMESTAMP()
))
ORDER BY total_elapsed_time DESC
LIMIT 10;
```

---

## Advanced Experimentation

### Experiment 1: Change Dynamic Table Lag
```sql
-- Make inventory updates faster
ALTER DYNAMIC TABLE STAGING.current_inventory 
    SET TARGET_LAG = '1 minute';
```

### Experiment 2: Add Task Dependency
```sql
-- Create a child task that runs after load_fact_sales
CREATE TASK STREAMS_TASKS.aggregate_daily_metrics
    WAREHOUSE = DE_PROJECT_WH
    AFTER STREAMS_TASKS.load_fact_sales
AS
BEGIN
    -- Your aggregation logic
    INSERT INTO analytics.daily_metrics
    SELECT ...;
END;

ALTER TASK STREAMS_TASKS.aggregate_daily_metrics RESUME;
```

### Experiment 3: Add Clustering
```sql
-- Improve query performance
ALTER TABLE ANALYTICS.fact_sales 
    CLUSTER BY (date_sk, customer_sk);

-- Monitor clustering impact
SELECT 
    SYSTEM$CLUSTERING_INFORMATION('fact_sales', '(date_sk)');
```

---

## Cleanup (Optional)

To remove everything and start fresh:

```sql
-- Suspend and drop tasks first
ALTER TASK STREAMS_TASKS.process_customer_changes SUSPEND;
ALTER TASK STREAMS_TASKS.load_fact_sales SUSPEND;
ALTER TASK STREAMS_TASKS.update_daily_summary SUSPEND;

DROP TASK IF EXISTS STREAMS_TASKS.process_customer_changes;
DROP TASK IF EXISTS STREAMS_TASKS.load_fact_sales;
DROP TASK IF EXISTS STREAMS_TASKS.update_daily_summary;

-- Drop streams
DROP STREAM IF EXISTS STREAMS_TASKS.orders_stream;
DROP STREAM IF EXISTS STREAMS_TASKS.order_items_stream;
DROP STREAM IF EXISTS STREAMS_TASKS.customers_stream;
DROP STREAM IF EXISTS STREAMS_TASKS.inventory_stream;

-- Drop dynamic tables
DROP DYNAMIC TABLE IF EXISTS STAGING.enriched_orders;
DROP DYNAMIC TABLE IF EXISTS STAGING.product_performance;
DROP DYNAMIC TABLE IF EXISTS STAGING.current_inventory;

-- Drop database (careful - removes everything)
DROP DATABASE IF EXISTS ECOMMERCE_DB;

-- Drop warehouse
DROP WAREHOUSE IF EXISTS DE_PROJECT_WH;
```

---

## Time Commitment Summary

| Step | Time | Activity |
|------|------|----------|
| 1 | 5 min | Run setup scripts |
| 2 | 10 min | Load sample data |
| 3 | 2 min | Activate tasks |
| 4 | 5-10 min | Wait for processing |
| 5 | 5 min | Run analytics queries |
| 6 | 5 min | Test real-time features |
| **Total** | **30-35 min** | **Complete setup** |

---

## Tips for Success

1. **Run scripts sequentially** - Don't skip steps
2. **Wait for processing** - Tasks run on schedule, give them time
3. **Check for errors** - Use monitoring queries frequently
4. **Start small** - Master the basics before extending
5. **Document changes** - Keep notes on what you modify
6. **Experiment safely** - Test on copies of tables first

---

## Support Resources

- **Full Documentation:** See README.md
- **Monitoring Queries:** See testing_monitoring_script.sql (Sections 2-10)
- **Sample Analytics:** See testing_monitoring_script.sql (Section 6)
- **Main Setup:** See snowflake_complete_project.sql

---

**Ready to start? Open `snowflake_complete_project.sql` and begin with STEP 1!** 🚀
