/*==============================================================================
  Tasty Bytes Data Quality Alerts
  
  This script creates multiple data quality alerts to monitor:
  - Missing or NULL values in critical fields
  - Data anomalies and outliers
  - Referential integrity issues
  - Business rule violations
==============================================================================*/

USE ROLE accountadmin;
USE DATABASE tasty_bytes;
USE SCHEMA raw_pos;

/*--
  Alert 1: Order Data Quality - Missing Critical Values
--*/

CREATE OR REPLACE ALERT tasty_bytes.raw_pos.order_missing_values_alert
  SCHEDULE = '30 MINUTE'
  IF (EXISTS (
    SELECT 1 
    FROM tasty_bytes.raw_pos.order_header 
    WHERE (ORDER_AMOUNT IS NULL OR ORDER_TOTAL IS NULL OR CUSTOMER_ID IS NULL)
    AND ORDER_TS > DATEADD(hour, -1, CURRENT_TIMESTAMP())
  ))
  THEN 
    BEGIN
      INSERT INTO tasty_bytes.telemetry.data_quality_alerts
      (alert_name, severity, category, message, record_count, affected_table)
      SELECT 
        'ORDER_MISSING_CRITICAL_VALUES',
        'ERROR',
        'DATA_QUALITY',
        'Orders detected with missing ORDER_AMOUNT, ORDER_TOTAL, or CUSTOMER_ID',
        COUNT(*),
        'raw_pos.order_header'
      FROM tasty_bytes.raw_pos.order_header 
      WHERE (ORDER_AMOUNT IS NULL OR ORDER_TOTAL IS NULL OR CUSTOMER_ID IS NULL)
      AND ORDER_TS > DATEADD(hour, -1, CURRENT_TIMESTAMP());
    END;

/*--
  Alert 2: Order Amount Anomaly Detection
--*/

CREATE OR REPLACE ALERT tasty_bytes.raw_pos.order_amount_anomaly_alert
  SCHEDULE = '1 HOUR'
  IF (EXISTS (
    SELECT 1 
    FROM tasty_bytes.raw_pos.order_header 
    WHERE ORDER_TOTAL > 1000  -- Unusually high order amounts
    AND ORDER_TS > DATEADD(hour, -1, CURRENT_TIMESTAMP())
  ))
  THEN 
    BEGIN
      INSERT INTO tasty_bytes.telemetry.data_quality_alerts
      (alert_name, severity, category, message, record_count, affected_table)
      SELECT 
        'ORDER_AMOUNT_ANOMALY',
        'WARNING',
        'ANOMALY_DETECTION',
        'Detected orders with unusually high amounts (>$1000)',
        COUNT(*),
        'raw_pos.order_header'
      FROM tasty_bytes.raw_pos.order_header 
      WHERE ORDER_TOTAL > 1000
      AND ORDER_TS > DATEADD(hour, -1, CURRENT_TIMESTAMP());
    END;

/*--
  Alert 3: Negative Order Totals
--*/

CREATE OR REPLACE ALERT tasty_bytes.raw_pos.negative_order_total_alert
  SCHEDULE = '15 MINUTE'
  IF (EXISTS (
    SELECT 1 
    FROM tasty_bytes.raw_pos.order_header 
    WHERE ORDER_TOTAL < 0
    AND ORDER_TS > DATEADD(hour, -1, CURRENT_TIMESTAMP())
  ))
  THEN 
    BEGIN
      INSERT INTO tasty_bytes.telemetry.data_quality_alerts
      (alert_name, severity, category, message, record_count, affected_table)
      SELECT 
        'NEGATIVE_ORDER_TOTAL',
        'CRITICAL',
        'DATA_QUALITY',
        'Orders detected with negative total amounts - possible data corruption',
        COUNT(*),
        'raw_pos.order_header'
      FROM tasty_bytes.raw_pos.order_header 
      WHERE ORDER_TOTAL < 0
      AND ORDER_TS > DATEADD(hour, -1, CURRENT_TIMESTAMP());
    END;

/*--
  Alert 4: Order-Detail Mismatch
--*/

CREATE OR REPLACE ALERT tasty_bytes.raw_pos.order_detail_mismatch_alert
  SCHEDULE = '1 HOUR'
  IF (EXISTS (
    SELECT 1
    FROM tasty_bytes.raw_pos.order_header oh
    LEFT JOIN tasty_bytes.raw_pos.order_detail od 
      ON oh.order_id = od.order_id
    WHERE od.order_id IS NULL
    AND oh.ORDER_TS > DATEADD(hour, -2, CURRENT_TIMESTAMP())
  ))
  THEN 
    BEGIN
      INSERT INTO tasty_bytes.telemetry.data_quality_alerts
      (alert_name, severity, category, message, record_count, affected_table)
      SELECT 
        'ORPHAN_ORDER_HEADERS',
        'WARNING',
        'REFERENTIAL_INTEGRITY',
        'Order headers found without corresponding order details',
        COUNT(*),
        'raw_pos.order_header'
      FROM tasty_bytes.raw_pos.order_header oh
      LEFT JOIN tasty_bytes.raw_pos.order_detail od 
        ON oh.order_id = od.order_id
      WHERE od.order_id IS NULL
      AND oh.ORDER_TS > DATEADD(hour, -2, CURRENT_TIMESTAMP());
    END;

/*--
  Alert 5: Customer Loyalty Data Quality
--*/

CREATE OR REPLACE ALERT tasty_bytes.raw_customer.customer_missing_email_alert
  SCHEDULE = '6 HOUR'
  IF (EXISTS (
    SELECT 1
    FROM tasty_bytes.raw_customer.customer_loyalty
    WHERE (E_MAIL IS NULL OR E_MAIL = '' OR NOT E_MAIL LIKE '%@%')
    AND SIGN_UP_DATE > DATEADD(day, -1, CURRENT_TIMESTAMP())
  ))
  THEN 
    BEGIN
      INSERT INTO tasty_bytes.telemetry.data_quality_alerts
      (alert_name, severity, category, message, record_count, affected_table)
      SELECT 
        'CUSTOMER_INVALID_EMAIL',
        'WARNING',
        'DATA_QUALITY',
        'New customer records with missing or invalid email addresses',
        COUNT(*),
        'raw_customer.customer_loyalty'
      FROM tasty_bytes.raw_customer.customer_loyalty
      WHERE (E_MAIL IS NULL OR E_MAIL = '' OR NOT E_MAIL LIKE '%@%')
      AND SIGN_UP_DATE > DATEADD(day, -1, CURRENT_TIMESTAMP());
    END;

/*--
  Alert 6: Menu Price Changes
--*/

CREATE OR REPLACE ALERT tasty_bytes.raw_pos.menu_price_spike_alert
  SCHEDULE = '12 HOUR'
  IF (EXISTS (
    SELECT 1
    FROM tasty_bytes.raw_pos.menu
    WHERE SALE_PRICE_USD > COST_OF_GOODS_USD * 5  -- Price is more than 5x cost
    OR SALE_PRICE_USD < COST_OF_GOODS_USD  -- Selling below cost
  ))
  THEN 
    BEGIN
      INSERT INTO tasty_bytes.telemetry.data_quality_alerts
      (alert_name, severity, category, message, record_count, affected_table)
      SELECT 
        'MENU_PRICING_ANOMALY',
        'WARNING',
        'BUSINESS_RULE',
        'Menu items with unusual pricing detected (>5x cost or below cost)',
        COUNT(*),
        'raw_pos.menu'
      FROM tasty_bytes.raw_pos.menu
      WHERE SALE_PRICE_USD > COST_OF_GOODS_USD * 5
      OR SALE_PRICE_USD < COST_OF_GOODS_USD;
    END;

/*--
  Alert 7: Duplicate Orders
--*/

CREATE OR REPLACE ALERT tasty_bytes.raw_pos.duplicate_orders_alert
  SCHEDULE = '2 HOUR'
  IF (EXISTS (
    SELECT 1
    FROM (
      SELECT 
        CUSTOMER_ID,
        ORDER_TS,
        ORDER_TOTAL,
        COUNT(*) as duplicate_count
      FROM tasty_bytes.raw_pos.order_header
      WHERE ORDER_TS > DATEADD(hour, -2, CURRENT_TIMESTAMP())
      GROUP BY CUSTOMER_ID, ORDER_TS, ORDER_TOTAL
      HAVING COUNT(*) > 1
    )
  ))
  THEN 
    BEGIN
      INSERT INTO tasty_bytes.telemetry.data_quality_alerts
      (alert_name, severity, category, message, record_count, affected_table)
      SELECT 
        'DUPLICATE_ORDERS_DETECTED',
        'WARNING',
        'DATA_QUALITY',
        'Potential duplicate orders detected (same customer, timestamp, and total)',
        SUM(duplicate_count - 1),
        'raw_pos.order_header'
      FROM (
        SELECT 
          CUSTOMER_ID,
          ORDER_TS,
          ORDER_TOTAL,
          COUNT(*) as duplicate_count
        FROM tasty_bytes.raw_pos.order_header
        WHERE ORDER_TS > DATEADD(hour, -2, CURRENT_TIMESTAMP())
        GROUP BY CUSTOMER_ID, ORDER_TS, ORDER_TOTAL
        HAVING COUNT(*) > 1
      );
    END;

/*--
  Alert 8: Future-Dated Orders
--*/

CREATE OR REPLACE ALERT tasty_bytes.raw_pos.future_dated_orders_alert
  SCHEDULE = '1 HOUR'
  IF (EXISTS (
    SELECT 1
    FROM tasty_bytes.raw_pos.order_header
    WHERE ORDER_TS > CURRENT_TIMESTAMP()
  ))
  THEN 
    BEGIN
      INSERT INTO tasty_bytes.telemetry.data_quality_alerts
      (alert_name, severity, category, message, record_count, affected_table)
      SELECT 
        'FUTURE_DATED_ORDERS',
        'ERROR',
        'DATA_QUALITY',
        'Orders detected with timestamps in the future',
        COUNT(*),
        'raw_pos.order_header'
      FROM tasty_bytes.raw_pos.order_header
      WHERE ORDER_TS > CURRENT_TIMESTAMP();
    END;

/*--
  Start All Alerts
--*/

ALTER ALERT tasty_bytes.raw_pos.order_missing_values_alert RESUME;
ALTER ALERT tasty_bytes.raw_pos.order_amount_anomaly_alert RESUME;
ALTER ALERT tasty_bytes.raw_pos.negative_order_total_alert RESUME;
ALTER ALERT tasty_bytes.raw_pos.order_detail_mismatch_alert RESUME;
ALTER ALERT tasty_bytes.raw_customer.customer_missing_email_alert RESUME;
ALTER ALERT tasty_bytes.raw_pos.menu_price_spike_alert RESUME;
ALTER ALERT tasty_bytes.raw_pos.duplicate_orders_alert RESUME;
ALTER ALERT tasty_bytes.raw_pos.future_dated_orders_alert RESUME;

/*--
  Show All Alerts
--*/

SHOW ALERTS IN SCHEMA tasty_bytes.raw_pos;
SHOW ALERTS IN SCHEMA tasty_bytes.raw_customer;

/*--
  Test Alerts with Bad Data
--*/

-- Insert test data that will trigger alerts

-- Trigger: Missing values alert
INSERT INTO tasty_bytes.raw_pos.order_header (
    ORDER_ID, TRUCK_ID, LOCATION_ID, CUSTOMER_ID, ORDER_TS, 
    ORDER_CURRENCY, ORDER_AMOUNT, ORDER_TOTAL
) VALUES 
(8001, 1, 1, NULL, CURRENT_TIMESTAMP(), 'USD', NULL, 45.00);  -- Missing CUSTOMER_ID and ORDER_AMOUNT

-- Trigger: Anomaly alert (high amount)
INSERT INTO tasty_bytes.raw_pos.order_header (
    ORDER_ID, TRUCK_ID, LOCATION_ID, CUSTOMER_ID, ORDER_TS, 
    ORDER_CURRENCY, ORDER_AMOUNT, ORDER_TOTAL
) VALUES 
(8002, 1, 1, 1, CURRENT_TIMESTAMP(), 'USD', 1500.00, 1500.00);  -- Unusually high amount

-- Trigger: Negative total alert
INSERT INTO tasty_bytes.raw_pos.order_header (
    ORDER_ID, TRUCK_ID, LOCATION_ID, CUSTOMER_ID, ORDER_TS, 
    ORDER_CURRENCY, ORDER_AMOUNT, ORDER_TOTAL
) VALUES 
(8003, 1, 1, 1, CURRENT_TIMESTAMP(), 'USD', -25.00, -25.00);  -- Negative amount

-- Trigger: Future dated order
INSERT INTO tasty_bytes.raw_pos.order_header (
    ORDER_ID, TRUCK_ID, LOCATION_ID, CUSTOMER_ID, ORDER_TS, 
    ORDER_CURRENCY, ORDER_AMOUNT, ORDER_TOTAL
) VALUES 
(8004, 1, 1, 1, DATEADD(day, 5, CURRENT_TIMESTAMP()), 'USD', 30.00, 30.00);  -- Future date

/*--
  Execute Alerts Manually for Testing
--*/

EXECUTE ALERT tasty_bytes.raw_pos.order_missing_values_alert;
EXECUTE ALERT tasty_bytes.raw_pos.order_amount_anomaly_alert;
EXECUTE ALERT tasty_bytes.raw_pos.negative_order_total_alert;
EXECUTE ALERT tasty_bytes.raw_pos.future_dated_orders_alert;

/*--
  Check Alert Results
--*/

SELECT * FROM tasty_bytes.telemetry.data_quality_alerts
ORDER BY alert_time DESC;

-- View recent alerts by severity
SELECT 
    severity,
    COUNT(*) as alert_count,
    LISTAGG(DISTINCT alert_name, ', ') as alert_types
FROM tasty_bytes.telemetry.data_quality_alerts
WHERE alert_time > DATEADD(day, -1, CURRENT_TIMESTAMP())
GROUP BY severity
ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'ERROR' THEN 2 
        WHEN 'WARNING' THEN 3 
        ELSE 4 
    END;

-- View alerts by category
SELECT 
    category,
    COUNT(*) as alert_count,
    MAX(alert_time) as last_alert_time
FROM tasty_bytes.telemetry.data_quality_alerts
WHERE alert_time > DATEADD(day, -7, CURRENT_TIMESTAMP())
GROUP BY category
ORDER BY alert_count DESC;

SELECT 'Data quality alerts created and activated successfully!' AS status;

/*
ALTER ALERT tasty_bytes.raw_pos.order_missing_values_alert SUSPEND;
ALTER ALERT tasty_bytes.raw_pos.order_amount_anomaly_alert SUSPEND;
ALTER ALERT tasty_bytes.raw_pos.negative_order_total_alert SUSPEND;
ALTER ALERT tasty_bytes.raw_pos.order_detail_mismatch_alert SUSPEND;
ALTER ALERT tasty_bytes.raw_customer.customer_missing_email_alert SUSPEND;
ALTER ALERT tasty_bytes.raw_pos.menu_price_spike_alert SUSPEND;
ALTER ALERT tasty_bytes.raw_pos.duplicate_orders_alert SUSPEND;
ALTER ALERT tasty_bytes.raw_pos.future_dated_orders_alert SUSPEND;
*/