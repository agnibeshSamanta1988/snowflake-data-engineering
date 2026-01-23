/*==============================================================================
  Tasty Bytes Monitoring & Observability Infrastructure Setup
  
  This script creates the telemetry infrastructure for monitoring the 
  Tasty Bytes data pipeline including:
  - Telemetry schema and tables for logs, traces, and alerts
  - Event logging tables
  - Notification infrastructure
  - Data quality monitoring tables
==============================================================================*/

USE ROLE accountadmin;
USE DATABASE tasty_bytes;

/*--
  Telemetry Schema Creation
--*/

-- Create telemetry schema for monitoring and observability
CREATE OR REPLACE SCHEMA tasty_bytes.telemetry
COMMENT = 'Schema for storing monitoring data, logs, traces, and alerts';

USE SCHEMA tasty_bytes.telemetry;

/*--
  Event and Log Tables
--*/

-- Pipeline events table for storing logs and traces
CREATE OR REPLACE TABLE tasty_bytes.telemetry.pipeline_events (
    event_id NUMBER AUTOINCREMENT,
    event_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    record_type VARCHAR(50),
    severity_level VARCHAR(20),
    procedure_name VARCHAR(200),
    trace_id VARCHAR(100),
    span_id VARCHAR(100),
    process_step VARCHAR(100),
    message TEXT,
    attributes VARIANT,
    error_flag BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    execution_time_ms NUMBER,
    records_processed NUMBER,
    CONSTRAINT pk_pipeline_events PRIMARY KEY (event_id)
)
COMMENT = 'Centralized table for pipeline execution logs, traces, and events';

-- Data quality alerts table
CREATE OR REPLACE TABLE tasty_bytes.telemetry.data_quality_alerts (
    alert_id NUMBER AUTOINCREMENT,
    alert_time TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    alert_name VARCHAR(200),
    severity VARCHAR(20),
    category VARCHAR(50),
    message TEXT,
    record_count INTEGER,
    affected_table VARCHAR(200),
    resolved_flag BOOLEAN DEFAULT FALSE,
    resolved_time TIMESTAMP_NTZ,
    CONSTRAINT pk_dq_alerts PRIMARY KEY (alert_id)
)
COMMENT = 'Data quality alerts and anomalies detected in the pipeline';

-- Operational metrics table
CREATE OR REPLACE TABLE tasty_bytes.telemetry.operational_metrics (
    metric_id NUMBER AUTOINCREMENT,
    metric_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    metric_name VARCHAR(100),
    metric_value NUMBER(38,4),
    metric_unit VARCHAR(20),
    dimension_attributes VARIANT,
    CONSTRAINT pk_op_metrics PRIMARY KEY (metric_id)
)
COMMENT = 'Operational metrics for monitoring pipeline performance';

-- Audit trail table
CREATE OR REPLACE TABLE tasty_bytes.telemetry.audit_trail (
    audit_id NUMBER AUTOINCREMENT,
    audit_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    user_name VARCHAR(100),
    role_name VARCHAR(100),
    operation_type VARCHAR(50),
    object_type VARCHAR(50),
    object_name VARCHAR(200),
    operation_details VARIANT,
    success_flag BOOLEAN,
    CONSTRAINT pk_audit_trail PRIMARY KEY (audit_id)
)
COMMENT = 'Audit trail for tracking data operations and changes';

-- Performance monitoring table
CREATE OR REPLACE TABLE tasty_bytes.telemetry.query_performance (
    query_id VARCHAR(100),
    query_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    query_text TEXT,
    execution_time_ms NUMBER,
    rows_processed NUMBER,
    bytes_scanned NUMBER,
    warehouse_name VARCHAR(100),
    user_name VARCHAR(100),
    CONSTRAINT pk_query_perf PRIMARY KEY (query_id)
)
COMMENT = 'Query performance metrics for optimization';

/*--
  Monitoring Views
--*/

-- Recent alerts view
CREATE OR REPLACE VIEW tasty_bytes.telemetry.recent_alerts_v AS
SELECT 
    alert_id,
    alert_time,
    alert_name,
    severity,
    category,
    message,
    record_count,
    affected_table,
    resolved_flag
FROM tasty_bytes.telemetry.data_quality_alerts
WHERE alert_time > DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY alert_time DESC;

-- Error summary view
CREATE OR REPLACE VIEW tasty_bytes.telemetry.error_summary_v AS
SELECT 
    DATE(event_timestamp) AS error_date,
    procedure_name,
    COUNT(*) AS error_count,
    LISTAGG(DISTINCT error_message, '; ') AS error_messages
FROM tasty_bytes.telemetry.pipeline_events
WHERE error_flag = TRUE
    AND event_timestamp > DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY DATE(event_timestamp), procedure_name
ORDER BY error_date DESC, error_count DESC;

-- Performance metrics view
CREATE OR REPLACE VIEW tasty_bytes.telemetry.performance_summary_v AS
SELECT 
    procedure_name,
    COUNT(*) AS execution_count,
    AVG(execution_time_ms) AS avg_execution_time_ms,
    MAX(execution_time_ms) AS max_execution_time_ms,
    MIN(execution_time_ms) AS min_execution_time_ms,
    SUM(records_processed) AS total_records_processed
FROM tasty_bytes.telemetry.pipeline_events
WHERE event_timestamp > DATEADD(day, -7, CURRENT_TIMESTAMP())
    AND execution_time_ms IS NOT NULL
GROUP BY procedure_name
ORDER BY avg_execution_time_ms DESC;

/*--
  Email Notification Integration
--*/

-- Create email notification integration
CREATE OR REPLACE NOTIFICATION INTEGRATION tasty_bytes_email_integration
TYPE = EMAIL
ENABLED = TRUE
ALLOWED_RECIPIENTS = ('data-team@tastybytes.com')  -- Update with actual email
COMMENT = 'Email integration for Tasty Bytes monitoring alerts';

/*--
  Account-Level Configuration
--*/

-- Set account-level logging
ALTER ACCOUNT SET LOG_LEVEL = 'INFO';

-- Enable query tags for better tracking
ALTER SESSION SET QUERY_TAG = 'TASTY_BYTES_MONITORING_SETUP';

/*--
  Grant Privileges
--*/

-- Grant usage on telemetry schema
GRANT USAGE ON SCHEMA tasty_bytes.telemetry TO ROLE sysadmin;

-- Grant select on all tables in telemetry schema
GRANT SELECT ON ALL TABLES IN SCHEMA tasty_bytes.telemetry TO ROLE sysadmin;

-- Grant select on all views in telemetry schema
GRANT SELECT ON ALL VIEWS IN SCHEMA tasty_bytes.telemetry TO ROLE sysadmin;

/*--
  Verification
--*/

-- Show created objects
SHOW TABLES IN SCHEMA tasty_bytes.telemetry;
SHOW VIEWS IN SCHEMA tasty_bytes.telemetry;
SHOW NOTIFICATION INTEGRATIONS LIKE 'tasty_bytes_email_integration';

SELECT 'Monitoring infrastructure setup completed successfully!' AS status;
