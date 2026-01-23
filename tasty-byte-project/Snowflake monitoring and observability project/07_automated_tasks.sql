/*==============================================================================
  Tasty Bytes Automated Tasks
  
  This script creates automated tasks to:
  - Process streams on a schedule
  - Run stored procedures
  - Perform data quality checks
  - Generate reports
==============================================================================*/

USE ROLE accountadmin;
USE DATABASE tasty_bytes;

/*--
  Create Task Warehouse
--*/

CREATE OR REPLACE WAREHOUSE tasty_bytes_task_wh
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse for running scheduled tasks';

/*--
  Task 1: Process Daily Sales (Runs every 15 minutes)
--*/

CREATE OR REPLACE TASK tasty_bytes.harmonized.process_daily_sales_task
  WAREHOUSE = tasty_bytes_task_wh
  SCHEDULE = '15 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('tasty_bytes.raw_pos.order_header_stream')
AS
  CALL tasty_bytes.harmonized.process_daily_sales();

/*--
  Task 2: Process Regional Sales (Runs every 30 minutes)
--*/

CREATE OR REPLACE TASK tasty_bytes.harmonized.process_regional_sales_task
  WAREHOUSE = tasty_bytes_task_wh
  SCHEDULE = '30 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('tasty_bytes.raw_pos.order_header_stream')
AS
  CALL tasty_bytes.harmonized.process_regional_sales();

/*--
  Task 3: Analyze Customer Activity (Runs every hour)
--*/

CREATE OR REPLACE TASK tasty_bytes.harmonized.analyze_customer_activity_task
  WAREHOUSE = tasty_bytes_task_wh
  SCHEDULE = '60 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('tasty_bytes.raw_pos.order_header_stream')
AS
  CALL tasty_bytes.harmonized.analyze_customer_activity();

/*--
  Task 4: Analyze Menu Performance (Runs every hour)
--*/

CREATE OR REPLACE TASK tasty_bytes.harmonized.analyze_menu_performance_task
  WAREHOUSE = tasty_bytes_task_wh
  SCHEDULE = '60 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('tasty_bytes.raw_pos.order_detail_stream')
AS
  CALL tasty_bytes.harmonized.analyze_menu_performance();

/*--
  Task 5: Daily Sales Summary Email (Runs daily at 8 AM)
--*/

CREATE OR REPLACE TASK tasty_bytes.telemetry.send_daily_sales_summary_task
  WAREHOUSE = tasty_bytes_task_wh
  SCHEDULE = 'USING CRON 0 8 * * * UTC'  -- 8 AM UTC daily
AS
  CALL tasty_bytes.telemetry.send_daily_sales_summary();

/*--
  Task 6: Cleanup Old Telemetry Data (Runs daily at 2 AM)
--*/

CREATE OR REPLACE TASK tasty_bytes.telemetry.cleanup_old_telemetry_task
  WAREHOUSE = tasty_bytes_task_wh
  SCHEDULE = 'USING CRON 0 2 * * * UTC'  -- 2 AM UTC daily
AS
BEGIN
  -- Delete pipeline events older than 90 days
  DELETE FROM tasty_bytes.telemetry.pipeline_events
  WHERE event_timestamp < DATEADD(day, -90, CURRENT_TIMESTAMP());
  
  -- Delete resolved alerts older than 30 days
  DELETE FROM tasty_bytes.telemetry.data_quality_alerts
  WHERE resolved_flag = TRUE
  AND resolved_time < DATEADD(day, -30, CURRENT_TIMESTAMP());
  
  -- Delete audit trail older than 365 days
  DELETE FROM tasty_bytes.telemetry.audit_trail
  WHERE audit_timestamp < DATEADD(day, -365, CURRENT_TIMESTAMP());
END;

/*--
  Task 7: Update Operational Metrics (Runs every 5 minutes)
--*/

CREATE OR REPLACE TASK tasty_bytes.telemetry.update_operational_metrics_task
  WAREHOUSE = tasty_bytes_task_wh
  SCHEDULE = '5 MINUTE'
AS
BEGIN
  -- Insert stream metrics
  INSERT INTO tasty_bytes.telemetry.operational_metrics 
  (metric_name, metric_value, metric_unit, dimension_attributes)
  SELECT 
    'stream_pending_records',
    COUNT(*),
    'records',
    OBJECT_CONSTRUCT(
      'stream_name', 'order_header_stream',
      'database', 'tasty_bytes',
      'schema', 'raw_pos'
    )
  FROM tasty_bytes.raw_pos.order_header_stream;
  
  -- Insert alert metrics
  INSERT INTO tasty_bytes.telemetry.operational_metrics 
  (metric_name, metric_value, metric_unit, dimension_attributes)
  SELECT 
    'unresolved_alerts',
    COUNT(*),
    'count',
    OBJECT_CONSTRUCT(
      'severity', severity,
      'time_window', 'last_24h'
    )
  FROM tasty_bytes.telemetry.data_quality_alerts
  WHERE resolved_flag = FALSE
  AND alert_time > DATEADD(day, -1, CURRENT_TIMESTAMP())
  GROUP BY severity;
  
  -- Insert procedure performance metrics
  INSERT INTO tasty_bytes.telemetry.operational_metrics 
  (metric_name, metric_value, metric_unit, dimension_attributes)
  SELECT 
    'avg_procedure_execution_time',
    AVG(execution_time_ms),
    'milliseconds',
    OBJECT_CONSTRUCT(
      'procedure_name', procedure_name,
      'time_window', 'last_1h'
    )
  FROM tasty_bytes.telemetry.pipeline_events
  WHERE execution_time_ms IS NOT NULL
  AND event_timestamp > DATEADD(hour, -1, CURRENT_TIMESTAMP())
  GROUP BY procedure_name;
END;

/*--
  Task DAG: Create Task Dependencies (Parent-Child Relationships)
--*/

-- Create root task that runs first
CREATE OR REPLACE TASK tasty_bytes.harmonized.root_orchestrator_task
  WAREHOUSE = tasty_bytes_task_wh
  SCHEDULE = '30 MINUTE'
AS
  SELECT 'Orchestrator started' AS status;

-- Create child tasks that depend on root task
CREATE OR REPLACE TASK tasty_bytes.harmonized.sales_processing_child_task
  WAREHOUSE = tasty_bytes_task_wh
  AFTER tasty_bytes.harmonized.root_orchestrator_task
  WHEN SYSTEM$STREAM_HAS_DATA('tasty_bytes.raw_pos.order_header_stream')
AS
  CALL tasty_bytes.harmonized.process_daily_sales();

CREATE OR REPLACE TASK tasty_bytes.harmonized.customer_analysis_child_task
  WAREHOUSE = tasty_bytes_task_wh
  AFTER tasty_bytes.harmonized.sales_processing_child_task
AS
  CALL tasty_bytes.harmonized.analyze_customer_activity();

/*--
  Resume All Tasks
--*/

-- Resume independent tasks
ALTER TASK tasty_bytes.harmonized.process_daily_sales_task RESUME;
ALTER TASK tasty_bytes.harmonized.process_regional_sales_task RESUME;
ALTER TASK tasty_bytes.harmonized.analyze_customer_activity_task RESUME;
ALTER TASK tasty_bytes.harmonized.analyze_menu_performance_task RESUME;
ALTER TASK tasty_bytes.telemetry.send_daily_sales_summary_task RESUME;
ALTER TASK tasty_bytes.telemetry.cleanup_old_telemetry_task RESUME;
ALTER TASK tasty_bytes.telemetry.update_operational_metrics_task RESUME;

-- Resume task DAG (child tasks must be resumed before parent)
ALTER TASK tasty_bytes.harmonized.customer_analysis_child_task RESUME;
ALTER TASK tasty_bytes.harmonized.sales_processing_child_task RESUME;
ALTER TASK tasty_bytes.harmonized.root_orchestrator_task RESUME;

/*--
  Show All Tasks
--*/

SHOW TASKS IN SCHEMA tasty_bytes.harmonized;
SHOW TASKS IN SCHEMA tasty_bytes.telemetry;

/*--
  Monitor Task Execution
--*/

-- View task history
SELECT 
    name,
    state,
    scheduled_time,
    completed_time,
    DATEDIFF(second, scheduled_time, completed_time) as execution_seconds,
    error_code,
    error_message
FROM TABLE(information_schema.task_history(
    scheduled_time_range_start => DATEADD(hour, -24, CURRENT_TIMESTAMP())
))
ORDER BY scheduled_time DESC;

/*--
  Create Monitoring View for Task Health
--*/

CREATE OR REPLACE VIEW tasty_bytes.analytics.task_health_v AS
SELECT 
    name AS task_name,
    database_name,
    schema_name,
    state,
    schedule,
    warehouse,
    created_on,
    DATEDIFF(day, created_on, CURRENT_TIMESTAMP()) AS days_since_created
FROM TABLE(information_schema.tasks)
WHERE database_name = 'TASTY_BYTES'
ORDER BY schema_name, name;

/*--
  Execute Tasks Manually for Testing
--*/

-- Execute tasks manually (for testing)
EXECUTE TASK tasty_bytes.harmonized.process_daily_sales_task;
EXECUTE TASK tasty_bytes.telemetry.update_operational_metrics_task;

-- Wait a moment, then check results
SELECT * FROM tasty_bytes.telemetry.operational_metrics 
ORDER BY metric_timestamp DESC 
LIMIT 20;

-- View task execution history
SELECT * FROM tasty_bytes.analytics.task_health_v;

/*--
  Suspend All Tasks (for maintenance)
--*/

/*
-- Uncomment to suspend all tasks
ALTER TASK tasty_bytes.harmonized.root_orchestrator_task SUSPEND;
ALTER TASK tasty_bytes.harmonized.sales_processing_child_task SUSPEND;
ALTER TASK tasty_bytes.harmonized.customer_analysis_child_task SUSPEND;
ALTER TASK tasty_bytes.harmonized.process_daily_sales_task SUSPEND;
ALTER TASK tasty_bytes.harmonized.process_regional_sales_task SUSPEND;
ALTER TASK tasty_bytes.harmonized.analyze_customer_activity_task SUSPEND;
ALTER TASK tasty_bytes.harmonized.analyze_menu_performance_task SUSPEND;
ALTER TASK tasty_bytes.telemetry.send_daily_sales_summary_task SUSPEND;
ALTER TASK tasty_bytes.telemetry.cleanup_old_telemetry_task SUSPEND;
ALTER TASK tasty_bytes.telemetry.update_operational_metrics_task SUSPEND;
*/

SELECT 'Automated tasks created and activated successfully!' AS status;
