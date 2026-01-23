# Tasty Bytes Monitoring & Observability Project

## Overview

This project implements a comprehensive monitoring, logging, tracing, alerting, and notification system for the Tasty Bytes food truck data pipeline built on Snowflake. It demonstrates enterprise-grade observability practices including:

- **Distributed Tracing** - Track data flow through the entire pipeline
- **Comprehensive Logging** - Detailed execution logs for all procedures
- **Data Quality Alerts** - Automated detection of data issues
- **Email Notifications** - Stakeholder notifications for critical events
- **Automated Tasks** - Scheduled execution of data processing jobs
- **Performance Monitoring** - Track metrics and KPIs across the pipeline

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Data Sources                              │
│  (Order Header, Order Detail, Customer Loyalty, etc.)        │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│                 Change Data Capture                          │
│              (Streams on Raw Tables)                         │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────┐
│              Stored Procedures with                          │
│           Logging & Distributed Tracing                      │
│  - process_daily_sales()                                     │
│  - process_regional_sales()                                  │
│  - analyze_customer_activity()                               │
│  - analyze_menu_performance()                                │
└────────────────────┬────────────────────────────────────────┘
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
┌─────────────┐ ┌──────────┐ ┌──────────────┐
│  Aggregated │ │  Logs &  │ │  Metrics &   │
│    Tables   │ │  Traces  │ │   Alerts     │
└─────────────┘ └──────────┘ └──────────────┘
                     │
                     ▼
            ┌────────────────────┐
            │  Alert Triggers    │
            │  - Data Quality    │
            │  - Anomalies       │
            │  - Errors          │
            └─────────┬──────────┘
                      │
                      ▼
            ┌────────────────────┐
            │  Email             │
            │  Notifications     │
            └────────────────────┘
```

## Project Structure

### SQL Scripts (Execute in Order)

1. **01_setup_monitoring_infrastructure.sql**
   - Creates telemetry schema and tables
   - Sets up logging, tracing, and alert infrastructure
   - Creates monitoring views
   - Configures email notification integration

2. **02_create_streams.sql**
   - Creates change data capture streams on key tables
   - Sets up target aggregation tables
   - Creates monitoring views for stream health

3. **03_stored_procedures_with_logging.sql**
   - Implements data processing procedures with comprehensive logging
   - `process_daily_sales()` - Aggregates daily sales metrics
   - `process_regional_sales()` - Regional performance analysis
   - Logs execution to `pipeline_events` table

4. **04_stored_procedures_with_tracing.sql**
   - Implements procedures with distributed tracing
   - `analyze_customer_activity()` - Customer behavior analysis with full trace instrumentation
   - `analyze_menu_performance()` - Menu item performance tracking with span events
   - Uses OpenTelemetry concepts (trace IDs, span IDs, attributes, events)

5. **05_data_quality_alerts.sql**
   - Creates 8 automated data quality alerts
   - Monitors for NULL values, anomalies, duplicates, referential integrity
   - Alerts run on schedule and log to `data_quality_alerts` table

6. **06_notification_procedures.sql**
   - Email notification procedures with HTML formatting
   - `notify_data_quality_team()` - Sends data quality alert emails
   - `send_daily_sales_summary()` - Daily sales report emails
   - `notify_critical_errors()` - Critical error notifications

7. **07_automated_tasks.sql**
   - Automated task scheduling
   - Task DAGs with dependencies
   - Operational metrics collection
   - Automated cleanup jobs

## Key Components

### Telemetry Tables

#### `telemetry.pipeline_events`
Stores all logs, traces, and execution events from stored procedures.

**Schema:**
- `event_id` - Unique identifier
- `event_timestamp` - When the event occurred
- `record_type` - 'LOG' or 'SPAN'
- `severity_level` - INFO, WARNING, ERROR
- `procedure_name` - Source procedure
- `trace_id` - Distributed trace identifier
- `span_id` - Span identifier for tracing
- `process_step` - Current execution step
- `message` - Event message
- `attributes` - JSON metadata
- `error_flag` - Boolean error indicator
- `execution_time_ms` - Execution duration
- `records_processed` - Number of records handled

#### `telemetry.data_quality_alerts`
Stores data quality issues detected by alerts.

**Schema:**
- `alert_id` - Unique identifier
- `alert_time` - When alert fired
- `alert_name` - Alert identifier
- `severity` - CRITICAL, ERROR, WARNING
- `category` - DATA_QUALITY, ANOMALY_DETECTION, BUSINESS_RULE
- `message` - Alert description
- `record_count` - Affected records
- `affected_table` - Source table
- `resolved_flag` - Resolution status

#### `telemetry.operational_metrics`
Stores operational KPIs and metrics.

**Schema:**
- `metric_timestamp` - Measurement time
- `metric_name` - Metric identifier
- `metric_value` - Numeric value
- `metric_unit` - Unit of measurement
- `dimension_attributes` - JSON metadata for grouping

### Data Quality Alerts

1. **order_missing_values_alert** - Detects NULL values in critical fields
2. **order_amount_anomaly_alert** - Identifies unusually high order amounts
3. **negative_order_total_alert** - Flags negative order totals
4. **order_detail_mismatch_alert** - Finds orphan order headers
5. **customer_missing_email_alert** - Validates customer email addresses
6. **menu_price_spike_alert** - Detects pricing anomalies
7. **duplicate_orders_alert** - Identifies potential duplicate records
8. **future_dated_orders_alert** - Flags orders with future timestamps

### Stored Procedures

#### Data Processing Procedures

**process_daily_sales()**
- Reads from `order_header_stream`
- Enriches with truck and menu data
- Aggregates by date, brand, and city
- Writes to `daily_sales_summary`
- Logs execution metrics

**process_regional_sales()**
- Processes orders by geographic region
- Joins with location data
- Aggregates regional metrics
- Writes to `regional_sales_summary`

**analyze_customer_activity()**
- Analyzes customer purchase behavior
- Aggregates favorite items and locations
- Implements full distributed tracing
- Writes to `customer_activity_summary`

**analyze_menu_performance()**
- Tracks menu item sales performance
- Calculates revenue and order frequency
- Includes trace instrumentation
- Writes to `menu_item_performance`

#### Notification Procedures

**notify_data_quality_team()**
- Queries unresolved data quality alerts
- Generates styled HTML email
- Includes severity summary and details
- Sends via email integration

**send_daily_sales_summary()**
- Generates daily sales report
- Includes KPI cards and detailed tables
- Professional HTML formatting
- Scheduled delivery

**notify_critical_errors()**
- Monitors pipeline_events for errors
- Immediate notification for critical issues
- Includes error details and trace IDs
- Urgent email alerts

## Usage

### Initial Setup

1. **Load Tasty Bytes Data**
   ```sql
   -- Run the load_tasty_bytes.sql script first
   @load_tasty_bytes.sql
   ```

2. **Setup Monitoring Infrastructure**
   ```sql
   @01_setup_monitoring_infrastructure.sql
   ```

3. **Create Streams**
   ```sql
   @02_create_streams.sql
   ```

4. **Deploy Stored Procedures**
   ```sql
   @03_stored_procedures_with_logging.sql
   @04_stored_procedures_with_tracing.sql
   ```

5. **Configure Alerts**
   ```sql
   @05_data_quality_alerts.sql
   ```

6. **Setup Notifications**
   ```sql
   -- Update email addresses in the script first!
   @06_notification_procedures.sql
   ```

7. **Enable Automation**
   ```sql
   @07_automated_tasks.sql
   ```

### Monitoring Operations

#### View Recent Logs
```sql
SELECT * 
FROM tasty_bytes.telemetry.pipeline_events 
WHERE record_type = 'LOG'
ORDER BY event_timestamp DESC 
LIMIT 50;
```

#### View Distributed Traces
```sql
SELECT 
    trace_id,
    procedure_name,
    process_step,
    message,
    execution_time_ms,
    attributes
FROM tasty_bytes.telemetry.pipeline_events 
WHERE record_type = 'SPAN'
  AND trace_id = '<your-trace-id>'
ORDER BY event_timestamp;
```

#### View Active Alerts
```sql
SELECT * 
FROM tasty_bytes.telemetry.recent_alerts_v
WHERE resolved_flag = FALSE;
```

#### View Performance Metrics
```sql
SELECT * 
FROM tasty_bytes.telemetry.performance_summary_v;
```

#### View Task Health
```sql
SELECT * 
FROM tasty_bytes.analytics.task_health_v;
```

### Manual Execution

#### Execute Stored Procedures
```sql
CALL tasty_bytes.harmonized.process_daily_sales();
CALL tasty_bytes.harmonized.analyze_customer_activity();
```

#### Execute Alerts
```sql
EXECUTE ALERT tasty_bytes.raw_pos.order_missing_values_alert;
```

#### Execute Tasks
```sql
EXECUTE TASK tasty_bytes.harmonized.process_daily_sales_task;
```

#### Send Notifications
```sql
CALL tasty_bytes.telemetry.notify_data_quality_team();
```

### Troubleshooting

#### Check Stream Status
```sql
SELECT * FROM tasty_bytes.analytics.stream_monitoring_v;
```

#### View Task Execution History
```sql
SELECT 
    name,
    state,
    scheduled_time,
    error_message
FROM TABLE(information_schema.task_history(
    scheduled_time_range_start => DATEADD(hour, -24, CURRENT_TIMESTAMP())
))
WHERE database_name = 'TASTY_BYTES'
ORDER BY scheduled_time DESC;
```

#### Check Alert Status
```sql
SHOW ALERTS IN DATABASE tasty_bytes;
```

#### View Error Summary
```sql
SELECT * FROM tasty_bytes.telemetry.error_summary_v;
```

## Configuration

### Email Recipients

Update email addresses in these files before deploying:
- `01_setup_monitoring_infrastructure.sql` - Email integration
- `06_notification_procedures.sql` - All notification procedures

### Alert Schedules

Adjust alert schedules in `05_data_quality_alerts.sql`:
```sql
SCHEDULE = '15 MINUTE'  -- Change as needed
```

### Task Schedules

Modify task schedules in `07_automated_tasks.sql`:
```sql
SCHEDULE = 'USING CRON 0 8 * * * UTC'  -- Cron expression
SCHEDULE = '30 MINUTE'                  -- Interval
```

## Best Practices

1. **Trace ID Management** - Each procedure execution generates a unique trace ID for correlation
2. **Span Instrumentation** - Break complex procedures into logical spans for better observability
3. **Error Handling** - Always log errors to telemetry tables with full context
4. **Alert Tuning** - Regularly review and adjust alert thresholds to reduce noise
5. **Performance Monitoring** - Track execution times and optimize slow procedures
6. **Data Retention** - Use the cleanup task to manage telemetry data growth

## Maintenance

### Daily
- Review data quality alerts
- Check error logs
- Monitor task execution

### Weekly
- Analyze performance trends
- Review and resolve alerts
- Optimize slow-running procedures

### Monthly
- Review alert effectiveness
- Update notification recipients
- Tune task schedules
- Archive old telemetry data

## Security Considerations

- Email integration uses Snowflake's secure notification system
- All sensitive data is logged with appropriate masking
- Access to telemetry schema should be restricted
- Regular audits of notification recipients

## Support

For issues or questions:
1. Check the telemetry tables for error details
2. Review trace IDs for distributed debugging
3. Examine alert history for patterns
4. Contact the data engineering team

## License

This project is part of the Tasty Bytes demonstration dataset and is provided as-is for educational purposes.
