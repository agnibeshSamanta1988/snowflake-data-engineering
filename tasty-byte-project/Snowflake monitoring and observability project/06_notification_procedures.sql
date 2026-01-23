/*==============================================================================
  Tasty Bytes Notification Procedures
  
  This script creates notification procedures that send email alerts for:
  - Data quality issues
  - Operational anomalies
  - Daily summary reports
  - Critical system events
==============================================================================*/

USE ROLE accountadmin;
USE DATABASE tasty_bytes;
USE SCHEMA telemetry;

/*--
  Notification Procedure 1: Data Quality Alert Notification
--*/

CREATE OR REPLACE PROCEDURE tasty_bytes.telemetry.notify_data_quality_team()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'notify_data_quality_team'
AS 
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
from datetime import datetime

def notify_data_quality_team(session: Session) -> str:
    # Query recent unresolved alerts
    alerts = session.table("tasty_bytes.telemetry.data_quality_alerts") \
                    .filter(F.col("RESOLVED_FLAG") == False) \
                    .filter(F.col("ALERT_TIME") > F.dateadd('hour', F.lit(-6), F.current_timestamp())) \
                    .select(
                        F.col("ALERT_NAME"),
                        F.col("SEVERITY"),
                        F.col("CATEGORY"),
                        F.col("MESSAGE"),
                        F.col("RECORD_COUNT"),
                        F.col("AFFECTED_TABLE"),
                        F.col("ALERT_TIME")
                    ) \
                    .order_by(F.col("SEVERITY"), F.col("ALERT_TIME").desc())
    
    alert_count = alerts.count()
    
    if alert_count == 0:
        return "No unresolved data quality alerts"
    
    # Convert to pandas for HTML formatting
    alerts_pd = alerts.to_pandas()
    
    # Create styled HTML table
    html_table = alerts_pd.to_html(index=False, classes='alert-table', na_rep='N/A')
    
    # Count alerts by severity
    critical_count = alerts_pd[alerts_pd['SEVERITY'] == 'CRITICAL'].shape[0]
    error_count = alerts_pd[alerts_pd['SEVERITY'] == 'ERROR'].shape[0]
    warning_count = alerts_pd[alerts_pd['SEVERITY'] == 'WARNING'].shape[0]
    
    # Build email content
    email_content = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #f5f5f5;
                margin: 0;
                padding: 20px;
            }}
            .container {{
                max-width: 900px;
                margin: 0 auto;
                background-color: white;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                overflow: hidden;
            }}
            .header {{
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                text-align: center;
            }}
            .header h1 {{
                margin: 0;
                font-size: 28px;
            }}
            .summary {{
                display: flex;
                justify-content: space-around;
                padding: 20px;
                background-color: #f8f9fa;
                border-bottom: 2px solid #e9ecef;
            }}
            .summary-item {{
                text-align: center;
                padding: 15px;
            }}
            .summary-item .number {{
                font-size: 32px;
                font-weight: bold;
                margin-bottom: 5px;
            }}
            .summary-item .label {{
                font-size: 14px;
                color: #6c757d;
                text-transform: uppercase;
            }}
            .critical {{ color: #dc3545; }}
            .error {{ color: #fd7e14; }}
            .warning {{ color: #ffc107; }}
            .content {{
                padding: 30px;
            }}
            .alert-table {{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
                font-size: 14px;
                box-shadow: 0 2px 3px rgba(0,0,0,0.1);
            }}
            .alert-table thead tr {{
                background-color: #667eea;
                color: white;
                text-align: left;
                font-weight: bold;
            }}
            .alert-table th,
            .alert-table td {{
                padding: 12px 15px;
                border-bottom: 1px solid #e9ecef;
            }}
            .alert-table tbody tr:hover {{
                background-color: #f8f9fa;
            }}
            .alert-table tbody tr:nth-of-type(even) {{
                background-color: #f8f9fa;
            }}
            .footer {{
                background-color: #f8f9fa;
                padding: 20px;
                text-align: center;
                font-size: 12px;
                color: #6c757d;
                border-top: 2px solid #e9ecef;
            }}
            .timestamp {{
                color: #6c757d;
                font-size: 12px;
                margin-top: 10px;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🚨 Tasty Bytes Data Quality Alert</h1>
                <p class="timestamp">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}</p>
            </div>
            
            <div class="summary">
                <div class="summary-item">
                    <div class="number critical">{critical_count}</div>
                    <div class="label">Critical</div>
                </div>
                <div class="summary-item">
                    <div class="number error">{error_count}</div>
                    <div class="label">Errors</div>
                </div>
                <div class="summary-item">
                    <div class="number warning">{warning_count}</div>
                    <div class="label">Warnings</div>
                </div>
                <div class="summary-item">
                    <div class="number">{alert_count}</div>
                    <div class="label">Total Alerts</div>
                </div>
            </div>
            
            <div class="content">
                <h2>Active Data Quality Alerts (Last 6 Hours)</h2>
                <p>The following data quality issues require attention:</p>
                {html_table}
                
                <h3>Recommended Actions:</h3>
                <ul>
                    <li>Review affected records in the specified tables</li>
                    <li>Investigate root cause of data quality issues</li>
                    <li>Implement data validation rules where appropriate</li>
                    <li>Update alert status once issues are resolved</li>
                </ul>
            </div>
            
            <div class="footer">
                <p>This is an automated notification from the Tasty Bytes Data Quality Monitoring System</p>
                <p>To manage your alert preferences or mark alerts as resolved, access the Snowflake console</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    # Send email
    session.call("system$send_email",
                 "tasty_bytes_email_integration",
                 "agnibesh.samanta@gmail.com",  # Update with actual email
                 f"⚠️ Data Quality Alert: {alert_count} Issues Detected",
                 email_content,
                 "text/html")
    
    return f"Data quality alert email sent successfully. {alert_count} alerts reported."
$$;

/*--
  Notification Procedure 2: Daily Sales Summary Report
--*/

CREATE OR REPLACE PROCEDURE tasty_bytes.telemetry.send_daily_sales_summary()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'send_daily_sales_summary'
AS 
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
from datetime import datetime, timedelta

def send_daily_sales_summary(session: Session) -> str:
    # Get yesterday's sales summary
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Query daily sales data
    daily_sales = session.table("tasty_bytes.harmonized.daily_sales_summary") \
                         .filter(F.col("SALES_DATE") == yesterday) \
                         .select(
                             F.col("TRUCK_BRAND_NAME"),
                             F.col("PRIMARY_CITY"),
                             F.col("REGION"),
                             F.col("TOTAL_ORDERS"),
                             F.col("TOTAL_REVENUE"),
                             F.col("UNIQUE_CUSTOMERS"),
                             F.col("AVG_ORDER_VALUE")
                         )
    
    record_count = daily_sales.count()
    
    if record_count == 0:
        return f"No sales data available for {yesterday}"
    
    # Calculate totals
    totals = daily_sales.agg(
        F.sum("TOTAL_ORDERS").alias("total_orders"),
        F.sum("TOTAL_REVENUE").alias("total_revenue"),
        F.sum("UNIQUE_CUSTOMERS").alias("unique_customers"),
        F.avg("AVG_ORDER_VALUE").alias("avg_order_value")
    ).collect()[0]
    
    # Convert to pandas for display
    sales_pd = daily_sales.order_by(F.col("TOTAL_REVENUE").desc()).to_pandas()
    
    # Format currency values
    sales_pd['TOTAL_REVENUE'] = sales_pd['TOTAL_REVENUE'].apply(lambda x: f'${x:,.2f}')
    sales_pd['AVG_ORDER_VALUE'] = sales_pd['AVG_ORDER_VALUE'].apply(lambda x: f'${x:,.2f}')
    
    html_table = sales_pd.to_html(index=False, classes='sales-table')
    
    # Build email
    email_content = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #f5f5f5;
                margin: 0;
                padding: 20px;
            }}
            .container {{
                max-width: 900px;
                margin: 0 auto;
                background-color: white;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            .header {{
                background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
                color: white;
                padding: 30px;
                text-align: center;
            }}
            .kpi-section {{
                display: grid;
                grid-template-columns: repeat(4, 1fr);
                gap: 15px;
                padding: 30px;
                background-color: #f8f9fa;
            }}
            .kpi-card {{
                background: white;
                padding: 20px;
                border-radius: 8px;
                text-align: center;
                box-shadow: 0 2px 3px rgba(0,0,0,0.1);
            }}
            .kpi-value {{
                font-size: 28px;
                font-weight: bold;
                color: #11998e;
                margin: 10px 0;
            }}
            .kpi-label {{
                font-size: 12px;
                color: #6c757d;
                text-transform: uppercase;
            }}
            .content {{
                padding: 30px;
            }}
            .sales-table {{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
                font-size: 14px;
            }}
            .sales-table thead tr {{
                background-color: #11998e;
                color: white;
                text-align: left;
            }}
            .sales-table th,
            .sales-table td {{
                padding: 12px 15px;
                border-bottom: 1px solid #e9ecef;
            }}
            .sales-table tbody tr:hover {{
                background-color: #f8f9fa;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>📊 Daily Sales Summary Report</h1>
                <h3>{yesterday}</h3>
            </div>
            
            <div class="kpi-section">
                <div class="kpi-card">
                    <div class="kpi-label">Total Orders</div>
                    <div class="kpi-value">{totals['TOTAL_ORDERS']:,}</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Total Revenue</div>
                    <div class="kpi-value">${totals['TOTAL_REVENUE']:,.2f}</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Unique Customers</div>
                    <div class="kpi-value">{totals['UNIQUE_CUSTOMERS']:,}</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-label">Avg Order Value</div>
                    <div class="kpi-value">${totals['AVG_ORDER_VALUE']:,.2f}</div>
                </div>
            </div>
            
            <div class="content">
                <h2>Sales by Brand and Location</h2>
                {html_table}
            </div>
        </div>
    </body>
    </html>
    """
    
    # Send email
    session.call("system$send_email",
                 "tasty_bytes_email_integration",
                 "agnibesh.samanta@gmail.com",  # Update with actual email
                 f"📈 Daily Sales Summary - {yesterday}",
                 email_content,
                 "text/html")
    
    return f"Daily sales summary sent for {yesterday}"
$$;

/*--
  Notification Procedure 3: Critical Error Notification
--*/

CREATE OR REPLACE PROCEDURE tasty_bytes.telemetry.notify_critical_errors()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'notify_critical_errors'
AS 
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
from datetime import datetime

def notify_critical_errors(session: Session) -> str:
    # Query recent errors from pipeline_events
    errors = session.table("tasty_bytes.telemetry.pipeline_events") \
                    .filter(F.col("ERROR_FLAG") == True) \
                    .filter(F.col("EVENT_TIMESTAMP") > F.dateadd('hour', F.lit(-1), F.current_timestamp())) \
                    .select(
                        F.col("EVENT_TIMESTAMP"),
                        F.col("PROCEDURE_NAME"),
                        F.col("PROCESS_STEP"),
                        F.col("ERROR_MESSAGE"),
                        F.col("TRACE_ID")
                    ) \
                    .order_by(F.col("EVENT_TIMESTAMP").desc())
    
    error_count = errors.count()
    
    if error_count == 0:
        return "No critical errors in the last hour"
    
    errors_pd = errors.to_pandas()
    html_table = errors_pd.to_html(index=False, classes='error-table')
    
    email_content = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                background-color: #f5f5f5;
                padding: 20px;
            }}
            .container {{
                max-width: 900px;
                margin: 0 auto;
                background: white;
                border-radius: 8px;
                overflow: hidden;
            }}
            .header {{
                background-color: #dc3545;
                color: white;
                padding: 30px;
                text-align: center;
            }}
            .content {{
                padding: 30px;
            }}
            .error-table {{
                width: 100%;
                border-collapse: collapse;
                margin: 20px 0;
            }}
            .error-table thead tr {{
                background-color: #dc3545;
                color: white;
            }}
            .error-table th,
            .error-table td {{
                padding: 12px;
                border: 1px solid #ddd;
                text-align: left;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🚨 CRITICAL: Pipeline Errors Detected</h1>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>
            <div class="content">
                <h2>{error_count} Error(s) in the Last Hour</h2>
                <p><strong>Immediate action required!</strong></p>
                {html_table}
                <h3>Next Steps:</h3>
                <ul>
                    <li>Review error messages and trace IDs</li>
                    <li>Check procedure execution logs</li>
                    <li>Verify data pipeline integrity</li>
                    <li>Contact DevOps if issue persists</li>
                </ul>
            </div>
        </div>
    </body>
    </html>
    """
    
    session.call("system$send_email",
                 "tasty_bytes_email_integration",
                 "agnibesh.samanta@gmail.com",  # Update with actual email
                 f"🚨 CRITICAL: {error_count} Pipeline Errors Detected",
                 email_content,
                 "text/html")
    
    return f"Critical error notification sent. {error_count} errors reported."
$$;

/*--
  Test Notification Procedures
--*/

-- Call notification procedures
CALL tasty_bytes.telemetry.notify_data_quality_team();
CALL tasty_bytes.telemetry.send_daily_sales_summary();
CALL tasty_bytes.telemetry.notify_critical_errors();

/*--
  Create Alert to Trigger Notifications
--*/

-- Alert that calls the data quality notification procedure
CREATE OR REPLACE ALERT tasty_bytes.telemetry.data_quality_notification_alert
  SCHEDULE = '6 HOUR'
  IF (EXISTS (
    SELECT 1 
    FROM tasty_bytes.telemetry.data_quality_alerts
    WHERE RESOLVED_FLAG = FALSE
    AND ALERT_TIME > DATEADD(hour, -6, CURRENT_TIMESTAMP())
  ))
  THEN 
    CALL tasty_bytes.telemetry.notify_data_quality_team();

-- Alert that calls the critical error notification procedure
CREATE OR REPLACE ALERT tasty_bytes.telemetry.critical_error_notification_alert
  SCHEDULE = '1 HOUR'
  IF (EXISTS (
    SELECT 1
    FROM tasty_bytes.telemetry.pipeline_events
    WHERE ERROR_FLAG = TRUE
    AND EVENT_TIMESTAMP > DATEADD(hour, -1, CURRENT_TIMESTAMP())
  ))
  THEN 
    CALL tasty_bytes.telemetry.notify_critical_errors();

-- Start notification alerts
ALTER ALERT tasty_bytes.telemetry.data_quality_notification_alert RESUME;
ALTER ALERT tasty_bytes.telemetry.critical_error_notification_alert RESUME;

SHOW ALERTS IN SCHEMA tasty_bytes.telemetry;

SELECT 'Notification procedures and alerts created successfully!' AS status;
