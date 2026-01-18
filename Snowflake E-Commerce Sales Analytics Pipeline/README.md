# 🏪 Snowflake E-Commerce Data Engineering Project
### Complete Real-Time Analytics Pipeline with Dynamic Tables, Streams & Tasks

[![Snowflake](https://img.shields.io/badge/Snowflake-Ready-29B5E8?style=for-the-badge&logo=snowflake)](https://www.snowflake.com/)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=for-the-badge&logo=streamlit)](https://streamlit.io/)
[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=for-the-badge&logo=python)](https://www.python.org/)
[![SQL](https://img.shields.io/badge/SQL-Advanced-CC2927?style=for-the-badge&logo=microsoft-sql-server)](https://docs.snowflake.com/en/sql-reference)

> **A production-ready, end-to-end data engineering project** demonstrating modern data warehouse patterns with Snowflake's advanced features. Perfect for learning, portfolios, and enterprise architecture reference.

---

## 📋 Table of Contents

- [🎯 Overview](#-overview)
- [✨ Key Features](#-features)
- [🎓 What You'll Learn](#-what-youll-learn)
- [🚀 Quick Start (15 min)](#-quick-start)
- [📊 Dashboard Preview](#-dashboard-preview)
- [🏗️ Architecture](#️-architecture)
- [📖 Detailed Guide](#-detailed-setup-guide)
- [💼 Business Use Cases](#-business-use-cases)
- [🐛 Troubleshooting](#-troubleshooting)
- [🎯 Best Practices](#-best-practices)
- [🤝 Contributing](#-contributing)

---

## 🎯 Overview

This project simulates a **real e-commerce business** with:
- 👥 **103 customers** across 10 countries
- 🛍️ **200 products** in 5 categories
- 📦 **5,000+ orders** with 15,000+ line items
- 📊 **30,000+ total records** for realistic analytics

**What makes this special:**
- ⚡ **Real-time processing** - Changes reflected within minutes
- 🤖 **Fully automated** - No manual intervention needed
- 📊 **Production-ready** - Error handling, monitoring, optimization
- 🎨 **Beautiful dashboard** - Dark theme, interactive charts
- 📚 **Comprehensive docs** - Learn as you build

**Perfect for:**
- 🎓 Students learning data engineering
- 💼 Professionals building portfolios
- 🏢 Teams studying modern architectures
- 📖 Anyone wanting hands-on Snowflake experience

---

## ✨ Features

### Snowflake Capabilities Demonstrated

| Feature | Description | Business Value |
|---------|-------------|----------------|
| **🔄 Dynamic Tables** | Auto-refreshing transformations | Real-time analytics without manual MERGE |
| **📡 Streams** | Change Data Capture (CDC) | Track every data modification |
| **⏰ Tasks** | Automated SQL workflows | Zero-touch data processing |
| **📈 SCD Type 2** | Historical tracking | Time-travel analytics |
| **⭐ Star Schema** | Dimensional modeling | Optimized query performance |
| **🎨 Streamlit** | Interactive dashboard | Business intelligence UI |

### Dashboard Features

- **7 Interactive Tabs:**
  - 📊 Overview - KPIs, trends, executive summary
  - 👥 Customers - Top customers, segmentation
  - 🛍️ Products - Profit margins, performance
  - 📈 Sales Trends - Time-series analysis
  - 📦 Inventory - Stock alerts, reorder points
  - 💳 Payments - Transaction analysis
  - 🌍 Geography - Global sales map

- **20+ Visualizations:**
  - Line charts, bar charts, donut charts
  - Scatter plots, heatmaps
  - Geographic choropleth map
  - KPI cards with deltas

---

## 🎓 What You'll Learn

### Beginner Topics ✅
- Setting up Snowflake environment
- Creating databases, schemas, tables
- Loading data with INSERT statements
- Writing analytical SQL queries
- Star schema dimensional modeling
- Deploying Streamlit dashboards

### Intermediate Topics 📊
- Implementing Streams for CDC
- Creating automated Tasks
- Building Dynamic Tables
- Slowly Changing Dimensions (SCD Type 2)
- Query optimization techniques
- Monitoring data pipelines

### Advanced Topics 🚀
- Multi-layered data architecture
- Real-time data processing patterns
- Cost optimization strategies
- Error handling & data quality
- Production deployment
- Performance tuning

---

## 🚀 Quick Start

### Prerequisites
- ✅ Snowflake account ([Free trial](https://signup.snowflake.com/))
- ✅ ACCOUNTADMIN role (or similar)
- ✅ Python 3.8+ (for dashboard)
- ✅ Basic SQL knowledge

### Setup in 4 Steps (15 minutes)

#### 1️⃣ Create Infrastructure (5 min)

Open Snowsight and run:

```sql
-- File: snowflake_complete_project.sql
-- Execute sections 1-6 sequentially

-- Creates:
-- ✅ Warehouse: DE_PROJECT_WH
-- ✅ Database: ECOMMERCE_DB
-- ✅ 4 Schemas: RAW_DATA, STAGING, ANALYTICS, STREAMS_TASKS
-- ✅ 5 Source tables
-- ✅ 4 Streams for CDC
-- ✅ 3 Dynamic tables
-- ✅ Star schema (dimensions + facts)
-- ✅ 3 Automated tasks
```

#### 2️⃣ Load Sample Data (5 min)

```sql
-- File: data_loading_script.sql
-- Run entire file

-- Generates:
-- ✅ 103 customers
-- ✅ 200 products
-- ✅ 5,003 orders
-- ✅ 15,008 order items
-- ✅ 10,004 inventory movements
-- ✅ 1,095 date dimension records
```

#### 3️⃣ Activate Automation (2 min)

```sql
-- Resume tasks to start processing
ALTER TASK STREAMS_TASKS.process_customer_changes RESUME;
ALTER TASK STREAMS_TASKS.load_fact_sales RESUME;
ALTER TASK STREAMS_TASKS.update_daily_summary RESUME;

-- Verify
SHOW TASKS IN SCHEMA STREAMS_TASKS;
-- Should show: state = 'started'
```

#### 4️⃣ Deploy Dashboard (3 min)

**Option A: Streamlit in Snowflake** (Recommended)

```sql
-- 1. Create stage
CREATE STAGE streamlit_apps;

-- 2. Upload streamlit_dashboard.py (via Snowsight UI)

-- 3. Create app
CREATE STREAMLIT ECOMMERCE_ANALYTICS_DASHBOARD
  ROOT_LOCATION = '@streamlit_apps'
  MAIN_FILE = 'streamlit_dashboard.py'
  QUERY_WAREHOUSE = DE_PROJECT_WH;

-- 4. Access: Snowsight → Streamlit menu
```

**Option B: Run Locally**

```bash
pip install -r requirements.txt
streamlit run streamlit_dashboard.py
```

### ✅ Verify Setup

```sql
-- Run monitoring query (testing_monitoring_script.sql - end of file)
-- Should show record counts for all tables
-- Streams should have 0 records (already processed)
-- Tasks should show recent executions
```

**🎉 Success!** Your pipeline is running and dashboard is live!

---

## 📊 Dashboard Preview

### Overview Tab
![Dashboard](https://via.placeholder.com/800x400/1a1a2e/00d9ff?text=Dashboard+Preview)

**Metrics Displayed:**
- 💰 Today's Revenue: $12,450 ↑ 5.2%
- 📦 Today's Orders: 250 ↑ 12
- 💎 Avg Order Value: $261.73
- 📈 Profit Margin: 33.5%

**Key Charts:**
- 30-day revenue trend (line chart with area fill)
- Revenue vs Profit comparison (grouped bars)
- Customer segment distribution (donut chart)
- Order status breakdown (bar chart)

### Inventory Tab - Critical Alerts

| Product | Stock | Status | Daily Sales | Action |
|---------|-------|--------|-------------|--------|
| Wireless Mouse | 8 | 🔴 LOW | 5/day | REORDER NOW |
| USB-C Cable | 12 | 🟡 MEDIUM | 3/day | Monitor |
| Phone Case | 18 | 🟡 MEDIUM | 4/day | Monitor |

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    DATA SOURCES                          │
│              (CSV Files, API, Databases)                 │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│                  RAW DATA LAYER                          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                │
│  │customers │ │ products │ │  orders  │                │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘                │
└───────┼────────────┼────────────┼──────────────────────┘
        │            │            │
        └────────────┴────────────┘
                     │ Streams (CDC)
                     ▼
┌──────────────────────────────────────────────────────────┐
│               STAGING LAYER                              │
│         (Dynamic Tables - Auto-refresh)                  │
│  ┌────────────────────────────────────┐                 │
│  │ enriched_orders (5 min lag)        │                 │
│  │ product_performance (10 min lag)   │                 │
│  │ current_inventory (2 min lag)      │                 │
│  └────────────────────────────────────┘                 │
└────────────────────┬─────────────────────────────────────┘
                     │ Tasks (Scheduled)
                     ▼
┌──────────────────────────────────────────────────────────┐
│            ANALYTICS LAYER (Star Schema)                 │
│                                                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │dim_      │  │dim_      │  │dim_date  │              │
│  │customer  │  │product   │  │          │              │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘              │
│       └─────────────┴──────────────┘                     │
│                     ▼                                    │
│            ┌─────────────────┐                          │
│            │   fact_sales    │                          │
│            └─────────────────┘                          │
└────────────────────┬─────────────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────────────┐
│              STREAMLIT DASHBOARD                         │
│  📊 Real-time Analytics & Visualizations                │
└──────────────────────────────────────────────────────────┘
```

**Data Flow Example:**
```
Order Placed → orders_stream → enriched_orders (5 min) 
  → load_fact_sales task (10 min) → fact_sales 
  → daily_summary task (30 min) → Dashboard (live)
```

---

## 📖 Detailed Setup Guide

### Understanding the Components

#### 1. Streams (Change Data Capture)

**What is it?**
A stream tracks INSERT, UPDATE, and DELETE operations on a table.

**Example:**
```sql
-- Customer gets updated
UPDATE customers SET segment='PREMIUM' WHERE id=101;

-- Stream captures:
SELECT * FROM customers_stream WHERE customer_id=101;
-- Returns 2 rows:
-- Row 1: METADATA$ACTION='DELETE' (old value)
-- Row 2: METADATA$ACTION='INSERT' (new value)
```

**Why use streams?**
- ✅ Process only changed data (efficient!)
- ✅ Exactly-once processing
- ✅ No full table scans

#### 2. Dynamic Tables (Auto-refresh)

**What is it?**
A table that automatically refreshes when source data changes.

**Example:**
```sql
-- Traditional: Must manually MERGE every 5 minutes
CREATE TABLE enriched_orders AS 
SELECT ... FROM orders JOIN customers ...;

-- Dynamic: Snowflake handles it automatically!
CREATE DYNAMIC TABLE enriched_orders
  TARGET_LAG = '5 minutes'
AS SELECT ... FROM orders JOIN customers ...;
```

**Benefits:**
- ✅ Always fresh (within lag)
- ✅ Incremental processing
- ✅ Zero maintenance

#### 3. Tasks (Automation)

**What is it?**
Scheduled SQL jobs that run automatically.

**Example:**
```sql
CREATE TASK load_fact_sales
  SCHEDULE = '10 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('order_items_stream')
AS
  INSERT INTO fact_sales SELECT ... FROM order_items_stream;
```

**Why use tasks?**
- ✅ Automation (no manual work)
- ✅ Conditional execution (save costs)
- ✅ Dependency chains

---

## 💼 Business Use Cases

### Use Case 1: Customer Segmentation

**Question:** Which segment generates most revenue?

**Query:**
```sql
SELECT 
  customer_segment,
  COUNT(DISTINCT customer_id) as customers,
  SUM(total_amount) as revenue
FROM STAGING.enriched_orders
WHERE order_status = 'COMPLETED'
GROUP BY customer_segment;
```

**Result:**
| Segment | Customers | Revenue | Revenue/Customer |
|---------|-----------|---------|------------------|
| PREMIUM | 32 | $145,678 | $4,552 |
| STANDARD | 58 | $89,234 | $1,539 |

**Insight:**
PREMIUM customers (31% of base) drive 54% of revenue with 3x higher spend per customer.

**Action:**
Launch upgrade campaign targeting top STANDARD customers.

**Expected ROI:**
25% increase in PREMIUM base = $36k annual revenue

---

### Use Case 2: Inventory Optimization

**Question:** Which products risk stockout?

**Alert:**
```
🔴 CRITICAL: Wireless Mouse
Stock: 8 units
Daily Sales: 5 units/day
Days Until Stockout: 1.6 days
→ EMERGENCY REORDER: 500 units
```

**Impact:**
Prevent $2,400 in lost sales

---

### Use Case 3: Product Profitability

**Finding:**
USB Flash Drives have 50% profit margin vs 15% on Gaming Consoles

**Strategy:**
- Promote high-margin accessories
- Bundle low-margin items with accessories
- Expected: Increase overall margin from 32% to 37%

---

## 🐛 Troubleshooting

### Issue 1: Tasks Not Running

**Symptom:** `state = 'suspended'`

**Fix:**
```sql
ALTER TASK STREAMS_TASKS.process_customer_changes RESUME;
ALTER TASK STREAMS_TASKS.load_fact_sales RESUME;
ALTER TASK STREAMS_TASKS.update_daily_summary RESUME;
```

---

### Issue 2: Dashboard Shows No Data

**Checklist:**
1. ✅ Data exists: `SELECT COUNT(*) FROM STAGING.enriched_orders;`
2. ✅ Warehouse running: `SHOW WAREHOUSES;`
3. ✅ Connection: Check "Connected via Snowpark" in dashboard
4. ✅ Cache: Click "🔄 Refresh Data" button

---

### Issue 3: High Costs

**Solutions:**
```sql
-- Reduce warehouse size
ALTER WAREHOUSE DE_PROJECT_WH SET WAREHOUSE_SIZE = 'XSMALL';

-- Optimize auto-suspend
ALTER WAREHOUSE DE_PROJECT_WH SET AUTO_SUSPEND = 60;

-- Add resource monitor
CREATE RESOURCE MONITOR project_monitor
  WITH CREDIT_QUOTA = 50
  TRIGGERS ON 100 PERCENT DO SUSPEND;
```

---

## 🎯 Best Practices

### 1. Resource Management
- Set resource monitors to prevent runaway costs
- Use XSMALL/SMALL warehouses for development
- Enable auto-suspend (60-300 seconds)
- Monitor credit usage weekly

### 2. Error Handling
- Add exception handlers in tasks
- Create error logging tables
- Set up email notifications for failures
- Test edge cases (null values, duplicates)

### 3. Data Quality
- Implement validation tasks
- Check for orphaned records
- Monitor for negative values
- Verify dimension integrity

### 4. Documentation
- Comment all objects with business context
- Document data lineage
- Maintain change log
- Version control DDL scripts

### 5. Performance
- Add clustering keys on large tables
- Use appropriate data types
- Optimize JOIN conditions
- Monitor query performance

---

## 🤝 Contributing

Contributions welcome! Here's how:

1. **Fork** the repository
2. **Create** a feature branch
3. **Make** your changes
4. **Test** thoroughly
5. **Submit** a pull request

**Ideas for contributions:**
- 📊 New analytics queries
- 🎨 Dashboard enhancements
- 📝 Additional documentation
- 🧪 Data quality tests
- 🌍 Internationalization

---

## 📚 Resources

### Documentation
- [Snowflake Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-intro)
- [Streams & Tasks Guide](https://docs.snowflake.com/en/user-guide/streams-intro)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)

### Learning
- [Snowflake Quickstarts](https://quickstarts.snowflake.com/)
- [Data Engineering Workshop](https://learn.snowflake.com/)
- [SnowPro Certification](https://www.snowflake.com/certifications/)

---

## 📜 License

MIT License - Use freely for learning, portfolios, and commercial projects!

---

## 📬 Support

- 📧 Questions? [Open an issue](https://github.com/yourusername/repo/issues)
- 💬 Discussions? [Join our community](https://github.com/yourusername/repo/discussions)
- ⭐ Found this helpful? **Star the repo!**

---

<div align="center">

### 🚀 Ready to Start Building?

**[↑ Back to Top](#-snowflake-e-commerce-data-engineering-project)**

---

Made with ❤️ for the Data Engineering Community

**If this project helped you, please ⭐ star it and share with others!**

</div>
