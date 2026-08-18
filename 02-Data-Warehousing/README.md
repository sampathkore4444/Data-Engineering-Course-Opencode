# 02 - Data Warehousing Fundamentals

## Table of Contents
1. [What is a Data Warehouse?](#1-what-is-a-data-warehouse)
2. [OLTP vs OLAP](#2-oltp-vs-olap)
3. [Data Warehouse vs Data Lake vs Data Lakehouse](#3-data-warehouse-vs-data-lake-vs-data-lakehouse)
4. [Inmon vs Kimball](#4-inmon-vs-kimball)
5. [Dimensional Modeling](#5-dimensional-modeling)
6. [Slowly Changing Dimensions](#6-slowly-changing-dimensions)
7. [Fact Tables](#7-fact-tables)
8. [Interview Questions](#8-interview-questions)

---

## 1. What is a Data Warehouse?

A **Data Warehouse** is a centralized repository that stores integrated data from multiple sources, optimized for analytical reporting and decision-making.

### Bill Inmon Definition
> "A data warehouse is a subject-oriented, integrated, non-volatile, and time-variant collection of data in support of management's decisions."

### Four Key Characteristics

| Characteristic | Meaning | Example |
|---------------|---------|---------|
| **Subject-Oriented** | Organized around business subjects | Customer, Product, Sales |
| **Integrated** | Data unified from multiple sources with consistent formats | Date format: YYYY-MM-DD everywhere |
| **Non-Volatile** | Data is stable; never deleted, only appended | Historical sales preserved forever |
| **Time-Variant** | Tracks historical data over time | Compare this quarter vs last quarter |

### Architecture

`
Presentation Layer     BI Tools, Dashboards, Reports
        |
Processing Layer       ETL / ELT Pipelines
        |
Storage Layer          Fact Tables, Dimension Tables, Staging
        |
Ingestion Layer        CRM, ERP, Web Logs, APIs, External Sources
`

---

## 2. OLTP vs OLAP

### OLTP (Online Transaction Processing)

OLTP systems handle day-to-day operational transactions. They are optimized for fast writes and reads of individual records.

**Characteristics:**
- High volume of short atomic transactions
- Normalized schema (3NF) to avoid redundancy
- Current data only
- Millisecond response time
- Optimized for INSERT, UPDATE, DELETE

**Example Query:**
`sql
SELECT * FROM accounts WHERE account_id = 'ACC-12345';
UPDATE accounts SET balance = balance - 500 WHERE account_id = 'ACC-12345';
`

### OLAP (Online Analytical Processing)

OLAP systems are optimized for complex analytical queries across large historical datasets.

**Characteristics:**
- Complex queries with GROUP BY, aggregations
- Denormalized schema (Star/Snowflake)
- Years of historical data
- Seconds to minutes response time
- Optimized for SELECT with aggregations

**Example Query:**
`sql
SELECT 
    d.quarter,
    p.category,
    SUM(f.revenue) as total_revenue,
    COUNT(DISTINCT f.customer_key) as unique_customers
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
WHERE d.year = 2024
GROUP BY d.quarter, p.category
ORDER BY total_revenue DESC;
`

### Comparison

| Aspect | OLTP | OLAP |
|--------|------|------|
| Purpose | Run business operations | Analyze business performance |
| Data | Current, operational | Historical, analytical |
| Schema | 3NF (normalized) | Star/Snowflake (denormalized) |
| Volume | GBs to low TBs | TBs to PBs |
| Query | Simple, indexed | Complex, aggregations |
| Users | 1000s (concurrent) | 10s to 100s |
| Latency | Milliseconds | Seconds to minutes |
| Operations | Read/Write | Read-only |

---

## 3. Data Warehouse vs Data Lake vs Data Lakehouse

### Data Warehouse

A centralized repository for **structured, processed data** optimized for BI and analytics.

**Pros:** High data quality, ACID transactions, fast BI queries
**Cons:** Expensive, schema-on-write (rigid), limited data types

### Data Lake

A storage repository that holds **raw data in native format** until needed.

**Pros:** Cheap storage, handles all data types, flexible schema-on-read
**Cons:** Data swamp risk, poor BI performance, no ACID transactions

### Data Lakehouse (Modern)

Combines data lake flexibility with data warehouse reliability using open table formats (Delta Lake, Iceberg, Hudi).

`
Data Lake           Data Lakehouse              Data Warehouse
+--------+         +------------------+         +--------+
| Raw    |         | Bronze (Raw)     |         | Curated|
| data,  |   =>    | Silver (Cleaned) |   =>    | data,  |
| any    |         | Gold (Business)  |         | structured|
| format |         | ACID + Schema    |         +--------+
+--------+         +------------------+
`

### Comparison

| Feature | Data Warehouse | Data Lake | Data Lakehouse |
|---------|---------------|-----------|----------------|
| Data Types | Structured only | All types | All types |
| Schema | On-write | On-read | Both |
| Storage Cost | High | Low | Low |
| ACID | Yes | No | Yes |
| BI Performance | Excellent | Poor | Excellent |
| ML Support | Limited | Excellent | Excellent |
| Data Quality | High | Low | High |

---

## 4. Inmon vs Kimball

### Bill Inmon (Top-Down)

Build a **centralized enterprise data warehouse (3NF)** first, then create data marts from it.

`
Source -> ETL -> Enterprise DW (3NF) -> Data Marts -> BI
`

**Pros:** Single source of truth, consistent data, strong governance
**Cons:** Longer time to value, higher upfront cost, complex

### Ralph Kimball (Bottom-Up)

Build **dimensional data marts** for each business process, integrate via conformed dimensions.

`
Source -> ETL -> Staging -> Data Marts (Star Schema) -> BI
`

**Pros:** Faster time to value, easier to understand, incremental
**Cons:** Potential redundancy, harder to maintain consistency

### Which to Choose?

| Scenario | Choose |
|----------|--------|
| Enterprise-wide integration needed | Inmon |
| Quick wins required | Kimball |
| Strong data governance exists | Inmon |
| Limited resources | Kimball |
| Long-term strategic investment | Inmon |
| Business processes well-defined | Kimball |

---

## 5. Dimensional Modeling

### Star Schema

Central **fact table** surrounded by **dimension tables**. The most common model for data warehousing.

`
dim_date              fact_sales              dim_product
+-----------+        +--------------+        +-----------+
| date_key  |<------| date_key     |------>| product_key|
| full_date |        | product_key  |        | prod_name |
| quarter   |        | customer_key |        | category  |
| year      |        | store_key    |        | brand     |
+-----------+        | quantity     |        | unit_price|
                     | revenue      |        +-----------+
dim_customer         | cost         |
+-----------+        +--------------+
| cust_key  |<------|
| name      |        dim_store
| segment   |        +-----------+
| city      |<-------| store_key |
+-----------+        | store_name|
                     | region    |
                     +-----------+
`

**SQL to Create Star Schema:**
`sql
-- Dimension: Date
CREATE TABLE dim_date (
    date_key        INT PRIMARY KEY,
    full_date       DATE NOT NULL,
    day_of_week     VARCHAR(10),
    month_name      VARCHAR(10),
    month_number    INT,
    quarter         INT,
    year            INT,
    is_holiday      BOOLEAN
);

-- Dimension: Customer
CREATE TABLE dim_customer (
    customer_key    INT PRIMARY KEY,
    customer_id     VARCHAR(20) NOT NULL,
    customer_name   VARCHAR(100),
    segment         VARCHAR(20),
    city            VARCHAR(50),
    state           VARCHAR(50),
    country         VARCHAR(50),
    effective_date  DATE,
    expiry_date     DATE
);

-- Dimension: Product
CREATE TABLE dim_product (
    product_key     INT PRIMARY KEY,
    product_id      VARCHAR(20) NOT NULL,
    product_name    VARCHAR(100),
    category        VARCHAR(50),
    subcategory     VARCHAR(50),
    brand           VARCHAR(50),
    unit_cost       DECIMAL(10,2)
);

-- Fact Table: Sales
CREATE TABLE fact_sales (
    sale_id         BIGINT PRIMARY KEY,
    date_key        INT REFERENCES dim_date(date_key),
    customer_key    INT REFERENCES dim_customer(customer_key),
    product_key     INT REFERENCES dim_product(product_key),
    quantity        INT,
    unit_price      DECIMAL(10,2),
    revenue         DECIMAL(12,2),
    cost            DECIMAL(12,2),
    profit          DECIMAL(12,2)
);
`

### Snowflake Schema

Normalized dimension tables to reduce redundancy.

`
dim_continent -> dim_country -> dim_region -> dim_store -> fact_sales
dim_brand -> dim_category -> dim_subcategory -> dim_product -> fact_sales
`

**Trade-off:** Less redundancy but more joins, slower queries.

---

## 6. Slowly Changing Dimensions (SCD)

SCD strategies handle how dimension attribute changes are tracked over time.

### SCD Type 0: Fixed
No changes allowed. Attribute is permanent.
`sql
-- Example: Social Security Number (never changes)
ALTER TABLE dim_customer ADD CONSTRAINT chk_ssn UNIQUE (ssn);
`

### SCD Type 1: Overwrite
Replace old value with new value. No history kept.
`sql
-- Customer moved cities - overwrite
UPDATE dim_customer 
SET city = 'New York', state = 'NY'
WHERE customer_id = 'C001';
`

### SCD Type 2: Add New Row (Most Common)
Add a new row for the change with effective/expiry dates.
`sql
-- Customer moved cities - add new version
-- Step 1: Expire the old record
UPDATE dim_customer 
SET expiry_date = CURRENT_DATE - 1
WHERE customer_id = 'C001' AND expiry_date = '9999-12-31';

-- Step 2: Insert new record
INSERT INTO dim_customer (customer_id, customer_name, city, state, 
                          effective_date, expiry_date, is_current)
VALUES ('C001', 'John Smith', 'New York', 'NY', 
        CURRENT_DATE, '9999-12-31', TRUE);
`

**Query current version:**
`sql
SELECT * FROM dim_customer 
WHERE customer_id = 'C001' AND is_current = TRUE;
`

**Query history:**
`sql
SELECT * FROM dim_customer 
WHERE customer_id = 'C001'
ORDER BY effective_date;
`

### SCD Type 3: Add New Column
Track limited history by adding columns.
`sql
ALTER TABLE dim_customer 
    ADD COLUMN previous_city VARCHAR(50),
    ADD COLUMN city_change_date DATE;

UPDATE dim_customer 
SET previous_city = city,
    city = 'New York',
    city_change_date = CURRENT_DATE
WHERE customer_id = 'C001';
`

### SCD Type 4: History Table
Create a separate history table.
`sql
-- Current table
CREATE TABLE dim_customer (
    customer_key INT PRIMARY KEY,
    customer_id VARCHAR(20),
    city VARCHAR(50),
    is_current BOOLEAN DEFAULT TRUE
);

-- History table
CREATE TABLE dim_customer_history (
    history_key INT PRIMARY KEY,
    customer_key INT,
    city VARCHAR(50),
    valid_from DATE,
    valid_to DATE
);
`

### SCD Type 5: Hybrid (Type 1 + Type 2)
Combine Type 1 overwrite with Type 2 history table.

### Summary

| Type | History | Storage | Complexity | Use Case |
|------|---------|---------|------------|----------|
| 0 | None | Minimal | Low | Immutable attributes |
| 1 | None | Minimal | Low | Non-correctable errors |
| 2 | Full | High | Medium | Full audit trail needed |
| 3 | Limited | Low | Medium | Recent history sufficient |
| 4 | Full | High | High | Large dimension tables |
| 5 | Full | Medium | High | Large tables, full history |

---

## 7. Fact Tables

### Transaction Fact Table
Records events at a specific point in time.
`sql
CREATE TABLE fact_orders (
    order_key       BIGINT PRIMARY KEY,
    order_date_key  INT,
    customer_key    INT,
    product_key     INT,
    store_key       INT,
    quantity        INT,
    unit_price      DECIMAL(10,2),
    discount        DECIMAL(10,2),
    total_amount    DECIMAL(12,2),
    cost_of_goods   DECIMAL(12,2),
    profit          DECIMAL(12,2)
);
`

### Periodic Snapshot Fact Table
Records state at regular intervals.
`sql
CREATE TABLE fact_account_balance_daily (
    account_key     INT,
    snapshot_date_key INT,
    opening_balance DECIMAL(15,2),
    closing_balance DECIMAL(15,2),
    total_debits    DECIMAL(15,2),
    total_credits   DECIMAL(15,2),
    transaction_count INT
);
`

### Accumulating Snapshot Fact Table
Tracks lifecycle of a process with multiple date keys.
`sql
CREATE TABLE fact_loan_application (
    application_key     INT PRIMARY KEY,
    customer_key        INT,
    application_date_key INT,
    approval_date_key   INT,
    disbursement_date_key INT,
    first_payment_date_key INT,
    maturity_date_key   INT,
    loan_amount         DECIMAL(15,2),
    interest_rate       DECIMAL(5,4),
    emi_amount          DECIMAL(12,2),
    status              VARCHAR(20)
);
`

### Factless Fact Table
Contains no measures - only dimension keys for counting occurrences.
`sql
CREATE TABLE fact_student_enrollment (
    student_key     INT,
    course_key      INT,
    semester_key    INT,
    department_key  INT
    -- No measures - just tracks which students enrolled in which courses
);
`

### Additive vs Semi-Additive vs Non-Additive Measures

| Type | Definition | Example | Can Sum? |
|------|-----------|---------|----------|
| Additive | Can be summed across all dimensions | Revenue, Quantity | Yes |
| Semi-Additive | Can sum across some dimensions | Account Balance | Partially |
| Non-Additive | Cannot be summed | Ratio, Percentage, Temperature | No |

---

## 8. Real-World Scenarios

### Scenario 1: Bank Data Warehouse

**Challenge:** A bank needs to consolidate data from core banking, loans, credit cards, and wealth management for regulatory reporting (Basel III, CCAR).

**Solution:**
`
Core Banking   Loans    Credit Cards   Wealth Mgmt
     |            |           |              |
     +-----+------+------+---+-----+--------+
           |
      ETL Processing
           |
    +------v-------+
    | Data Warehouse|
    |               |
    | Fact Tables:  |
    | - Daily Balances (periodic snapshot)|
    | - Transactions  (transaction)       |
    | - Loan Applications (accumulating)  |
    |               |
    | Dim Tables:   |
    | - Customer (SCD Type 2)             |
    | - Product, Branch, Date             |
    +---------------+
           |
    Regulatory Reports: Basel III, CCAR, Call Reports
    Business Analytics: Profitability, Risk, Customer 360
`

### Scenario 2: E-Commerce Data Platform

**Challenge:** An e-commerce company needs real-time inventory, customer analytics, and sales forecasting.

**Solution:**
`
Web App   Mobile App   POS   Suppliers   CRM
  |           |          |       |         |
  +-----+-----+----+-----+-----+----+-----+
               |
          ETL / ELT
               |
    +----------v----------+
    |    Data Lakehouse    |
    |                      |
    | Bronze: Raw data     |
    | Silver: Cleansed     |
    | Gold: Business-ready |
    +----------+-----------+
               |
    +----------v-----------+
    |    Data Warehouse     |
    |                       |
    | Sales Fact Table      |
    | Customer 360 View     |
    | Product Performance   |
    | Inventory Snapshot    |
    +-----------------------+
`

---

## 9. Banking Examples

### Example 1: Anti-Money Laundering (AML) Data Warehouse

**Business Problem:** Banks must detect and report suspicious activities to regulators (FinCEN).

**Data Model:**
`
fact_suspicious_activity
+-------------------+
| activity_key (PK) |
| customer_key (FK) |
| transaction_key   |
| alert_date_key    |
| branch_key (FK)   |
| activity_amount   |
| risk_score        |
| alert_type        |
| status            |
+-------------------+

dim_customer (SCD Type 2)
+--------------------+
| customer_key (PK)  |
| customer_id        |
| name               |
| risk_rating        |  <- Changes trigger SCD
| pep_status         |  <- Politically Exposed Person
| effective_date     |
| expiry_date        |
+--------------------+
`

**Key Queries:**
`sql
-- High-risk customers with recent suspicious activity
SELECT 
    c.customer_name,
    c.risk_rating,
    COUNT(a.activity_key) as suspicious_count,
    SUM(a.activity_amount) as total_amount
FROM fact_suspicious_activity a
JOIN dim_customer c ON a.customer_key = c.customer_key
WHERE c.is_current = TRUE
  AND a.alert_date_key >= 20240101
GROUP BY c.customer_name, c.risk_rating
HAVING SUM(a.activity_amount) > 10000
ORDER BY total_amount DESC;
`

### Example 2: Loan Portfolio Analytics

**Business Problem:** Analyze loan performance, delinquency rates, and predict defaults.

**Accumulating Snapshot Fact Table:**
`sql
CREATE TABLE fact_loan (
    loan_key            INT PRIMARY KEY,
    customer_key        INT,
    product_key         INT,
    branch_key          INT,
    origination_date_key INT,
    first_payment_date_key INT,
    maturity_date_key   INT,
    last_payment_date_key INT,
    loan_amount         DECIMAL(15,2),
    outstanding_balance DECIMAL(15,2),
    interest_rate       DECIMAL(5,4),
    emi_amount          DECIMAL(12,2),
    days_past_due       INT,
    classification      VARCHAR(20)  -- Standard, Substandard, Doubtful, Loss
);
`

---

## 10. E-Commerce Examples

### Example 1: Customer 360 Data Warehouse

**Business Problem:** Build a unified view of customers across all channels.

`sql
-- Fact: Customer daily activity snapshot
CREATE TABLE fact_customer_daily (
    customer_key    INT,
    date_key        INT,
    channel_key     INT,
    session_count   INT,
    page_views      INT,
    items_viewed    INT,
    items_carted    INT,
    orders_placed   INT,
    revenue         DECIMAL(12,2),
    items_returned  INT,
    return_amount   DECIMAL(12,2)
);

-- Dimension: Customer (SCD Type 2)
CREATE TABLE dim_customer (
    customer_key    INT PRIMARY KEY,
    customer_id     VARCHAR(20),
    email           VARCHAR(100),
    name            VARCHAR(100),
    segment         VARCHAR(20),     -- Premium, Regular, New, At-Risk
    loyalty_tier    VARCHAR(20),     -- Gold, Silver, Bronze
    lifetime_value  DECIMAL(12,2),   -- Updated periodically
    registration_date DATE,
    city            VARCHAR(50),
    country         VARCHAR(50),
    is_current      BOOLEAN,
    effective_date  DATE,
    expiry_date     DATE
);
`

**Key Query - Customer Segmentation:**
`sql
SELECT 
    c.segment,
    c.loyalty_tier,
    COUNT(DISTINCT c.customer_key) as customer_count,
    SUM(f.revenue) as total_revenue,
    AVG(f.revenue) as avg_revenue_per_customer
FROM fact_customer_daily f
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.is_current = TRUE
  AND f.date_key BETWEEN 20240101 AND 20240331
GROUP BY c.segment, c.loyalty_tier;
`

### Example 2: Sales and Inventory Analytics

**Business Problem:** Optimize inventory levels and maximize sales.

`sql
-- Fact: Daily sales by product and store
CREATE TABLE fact_daily_sales (
    product_key     INT,
    store_key       INT,
    date_key        INT,
    units_sold      INT,
    revenue         DECIMAL(12,2),
    discount_given  DECIMAL(12,2),
    units_returned  INT,
    stock_level     INT,          -- End of day inventory
    reorder_point   INT
);

-- Key Query: Low stock alerts with sales velocity
SELECT 
    p.product_name,
    s.store_name,
    f.stock_level,
    AVG(f.units_sold) as avg_daily_sales,
    f.stock_level / NULLIF(AVG(f.units_sold), 0) as days_of_stock,
    CASE 
        WHEN f.stock_level / NULLIF(AVG(f.units_sold), 0) < 3 
        THEN 'CRITICAL'
        WHEN f.stock_level / NULLIF(AVG(f.units_sold), 0) < 7 
        THEN 'LOW'
        ELSE 'OK'
    END as stock_status
FROM fact_daily_sales f
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_store s ON f.store_key = s.store_key
WHERE f.date_key BETWEEN 20240301 AND 20240331
GROUP BY p.product_name, s.store_name, f.stock_level;
`

---

## 11. Interview Questions

### Q1: What is the difference between a data warehouse and a data lake?

**Answer:**
A **data warehouse** stores structured, processed data optimized for BI and analytics using schema-on-write. It's ideal for consistent, high-quality reporting. A **data lake** stores raw data in native format (structured, semi-structured, unstructured) using schema-on-read, ideal for ML and data exploration. Modern architectures use **data lakehouses** (Delta Lake, Iceberg) combining both benefits: cheap object storage with ACID transactions and schema enforcement.

### Q2: Explain SCD Type 2 with an example.

**Answer:**
SCD Type 2 preserves full history by adding new rows. When a customer changes address:
1. **Expire** the current record by setting expiry_date = today - 1
2. **Insert** a new record with effective_date = today and expiry_date = 9999-12-31
3. Use is_current = TRUE/FALSE flag for easy current record retrieval

This maintains complete audit trail for regulatory compliance and historical analysis.

### Q3: Star schema vs Snowflake schema - when to use which?

**Answer:**
**Star schema** (denormalized dimensions): Use for most data warehouses. Fewer joins = faster queries. Better for BI tools. Redundancy is acceptable. **Snowflake schema** (normalized dimensions): Use when dimension tables are very large and storage cost matters. Better data integrity. Use when updates to dimension attributes are frequent. In practice, star schema is preferred 90% of the time.

### Q4: What are the different types of fact tables?

**Answer:**
1. **Transaction fact table**: One row per event/transaction (e.g., each sale)
2. **Periodic snapshot**: One row per entity per time period (e.g., daily account balance)
3. **Accumulating snapshot**: One row per process instance with multiple date keys tracking milestones (e.g., order lifecycle: ordered -> shipped -> delivered)
4. **Factless fact table**: No measures, only dimension keys (e.g., student enrollment)

### Q5: Inmon vs Kimball - which approach is better?

**Answer:**
Neither is universally better - it depends on context. **Inmon** (top-down) is better for large enterprises needing a single source of truth with strong governance, but requires more upfront investment. **Kimball** (bottom-up) is better for organizations needing faster time-to-value with well-defined business processes, using conformed dimensions for integration. Most modern organizations use a hybrid approach.

---

*Next Section: [03 - Data Modeling](../03-Data-Modeling/README.md)*
