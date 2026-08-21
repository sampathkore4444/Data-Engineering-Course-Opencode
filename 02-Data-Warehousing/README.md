# 02 - Data Warehousing Fundamentals

## Table of Contents
1. [What is a Data Warehouse?](#1-what-is-a-data-warehouse)
2. [OLTP vs OLAP](#2-oltp-vs-olap)
3. [Data Warehouse vs Data Lake vs Data Lakehouse](#3-data-warehouse-vs-data-lake-vs-data-lakehouse)
4. [Inmon vs Kimball](#4-inmon-vs-kimball)
5. [Dimensional Modeling](#5-dimensional-modeling)
   - [What is a Star Schema?](#what-is-a-star-schema)
   - [What is a Snowflake Schema?](#what-is-a-snowflake-schema)
   - [Star Schema vs Snowflake Schema](#star-schema-vs-snowflake-schema-detailed-comparison)
   - [Choosing the Right Schema](#choosing-the-right-schema)
6. [Slowly Changing Dimensions](#6-slowly-changing-dimensions)
7. [Fact Tables](#7-fact-tables)
8. [Real-World Scenarios](#8-real-world-scenarios)
9. [Banking Examples](#9-banking-examples)
10. [E-Commerce Examples](#10-e-commerce-examples)
11. [Performance Optimization](#11-performance-optimization)
12. [Hands-On Exercises](#12-hands-on-exercises)
   - [Exercise 1: Star Schema vs Snowflake Schema Design](#exercise-1-star-schema-vs-snowflake-schema-design)
   - [Exercise 2: Schema Selection Scenarios](#exercise-2-schema-selection-scenarios)
   - [Exercise 3: Convert Star Schema to Snowflake](#exercise-3-convert-star-schema-to-snowflake)
   - [Exercise 4: Query Performance Analysis](#exercise-4-query-performance-analysis)
   - [Exercise 5: Hybrid Approach Implementation](#exercise-5-hybrid-approach-implementation)
13. [Interview Questions](#13-interview-questions)
6. [Slowly Changing Dimensions](#6-slowly-changing-dimensions)
7. [Fact Tables](#7-fact-tables)
8. [Real-World Scenarios](#8-real-world-scenarios)
9. [Banking Examples](#9-banking-examples)
10. [E-Commerce Examples](#10-e-commerce-examples)
11. [Performance Optimization](#11-performance-optimization)
12. [Hands-On Exercises](#12-hands-on-exercises)
13. [Interview Questions](#13-interview-questions)

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

```
Presentation Layer     BI Tools, Dashboards, Reports
        |
Processing Layer       ETL / ELT Pipelines
        |
Storage Layer          Fact Tables, Dimension Tables, Staging
        |
Ingestion Layer        CRM, ERP, Web Logs, APIs, External Sources
```

### Modern Data Warehouse Tools

| Category | Tools | Description |
|----------|-------|-------------|
| **Cloud Warehouses** | Snowflake, Google BigQuery, Amazon Redshift, Azure Synapse | Fully managed, scalable cloud solutions |
| **Lakehouse Platforms** | Databricks, Snowflake (with Iceberg), Starburst | Open formats with warehouse capabilities |
| **Transformation** | dbt, SQLMesh, Dataform | Version-controlled SQL transformations |
| **Orchestration** | Apache Airflow, Dagster, Prefect | Pipeline scheduling and monitoring |
| **Data Quality** | Great Expectations, dbt tests, Soda | Validation and testing |
| **BI & Analytics** | Looker, Tableau, Power BI, Metabase | Visualization and reporting |

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
```sql
SELECT * FROM accounts WHERE account_id = 'ACC-12345';
UPDATE accounts SET balance = balance - 500 WHERE account_id = 'ACC-12345';
```

### OLAP (Online Analytical Processing)

OLAP systems are optimized for complex analytical queries across large historical datasets.

**Characteristics:**
- Complex queries with GROUP BY, aggregations
- Denormalized schema (Star/Snowflake)
- Years of historical data
- Seconds to minutes response time
- Optimized for SELECT with aggregations

**Example Query: SQL**
```
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
```

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

```
Data Lake           Data Lakehouse              Data Warehouse
+--------+         +------------------+         +--------+
| Raw    |         | Bronze (Raw)     |         | Curated|
| data,  |   =>    | Silver (Cleaned) |   =>    | data,  |
| any    |         | Gold (Business)  |         | structured|
| format |         | ACID + Schema    |         +--------+
+--------+         +------------------+
```

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

### Modern Tools Comparison

| Tool | Type | Best For | Pricing Model |
|------|------|----------|---------------|
| **Snowflake** | Cloud DW | Enterprise analytics | Per-second compute |
| **Google BigQuery** | Cloud DW | Serverless analytics | Per-TB scanned |
| **Amazon Redshift** | Cloud DW | AWS ecosystem | Per-node hours |
| **Databricks** | Lakehouse | ML + Analytics | Per-DBU |
| **Delta Lake** | Table Format | ACID on data lakes | Open source |
| **Apache Iceberg** | Table Format | Large-scale lakehouse | Open source |
| **dbt** | Transformation | ELT workflows | Free / Cloud plans |

---

## 4. Inmon vs Kimball

### Bill Inmon (Top-Down)

Build a **centralized enterprise data warehouse (3NF)** first, then create data marts from it.

```
Source -> ETL -> Enterprise DW (3NF) -> Data Marts -> BI
```

**Pros:** Single source of truth, consistent data, strong governance

**Cons:** Longer time to value, higher upfront cost, complex

### Ralph Kimball (Bottom-Up)

Build **dimensional data marts** for each business process, integrate via conformed dimensions.

```
Source -> ETL -> Staging -> Data Marts (Star Schema) -> BI
```

#### What is Staging?

**Staging** is a temporary, intermediate storage area where raw data lands after extraction but **before** transformation into the final star schema. It's essentially a "holding zone" between your source systems and the data marts.

| Purpose | Description |
|---------|-------------|
| **Decouple extraction from transformation** | Source systems can push data quickly; transformation happens separately |
| **Data validation** | Check for completeness, duplicates, nulls before loading into marts |
| **Data cleansing** | Standardize formats, fix encoding issues, handle missing values |
| **Performance** | Avoid slow, repeated reads from source systems |
| **Audit trail** | Keep a copy of raw data before it's transformed |

Staging tables mirror the **source system structure** — not the final star schema:

```sql
-- Staging tables (raw, ugly, temporary)
stg_orders          -- mirrors ERP orders table
stg_customers       -- mirrors CRM customers table
stg_products        -- mirrors inventory system
```

These are typically **not meant for querying** — they're intermediate. After transformation, the clean data goes into the star schema:

```sql
-- Final star schema (clean, query-optimized)
fact_sales
dim_customer
dim_product
dim_date
```

##### The Full Kimball Flow Visualized

```text
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                           │
│  Oracle ERP    MySQL CRM    API Logs    Flat Files (CSV)       │
└────────────────────────────┬────────────────────────────────────┘
                             │ Extract
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      STAGING AREA                               │
│                                                                 │
│  stg_orders      ← raw, mirrors source, not queryable          │
│  stg_customers   ← same structure as source                    │
│  stg_products    ← temporary, gets truncated each run          │
│                                                                 │
│  Purpose: land, validate, clean, deduplicate                   │
└────────────────────────────┬────────────────────────────────────┘
                             │ Transform (clean, join, conform)
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     DATA MARTS (Star Schema)                    │
│                                                                 │
│     dim_date ───── fact_sales ───── dim_customer                │
│                         │                                       │
│                    dim_product                                   │
│                                                                 │
│  Clean, denormalized, optimized for BI queries                  │
└────────────────────────────┬────────────────────────────────────┘
                             │ Load
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│  BI / REPORTING                                                 │
│  Power BI, Tableau, Metabase, dashboards, regulatory reports   │
└─────────────────────────────────────────────────────────────────┘
```

##### Why Staging Matters in Banking

Imagine you have 5 source systems feeding into a risk data mart:

| Source System | Issues Staging Solves |
|---------------|----------------------|
| Core Banking (Oracle) | Dates in different timezone, NULL values in some fields |
| CRM (MySQL) | Customer names have inconsistent casing |
| Cards (API JSON) | Nested JSON needs flattening |
| Payments (Flat file) | Mixed encoding, duplicate rows |
| Loans (Legacy system) | Outdated status codes need mapping |

Staging catches all of this **before** it contaminates the data mart. The transformation layer cleans it, and the mart stores only clean, conformed data.

> **In short:** Staging = "Clean room" between raw source data and your final star schema. It's where you land, inspect, clean, and prepare data before building the dimensional models that BI tools actually query.

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


The main difference is **where you start building the data warehouse** and **how you organize the data**.

### High-level difference

| Aspect            | **Bill Inmon — Top-Down**  | **Ralph Kimball — Bottom-Up**              |
| ----------------- | -------------------------- | ------------------------------------------ |
| Starting point    | Enterprise-wide DW         | Individual business areas                  |
| Core DW           | **3NF / normalized**       | **Star schema / dimensional**              |
| Approach          | Build enterprise DW first  | Build data marts first                     |
| Data marts        | Created from Enterprise DW | Data marts are the primary building blocks |
| Design focus      | Enterprise integration     | Business/user reporting                    |
| Implementation    | Usually longer             | Usually faster                             |
| Data redundancy   | Lower                      | Higher                                     |
| Query performance | DW may require more joins  | Star schema is usually BI-friendly         |
| Governance        | Strong centralized model   | More business-oriented                     |
| Best suited for   | Large, complex enterprises | Fast analytics/BI delivery                 |

---

# Inmon — Top-Down

Think:

**"Let's build the company's central data warehouse first."**

```text
              ┌── Source: Core Banking
              │
              ├── Source: CRM
              │
              ├── Source: Cards
              │
              └── Source: Payments
                       │
                       ▼
                    ETL/ELT
                       │
                       ▼
          ┌──────────────────────────┐
          │ Enterprise Data Warehouse│
          │       3NF / Normalized   │
          └──────────────────────────┘
                       │
              ┌────────┼────────┐
              ▼        ▼        ▼
           Finance   Risk     Customer
           Mart      Mart       Mart
              │        │        │
              └────────┼────────┘
                       ▼
                      BI
```

The **Enterprise Data Warehouse is the central source of truth**.

For example, you might have:

```text
CUSTOMER
ACCOUNT
PRODUCT
BRANCH
TRANSACTION
LOAN
```

with relationships between them.

The data is highly normalized.

Then you create business-specific marts:

```text
Enterprise DW
     │
     ├── Finance Mart
     ├── Risk Mart
     ├── Marketing Mart
     ├── Credit Card Mart
     └── Customer Analytics Mart
```

### Inmon philosophy

> **Enterprise DW → Data Marts**

The enterprise model comes first.

---

# Kimball — Bottom-Up

Kimball says:

**"Let's solve business problems one by one using dimensional models, then integrate them."**

For example, start with Sales:

```text
Sales System
     │
     ▼
   ETL/ELT
     │
     ▼
┌─────────────────┐
│ Sales Data Mart │
│                 │
│ FactSales       │
│ DimCustomer     │
│ DimProduct      │
│ DimDate         │
└─────────────────┘
     │
     ▼
    BI
```

Then build another area:

```text
Loan System
     │
     ▼
   ETL/ELT
     │
     ▼
┌─────────────────┐
│ Loan Data Mart  │
│                 │
│ FactLoan        │
│ DimCustomer     │
│ DimProduct      │
│ DimDate         │
└─────────────────┘
```

Then another:

```text
Payments
   │
   ▼
Payment Data Mart
```

Eventually:

```text
             ┌── Sales Mart
             │
             ├── Loan Mart
             │
             ├── Payment Mart
             │
             ├── Customer Mart
             │
             └── Finance Mart
                    │
                    ▼
              Enterprise BI
```

The important concept is **conformed dimensions**.

For example, all marts should use a consistent:

```text
DimCustomer
DimDate
DimProduct
DimBranch
```

So you can answer:

> "How much did customers borrow, spend on cards, and transfer through payments?"

across multiple marts.

---

# The biggest technical difference

Consider a banking transaction.

### Inmon

You might have a normalized structure:

```text
CUSTOMER
   │
   ├── CUSTOMER_ACCOUNT
   │          │
   │          ▼
   │      ACCOUNT
   │          │
   │          ▼
   │     TRANSACTION
   │          │
   │          ▼
   │      PRODUCT
```

Lots of relationships and normalization.

A query may require several joins.

---

### Kimball

You would typically create:

```text
             DimCustomer
                  │
                  │
DimDate ───── FactTransaction ───── DimAccount
                  │
                  │
             DimProduct
                  │
                  │
              DimBranch
```

This is a **star schema**.

The central table is the **fact table**:

```text
FactTransaction
----------------
transaction_key
customer_key
account_key
product_key
branch_key
date_key
amount
transaction_count
```

And surrounding it are dimensions:

```text
DimCustomer
DimAccount
DimProduct
DimBranch
DimDate
```

This is generally much easier for BI tools and analytical queries.

---

# Why are they called Top-Down and Bottom-Up?

This is the easiest way to remember it.

### Inmon = Top-Down

Start at the top:

```text
Enterprise
    ↓
Enterprise DW
    ↓
Departments / Data Marts
    ↓
BI
```

### Kimball = Bottom-Up

Start from individual business processes:

```text
Sales ──────┐
Loans ──────┤
Payments ───┤
Cards ──────┤
Risk ───────┘
      ↓
Integrated dimensional environment
      ↓
Enterprise BI
```

---

# A very important point

**Inmon ≠ 3NF only**

and

**Kimball ≠ Star Schema only.**

The more important philosophical difference is:

> **Inmon:** Build an integrated enterprise data warehouse first, then derive marts.

> **Kimball:** Build dimensional data marts around business processes, using conformed dimensions to integrate them.

The 3NF vs Star Schema distinction is a consequence of the typical approaches, but **it's not the entire definition**.

---

# Which one should you use today?

For a modern banking analytics platform, I would usually recommend a **hybrid approach** rather than strictly following either methodology.

For example:

```text
                 SOURCE SYSTEMS
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
       Oracle       MySQL        Kafka/CDC
       FLEXCUBE     Payments      Events
          │            │            │
          └────────────┼────────────┘
                       ▼
                 Raw / Bronze
                       │
                       ▼
                Integrated Layer
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
        Enterprise/            Dimensional
        3NF Layer              Gold Layer
             │                   │
             │            ┌──────┼──────┐
             │            ▼      ▼      ▼
             │          Risk   Finance Customer
             │           Mart    Mart    Mart
             │
             └──────────────┐
                            ▼
                           BI
```

This gives you:

**Inmon strengths**

* Enterprise integration
* Strong governance
* Centralized source of truth
* Historical consistency

plus **Kimball strengths**

* Fast BI queries
* Star schemas
* Business-friendly models
* Easier Power BI/Tableau reporting

### In one sentence:

**Inmon asks: "How do we build the enterprise data warehouse?"**

**Kimball asks: "How do we deliver useful analytics for each business process and integrate them?"**

For **banking**, this distinction becomes especially important when you have **Core Banking + Cards + Loans + Payments + CRM + Risk + IFRS9 + Fraud** all feeding the same analytics platform.

---

## 5. Dimensional Modeling

### Resource datacamp.com
https://www.datacamp.com/blog/star-schema-vs-snowflake-schema

### What is a Star Schema?

A **star schema** is a way to organize data in a database, especially in data warehouses, to make it easier and faster to analyze. At the center, there's a main table called the **fact table**, which holds measurable data like sales or revenue. Around it are **dimension tables**, which add details like product names, customer info, or dates. This layout forms a star-like shape.

```
                    dim_date
                   +-----------+
                   | date_key  |
                   | full_date |
                   | quarter   |
                   | year      |
                   +-----------+
                         |
                         v
 dim_product          fact_sales          dim_customer
+-----------+        +--------------+        +-----------+
| product_key|<------| date_key     |------>| cust_key  |
| prod_name |        | product_key  |        | name      |
| category  |        | customer_key |        | segment   |
| brand     |        | store_key    |        | city      |
+-----------+        | quantity     |        +-----------+
                     | revenue      |
                     | cost         |
                     +--------------+
                           |
                           v
                       dim_store
                      +-----------+
                      | store_key |
                      | store_name|
                      | region    |
                      +-----------+
```

### Key Features of Star Schema

| Feature | Description |
|---------|-------------|
| **Single-level dimension tables** | Dimension tables connect directly to the fact table without extra layers. Each table focuses on one area (products, regions, time). |
| **Denormalized design** | Related data is stored together in one table. For example, a product table may include product ID, name, and category in the same place. |
| **Common in data warehousing** | Used for quick analysis. Can easily filter or calculate totals, making it ideal for fast insights. |

### Star Schema Example

Let's look at a real-world example. The fact table **Sales** is in the center and holds the numeric data you want to analyze (sales, profits). Connected to it are dimension tables with descriptive details:

**SQL to Create Star Schema:**
```sql
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
```

### What is a Snowflake Schema?

A **snowflake schema** is another way of organizing data. In this schema, dimension tables are split into smaller sub-dimensions to keep data more organized and detailed — just like snowflakes in a large lake.

```
                           fact_sales
                              |
            +-----------------+------------------+
            |                 |                  |
            v                 v                  v
     dim_product         dim_customer        dim_date
     +-----------+       +-----------+       +-----------+
     | product_key|      | cust_key  |       | date_key  |
     | prod_name |       | name      |       | full_date |
     | category_id|      | location_id|      | year      |
     | manufacturer_id|  +-----------+       +-----------+
     +-----------+            |
          |                   v
          v              dim_location
   +--------------+     +-----------+
   |              |     | location_id|
   v              v     | city      |
dim_category  dim_manufacturer | state     |
+-----------+ +-----------+  | country   |
| cat_id    | | mfg_id    |  +-----------+
| cat_name  | | mfg_name  |
+-----------+ +-----------+
```

### Key Features of Snowflake Schema

| Feature | Description |
|---------|-------------|
| **Multi-level dimension tables** | Dimension tables are broken down into smaller, more specific tables. For example, location can be split into separate tables for countries, states, and cities. |
| **Normalization for storage efficiency** | Follows a normalized design to avoid data duplication. Rather than repeating a category name for every product, it's stored in a separate table. |
| **Suitability for complex data environments** | Works best for complex data environments with multi-level tables to handle intricate relationships and hierarchical data structures. |

### Snowflake Schema Example

At the center is the fact table with measurable data. It connects to dimension tables that further branch out into sub-dimension tables, forming a snowflake-like structure.

**SQL to Create Snowflake Schema:**

```sql
-- Fact table remains the same
CREATE TABLE Sales (
    Sales_ID INT PRIMARY KEY,
    Product_ID INT,
    Customer_ID INT,
    Date_ID INT,
    Sales_Amount DECIMAL(10, 2),
    FOREIGN KEY (Product_ID) REFERENCES Product(Product_ID),
    FOREIGN KEY (Customer_ID) REFERENCES Customers(Customer_ID),
    FOREIGN KEY (Date_ID) REFERENCES Dates(Date_ID)
);

-- Dimension table: Product (normalized)
CREATE TABLE Product (
    Product_ID INT PRIMARY KEY,
    Product_Name VARCHAR(100),
    Category_ID INT,
    Manufacturer_ID INT,
    FOREIGN KEY (Category_ID) REFERENCES Category(Category_ID),
    FOREIGN KEY (Manufacturer_ID) REFERENCES Manufacturer(Manufacturer_ID)
);

-- Sub-dimension table: Category
CREATE TABLE Category (
    Category_ID INT PRIMARY KEY,
    Category_Name VARCHAR(50)
);

-- Sub-dimension table: Manufacturer
CREATE TABLE Manufacturer (
    Manufacturer_ID INT PRIMARY KEY,
    Manufacturer_Name VARCHAR(100)
);
```

**Query Example (Snowflake Schema):**

```sql
-- Calculate total sales by product category
-- More joins than star schema, but more storage-efficient
SELECT 
    cat.Category_Name, 
    SUM(s.Sales_Amount) AS TotalSales
FROM Sales s
JOIN Product p ON s.Product_ID = p.Product_ID
JOIN Category cat ON p.Category_ID = cat.Category_ID
GROUP BY cat.Category_Name;
```

### Advantages and Limitations

**Star Schema:**
| Advantages | Limitations |
|------------|-------------|
| Faster query performance (fewer joins) | Higher data redundancy |
| Easy to understand and maintain | More storage space required |
| Ideal for BI tools and dashboards | May not suit very complex hierarchies |

**Snowflake Schema:**
| Advantages | Limitations |
|------------|-------------|
| Less data redundancy | Slower queries (more joins) |
| Efficient storage for large datasets | More complex to design and maintain |
| Better data integrity | Requires experienced DBA team |

### Hybrid Approach

In real projects, it's common to use both patterns at different layers:

- **Warehouse layer:** Keep more normalized (snowflaked) structures for consistency and easier maintenance
- **Data marts:** Publish star-shaped marts or denormalized views for BI and reporting

This lets teams balance data integrity and governance with fast, simple analytics consumption.

### Star Schema vs Snowflake Schema: Detailed Comparison

| Feature | Star Schema | Snowflake Schema | Hybrid Approach |
|---------|-------------|------------------|------------------|
| **Structure** | Central fact table linked to denormalized dimensions | Central fact table linked to normalized dimensions | Normalized core model, plus star-shaped marts or denormalized views |
| **Complexity** | Simple, with fewer joins | Complex, with more joins | Medium, with more moving parts, but each layer stays simpler |
| **Data redundancy** | Higher redundancy due to denormalized dimensions | Lower redundancy due to normalized dimensions | Medium redundancy due to selective denormalization |
| **Query performance** | Faster queries due to simpler structure | Slower queries because of additional joins | Fast for BI because the consumption layer is denormalized |
| **Storage** | Requires more storage because of redundancy | Requires less storage due to normalization | Requires moderate storage because marts/views may add some duplication |
| **Ease of maintenance** | Easier to design and maintain | More complex to design and maintain | Easy to maintain, as marts can be rebuilt from the controlled core |
| **Best suited for** | Small to medium-sized datasets | Large and complex datasets | Modern data platforms with governance + BI performance needs |

### Choosing the Right Schema

#### When to Use a Star Schema

| Scenario | Why Star Schema Works |
|----------|----------------------|
| **BI tool semantic models** | Low number of tables and relationships, supports intuitive filtering/grouping |
| **Simple analytical queries** | "Find total sales by region" — avoids unnecessary complexity |
| **Speed is priority** | Minimizes table joins, queries run faster |
| **Small to medium datasets** | Redundancy won't be a problem, works fine without overwhelming storage |

#### When to Use a Snowflake Schema

| Scenario | Why Snowflake Schema Works |
|----------|---------------------------|
| **Clear hierarchies** | Country → State/Region → City modeled as separate tables |
| **Shared reference data** | Standard lists like categories, manufacturers, geographies |
| **Frequent dimension updates** | Updating region names maintains consistency across all related data |
| **Multi-level analysis** | Organize and represent complex relationships in a structured way |

#### Schema Selection in Cloud Data Warehouses

In many modern cloud data warehouses, **storage is relatively inexpensive compared to compute**. That means the "extra storage" from denormalized dimensions is often less important than the compute cost of scanning and joining data.

When choosing between star and snowflake, consider:
- **Platform pricing model** (compute vs. storage costs)
- **Query concurrency** requirements
- **Caching/materialized views** availability to keep query costs down

> **Best Practice:** Start with a star schema for simplicity and performance. Only normalize to snowflake when you have specific requirements like very large dimension tables, strict data governance, or complex hierarchies that benefit from normalization.

---

## 6. Slowly Changing Dimensions (SCD)

```
Slowly Changing Dimensions

Slowly Changing Dimensions (SCD) refer to the techniques used in data warehousing to manage and track changes in dimension attributes over time. Given that certain dimension attributes, like a customer’s address or a product’s description, can change without the need for a new unique identifier, SCDs ensure that historical data remains consistent and accurate. Various SCD types provide strategies to handle these changes, such as overwriting old data, adding new records, or maintaining a history of changes, enabling analysts to observe data trends and changes across timeframes accurately.

Let’s delve into each of the SCD types:
```


### Type 0 — The passive method:
```
Type 0 treats dimensions as static and doesn’t track changes. Once an attribute is set, it remains unchanged throughout the dimension’s lifetime, regardless of changes in the source system.

Example: Assume a student’s major at entry to college is stored. Even if the student changes majors later, the recorded major remains unchanged.

Before:
| Student ID  | Major.           |
| - - - - - - | - - - - - - - - -|
| 001         | Computer Science |


After the student changes to "Mathematics":
| Student ID  | Major            |
| - - - - - - | - - - - - - - - -|
| 001         | Computer Science |
```


### Type 1 — Overwriting the old value:
```
With Type 1, when a dimension attribute changes, the old value is simply overwritten with the new value. This method doesn’t keep any history of old values.

Example: If a customer changes their address, the new address simply replaces the old one in the customer dimension table.


Before:
| Customer ID  | Address         |
| - - - - - - -| - - - - - - - - |
| 100          | 123 Main St.    |



After the customer moves to "456 Elm St.":
| Customer ID  | Address         |
| - - - - - - -| - - - - - - - - |
| 100          | 456 Elm St.     |
```


### Type 2 — Creating a new additional record:
```
In Type 2, when an attribute changes, a new record is added to the dimension table, and the old record is marked as outdated. This approach maintains a full history of attribute values.

Example: When a product’s price changes, a new product record is created with the updated price, while the old record is flagged as inactive or is given an end date.

Before:
| Product ID  | Price  | Active  |
| - - - - - - | - - - -| - - - - |
| P001        | $10    | Yes     |


After a price change to $12:
| Product ID  | Price  | Active  |
| - - - - - - | - - - -| - - - - |
| P001        | $10    | No      |
| P002        | $12    | Yes     |
```


### Type 3 — Adding a new column:
```
Type 3 adds a new column to track changes. When an attribute changes, the current value is moved to this new column, and the original column is overwritten with the new value. This method only maintains the previous value.

Example: If an employee’s role changes, their previous role is stored in a “Previous Role” column, and the main “Role” column is updated with the new role.

Before:
| Employee ID  | Role       |
| - - - - - - -| - - - - - -|
| E001         | Developer  |


After changing role to "Manager":
| Employee ID  | Role     | Previous Role  |
| - - - - - - -| - - - - -| - - - - - - - -|
| E001         | Manager  | Developer      |
```


### Type 4 — Using historical table:
```
With Type 4, a separate history table is maintained. Whenever there’s a change, the current state of the dimension is written into the history table before the change is applied to the dimension table.

Example: If a store’s location changes, before updating the main table, the current store record is pushed into a “Store History” table, preserving all details prior to the change.

Before:

| Store ID  | Location |
| - - - - - | - - - - -|
| S001      | Downtown |


After relocating to "Uptown":
Main Table:
| Store ID  | Location |
| - - - - - | - - - - -|
| S001      | Uptown   |

History Table:
| Store ID  | Location |
| - - - - - | - - - - -|
| S001      | Downtown |
```


### Type 6 — Combine approaches of types 1,2,3 (1+2+3=6):
```
Type 6 is a hybrid approach, blending elements from Types 1, 2, and 3. It allows overwriting certain attributes (Type 1), adding new records for others (Type 2), and creating new columns for some (Type 3).

Example: Consider a salesperson’s region. If they change regions, a new record is created (Type 2), their “Previous Region” is updated (Type 3), and some attributes like their contact number might just be overwritten if they get a new one (Type 1).

Before:
| Salesperson ID | Region | Previous Region | Contact Number |
| - - - - - - - -| - - - -| - - - - - - - - | - - - - - - - -|
| SP001          | East   | None            | 555–1234       |

After changing to "West" region and new contact number "555–5678":
| Salesperson ID | Region  | Previous Region | Contact Number |
| - - - - - - - -| - - - - | - - - - - - - - | - - - - - - - -|
| SP001          | East    | None            | 555–1234       |
| SP002          | West    | East            | 555–5678       |
These techniques help businesses choose the most appropriate way to handle historical data, depending on their specific analytical and operational needs.
```

SCD strategies handle how dimension attribute changes are tracked over time.

### SCD Type 0: Fixed
No changes allowed. Attribute is permanent.
```sql
-- Example: Social Security Number (never changes)
ALTER TABLE dim_customer ADD CONSTRAINT chk_ssn UNIQUE (ssn);
```

### SCD Type 1: Overwrite
Replace old value with new value. No history kept.
```sql
-- Customer moved cities - overwrite
UPDATE dim_customer 
SET city = 'New York', state = 'NY'
WHERE customer_id = 'C001';
```

### SCD Type 2: Add New Row (Most Common)
Add a new row for the change with effective/expiry dates.
```sql
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
```

**Query current version:**
```sql
SELECT * FROM dim_customer 
WHERE customer_id = 'C001' AND is_current = TRUE;
```

**Query history:**
```sql
SELECT * FROM dim_customer 
WHERE customer_id = 'C001'
ORDER BY effective_date;
```

### SCD Type 3: Add New Column
Track limited history by adding columns.
```sql
ALTER TABLE dim_customer 
    ADD COLUMN previous_city VARCHAR(50),
    ADD COLUMN city_change_date DATE;

UPDATE dim_customer 
SET previous_city = city,
    city = 'New York',
    city_change_date = CURRENT_DATE
WHERE customer_id = 'C001';
```

### SCD Type 4: History Table
Create a separate history table.
```sql
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
```

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
```sql
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
```

### Periodic Snapshot Fact Table
Records state at regular intervals.
```sql
CREATE TABLE fact_account_balance_daily (
    account_key     INT,
    snapshot_date_key INT,
    opening_balance DECIMAL(15,2),
    closing_balance DECIMAL(15,2),
    total_debits    DECIMAL(15,2),
    total_credits   DECIMAL(15,2),
    transaction_count INT
);
```

### Accumulating Snapshot Fact Table
Tracks lifecycle of a process with multiple date keys.
```sql
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
```

### Factless Fact Table
Contains no measures - only dimension keys for counting occurrences.
```sql
CREATE TABLE fact_student_enrollment (
    student_key     INT,
    course_key      INT,
    semester_key    INT,
    department_key  INT
    -- No measures - just tracks which students enrolled in which courses
);
```

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
```
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
```

### Scenario 2: E-Commerce Data Platform

**Challenge:** An e-commerce company needs real-time inventory, customer analytics, and sales forecasting.

**Solution:**
```
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
```

---

## 9. Banking Examples

### Example 1: Anti-Money Laundering (AML) Data Warehouse

**Business Problem:** Banks must detect and report suspicious activities to regulators (FinCEN).

**Data Model:**
```
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
```

**Key Queries:**
```sql
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
```

### Example 2: Loan Portfolio Analytics

**Business Problem:** Analyze loan performance, delinquency rates, and predict defaults.

**Accumulating Snapshot Fact Table:**
```sql
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
```

---

## 10. E-Commerce Examples

### Example 1: Customer 360 Data Warehouse

**Business Problem:** Build a unified view of customers across all channels.

```sql
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
```

**Key Query - Customer Segmentation:**
```sql
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
```

### Example 2: Sales and Inventory Analytics

**Business Problem:** Optimize inventory levels and maximize sales.

```sql
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
```

---

## 11. Performance Optimization

### 11.1 Query Optimization Techniques

| Technique | Description | Impact |
|-----------|-------------|--------|
| **Columnar Storage** | Store data by column, not row | 10-100x faster aggregations |
| **Partitioning** | Split tables by date/key | Reduces data scanned |
| **Clustering** | Sort data within partitions | Improves range queries |
| **Materialized Views** | Pre-computed query results | 10-1000x faster reads |
| **Approximate Queries** | Use sketches for large datasets | 10-100x faster with ~1% error |

### 11.2 Partitioning Strategies

```sql
-- Date-based partitioning (most common)
CREATE TABLE fact_sales (
    sale_id BIGINT,
    sale_date DATE,
    customer_key INT,
    amount DECIMAL(12,2)
) PARTITION BY RANGE (sale_date);

-- Partition by month
CREATE TABLE fact_sales_monthly PARTITION OF fact_sales
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

-- Hive-style partitioning (for data lakes)
-- s3://data-lake/fact_sales/year=2024/month=01/day=15/
```

### 11.3 Materialized Views

```sql
-- Create materialized view for common aggregation
CREATE MATERIALIZED VIEW mv_monthly_sales AS
SELECT 
    d.year,
    d.month_number,
    p.category,
    SUM(f.revenue) as total_revenue,
    COUNT(DISTINCT f.customer_key) as unique_customers
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY d.year, d.month_number, p.category;

-- Refresh when underlying data changes
REFRESH MATERIALIZED VIEW mv_monthly_sales;

-- Query is now instant
SELECT * FROM mv_monthly_sales 
WHERE year = 2024 AND category = 'Electronics';
```

### 11.4 dbt for Data Transformation

```sql
-- models/staging/stg_orders.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),

renamed AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        status,
        amount,
        _loaded_at
    FROM source
)

SELECT * FROM renamed;

-- models/marts/fct_orders.sql
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

final AS (
    SELECT
        orders.order_id,
        customers.customer_key,
        orders.order_date,
        orders.amount,
        orders.status
    FROM orders
    LEFT JOIN customers ON orders.customer_id = customers.customer_id
)

SELECT * FROM final;
```

### 11.5 Performance Best Practices

| Do | Don't |
|----|-------|
| Use columnar formats (Parquet, ORC) | Store data in CSV for analytics |
| Partition large tables by date | Query full table without filters |
| Use materialized views for dashboards | Recompute aggregations every query |
| Implement incremental models | Full refresh for large datasets |
| Use SORT KEY / CLUSTER BY | Ignore data distribution |
| Cache frequently accessed data | Hit storage for repeated queries |

---

## 12. Hands-On Exercises

### Exercise 1: Star Schema vs Snowflake Schema Design

**Task:** Design both a Star Schema and Snowflake Schema for an e-commerce company, then compare them.

#### Scenario
An e-commerce company tracks sales across multiple regions. They need:
- Sales transactions with amounts, quantities, and dates
- Product information including categories and manufacturers
- Customer information including locations (country, state, city)
- Store/warehouse information

#### Part A: Star Schema Design

```sql
-- Star Schema: Denormalized dimensions

-- Fact Table
CREATE TABLE fact_sales_star (
    sale_key BIGSERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date_star(date_key),
    product_key INT REFERENCES dim_product_star(product_key),
    customer_key INT REFERENCES dim_customer_star(customer_key),
    store_key INT REFERENCES dim_store_star(store_key),
    quantity INT,
    unit_price DECIMAL(10,2),
    revenue DECIMAL(12,2),
    cost DECIMAL(12,2)
);

-- Dimension: Date (same for both schemas)
CREATE TABLE dim_date_star (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week VARCHAR(10),
    month_name VARCHAR(10),
    quarter INT,
    year INT
);

-- Dimension: Product (denormalized - all info in one table)
CREATE TABLE dim_product_star (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20) UNIQUE NOT NULL,
    product_name VARCHAR(100),
    category VARCHAR(50),        -- Category stored directly
    subcategory VARCHAR(50),
    brand VARCHAR(50),           -- Brand/manufacturer stored directly
    manufacturer VARCHAR(100),
    unit_cost DECIMAL(10,2)
);

-- Dimension: Customer (denormalized - all location info in one table)
CREATE TABLE dim_customer_star (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) UNIQUE NOT NULL,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    country VARCHAR(50),         -- Location hierarchy flattened
    state VARCHAR(50),
    city VARCHAR(50),
    segment VARCHAR(20)
);

-- Dimension: Store (denormalized)
CREATE TABLE dim_store_star (
    store_key SERIAL PRIMARY KEY,
    store_id VARCHAR(20) UNIQUE NOT NULL,
    store_name VARCHAR(100),
    region VARCHAR(50),          -- Region stored directly
    country VARCHAR(50),
    city VARCHAR(50)
);
```

#### Part B: Snowflake Schema Design

```sql
-- Snowflake Schema: Normalized dimensions with sub-dimensions

-- Fact Table (same structure)
CREATE TABLE fact_sales_snowflake (
    sale_key BIGSERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date_snowflake(date_key),
    product_key INT REFERENCES dim_product_snowflake(product_key),
    customer_key INT REFERENCES dim_customer_snowflake(customer_key),
    store_key INT REFERENCES dim_store_snowflake(store_key),
    quantity INT,
    unit_price DECIMAL(10,2),
    revenue DECIMAL(12,2),
    cost DECIMAL(12,2)
);

-- Dimension: Date (same)
CREATE TABLE dim_date_snowflake (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week VARCHAR(10),
    month_name VARCHAR(10),
    quarter INT,
    year INT
);

-- Dimension: Product (normalized - category in separate table)
CREATE TABLE dim_product_snowflake (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20) UNIQUE NOT NULL,
    product_name VARCHAR(100),
    category_key INT REFERENCES dim_category(category_key),
    manufacturer_key INT REFERENCES dim_manufacturer(manufacturer_key),
    unit_cost DECIMAL(10,2)
);

-- Sub-dimension: Category
CREATE TABLE dim_category (
    category_key SERIAL PRIMARY KEY,
    category_name VARCHAR(50) UNIQUE NOT NULL,
    subcategory VARCHAR(50)
);

-- Sub-dimension: Manufacturer
CREATE TABLE dim_manufacturer (
    manufacturer_key SERIAL PRIMARY KEY,
    manufacturer_name VARCHAR(100) UNIQUE NOT NULL,
    country VARCHAR(50)
);

-- Dimension: Customer (normalized - location in separate table)
CREATE TABLE dim_customer_snowflake (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) UNIQUE NOT NULL,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    location_key INT REFERENCES dim_location(location_key),
    segment VARCHAR(20)
);

-- Sub-dimension: Location (hierarchical)
CREATE TABLE dim_location (
    location_key SERIAL PRIMARY KEY,
    city VARCHAR(50),
    state_key INT REFERENCES dim_state(state_key)
);

CREATE TABLE dim_state (
    state_key SERIAL PRIMARY KEY,
    state_name VARCHAR(50),
    country_key INT REFERENCES dim_country(country_key)
);

CREATE TABLE dim_country (
    country_key SERIAL PRIMARY KEY,
    country_name VARCHAR(50) UNIQUE NOT NULL,
    continent VARCHAR(20)
);

-- Dimension: Store (normalized - region in separate table)
CREATE TABLE dim_store_snowflake (
    store_key SERIAL PRIMARY KEY,
    store_id VARCHAR(20) UNIQUE NOT NULL,
    store_name VARCHAR(100),
    region_key INT REFERENCES dim_region(region_key)
);

CREATE TABLE dim_region (
    region_key SERIAL PRIMARY KEY,
    region_name VARCHAR(50),
    country_key INT REFERENCES dim_country(country_key)
);
```

#### Part C: Query Comparison

```sql
-- Star Schema Query: Total sales by country
-- Simple: 1 join only
SELECT 
    c.country,
    SUM(s.revenue) AS total_sales
FROM fact_sales_star s
JOIN dim_customer_star c ON s.customer_key = c.customer_key
GROUP BY c.country;

-- Snowflake Schema Query: Same analysis
-- Complex: 4 joins required
SELECT 
    co.country_name,
    SUM(s.revenue) AS total_sales
FROM fact_sales_snowflake s
JOIN dim_customer_snowflake c ON s.customer_key = c.customer_key
JOIN dim_location l ON c.location_key = l.location_key
JOIN dim_state st ON l.state_key = st.state_key
JOIN dim_country co ON st.country_key = co.country_key
GROUP BY co.country_name;
```

**Questions to Answer:**
1. Which schema has fewer joins for a query by country?
2. Which schema uses more storage for the customer dimension?
3. If you need to update "Electronics" to "Consumer Electronics" in the category, which schema requires fewer updates?

---

### Exercise 2: Schema Selection Scenarios

**Task:** For each scenario, choose Star or Snowflake schema and explain why.

| Scenario | Your Choice | Reason |
|----------|-------------|--------|
| 1. BI dashboard showing daily sales by product category | | |
| 2. Financial reporting with complex regulatory hierarchies | | |
| 3. Real-time analytics with sub-second query requirements | | |
| 4. Data warehouse with 10TB+ of product data and frequent category updates | | |
| 5. Small startup with 100GB data and 3-person data team | | |
| 6. Enterprise with strict data governance and audit requirements | | |

**Sample Answers:**

| Scenario | Choice | Reason |
|----------|--------|--------|
| 1. BI dashboard | **Star** | Fewer joins = faster queries for interactive dashboards |
| 2. Financial reporting | **Snowflake** | Complex hierarchies benefit from normalization |
| 3. Real-time analytics | **Star** | Minimal joins for sub-second performance |
| 4. Large product data | **Snowflake** | Storage savings significant at 10TB+, easy category updates |
| 5. Small startup | **Star** | Simpler to maintain with small team |
| 6. Enterprise governance | **Snowflake** | Better data integrity and audit trail |

---

### Exercise 3: Convert Star Schema to Snowflake

**Task:** Normalize the following Star Schema into a Snowflake Schema.

**Given Star Schema:**
```sql
-- Current Star Schema
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    brand VARCHAR(50),
    supplier VARCHAR(100),
    supplier_country VARCHAR(50)
);
```

**Your Task:** Write SQL to create the normalized Snowflake version.

**Solution:**
```sql
-- Snowflake Schema: Normalized
CREATE TABLE dim_product_normalized (
    product_key SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category_key INT REFERENCES dim_category(category_key),
    brand_key INT REFERENCES dim_brand(brand_key),
    supplier_key INT REFERENCES dim_supplier(supplier_key)
);

CREATE TABLE dim_category (
    category_key SERIAL PRIMARY KEY,
    category_name VARCHAR(50),
    subcategory VARCHAR(50)
);

CREATE TABLE dim_brand (
    brand_key SERIAL PRIMARY KEY,
    brand_name VARCHAR(50)
);

CREATE TABLE dim_supplier (
    supplier_key SERIAL PRIMARY KEY,
    supplier_name VARCHAR(100),
    country VARCHAR(50)
);
```

**Questions:**
1. How many tables did you create?
2. What is the storage benefit if you have 1 million products with 100 unique categories?
3. How many joins are needed to get product name, category, and supplier country?

---

### Exercise 4: Query Performance Analysis

**Task:** Analyze the performance implications of each schema.

```sql
-- Scenario: Find total sales by category for Q1 2024

-- Star Schema Query
EXPLAIN ANALYZE
SELECT 
    p.category,
    SUM(s.revenue) as total_sales,
    COUNT(*) as transaction_count
FROM fact_sales s
JOIN dim_product p ON s.product_key = p.product_key
JOIN dim_date d ON s.date_key = d.date_key
WHERE d.year = 2024 AND d.quarter = 1
GROUP BY p.category;
-- Expected: 2 joins, fast execution

-- Snowflake Schema Query
EXPLAIN ANALYZE
SELECT 
    c.category_name,
    SUM(s.revenue) as total_sales,
    COUNT(*) as transaction_count
FROM fact_sales s
JOIN dim_product p ON s.product_key = p.product_key
JOIN dim_category c ON p.category_key = c.category_key
JOIN dim_date d ON s.date_key = d.date_key
WHERE d.year = 2024 AND d.quarter = 1
GROUP BY c.category_name;
-- Expected: 3 joins, slightly slower
```

**Analysis Questions:**
1. Compare the execution plans - how many joins does each query use?
2. Estimate the performance difference (assume 100M rows in fact table)
3. If you add an index on dim_category.category_name, does it help the Snowflake query?

---

### Exercise 5: Hybrid Approach Implementation

**Task:** Implement a hybrid approach where the warehouse layer uses Snowflake schema but the data mart uses Star schema.

```sql
-- Layer 1: Warehouse (Snowflake Schema - normalized for governance)
CREATE TABLE wh_product (
    product_key SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category_key INT REFERENCES wh_category(category_key),
    manufacturer_key INT REFERENCES wh_manufacturer(manufacturer_key)
);

CREATE TABLE wh_category (
    category_key SERIAL PRIMARY KEY,
    category_name VARCHAR(50),
    subcategory VARCHAR(50)
);

CREATE TABLE wh_manufacturer (
    manufacturer_key SERIAL PRIMARY KEY,
    manufacturer_name VARCHAR(100),
    country VARCHAR(50)
);

-- Layer 2: Data Mart (Star Schema - denormalized for BI consumption)
CREATE VIEW mart_product AS
SELECT 
    p.product_key,
    p.product_name,
    c.category_name,
    c.subcategory,
    m.manufacturer_name,
    m.country as manufacturer_country
FROM wh_product p
JOIN wh_category c ON p.category_key = c.category_key
JOIN wh_manufacturer m ON p.manufacturer_key = m.manufacturer_key;

-- Now BI tools query the denormalized view
SELECT 
    category_name,
    COUNT(*) as product_count
FROM mart_product
GROUP BY category_name;
```

**Benefits of this approach:**
- Warehouse layer maintains data integrity with normalized structures
- Data mart provides fast queries for BI tools
- Changes to categories only need to update one row in wh_category
- BI tools see a simple star schema without complex joins

---

### Exercise 6: Create a Star Schema (SQL)
```sql
-- Task: Create a complete star schema for an e-commerce platform

-- Step 1: Create dimension tables
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_of_week VARCHAR(10),
    month_name VARCHAR(10),
    quarter INT,
    year INT,
    is_weekend BOOLEAN
);

CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) UNIQUE NOT NULL,
    customer_name VARCHAR(100),
    email VARCHAR(100),
    segment VARCHAR(20),
    city VARCHAR(50),
    country VARCHAR(50),
    registration_date DATE
);

CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20) UNIQUE NOT NULL,
    product_name VARCHAR(100),
    category VARCHAR(50),
    brand VARCHAR(50),
    unit_cost DECIMAL(10,2),
    unit_price DECIMAL(10,2)
);

-- Step 2: Create fact table
CREATE TABLE fact_sales (
    sale_key BIGSERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    customer_key INT REFERENCES dim_customer(customer_key),
    product_key INT REFERENCES dim_product(product_key),
    quantity INT,
    revenue DECIMAL(12,2),
    cost DECIMAL(12,2),
    profit DECIMAL(12,2)
);

-- Step 3: Create indexes for performance
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
```

### Exercise 2: Implement SCD Type 2 (SQL)
```sql
-- Task: Implement SCD Type 2 for customer dimension

-- Add SCD columns
ALTER TABLE dim_customer ADD COLUMN effective_date DATE;
ALTER TABLE dim_customer ADD COLUMN expiry_date DATE DEFAULT '9999-12-31';
ALTER TABLE dim_customer ADD COLUMN is_current BOOLEAN DEFAULT TRUE;

-- Procedure to update customer with SCD Type 2
CREATE OR REPLACE PROCEDURE update_customer_scd(
    p_customer_id VARCHAR(20),
    p_new_city VARCHAR(50),
    p_new_segment VARCHAR(20)
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Expire current record
    UPDATE dim_customer 
    SET expiry_date = CURRENT_DATE - 1,
        is_current = FALSE
    WHERE customer_id = p_customer_id 
      AND is_current = TRUE;
    
    -- Insert new record
    INSERT INTO dim_customer (
        customer_id, customer_name, email, segment, city, country,
        effective_date, expiry_date, is_current
    )
    SELECT 
        customer_id, customer_name, email, p_new_segment, p_new_city, country,
        CURRENT_DATE, '9999-12-31', TRUE
    FROM dim_customer
    WHERE customer_id = p_customer_id AND is_current = FALSE
    ORDER BY expiry_date DESC
    LIMIT 1;
END;
$$;

-- Test the procedure
CALL update_customer_scd('C001', 'New York', 'Premium');

-- Verify history
SELECT * FROM dim_customer WHERE customer_id = 'C001' ORDER BY effective_date;
```

### Exercise 3: Query Optimization Challenge
```sql
-- Task: Optimize this slow query

-- BEFORE (slow - scans full table)
SELECT 
    DATE_TRUNC('month', order_date) as month,
    category,
    SUM(amount) as revenue
FROM fact_orders o
JOIN dim_product p ON o.product_key = p.product_key
WHERE order_date >= '2024-01-01'
GROUP BY 1, 2;

-- AFTER (optimized with materialized view)
CREATE MATERIALIZED VIEW mv_monthly_category_sales AS
SELECT 
    d.year,
    d.month_number,
    p.category,
    SUM(o.revenue) as total_revenue,
    COUNT(*) as order_count
FROM fact_orders o
JOIN dim_date d ON o.date_key = d.date_key
JOIN dim_product p ON o.product_key = p.product_key
GROUP BY d.year, d.month_number, p.category;

-- Now query is instant
SELECT * FROM mv_monthly_category_sales 
WHERE year = 2024 AND month_number = 1;
```

### Exercise 4: dbt Model Development
```yaml
-- models/staging/stg_ecommerce.yml
version: 2

models:
  - name: stg_orders
    description: "Staged orders from source system"
    columns:
      - name: order_id
        description: "Primary key"
        tests:
          - unique
          - not_null
      - name: customer_id
        description: "Foreign key to customers"
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id
      - name: order_date
        description: "Order timestamp"
        tests:
          - not_null
      - name: status
        description: "Order status"
        tests:
          - accepted_values:
              values: ['pending', 'shipped', 'delivered', 'cancelled']

sources:
  - name: raw
    database: raw
    schema: ecommerce
    tables:
      - name: orders
        loaded_at_field: _loaded_at
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 24, period: hour}
```

---

## 13. Interview Questions

### Q1: What is the difference between a data warehouse and a data lake?

**Answer:**

A **data warehouse** stores structured, processed data optimized for BI and analytics using schema-on-write. It's ideal for consistent, high-quality reporting. 

A **data lake** stores raw data in native format (structured, semi-structured, unstructured) using schema-on-read, ideal for ML and data exploration. 

Modern architectures use **data lakehouses** (Delta Lake, Iceberg) combining both benefits: cheap object storage with ACID transactions and schema enforcement.

### Q2: Explain SCD Type 2 with an example.

**Answer:**
SCD Type 2 preserves full history by adding new rows. When a customer changes address:
1. **Expire** the current record by setting expiry_date = today - 1
2. **Insert** a new record with effective_date = today and expiry_date = 9999-12-31
3. Use is_current = TRUE/FALSE flag for easy current record retrieval

This maintains complete audit trail for regulatory compliance and historical analysis.

### Q3: Star schema vs Snowflake schema - when to use which?

**Answer:**

**Star schema** (denormalized dimensions): Use for most data warehouses. Fewer joins = faster queries. Better for BI tools. Redundancy is acceptable. 

**Snowflake schema** (normalized dimensions): Use when dimension tables are very large and storage cost matters. Better data integrity. Use when updates to dimension attributes are frequent. In practice, star schema is preferred 90% of the time.

### Q4: What are the different types of fact tables?

**Answer:**
1. **Transaction fact table**: One row per event/transaction (e.g., each sale)
2. **Periodic snapshot**: One row per entity per time period (e.g., daily account balance)
3. **Accumulating snapshot**: One row per process instance with multiple date keys tracking milestones (e.g., order lifecycle: ordered -> shipped -> delivered)
4. **Factless fact table**: No measures, only dimension keys (e.g., student enrollment)

### Q5: Inmon vs Kimball - which approach is better?

**Answer:**
Neither is universally better - it depends on context. 

**Inmon** (top-down) is better for large enterprises needing a single source of truth with strong governance, but requires more upfront investment. 

**Kimball** (bottom-up) is better for organizations needing faster time-to-value with well-defined business processes, using conformed dimensions for integration. Most modern organizations use a hybrid approach.

---

*Next Section: [03 - Data Modeling](../03-Data-Modeling/README.md)*
