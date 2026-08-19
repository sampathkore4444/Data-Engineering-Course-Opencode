# 03 - Data Modeling

## Table of Contents
1. [Conceptual Modeling](#1-conceptual-modeling)
   - [What is an ERD?](#what-is-an-erd)
   - [Business Process Identification](#business-process-identification)
2. [Logical Modeling](#2-logical-modeling)
   - [What is Normalization?](#what-is-normalization)
   - [Third Normal Form (3NF)](#third-normal-form-3nf)
   - [Normalization vs Denormalization](#normalization-vs-denormalization-when-to-use-each)
3. [Physical Modeling](#3-physical-modeling)
4. [Advanced Patterns](#4-advanced-patterns)
   - [What is Data Vault Modeling?](#what-is-data-vault-modeling)
   - [Data Vault vs Star Schema](#data-vault-vs-star-schema-comparison)
5. [Real-World Scenarios](#5-real-world-scenarios)
6. [Banking Examples](#6-banking-examples)
7. [E-Commerce Examples](#7-e-commerce-examples)
8. [Choosing the Right Modeling Approach](#choosing-the-right-modeling-approach)
9. [Hands-On Exercises](#8-hands-on-exercises)
10. [Interview Questions](#9-interview-questions)
11. [Summary Checklist](#summary-checklist)

---

## 1. Conceptual Modeling

Conceptual modeling is the high-level design that identifies the main business entities, their relationships, and the scope of the data model. It focuses on WHAT data exists, not HOW it's stored.

### What is an ERD?

An **Entity-Relationship Diagram (ERD)** is a visual representation of the entities (tables), attributes (columns), and relationships between entities in a data system. It serves as the foundation for database design.

```
+----------------+          +----------------+          +----------------+
|   CUSTOMER     |          |     ORDER      |          |    PRODUCT     |
|----------------|          |----------------|          |----------------|
| customer_id PK |<-------->| order_id PK    |<-------->| product_id PK  |
| name           |  1    M  | customer_id FK |  M    M  | product_name   |
| email          |          | order_date     |          | category       |
| phone          |          | total_amount   |          | price          |
| created_at     |          | status         |          | stock_qty      |
+----------------+          +----------------+          +----------------+
```

### Why ERDs Matter

| Benefit | Description |
|---------|-------------|
| **Communication** | Provides a common language for business and technical teams |
| **Documentation** | Serves as living documentation of the data architecture |
| **Design Validation** | Helps identify gaps and issues before implementation |
| **Maintenance** | Makes it easier to understand and modify the database |

### When to Use ERDs

| Scenario | ERD Type |
|----------|----------|
| New application development | Full conceptual + logical ERD |
| Database migration | Physical ERD of existing system |
| Data warehouse design | Dimensional model ERD (star/snowflake) |
| Quick documentation | Mermaid or dbdiagram.io sketch |

### Modern ERD Tools

| Tool | Type | Best For | Pricing |
|------|------|----------|--------|
| **dbdiagram.io** | Web-based | Quick schemas, dbt integration | Free tier |
| **DrawSQL** | Web-based | Database schema visualization | Free / Paid |
| **DBeaver** | Desktop | Database management + ERD | Free / Community |
| **ERwin** | Enterprise | Complex data modeling | Licensed |
| **Lucidchart** | Web-based | Team collaboration | Paid |
| **Mermaid** | Code-based | Documentation, Git-friendly | Free |

### Crow's Foot Notation

```
CUSTOMER ||--o{ ORDER         One customer has zero or many orders
ORDER }o--|{ ORDER_ITEM      One order has one or many items
PRODUCT ||--o{ ORDER_ITEM    One product has zero or many order items
```

### Business Process Identification

Before modeling, identify key business processes:

| Business Process | Key Metrics | Grain |
|-----------------|-------------|-------|
| Sales | Revenue, Quantity, Discount | One row per line item |
| Inventory | Stock Level, Turns | One row per product per day |
| Shipping | Delivery Time, Cost | One row per shipment |
| Customer Service | Resolution Time, Satisfaction | One row per ticket |

### Conceptual Modeling Best Practices

1. **Start with business questions** - What decisions will this data support?
2. **Identify entities** - What things does the business track?
3. **Define relationships** - How are entities related?
4. **Keep it simple** - No technical details, just business concepts
5. **Validate with stakeholders** - Ensure it matches business understanding

---

## 2. Logical Modeling

### What is Normalization?

**Normalization** is the process of organizing data to reduce redundancy and improve data integrity. It involves splitting large tables into smaller, related tables and defining relationships between them.

### Third Normal Form (3NF)

**Purpose:** Eliminate data redundancy in operational/transactional systems.

**Rules:**
1. **1NF:** Each column contains atomic values, no repeating groups
2. **2NF:** All non-key columns depend on the entire primary key
3. **3NF:** No non-key column depends on another non-key column

### Example - Normalizing Customer Orders

**Denormalized (Bad):** All data in one table with lots of repetition
```
+--------+-----------+-----------+--------+----------+----------+
| order  | customer  | customer  | product| product  | order    |
| _id    | _id       | _name     | _id    | _price   | _amount  |
+--------+-----------+-----------+--------+----------+----------+
| 1      | C001      | John      | P001   | 10.00    | 50.00    |
| 2      | C001      | John      | P002   | 20.00    | 20.00    |
| 3      | C002      | Jane      | P001   | 10.00    | 30.00    |
+--------+-----------+-----------+--------+----------+----------+
Problem: Customer name repeated, product price repeated
```

**Normalized (3NF):** Split into related tables
```
customers:        orders:           order_items:     products:
+-----------+    +----------+     +-----------+    +----------+
| cust_id   |    | order_id |     | order_id  |    | prod_id  |
| cust_name |    | cust_id  |     | prod_id   |    | prod_name|
| email     |    | order_dt |     | quantity  |    | price    |
+-----------+    | status   |     | unit_price|    +-----------+
                 +----------+     +-----------+
```

### Normalization Benefits and Trade-offs

| Aspect | Benefits | Trade-offs |
|--------|----------|------------|
| **Storage** | Less redundant data | More tables to manage |
| **Integrity** | Single source of truth | More joins required |
| **Updates** | Update once, reflected everywhere | Complex queries |
| **Performance** | Better for write-heavy systems | Slower for read-heavy analytics |

### Denormalization for Analytics

For data warehouses, we deliberately denormalize for query performance. **Denormalization** is the process of intentionally adding redundant data to optimize read performance.

```sql
-- Instead of joining multiple tables, create a wide denormalized table
CREATE TABLE fact_sales_denormalized (
    -- Keys
    sale_id BIGINT,
    date_key INT,
    customer_key INT,
    product_key INT,
    -- Denormalized from dim_customer
    customer_name VARCHAR(100),
    customer_segment VARCHAR(20),
    customer_city VARCHAR(50),
    -- Denormalized from dim_product
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    product_brand VARCHAR(50),
    -- Measures
    quantity INT,
    revenue DECIMAL(12,2),
    cost DECIMAL(12,2)
);
```

### Normalization vs Denormalization: When to Use Each

| Use Case | Approach | Why |
|----------|----------|-----|
| **OLTP systems** (e-commerce, banking) | Normalized (3NF) | Data integrity, fast writes |
| **OLAP systems** (data warehouses) | Denormalized (Star Schema) | Query performance, fewer joins |
| **Data lakes** | Both (raw normalized, curated denormalized) | Flexibility for different workloads |
| **Real-time analytics** | Denormalized | Sub-second query response |

### Normalization Rules Explained

| Normal Form | Rule | Example of Violation | Fix |
|-------------|------|---------------------|-----|
| **1NF** | Atomic values, no repeating groups | `phone_numbers: "123,456,789"` | Split into separate rows or table |
| **2NF** | No partial dependencies (non-key depends on entire PK) | `order_id + product_id -> product_category` | Move category to product table |
| **3NF** | No transitive dependencies (non-key depends on another non-key) | `customer_id -> city -> state` | Move state to city table or keep in customer |

---

## 3. Physical Modeling

### Indexing Strategies

#### B-Tree Index (Default)
Best for equality and range queries.
```sql
CREATE INDEX idx_customer_email ON dim_customer(email);
-- Speeds up: WHERE email = 'user@example.com'
-- Speeds up: WHERE email LIKE 'user%'
```

#### Composite Index
For queries filtering on multiple columns.
```sql
CREATE INDEX idx_sales_date_product 
ON fact_sales(date_key, product_key);
-- Speeds up: WHERE date_key = 20240101 AND product_key = 100
```

#### Bitmap Index
Best for low-cardinality columns (few distinct values).
```sql
CREATE BITMAP INDEX idx_sales_region 
ON fact_sales(region_id);
-- Ideal for columns with < 1000 distinct values
-- Great for analytical queries with multiple WHERE conditions
```

#### Covering Index
Includes all columns needed by a query - no table lookup needed.
```sql
CREATE INDEX idx_orders_covering 
ON fact_orders(customer_key, order_date_key) 
INCLUDE (revenue, profit);
-- Query satisfied entirely from index
SELECT revenue, profit FROM fact_orders 
WHERE customer_key = 100 AND order_date_key = 20240101;
```

### Partitioning

#### Range Partitioning
```sql
-- Partition by date range
CREATE TABLE fact_sales (
    sale_id BIGINT,
    date_key INT,
    revenue DECIMAL(12,2)
) PARTITION BY RANGE (date_key);

CREATE TABLE fact_sales_2023 PARTITION OF fact_sales
    FOR VALUES FROM (20230101) TO (20240101);

CREATE TABLE fact_sales_2024 PARTITION OF fact_sales
    FOR VALUES FROM (20240101) TO (20250101);
```

#### Hash Partitioning
```sql
-- Distribute evenly by hash
CREATE TABLE fact_sales (
    sale_id BIGINT,
    customer_key INT,
    revenue DECIMAL(12,2)
) PARTITION BY HASH (customer_key);
```

#### Benefits of Partitioning
- **Partition pruning:** Query scans only relevant partitions
- **Manageability:** Drop old data by dropping partitions
- **Parallelism:** Different partitions processed in parallel

### Compression

| Algorithm | Best For | Compression Ratio |
|-----------|----------|-------------------|
| Snappy | General purpose, fast decompression | 2-3x |
| Gzip | Text data, archival | 5-10x |
| Zstd | Balanced speed/ratio | 3-5x |
| LZO | Fast decompression needed | 2-3x |
| Run-length | Sequential repeated values | Variable |

### Physical Modeling Tools

| Tool | Purpose | Platform |
|------|---------|----------|
| **Apache Spark** | Large-scale data modeling | Cloud / On-prem |
| **dbt** | SQL-based transformations | Cloud warehouses |
| **Liquibase** | Schema version control | Any database |
| **Flyway** | Database migrations | Any database |
| **SchemaSpy** | Schema documentation | Open source |

---

## 4. Advanced Patterns

### What is Data Vault Modeling?

**Data Vault** is a modeling approach designed for enterprise data warehouses that need **auditability**, **flexibility**, and **scalability**. It was created by Dan Linstedt to solve problems with traditional modeling approaches in large, complex data environments.

### Why Use Data Vault?

| Benefit | Description |
|---------|-------------|
| **Full Audit Trail** | Every change is tracked with load timestamps and source systems |
| **Flexibility** | Easy to add new source systems without changing existing structures |
| **Parallel Loading** | Multiple source systems can load simultaneously without locks |
| **Business Keys** | Uses natural business keys, not surrogate keys |
| **Historical Tracking** | Satellites store complete history of attribute changes |

### When to Use Data Vault

| Scenario | Use Data Vault? | Why |
|----------|-----------------|-----|
| Enterprise DW with 10+ source systems | ✅ Yes | Handles multiple sources elegantly |
| Regulatory compliance (banking, healthcare) | ✅ Yes | Full auditability required |
| Frequently changing source systems | ✅ Yes | Flexible schema evolution |
| Small data warehouse (< 5 tables) | ❌ No | Overkill for simple systems |
| Quick BI/Analytics project | ❌ No | Star schema is simpler and faster |

### Three Core Components

```
HUB (Business Keys)          LINK (Relationships)         SATELLITE (Details)
+-------------------+       +-------------------+       +-------------------+
| Hub_Load_Datetime |       | Link_Load_Datetime|       | Sat_Load_Datetime |
| Hub_Business_Key  |       | Hub1_Business_Key |       | Hub_Business_Key  |
| Hash_Key          |       | Hub2_Business_Key |       | Attributes...     |
| Source_System     |       | Hash_Key          |       | Hash_Diff         |
+-------------------+       | Source_System     |       | Source_System     |
                            +-------------------+       +-------------------+
```

| Component | Purpose | Example |
|-----------|---------|---------|
| **Hub** | Stores unique business keys | Customer number, Account number |
| **Link** | Stores relationships between hubs | Customer-Account relationship |
| **Satellite** | Stores descriptive attributes with history | Customer name, address, phone |

### Data Vault Example - Banking

```sql
-- HUB: Business keys only (customer_number is the business key)
CREATE TABLE hub_customer (
    hub_customer_hash_key VARCHAR(64) PRIMARY KEY,
    customer_number VARCHAR(20) NOT NULL,
    load_datetime TIMESTAMP,
    source_system VARCHAR(50)
);

-- LINK: Relationships between hubs (customer owns account)
CREATE TABLE link_account_customer (
    link_hash_key VARCHAR(64) PRIMARY KEY,
    hub_account_hash_key VARCHAR(64),
    hub_customer_hash_key VARCHAR(64),
    load_datetime TIMESTAMP,
    source_system VARCHAR(50)
);

-- SATELLITE: Descriptive attributes with full history
CREATE TABLE sat_customer_details (
    hub_customer_hash_key VARCHAR(64),
    load_datetime TIMESTAMP,
    load_end_datetime TIMESTAMP,
    hash_diff VARCHAR(64),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    address_line1 VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    source_system VARCHAR(50),
    PRIMARY KEY (hub_customer_hash_key, load_datetime)
);
```

### Data Vault vs Star Schema Comparison

| Aspect | Data Vault | Star Schema |
|--------|------------|-------------|
| **Purpose** | Enterprise data integration | Analytics and BI |
| **Structure** | Hubs, Links, Satellites | Facts and Dimensions |
| **Auditability** | Full history in satellites | SCD Type 2 for dimensions |
| **Query complexity** | Complex (many joins) | Simple (few joins) |
| **Load performance** | Parallel loading possible | May require locks |
| **Best for** | Multiple source systems | Single/consistent sources |

### Anchor Modeling

Similar to Data Vault but even more granular - separates each attribute into its own satellite. Use when you need maximum flexibility and have very complex attribute relationships.

### One Big Table (OBT)

A single denormalized table optimized for specific analytical queries. Think of it as a "flat file" in database form.

#### When to Use OBT

| Scenario | Use OBT? | Why |
|----------|----------|-----|
| Specific dashboard with fixed queries | ✅ Yes | Maximum query performance |
| Data science feature engineering | ✅ Yes | Easy to extract features |
| General-purpose analytics | ❌ No | Too rigid for diverse queries |
| Data with many relationships | ❌ No | Loses relationship context |

```sql
-- OBT for e-commerce analytics
CREATE TABLE obt_ecommerce_analytics (
    -- All dimensions flattened
    order_date DATE,
    order_month VARCHAR(7),
    order_quarter VARCHAR(2),
    order_year INT,
    customer_id VARCHAR(20),
    customer_name VARCHAR(100),
    customer_segment VARCHAR(20),
    customer_city VARCHAR(50),
    customer_country VARCHAR(50),
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    product_brand VARCHAR(50),
    -- Measures
    quantity INT,
    revenue DECIMAL(12,2),
    cost DECIMAL(12,2),
    profit DECIMAL(12,2),
    -- Calculated
    avg_order_value DECIMAL(12,2),
    days_since_first_order INT
);
```

---

## 5. Real-World Scenarios

### Scenario 1: Retail Data Model

```
Star Schema for Retail Analytics:

                    dim_date
                       |
dim_store -- fact_sales -- dim_product
                       |
                  dim_promotion
                       |
                  dim_customer

Key Metrics:
- Daily sales by store, product, promotion
- Customer purchase patterns
- Inventory turnover
- Promotion effectiveness
```

### Scenario 2: Healthcare Data Model

```
Star Schema for Patient Analytics:

                    dim_date
                       |
dim_provider -- fact_visits -- dim_patient
                       |
                  dim_department
                       |
                  dim_diagnosis

Key Metrics:
- Patient visit patterns
- Provider productivity
- Department utilization
- Diagnosis trends
```

---

## 6. Banking Examples

### Example 1: Core Banking Data Model

```sql
-- Hub: Customer (Data Vault style)
CREATE TABLE hub_customer (
    customer_hash_key VARCHAR(64) PRIMARY KEY,
    customer_number VARCHAR(20) UNIQUE NOT NULL,
    load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'CORE_BANKING'
);

-- Hub: Account
CREATE TABLE hub_account (
    account_hash_key VARCHAR(64) PRIMARY KEY,
    account_number VARCHAR(20) UNIQUE NOT NULL,
    load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'CORE_BANKING'
);

-- Link: Customer-Account relationship
CREATE TABLE link_customer_account (
    link_hash_key VARCHAR(64) PRIMARY KEY,
    hub_customer_hash_key VARCHAR(64) REFERENCES hub_customer,
    hub_account_hash_key VARCHAR(64) REFERENCES hub_account,
    relationship_type VARCHAR(20), -- Primary, Joint, Beneficiary
    load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Satellite: Account details
CREATE TABLE sat_account_details (
    hub_account_hash_key VARCHAR(64),
    load_datetime TIMESTAMP,
    load_end_datetime TIMESTAMP DEFAULT '9999-12-31',
    hash_diff VARCHAR(64),
    account_type VARCHAR(20), -- Savings, Checking, Fixed Deposit
    currency VARCHAR(3),
    open_date DATE,
    close_date DATE,
    status VARCHAR(20), -- Active, Closed, Frozen
    interest_rate DECIMAL(5,4),
    balance DECIMAL(15,2),
    source_system VARCHAR(50)
);

-- Satellite: Customer personal details
CREATE TABLE sat_customer_personal (
    hub_customer_hash_key VARCHAR(64),
    load_datetime TIMESTAMP,
    load_end_datetime TIMESTAMP DEFAULT '9999-12-31',
    hash_diff VARCHAR(64),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    date_of_birth DATE,
    gender VARCHAR(10),
    pan_number VARCHAR(20),
    aadhaar_number VARCHAR(20),
    email VARCHAR(100),
    phone VARCHAR(20),
    address_line1 VARCHAR(100),
    city VARCHAR(50),
    state VARCHAR(50),
    country VARCHAR(50),
    risk_category VARCHAR(20),
    source_system VARCHAR(50)
);
```

### Example 2: Credit Card Transaction Model

```sql
-- Fact: Credit card transactions
CREATE TABLE fact_credit_card_txn (
    txn_hash_key VARCHAR(64) PRIMARY KEY,
    hub_card_hash_key VARCHAR(64),
    hub_merchant_hash_key VARCHAR(64),
    hub_customer_hash_key VARCHAR(64),
    txn_timestamp TIMESTAMP,
    amount DECIMAL(12,2),
    currency VARCHAR(3),
    merchant_category VARCHAR(50),
    txn_type VARCHAR(20), -- Purchase, Payment, Refund, Cash Advance
    authorization_code VARCHAR(10),
    is_international BOOLEAN,
    risk_score DECIMAL(5,2),
    load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Satellite: Transaction location details
CREATE TABLE sat_txn_location (
    txn_hash_key VARCHAR(64),
    load_datetime TIMESTAMP,
    country VARCHAR(50),
    city VARCHAR(50),
    merchant_name VARCHAR(100),
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    source_system VARCHAR(50)
);
```

---

## 7. E-Commerce Examples

### Example 1: Product Catalog Model

```sql
-- Dimension: Product hierarchy
CREATE TABLE dim_product (
    product_key INT PRIMARY KEY,
    product_id VARCHAR(20) UNIQUE,
    product_name VARCHAR(200),
    sku VARCHAR(50),
    category_l1 VARCHAR(50),  -- Electronics
    category_l2 VARCHAR(50),  -- Mobile Phones
    category_l3 VARCHAR(50),  -- Smartphones
    brand VARCHAR(50),
    manufacturer VARCHAR(100),
    weight_kg DECIMAL(8,3),
    is_active BOOLEAN,
    launch_date DATE,
    effective_date DATE,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

-- Fact: Product inventory snapshots
CREATE TABLE fact_inventory_snapshot (
    product_key INT,
    warehouse_key INT,
    snapshot_date_key INT,
    stock_on_hand INT,
    stock_reserved INT,
    stock_available INT,
    reorder_quantity INT,
    days_of_supply INT,
    inventory_value DECIMAL(15,2)
);
```

### Example 2: E-Commerce Order Model

```sql
-- Fact: Order line items (transaction grain)
CREATE TABLE fact_order_items (
    order_item_key BIGINT PRIMARY KEY,
    order_key INT,
    order_date_key INT,
    customer_key INT,
    product_key INT,
    promotion_key INT,
    channel_key INT,
    quantity INT,
    unit_price DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    cost_of_goods DECIMAL(10,2),
    profit DECIMAL(10,2),
    shipping_cost DECIMAL(8,2),
    is_returned BOOLEAN,
    return_reason VARCHAR(50)
);

-- Dimension: Sales channel
CREATE TABLE dim_channel (
    channel_key INT PRIMARY KEY,
    channel_name VARCHAR(50),  -- Web, Mobile, Store, Marketplace
    channel_type VARCHAR(20),  -- Online, Offline
    platform VARCHAR(50),      -- iOS, Android, Desktop
    is_active BOOLEAN
);

-- Dimension: Promotion
CREATE TABLE dim_promotion (
    promotion_key INT PRIMARY KEY,
    promotion_id VARCHAR(20),
    promotion_name VARCHAR(100),
    discount_type VARCHAR(20),  -- Percentage, Fixed, BOGO
    discount_value DECIMAL(10,2),
    start_date DATE,
    end_date DATE,
    is_active BOOLEAN
);
```

---

## Choosing the Right Modeling Approach

### Decision Matrix

| Scenario | Recommended Approach | Why |
|----------|---------------------|-----|
| **OLTP system** (e-commerce, banking) | 3NF Normalization | Data integrity, fast writes |
| **Data warehouse for BI** | Star Schema | Simple queries, fast analytics |
| **Enterprise DW with many sources** | Data Vault | Flexibility, auditability |
| **Real-time analytics dashboard** | OBT or Star Schema | Maximum query performance |
| **Data lake curated layer** | Star Schema or OBT | Balance of flexibility and performance |
| **ML feature store** | OBT | Easy feature extraction |

### Modeling Approach Comparison

| Aspect | 3NF | Star Schema | Data Vault | OBT |
|--------|-----|-------------|------------|-----|
| **Redundancy** | Low | High | Low | Very High |
| **Query Performance** | Slow (many joins) | Fast (few joins) | Slow (complex) | Fastest |
| **Write Performance** | Fast | Slow | Fast | Slow |
| **Flexibility** | Medium | Low | High | Very Low |
| **Auditability** | Low | Medium | High | Low |
| **Best For** | OLTP | BI/Analytics | Enterprise DW | Specific analytics |

### Real-World Implementation

Most modern data platforms use a **layered approach**:

```
Layer 1: Raw Data (Schema-on-read)
    ↓
Layer 2: Data Vault (Enterprise integration, auditability)
    ↓
Layer 3: Star Schema / OBT (Analytics, BI consumption)
```

This gives you the best of all worlds: flexibility and auditability in the enterprise layer, performance in the analytics layer.

---

## 8. Hands-On Exercises

### Exercise 1: Design a Star Schema (Conceptual)
```sql
-- Task: Design a star schema for a hotel booking system

-- Dimension: Hotel
CREATE TABLE dim_hotel (
    hotel_key INT PRIMARY KEY,
    hotel_id VARCHAR(20) UNIQUE,
    hotel_name VARCHAR(100),
    city VARCHAR(50),
    country VARCHAR(50),
    star_rating INT,
    brand VARCHAR(50)
);

-- Dimension: Guest
CREATE TABLE dim_guest (
    guest_key INT PRIMARY KEY,
    guest_id VARCHAR(20) UNIQUE,
    guest_name VARCHAR(100),
    email VARCHAR(100),
    membership_tier VARCHAR(20),  -- Gold, Silver, Bronze
    country VARCHAR(50)
);

-- Dimension: Room Type
CREATE TABLE dim_room_type (
    room_type_key INT PRIMARY KEY,
    room_type_id VARCHAR(20) UNIQUE,
    room_type_name VARCHAR(50),  -- Standard, Deluxe, Suite
    base_rate DECIMAL(10,2)
);

-- Fact: Bookings
CREATE TABLE fact_bookings (
    booking_key BIGINT PRIMARY KEY,
    date_key INT,
    hotel_key INT,
    guest_key INT,
    room_type_key INT,
    nights INT,
    room_rate DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    discount_amount DECIMAL(10,2),
    is_cancelled BOOLEAN
);
```

### Exercise 2: Implement Data Vault (SQL)
```sql
-- Task: Convert star schema to Data Vault for a banking system

-- Hub: Customer
CREATE TABLE hub_customer (
    hub_customer_hk VARCHAR(64) PRIMARY KEY,
    customer_number VARCHAR(20) NOT NULL,
    load_dts TIMESTAMP,
    record_source VARCHAR(50)
);

-- Hub: Account
CREATE TABLE hub_account (
    hub_account_hk VARCHAR(64) PRIMARY KEY,
    account_number VARCHAR(20) NOT NULL,
    load_dts TIMESTAMP,
    record_source VARCHAR(50)
);

-- Link: Customer-Account
CREATE TABLE link_customer_account (
    link_hk VARCHAR(64) PRIMARY KEY,
    hub_customer_hk VARCHAR(64),
    hub_account_hk VARCHAR(64),
    load_dts TIMESTAMP,
    record_source VARCHAR(50)
);

-- Satellite: Customer Details
CREATE TABLE sat_customer_details (
    hub_customer_hk VARCHAR(64),
    load_dts TIMESTAMP,
    load_end_dts TIMESTAMP DEFAULT '9999-12-31',
    hash_diff VARCHAR(64),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    risk_rating VARCHAR(20),
    record_source VARCHAR(50),
    PRIMARY KEY (hub_customer_hk, load_dts)
);

-- Generate hash keys
INSERT INTO hub_customer (hub_customer_hk, customer_number, load_dts, record_source)
SELECT 
    MD5(customer_id),
    customer_id,
    CURRENT_TIMESTAMP,
    'CORE_BANKING'
FROM source_customers;
```

### Exercise 3: Implement SCD Type 2 (SQL)
```sql
-- Task: Track product price changes over time

-- Create dimension with SCD columns
CREATE TABLE dim_product_scd (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20),
    product_name VARCHAR(100),
    category VARCHAR(50),
    unit_price DECIMAL(10,2),  -- Tracked attribute
    effective_date DATE,
    expiry_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

-- Procedure to handle price changes
CREATE OR REPLACE PROCEDURE update_product_price(
    p_product_id VARCHAR(20),
    p_new_price DECIMAL(10,2)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_current_price DECIMAL(10,2);
BEGIN
    -- Get current price
    SELECT unit_price INTO v_current_price
    FROM dim_product_scd
    WHERE product_id = p_product_id AND is_current = TRUE;
    
    -- Only update if price changed
    IF v_current_price != p_new_price THEN
        -- Expire current record
        UPDATE dim_product_scd
        SET expiry_date = CURRENT_DATE - 1,
            is_current = FALSE
        WHERE product_id = p_product_id AND is_current = TRUE;
        
        -- Insert new version
        INSERT INTO dim_product_scd (
            product_id, product_name, category, unit_price,
            effective_date, expiry_date, is_current
        )
        SELECT 
            product_id, product_name, category, p_new_price,
            CURRENT_DATE, '9999-12-31', TRUE
        FROM dim_product_scd
        WHERE product_id = p_product_id AND is_current = FALSE
        ORDER BY expiry_date DESC
        LIMIT 1;
    END IF;
END;
$$;

-- Test
CALL update_product_price('PROD001', 29.99);

-- Query price history
SELECT product_id, unit_price, effective_date, expiry_date
FROM dim_product_scd
WHERE product_id = 'PROD001'
ORDER BY effective_date;
```

### Exercise 4: Create an Index Strategy (SQL)
```sql
-- Task: Optimize a large fact table for common queries

-- Analyze common query patterns
-- 1. Daily sales by product category
-- 2. Monthly revenue by region
-- 3. Customer purchase history

-- Create appropriate indexes

-- Partition by date for time-range queries
CREATE TABLE fact_sales_large (
    sale_id BIGINT,
    date_key INT,
    customer_key INT,
    product_key INT,
    region_id INT,
    revenue DECIMAL(12,2)
) PARTITION BY RANGE (date_key);

-- Create partitions
CREATE TABLE fact_sales_2024 PARTITION OF fact_sales_large
    FOR VALUES FROM (20240101) TO (20250101);

-- Bitmap index for low-cardinality region
CREATE INDEX idx_sales_region ON fact_sales_large(region_id);

-- Composite index for product queries
CREATE INDEX idx_sales_product_date ON fact_sales_large(product_key, date_key);

-- Analyze query performance
EXPLAIN ANALYZE
SELECT 
    p.category,
    SUM(f.revenue)
FROM fact_sales_large f
JOIN dim_product p ON f.product_key = p.product_key
WHERE f.date_key BETWEEN 20240101 AND 20240131
GROUP BY p.category;
```

### Exercise 5: dbt Model with Tests (YAML + SQL)
```yaml
-- models/staging/schema.yml
version: 2

models:
  - name: stg_products
    description: "Staged product data from source system"
    columns:
      - name: product_id
        description: "Unique product identifier"
        tests:
          - unique
          - not_null
      - name: product_name
        description: "Product display name"
        tests:
          - not_null
      - name: category
        description: "Product category"
        tests:
          - not_null
          - accepted_values:
              values: ['Electronics', 'Clothing', 'Food', 'Home', 'Sports']
      - name: unit_price
        description: "Current unit price"
        tests:
          - not_null
          - dbt_utils.expression_is_true:
              expression: ">= 0"
```

```sql
-- models/marts/dim_products.sql
WITH source AS (
    SELECT * FROM {{ ref('stg_products') }}
),

renamed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
        product_id,
        product_name,
        category,
        unit_price,
        _loaded_at AS loaded_at
    FROM source
)

SELECT * FROM renamed
```

---

## 9. Interview Questions

### Q1: What is the difference between normalization and denormalization?

**Answer:** 

**Normalization** (3NF) eliminates data redundancy by splitting data into related tables. It's ideal for OLTP systems where data integrity and write performance matter. 

**Denormalization** deliberately adds redundancy to optimize read performance by reducing joins. It's ideal for OLAP/warehouse systems where complex analytical queries dominate. The trade-off: normalization saves storage and ensures consistency, denormalization improves query performance at the cost of storage and update complexity.

### Q2: Explain Data Vault modeling. When would you use it?

**Answer:** Data Vault separates data into Hubs (business keys), Links (relationships), and Satellites (descriptive attributes with full history). 

Use it when: building enterprise data warehouses needing full auditability, integrating data from many source systems, requiring parallel loading without locks, or when source systems change frequently. 

It's more flexible than dimensional modeling but harder to query directly (needs views/layers on top).

### Q3: What is the difference between a composite key and a surrogate key?

**Answer:** 

A **composite key** uses multiple business columns as the primary key (e.g., order_id + product_id). 

A **surrogate key** is an artificially generated integer (auto-increment or hash) used as the primary key. 

Surrogate keys are preferred in data warehouses because: they're smaller (integer vs multiple columns), immune to source system changes, enable SCD tracking, and simplify joins. 

Composite natural keys are better for operational systems where the business key is stable.

### Q4: What indexing strategy would you use for a fact table with billions of rows?

**Answer:** 

I'd use a combination: 

**Partitioning** by date (range partition) to enable partition pruning for date-range queries. 

**Clustering/Sort keys** on frequently filtered columns (e.g., customer_key, product_key) to group related data physically. 

**Bitmap indexes** on low-cardinality foreign keys (region, status) for analytical WHERE clauses. 

Avoid B-tree indexes on high-cardinality columns in fact tables. For columnar stores (Redshift, BigQuery), use distribution keys and sort keys instead of traditional indexes.

### Q5: How do you handle schema evolution in a data warehouse?

**Answer:** Several strategies: 

1) **Additive changes** (new columns): Add column with default value, existing queries unaffected. 

2) **Column type changes**: Create new column, backfill from old, switch queries, drop old. 

3) **Column removal**: Deprecate first, stop loading, then drop after consumers migrate. 

4) **Use schema registry** for Avro/Parquet with schema evolution support. 

5) **Version your schemas** and maintain backward compatibility. 

6) **For Data Vault**: New satellites handle schema changes naturally without affecting existing ones.

### Q6: When would you use Data Vault vs Star Schema?

**Answer:**
**Data Vault** is best for:
- Enterprise data warehouses with multiple source systems
- Need for full auditability and traceability
- Frequently changing source systems
- Regulatory compliance requirements (banking, healthcare)

**Star Schema** is best for:
- Analytics and BI workloads
- Faster time-to-insight
- Simpler query patterns
- Single or few source systems

In practice, many organizations use **both**: Data Vault in the enterprise layer, Star Schema in the presentation/analytics layer.

---

## Summary Checklist

### Conceptual & Logical Modeling
- [ ] Understand ERD notation and Crow's Foot symbols
- [ ] Can identify business processes and define grain
- [ ] Know 3NF rules and when to normalize
- [ ] Understand denormalization for analytics

### Physical Modeling
- [ ] Know indexing strategies (B-Tree, Bitmap, Composite, Covering)
- [ ] Understand partitioning (Range, Hash) and benefits
- [ ] Know compression algorithms and trade-offs

### Advanced Patterns
- [ ] Can design Data Vault (Hubs, Links, Satellites)
- [ ] Understand One Big Table (OBT) use cases
- [ ] Know when to use Anchor Modeling

### Modern Tools
- [ ] Familiar with ERD tools (dbdiagram.io, DBeaver, Lucidchart)
- [ ] Understand dbt for SQL transformations
- [ ] Know schema versioning tools (Liquibase, Flyway)

### Practical Skills
- [ ] Can design star schemas from business requirements
- [ ] Implement SCD Type 2 for dimension tracking
- [ ] Create appropriate indexes for query optimization
- [ ] Build dbt models with tests and documentation

---

*Next Section: [04 - SQL Mastery](../04-SQL-Mastery/README.md)*
