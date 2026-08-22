# 🏦 dbt Project - Banking Data Transformations

> **SQL-based data transformations for the Banking Data Lakehouse**

---

## 📋 Table of Contents

1. [Overview](#1-overview)
2. [Project Structure](#2-project-structure)
3. [Understanding the Layers](#3-understanding-the-layers)
4. [Essential Configuration Files](#4-essential-configuration-files)
5. [Model Types Explained](#5-model-types-explained)
6. [Data Flow](#6-data-flow)
7. [Running the Project](#7-running-the-project)
8. [Testing & Documentation](#8-testing--documentation)

---

## 1. Overview

This dbt project transforms raw banking data from the **Bronze layer** (source systems) through the **Silver layer** (cleansed) to the **Gold layer** (business-ready).

### What is dbt?

**dbt (data build tool)** is a SQL-based transformation tool that:
- Transforms data **after** it's loaded into your data warehouse
- Uses **SQL SELECT statements** (no INSERT/UPDATE)
- Provides **testing** and **documentation** out of the box
- Follows **software engineering best practices** (version control, CI/CD)

### Why dbt for Banking?

| Benefit | Banking Use Case |
|---------|------------------|
| **Data Quality** | Test customer IDs are unique, amounts are positive |
| **Documentation** | Auto-generate data dictionary for auditors |
| **Version Control** | Track changes to transformation logic |
| **Modularity** | Reusable SQL components across models |
| **Lineage** | Understand how data flows from source to report |

---

## 2. Project Structure

```
dbt/
├── dbt_project.yml                    # Project configuration
├── README.md                          # This file
│
└── models/
    ├── sources.yml                    # Source table definitions (Bronze)
    │
    ├── staging/                       # LAYER 1: Staging (Bronze → Silver)
    │   ├── schema.yml                 # Tests & documentation
    │   ├── stg_customers.sql          # Clean customer data
    │   ├── stg_accounts.sql           # Clean account data
    │   ├── stg_transactions.sql       # Clean transaction data
    │   ├── stg_cards.sql              # Clean credit card data
    │   └── stg_loans.sql              # Clean loan data
    │
    ├── intermediate/                  # LAYER 2: Intermediate (Silver processing)
    │   ├── int_customer_accounts.sql  # Aggregate accounts per customer
    │   ├── int_customer_cards.sql     # Aggregate cards per customer
    │   └── int_customer_loans.sql     # Aggregate loans per customer
    │
    └── marts/                         # LAYER 3: Marts (Silver → Gold)
        ├── schema.yml                 # Tests & documentation
        ├── mart_customer_360.sql      # Customer 360° view
        ├── mart_daily_transactions.sql # Daily transaction summary
        └── mart_credit_risk.sql       # Credit risk dashboard
```

---

## 3. Understanding the Layers

### The Medallion Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    DBT TRANSFORMATION LAYERS                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  SOURCES (Bronze Layer)                                         │    │
│  │  Raw data from source systems (Oracle, Mainframe, SQL Server)  │    │
│  │  → Defined in: sources.yml                                      │    │
│  │  → Example: raw.customers, raw.accounts, raw.transactions      │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                      │                                 │
│                                      ▼                                 │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  STAGING (Silver Layer - Part 1)                                │    │
│  │  Clean, validate, and standardize raw data                      │    │
│  │  → 1:1 mapping with source tables                               │    │
│  │  → Example: stg_customers, stg_accounts, stg_transactions      │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                      │                                 │
│                                      ▼                                 │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  INTERMEDIATE (Silver Layer - Part 2)                           │    │
│  │  Business logic transformations and aggregations                 │    │
│  │  → Combines multiple staging models                              │    │
│  │  → Example: int_customer_accounts, int_customer_cards           │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                      │                                 │
│                                      ▼                                 │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  MARTS (Gold Layer)                                             │    │
│  │  Business-ready datasets for analytics and reporting            │    │
│  │  → Final output for BI tools and dashboards                     │    │
│  │  → Example: mart_customer_360, mart_daily_transactions          │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 4. Essential Configuration Files

### Do We Need Both `dbt_project.yml` and `sources.yml`?

**Yes! Both are required** - they serve different purposes.

| File | Purpose | What It Does |
|------|---------|-------------|
| **`dbt_project.yml`** | Project configuration | Defines project name, models, materializations |
| **`sources.yml`** | Source definitions | Defines raw tables in your database |

### What Each File Does

#### `dbt_project.yml` - The Brain

```yaml
# What: Project configuration
# Purpose: How dbt should run

name: 'banking_dwh'           # Project name
profile: 'banking'            # Database connection
models:                       # Model settings
  staging:
    +materialized: view       # How to store staging models
  marts:
    +schema: gold             # Output schema for marts
```

#### `sources.yml` - The Map

```yaml
# What: Source table definitions
# Purpose: Where data comes from

sources:
  - name: raw                 # Source namespace
    tables:
      - name: customers       # Table name
        description: "Customer master data"
        freshness:
          warn_after: 24h     # Alert if data > 24h old
```

### How They Work Together

```
┌─────────────────────────────────────────────────────────────────┐
│                    dbt PROJECT FILES                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────┐      ┌─────────────────────┐          │
│  │  dbt_project.yml    │      │  sources.yml         │          │
│  │  ─────────────────  │      │  ─────────────────── │          │
│  │  • Project name     │      │  • Source tables     │          │
│  │  • Model settings   │      │  • Freshness checks  │          │
│  │  • Materializations │      │  • Documentation     │          │
│  │  • Schema config    │      │  • Column definitions│          │
│  └──────────┬──────────┘      └──────────┬──────────┘          │
│             │                            │                      │
│             ▼                            ▼                      │
│  ┌─────────────────────┐      ┌─────────────────────┐          │
│  │  Staging Models     │      │  Source References   │          │
│  │  (stg_customers.sql)│      │  source('raw',       │          │
│  │                     │◄────►│    'customers')     │          │
│  └─────────────────────┘      └─────────────────────┘          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Without vs With

| Without `dbt_project.yml` | With `dbt_project.yml` |
|---------------------------|------------------------|
| ❌ `dbt run` fails | ✅ `dbt run` works |
| ❌ No schema configuration | ✅ Auto-creates schemas |
| ❌ No materialization control | ✅ Views, tables, ephemeral |
| ❌ No tag-based selection | ✅ `dbt run --select staging` |
| ❌ No documentation | ✅ `dbt docs generate` |

| Without `sources.yml` | With `sources.yml` |
|----------------------|-------------------|
| ❌ Can't reference raw tables | ✅ `source('raw', 'customers')` |
| ❌ No freshness checks | ✅ `dbt source freshness` |
| ❌ No source documentation | ✅ Auto-generated docs |
| ❌ Hardcoded table names | ✅ Clean namespace |

### Example Usage

**In `sources.yml`:**
```yaml
sources:
  - name: raw
    tables:
      - name: customers
```

**In `stg_customers.sql`:**
```sql
-- References the source defined in sources.yml
SELECT * FROM {{ source('raw', 'customers') }}
```

**In `dbt_project.yml`:**
```yaml
models:
  banking_dwh:
    staging:
      +materialized: view  # Applies to stg_customers.sql
```

---

## 5. Model Types Explained

### 5.1 Sources (`sources.yml`)

**What:** Defines the raw tables in your database that dbt reads from.

**Purpose:** 
- Document where data comes from
- Enable freshness checks
- Provide a clean namespace (`source('raw', 'customers')` instead of `raw.customers`)

**Example:**
```yaml
sources:
  - name: raw
    tables:
      - name: customers
        description: "Customer master data from Core Banking"
        freshness:
          warn_after: {count: 24, period: hour}
```

**When to use:** Always - every dbt project should have sources defined.

---

### 5.2 Staging Models (`staging/`)

**What:** Clean and transform raw data from source systems.

**Purpose:**
- **1:1 mapping** with source tables (one staging model per source table)
- **Clean** data (trim whitespace, standardize formats)
- **Validate** data (remove nulls, check constraints)
- **Rename** columns to consistent naming conventions
- **Cast** data types (string → date, string → number)

**Materialization:** `view` (not stored, computed on query)

**Example from `stg_customers.sql`:**
```sql
-- Input: raw.customers (messy data)
-- Output: staging.stg_customers (clean data)

SELECT
    customer_id,
    TRIM(UPPER(customer_name)) AS customer_name,  -- Standardize
    CASE 
        WHEN UPPER(gender) IN ('M', 'MALE') THEN 'MALE'
        WHEN UPPER(gender) IN ('F', 'FEMALE') THEN 'FEMALE'
        ELSE 'OTHER'
    END AS gender,  -- Standardize
    CASE 
        WHEN email LIKE '%@%.%' THEN LOWER(TRIM(email))
        ELSE NULL
    END AS email  -- Validate
FROM source
WHERE customer_id IS NOT NULL  -- Remove invalid records
```

**When to use:** 
- For every source table you need
- When raw data needs cleaning
- When you want consistent naming across the project

**Best Practices:**
- ✅ One staging model per source table
- ✅ Use `stg_` prefix
- ✅ Don't add business logic here (keep it simple)
- ✅ Always test for uniqueness and not-null on primary keys

---

### 5.3 Intermediate Models (`intermediate/`)

**What:** Apply business logic and combine multiple staging models.

**Purpose:**
- **Aggregate** data (count, sum, average per customer)
- **Combine** multiple staging models (customers + accounts)
- **Calculate** derived fields (risk scores, segments)
- **Filter** for specific business rules

**Materialization:** `ephemeral` (not stored, inlined into downstream models)

**Example from `int_customer_accounts.sql`:**
```sql
-- Input: staging.stg_accounts (individual accounts)
-- Output: intermediate.int_customer_accounts (aggregated per customer)

SELECT
    customer_id,
    COUNT(*) AS total_accounts,
    SUM(current_balance) AS total_balance
FROM staging.stg_accounts
WHERE status_standardized = 'ACTIVE'
GROUP BY customer_id
```

**When to use:**
- When you need to aggregate data before the mart layer
- When business logic is complex and should be broken down
- When multiple marts need the same intermediate calculation

**Best Practices:**
- ✅ Use `int_` prefix
- ✅ Keep models focused on one transformation
- ✅ Use `ephemeral` materialization (no storage cost)
- ✅ Document business logic in comments

---

### 5.4 Mart Models (`marts/`)

**What:** Business-ready datasets for analytics and reporting.

**Purpose:**
- **Final output** for BI tools (Power BI, Tableau, Dremio)
- **Combine** intermediate models into wide tables
- **Add** business metrics and KPIs
- **Optimize** for query performance

**Materialization:** `view` or `table` (stored for performance)

**Example from `mart_customer_360.sql`:**
```sql
-- Input: staging.stg_customers + intermediate.int_customer_accounts + int_customer_cards + int_customer_loans
-- Output: marts.mart_customer_360 (complete customer view)

SELECT
    c.customer_id,
    c.customer_name,
    COALESCE(a.total_accounts, 0) AS total_accounts,
    COALESCE(a.total_balance, 0) AS total_balance,
    COALESCE(cr.total_cards, 0) AS total_cards,
    COALESCE(cr.total_credit_used, 0) AS total_card_outstanding,
    COALESCE(l.total_loans, 0) AS total_loans,
    COALESCE(l.total_loan_outstanding, 0) AS total_loan_outstanding,
    
    -- Business metrics
    (COALESCE(a.total_balance, 0) 
     + COALESCE(cr.total_card_limit, 0) 
     - COALESCE(l.total_loan_outstanding, 0)) AS net_relationship_value,
    
    -- Customer segmentation
    CASE 
        WHEN net_relationship_value >= 10000000000 THEN 'PLATINUM'
        WHEN net_relationship_value >= 5000000000 THEN 'GOLD'
        WHEN net_relationship_value >= 1000000000 THEN 'SILVER'
        ELSE 'STANDARD'
    END AS customer_segment
    
FROM staging.stg_customers c
LEFT JOIN intermediate.int_customer_accounts a ON c.customer_id = a.customer_id
LEFT JOIN intermediate.int_customer_cards cr ON c.customer_id = cr.customer_id
LEFT JOIN intermediate.int_customer_loans l ON c.customer_id = l.customer_id
```

**When to use:**
- For final datasets consumed by BI tools
- When you need to join multiple intermediate models
- When business users need self-service access

**Best Practices:**
- ✅ Use `mart_` prefix
- ✅ Always use `COALESCE` for optional joins
- ✅ Add business metrics and KPIs
- ✅ Document all columns
- ✅ Test for data quality

---

## 6. Data Flow

### Visual Data Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA FLOW DIAGRAM                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  SOURCE SYSTEMS                 dbt MODELS              OUTPUT           │
│  ═══════════════                ═══════════             ══════           │
│                                                                         │
│  ┌──────────────┐                                                      │
│  │   Oracle     │──────┐                                               │
│  │  (Core Bank) │      │      ┌─────────────┐                         │
│  └──────────────┘      ├─────►│stg_customers │──┐                     │
│                        │      └─────────────┘  │                     │
│  ┌──────────────┐      │      ┌─────────────┐  │    ┌────────────┐  │
│  │  PostgreSQL  │──────┤─────►│stg_accounts  │──┼───►│int_customer│  │
│  │  (Accounts)  │      │      └─────────────┘  │    │_accounts   │──┤
│  └──────────────┘      │      ┌─────────────┐  │    └────────────┘  │
│                        │      │stg_txn      │  │    ┌────────────┐  │
│  ┌──────────────┐      ├─────►│(transactions)│──┼───►│mart_       │  │
│  │  SQL Server  │──────┤      └─────────────┘  │    │customer_360│  │
│  │   (Loans)    │      │      ┌─────────────┐  │    └────────────┘  │
│  └──────────────┘      │      │stg_cards    │──┤                     │
│                        ├─────►└─────────────┘  │                     │
│  ┌──────────────┐      │      ┌─────────────┐  │                     │
│  │   Mainframe  │──────┘─────►│stg_loans    │──┘                     │
│  │   (Cards)    │             └─────────────┘                         │
│  └──────────────┘                                                      │
│                                                                         │
│                                      │                                 │
│                                      ▼                                 │
│                          ┌─────────────────────┐                       │
│                          │  DREMIO / POWER BI  │                       │
│                          │  (BI Dashboards)    │                       │
│                          └─────────────────────┘                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Table Relationships

```
stg_customers (1) ──────┬──── (N) stg_accounts
                        │
                        ├──── (N) stg_cards
                        │
                        └──── (N) stg_loans

stg_accounts (1) ──────── (N) stg_transactions

int_customer_accounts ────┐
int_customer_cards ───────┼───► mart_customer_360
int_customer_loans ───────┘
```

---

## 7. Running the Project

### Prerequisites

```bash
# Install dbt
pip install dbt-postgres  # or dbt-snowflake, dbt-bigquery, etc.

# Initialize dbt project
dbt init banking_dwh
```

### Commands

```bash
# Install dependencies
dbt deps

# Run all models
dbt run

# Run specific model
dbt run --select stg_customers

# Run models downstream of stg_customers
dbt run --select +stg_customers

# Run all tests
dbt test

# Run specific test
dbt test --select stg_customers

# Generate documentation
dbt docs generate

# Serve documentation
dbt docs serve

# Run freshness checks
dbt source freshness
```

### Model Materialization

| Layer | Materialization | Why |
|-------|-----------------|-----|
| **Staging** | `view` | No storage cost, always fresh |
| **Intermediate** | `ephemeral` | Inlined, no storage cost |
| **Marts** | `view` or `table` | `view` for freshness, `table` for performance |

---

## 8. Testing & Documentation

### Data Quality Tests

```yaml
# In schema.yml
models:
  - name: stg_customers
    columns:
      - name: customer_id
        tests:
          - unique          # No duplicate customer IDs
          - not_null        # No null customer IDs
      - name: email
        tests:
          - not_null:
              config:
                where: "email IS NOT NULL"  # Only test non-null emails
```

### Documentation

```yaml
# In schema.yml
models:
  - name: mart_customer_360
    description: "Complete customer view across all banking products"
    columns:
      - name: customer_id
        description: "Unique customer identifier"
      - name: net_relationship_value
        description: "Net relationship value = Assets - Liabilities"
```

### Generate Docs

```bash
# Generate and serve documentation
dbt docs generate
dbt docs serve --port 8080

# Access at http://localhost:8080
```

---

## 📚 Key Concepts

| Concept | Definition | Example |
|---------|------------|---------|
| **Model** | A SQL SELECT statement | `stg_customers.sql` |
| **Source** | Raw table in database | `raw.customers` |
| **Ref** | Reference to another model | `{{ ref('stg_customers') }}` |
| **Materialization** | How model is stored | `view`, `table`, `ephemeral` |
| **Test** | Data quality check | `unique`, `not_null`, `accepted_values` |
| **Schema** | Model configuration | `schema.yml` |
| **Lineage** | Data flow diagram | `dbt docs generate` |

---

## 🎯 Best Practices Summary

### Staging Models
- ✅ One model per source table
- ✅ Use `stg_` prefix
- ✅ Clean and validate data
- ✅ Standardize naming conventions
- ❌ Don't add business logic
- ❌ Don't join with other tables

### Intermediate Models
- ✅ Use `int_` prefix
- ✅ Use `ephemeral` materialization
- ✅ Break complex logic into steps
- ✅ Document business rules
- ❌ Don't skip testing

### Mart Models
- ✅ Use `mart_` prefix
- ✅ Use `COALESCE` for optional joins
- ✅ Add business metrics
- ✅ Document all columns
- ❌ Don't put business logic in BI tools

---

*Built with ❤️ for Data Engineers learning dbt and Banking Data Architecture*
