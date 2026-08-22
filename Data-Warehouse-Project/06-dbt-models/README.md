# 06 - dbt Models (Data Warehouse)

## Overview

This folder contains **dbt (data build tool)** models for transforming data in the Banking Data Warehouse. dbt uses SQL to transform data in your warehouse following software engineering best practices.

---

## Table of Contents

1. [What is dbt?](#1-what-is-dbt)
2. [Project Structure](#2-project-structure)
3. [Model Layers](#3-model-layers)
4. [How Models Work](#4-how-models-work)
5. [Configuration Files](#5-configuration-files)
6. [Running dbt](#6-running-dbt)
7. [Best Practices](#7-best-practices)

---

## 1. What is dbt?

**dbt (data build tool)** transforms data in your warehouse using SQL.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         WHAT IS dbt?                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Traditional ETL:                                                           │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐            │
│  │ Extract  │───►│Transform │───►│  Load    │───►│  Data    │            │
│  │ (Python) │    │ (Python) │    │ (Python) │    │ Warehouse│            │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘            │
│       ▲              ▲              ▲                                      │
│       │              │              │                                      │
│   Complex code   Complex code   Complex code                               │
│                                                                             │
│  dbt Approach:                                                              │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐            │
│  │ Extract  │───►│  Load    │───►│ dbt SQL  │───►│  Data    │            │
│  │ (Airflow)│    │ (Airflow)│    │ (Simple) │    │ Warehouse│            │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘            │
│       ▲                              ▲                                      │
│       │                              │                                      │
│   Airflow does it             Just write SQL!                               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Why dbt?

| Feature | Benefit |
|---------|---------|
| **SQL-based** | Data analysts can write transformations |
| **Version control** | Git-friendly, track changes |
| **Testing** | Built-in data quality tests |
| **Documentation** | Auto-generated docs |
| **Modularity** | Reusable models |
| **Incremental** | Only process new data |

---

## 2. Project Structure

```
06-dbt-models/
│
├── dbt_project.yml              # Project configuration
│
└── models/
    ├── staging/                 # Layer 1: Raw data cleaning
    │   ├── sources.yml          # Source definitions
    │   ├── stg_customers.sql    # Clean customers
    │   ├── stg_accounts.sql     # Clean accounts
    │   └── stg_transactions.sql # Clean transactions
    │
    ├── intermediate/            # Layer 2: Business logic
    │   └── int_customer_accounts.sql  # Join customers + accounts
    │
    └── marts/                   # Layer 3: Business-ready tables
        ├── dim_customer.sql     # Customer dimension (SCD Type 2)
        └── fact_transactions.sql # Transaction facts
```

---

## 3. Model Layers

### Layer 1: Staging (Views)

**Purpose:** Clean and standardize raw data from source systems.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STAGING LAYER                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Input: Raw data from staging tables                                        │
│  Output: Clean, standardized views                                          │
│                                                                             │
│  ┌─────────────────────┐    ┌─────────────────────┐                        │
│  │  stg_customers      │    │  stg_accounts       │                        │
│  │  ─────────────────  │    │  ─────────────────  │                        │
│  │  • Trim whitespace  │    │  • Standardize types │                        │
│  │  • Uppercase codes  │    │  • Clean balances    │                        │
│  │  • Validate email   │    │  • Add metadata      │                        │
│  │  • Add metadata     │    │                      │                        │
│  └─────────────────────┘    └─────────────────────┘                        │
│                                                                             │
│  Materialization: VIEW (lightweight, fast refresh)                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Example: stg_customers.sql**

```sql
with source as (
    select * from {{ source('staging', 'stg_customers') }}
),

cleaned as (
    select
        customer_id::varchar as customer_id,
        upper(trim(full_name)) as customer_name,
        lower(trim(email)) as email,
        phone as phone_number,
        upper(customer_type) as customer_type,
        upper(kyc_status) as kyc_status,
        current_timestamp as dbt_loaded_at
    from source
    where customer_id is not null
)

select * from cleaned
```

---

### Layer 2: Intermediate (Ephemeral)

**Purpose:** Business logic, joins, calculations.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       INTERMEDIATE LAYER                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Input: Staging models                                                      │
│  Output: Intermediate tables (not materialized)                            │
│                                                                             │
│  ┌─────────────────────┐                                                   │
│  │  int_customer_accounts                                                   │
│  │  ─────────────────────                                                   │
│  │  • Join customers + accounts                                             │
│  │  • Calculate aggregations                                                │
│  │  • Add business logic                                                    │
│  └─────────────────────┘                                                   │
│                                                                             │
│  Materialization: EPHEMERAL (computed on-the-fly, no table created)        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Example: int_customer_accounts.sql**

```sql
with customers as (
    select * from {{ ref('stg_customers') }}
),

accounts as (
    select * from {{ ref('stg_accounts') }}
),

customer_accounts as (
    select
        c.customer_id,
        c.customer_name,
        c.email,
        c.customer_type,
        c.kyc_status,
        count(distinct a.account_id) as total_accounts,
        sum(a.balance) as total_balance,
        case
            when sum(a.balance) >= 1000000000 then 'PLATINUM'
            when sum(a.balance) >= 500000000 then 'GOLD'
            when sum(a.balance) >= 100000000 then 'SILVER'
            else 'STANDARD'
        end as customer_segment
    from customers c
    left join accounts a on c.customer_id = a.customer_id
    group by c.customer_id, c.customer_name, c.email, c.customer_type, c.kyc_status
)

select * from customer_accounts
```

---

### Layer 3: Marts (Tables)

**Purpose:** Business-ready tables for reporting and analytics.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          MARTS LAYER                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Input: Intermediate models                                                 │
│  Output: Materialized tables optimized for queries                          │
│                                                                             │
│  ┌─────────────────────┐    ┌─────────────────────┐                        │
│  │  dim_customer       │    │  fact_transactions  │                        │
│  │  ─────────────────  │    │  ─────────────────  │                        │
│  │  • SCD Type 2       │    │  • Incremental      │                        │
│  │  • History tracking │    │  • Partitioned      │                        │
│  │  • Surrogate keys   │    │  • Indexed          │                        │
│  └─────────────────────┘    └─────────────────────┘                        │
│                                                                             │
│  Materialization: TABLE (materialized, query-optimized)                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. How Models Work

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         dbt DATA FLOW                                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Source Tables                dbt Models              Output Tables         │
│  ─────────────               ──────────              ──────────────         │
│                                                                             │
│  ┌─────────────┐                                                        │
│  │stg_customers│──┐                                                     │
│  └─────────────┘  │                                                     │
│                   ├──►┌──────────────────┐    ┌─────────────────┐        │
│  ┌─────────────┐  │   │int_customer_     │───►│dim_customer     │        │
│  │stg_accounts │──┘   │accounts          │    │(Gold Table)     │        │
│  └─────────────┘      └──────────────────┘    └─────────────────┘        │
│                                                                             │
│  ┌─────────────┐      ┌──────────────────┐    ┌─────────────────┐        │
│  │stg_         │─────►│fact_             │───►│fact_            │        │
│  │transactions │      │transactions      │    │transactions     │        │
│  └─────────────┘      └──────────────────┘    │(Gold Table)     │        │
│                                               └─────────────────┘        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### References

```sql
-- Reference another model
select * from {{ ref('stg_customers') }}

-- Reference a source
select * from {{ source('core_banking', 'customers') }}

-- Use variables
select * from {{ var('start_date') }}
```

---

## 5. Configuration Files

### dbt_project.yml

**Purpose:** Project-wide configuration.

```yaml
name: 'banking_dw'
version: '1.0.0'
profile: 'banking_dw'

models:
  banking_dw:
    staging:
      +materialized: view    # Staging = Views
    intermediate:
      +materialized: ephemeral  # Intermediate = No table
    marts:
      +materialized: table    # Marts = Tables
      +schema: gold           # Output to gold schema
```

### sources.yml

**Purpose:** Define source tables and freshness checks.

```yaml
sources:
  - name: core_banking
    database: core_banking
    tables:
      - name: customers
        loaded_at_field: updated_at
        freshness:
          warn_after: {count: 24, period: hour}
          error_after: {count: 48, period: hour}
```

---

## 6. Running dbt

### Prerequisites

```bash
# Install dbt
pip install dbt-postgres

# Initialize connection
dbt init banking_dw
```

### Commands

```bash
# Run all models
dbt run

# Run specific model
dbt run --select stg_customers

# Run staging only
dbt run --select staging

# Run marts only
dbt run --select marts

# Run tests
dbt test

# Generate docs
dbt docs generate
dbt docs serve

# Check freshness
dbt source freshness
```

### Output

```
$ dbt run

Running with dbt=1.7.0
Found 5 models, 3 tests, 1 source, 0 exposures, 0 metrics

15:30:00 | 1 of 5 START view model staging.stg_customers
15:30:01 | 1 of 5 OK created view model staging.stg_customers
15:30:01 | 2 of 5 START view model staging.stg_accounts
15:30:02 | 2 of 5 OK created view model staging.stg_accounts
15:30:02 | 3 of 5 START view model staging.stg_transactions
15:30:03 | 3 of 5 OK created view model staging.stg_transactions
15:30:03 | 4 of 5 START ephemeral model intermediate.int_customer_accounts
15:30:04 | 4 of 5 OK created ephemeral model intermediate.int_customer_accounts
15:30:04 | 5 of 1 START table model gold.dim_customer
15:30:05 | 5 of 1 OK created table model gold.dim_customer
15:30:05 | 6 of 1 START table model gold.fact_transactions
15:30:06 | 6 of 1 OK created table model gold.fact_transactions

Done. PASS=6 WARN=0 ERROR=0 SKIP=0
```

---

## 7. Best Practices

### Do's ✅

| Practice | Why |
|----------|-----|
| **Use staging for all sources** | Consistent naming, cleaning |
| **Keep models small** | Easier to test and debug |
| **Use ephemeral for intermediate** | No extra tables, faster |
| **Add tests to models** | Data quality assurance |
| **Document everything** | Auto-generated docs |
| **Use Git for version control** | Track changes |

### Don'ts ❌

| Practice | Why |
|----------|-----|
| **Don't skip staging** | Leads to inconsistent data |
| **Don't hardcode values** | Use variables instead |
| **Don't create huge models** | Hard to maintain |
| **Don't skip tests** | Data quality issues |
| **Don't ignore documentation** | Hard to understand later |

---

## Summary

| Layer | Materialization | Purpose |
|-------|-----------------|---------|
| **Staging** | View | Clean raw data |
| **Intermediate** | Ephemeral | Business logic |
| **Marts** | Table | Analytics-ready |

**dbt makes SQL transformations:**
- ✅ Version controlled
- ✅ Tested
- ✅ Documented
- ✅ Modular
- ✅ Maintainable

---

*Back to: [ETL Pipelines](../04-etl-pipelines/README.md)*
