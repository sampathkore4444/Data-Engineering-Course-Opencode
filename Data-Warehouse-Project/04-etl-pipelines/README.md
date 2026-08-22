# 🔄 ETL Pipelines - Banking Data Warehouse

> **Three approaches to data transformation: Airflow, dbt, and CDC**

---

## 📋 Table of Contents

1. [Overview](#1-overview)
2. [ETL Pipeline Comparison](#2-etl-pipeline-comparison)
3. [Pipeline 1: Apache Airflow](#3-pipeline-1-apache-airflow)
4. [Pipeline 2: dbt (Data Build Tool)](#4-pipeline-2-dbt-data-build-tool)
5. [Pipeline 3: CDC (Change Data Capture)](#5-pipeline-3-cdc-change-data-capture)
6. [Data Loading Patterns](#6-data-loading-patterns-batch-vs-incremental)
7. [When to Use Which Tool?](#7-when-to-use-which-tool)
8. [Recommended Approach](#8-recommended-approach)
9. [Running the Pipelines](#9-running-the-pipelines)

---

## 1. Overview

This project includes **3 different ETL pipeline approaches** for the banking Data Warehouse. Each approach has its own strengths and use cases.

### What is ETL?

| Term | Meaning | Example |
|------|---------|---------|
| **E**xtract | Get data from source systems | Read from PostgreSQL OLTP databases |
| **T**ransform | Clean, validate, aggregate | Remove duplicates, calculate totals |
| **L**oad | Write to target system | Save to Data Warehouse (star schema) |

### Why 3 Approaches?

| Reason | Explanation |
|--------|-------------|
| **Learning** | Understand different tools and when to use each |
| **Flexibility** | Real projects may use 1-2 approaches together |
| **Comparison** | See pros/cons of each approach |
| **Production** | Choose the right tool for your use case |

---

## 2. ETL Pipeline Comparison

### Quick Comparison Table

| Feature | Airflow | dbt | CDC |
|---------|---------|-----|-----|
| **Primary Use** | Orchestration | SQL Transformations | Real-time Sync |
| **Language** | Python | SQL | Python/SQL |
| **Best For** | Scheduling, dependencies | Analytics engineering | Live data updates |
| **Learning Curve** | Medium | Easy | Medium |
| **Latency** | Hours (batch) | Hours (batch) | Minutes (near real-time) |
| **Production Ready** | ✅ Yes | ✅ Yes | ✅ Yes |

### When to Use Each

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    ETL PIPELINE SELECTION GUIDE                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  USE AIRFLOW WHEN:                                                      │
│  ├── You need to schedule jobs (daily, hourly)                         │
│  ├── Jobs have dependencies (A must finish before B starts)            │
│  ├── You need to monitor and alert on failures                         │
│  ├── You're orchestrating multiple tools (dbt + CDC)                   │
│  └── You need retry logic and error handling                           │
│                                                                         │
│  USE dbt WHEN:                                                          │
│  ├── Transformations are mostly SQL (SELECT statements)                 │
│  ├── You want built-in testing and documentation                       │
│  ├── Multiple analysts need to understand the transformations          │
│  ├── You're building a data warehouse or analytics layer               │
│  └── You want version-controlled SQL transformations                   │
│                                                                         │
│  USE CDC WHEN:                                                          │
│  ├── You need real-time or near-real-time data                         │
│  ├── You want to minimize source system load                           │
│  ├── You need to capture all changes (INSERT, UPDATE, DELETE)          │
│  ├── You're building a real-time dashboard                             │
│  └── You need audit trail of all data changes                          │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Pipeline 1: Apache Airflow

### What is Airflow?

**Apache Airflow** is a workflow orchestration platform that programmatically author, schedule, and monitor workflows.

### Files in This Project

```
04-etl-pipelines/airflow/dags/
├── extract_source_data.py      # Extract from OLTP to Staging
├── load_dimensions.py          # Load dimension tables (SCD Type 1/2)
├── load_facts.py               # Load fact tables
└── cdc_pipeline.py             # Real-time CDC sync
```

### What Each DAG Does

| DAG | Purpose | Schedule | Inputs | Outputs |
|-----|---------|----------|--------|---------|
| `extract_source_data.py` | Extract from 3 OLTP databases | Daily (2 AM) | 7 source tables | 7 staging tables |
| `load_dimensions.py` | Load dimensions with SCD | Daily (3 AM) | Staging tables | 5 dimension tables |
| `load_facts.py` | Load fact tables | Daily (4 AM) | Staging + Dimensions | 3 fact tables |
| `cdc_pipeline.py` | Real-time CDC sync | Every 5 min | Source DBs (WAL) | Staging tables |

### How Airflow Works

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    AIRFLOW WORKFLOW                                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  BATCH PIPELINES (Daily)                                                │
│  ─────────────────────────                                              │
│                                                                         │
│  ┌─────────────────┐                                                   │
│  │extract_source_  │  Schedule: Daily 2 AM                             │
│  │data             │  Run: Extract from 3 OLTP databases               │
│  └────────┬────────┘                                                   │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐                                                   │
│  │load_dimensions  │  Schedule: Daily 3 AM                             │
│  │                 │  Run: Load 5 dimensions (SCD Type 1/2)            │
│  └────────┬────────┘                                                   │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐                                                   │
│  │load_facts       │  Schedule: Daily 4 AM                             │
│  │                 │  Run: Load 3 facts with surrogate keys            │
│  └─────────────────┘                                                   │
│                                                                         │
│  REAL-TIME PIPELINE (Continuous)                                        │
│  ────────────────────────────────                                       │
│                                                                         │
│  ┌─────────────────┐                                                   │
│  │cdc_realtime_    │  Schedule: Every 5 minutes                        │
│  │sync             │  Run: Capture and sync real-time changes          │
│  └─────────────────┘                                                   │
│                                                                         │
│  KEY FEATURES:                                                          │
│  • Automatic retries on failure                                         │
│  • Email alerts for failures                                            │
│  • Visual monitoring in Airflow UI                                      │
│  • Dependency management (Extract → Dimensions → Facts)                 │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Importance of Airflow

| Importance | Why |
|------------|-----|
| **Orchestration** | Coordinates all data pipelines |
| **Scheduling** | Automates daily/hourly runs |
| **Monitoring** | Track pipeline health and failures |
| **Dependencies** | Ensures correct execution order |
| **Retry Logic** | Automatically retries failed tasks |
| **Alerting** | Notifies team of issues |

### Airflow Code Example

```python
# extract_source_data.py - Extract data from OLTP sources

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

def extract_customers():
    """Extract customers from core_banking database"""
    source_hook = PostgresHook(postgres_conn_id='source_core_banking')
    dw_hook = PostgresHook(postgres_conn_id='data_warehouse')
    
    # Extract from source
    query = "SELECT * FROM customers"
    customers = source_hook.get_records(query)
    
    # Load to staging
    for customer in customers:
        dw_hook.run(
            """INSERT INTO staging.stg_customers 
               (customer_id, customer_name, customer_type, phone, email)
               VALUES (%s, %s, %s, %s, %s)
               ON CONFLICT (customer_id) DO NOTHING""",
            parameters=customer
        )

with DAG('extract_source_data', schedule_interval='0 2 * * *') as dag:
    extract = PythonOperator(
        task_id='extract_customers',
        python_callable=extract_customers
    )
```

---

## 4. Pipeline 2: dbt (Data Build Tool)

### What is dbt?

**dbt (data build tool)** is a SQL-based transformation tool that enables analysts and engineers to transform data in the warehouse using SQL SELECT statements.

### Files in This Project

```
04-etl-pipelines/dbt/
├── dbt_project.yml
└── models/
    ├── staging/
    │   ├── sources.yml
    │   ├── stg_customers.sql
    │   ├── stg_accounts.sql
    │   └── stg_transactions.sql
    └── intermediate/
        └── int_customer_accounts.sql

06-dbt-models/
├── dbt_project.yml
└── models/
    ├── staging/
    │   ├── sources.yml
    │   ├── stg_customers.sql
    │   ├── stg_accounts.sql
    │   └── stg_transactions.sql
    ├── intermediate/
    │   └── int_customer_accounts.sql
    └── marts/
        ├── dim_customer.sql
        └── fact_transactions.sql
```

### What Each Model Does

| Layer | Model | Purpose |
|-------|-------|---------|
| **Staging** | `stg_customers` | Clean and standardize customer data |
| **Staging** | `stg_accounts` | Clean and validate account data |
| **Staging** | `stg_transactions` | Clean and enrich transaction data |
| **Intermediate** | `int_customer_accounts` | Aggregate accounts per customer |
| **Mart** | `dim_customer` | Final customer dimension (SCD Type 2) |
| **Mart** | `fact_transactions` | Final transaction fact table |

### How dbt Works

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    dbt TRANSFORMATION FLOW                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐                                                   │
│  │  sources.yml    │  Define raw tables in database                    │
│  │  (Bronze)       │  → source('raw', 'customers')                    │
│  └────────┬────────┘                                                   │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐                                                   │
│  │  STAGING        │  Clean and validate (1:1 with source)            │
│  │  stg_customers  │  → Trim, standardize, validate                   │
│  │  stg_accounts   │  → Materialized as VIEW                          │
│  │  stg_txn        │                                                   │
│  └────────┬────────┘                                                   │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐                                                   │
│  │  INTERMEDIATE   │  Business logic transformations                   │
│  │  int_customer_  │  → Aggregate, calculate, combine                 │
│  │  int_accounts   │  → Materialized as EPHEMERAL                     │
│  └────────┬────────┘                                                   │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐                                                   │
│  │  MARTS          │  Business-ready datasets                          │
│  │  dim_customer   │  → Final output for BI tools                      │
│  │  fact_txn       │  → Materialized as TABLE                          │
│  └─────────────────┘                                                   │
│                                                                         │
│  KEY FEATURES:                                                          │
│  • SQL-only (no Python needed)                                          │
│  • Built-in testing (uniqueness, not-null)                              │
│  • Auto-generated documentation                                         │
│  • Version-controlled transformations                                   │
│  • Data lineage tracking                                                │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Importance of dbt

| Importance | Why |
|------------|-----|
| **Data Quality** | Built-in tests ensure data correctness |
| **Documentation** | Auto-generates data dictionary |
| **Version Control** | Track changes to SQL logic |
| **Modularity** | Reusable SQL components |
| **Lineage** | Understand data flow from source to report |
| **Collaboration** | Analysts can understand and modify SQL |

### dbt Code Example

```sql
-- stg_customers.sql - Clean customer data

WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),

cleaned AS (
    SELECT
        customer_id,
        TRIM(UPPER(customer_name)) AS customer_name,
        CASE 
            WHEN customer_type IN ('individual', 'personal') THEN 'Individual'
            WHEN customer_type IN ('corporate', 'business') THEN 'Corporate'
            ELSE 'Other'
        END AS customer_type,
        TRIM(phone) AS phone,
        LOWER(TRIM(email)) AS email,
        created_at,
        updated_at
    FROM source
    WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned
```

---

## 5. Pipeline 3: CDC (Change Data Capture)

### What is CDC?

**Change Data Capture (CDC)** captures changes made to data in source databases and streams them to downstream systems in real-time.

### Files in This Project

```
04-etl-pipelines/
├── airflow/dags/
│   └── cdc_pipeline.py              # CDC Airflow DAG
└── cdc/
    └── metadata/
        └── cdc_metadata.sql         # CDC metadata tables
```

### How CDC Works

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CDC DATA FLOW                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐         │
│  │  Source  │───►│ Debezium │───►│  Kafka   │───►│  DW      │         │
│  │  Database│    │  (CDC)   │    │  Topics  │    │ Staging  │         │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘         │
│                                                                         │
│  1. Application writes to database                                      │
│  2. Debezium reads database WAL (Write-Ahead Log)                       │
│  3. Change event published to Kafka topic                               │
│  4. CDC Pipeline consumes and writes to staging                         │
│                                                                         │
│  LATENCY: Seconds to minutes (near real-time)                           │
│                                                                         │
│  OPERATIONS CAPTURED:                                                   │
│  • INSERT - New record added                                            │
│  • UPDATE - Existing record modified                                    │
│  • DELETE - Record removed                                              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### CDC Metadata Tables

| Table | Purpose |
|-------|---------|
| `cdc_metadata.processed_lsn` | Track last processed Log Sequence Number |
| `cdc_metadata.sync_log` | Log all sync operations |
| `cdc_metadata.sync_errors` | Track sync errors |
| `cdc_metadata.cdc_config` | CDC configuration per table |
| `cdc_metadata.vw_cdc_monitoring` | Real-time monitoring view |
| `cdc_metadata.vw_cdc_performance` | Performance metrics view |

### Importance of CDC

| Importance | Why |
|------------|-----|
| **Real-time Sync** | Data synced within minutes, not hours |
| **Low Impact** | Reads WAL log, not database queries |
| **No Data Loss** | Captures all changes (INSERT, UPDATE, DELETE) |
| **Audit Trail** | Complete history of all changes |
| **Reduced Latency** | Near real-time dashboards possible |

### CDC Code Example

```python
# cdc_pipeline.py - Capture real-time changes

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def capture_customer_changes():
    """Capture changes from core_banking.customers"""
    source_hook = PostgresHook(postgres_conn_id='source_core_banking')
    dw_hook = PostgresHook(postgres_conn_id='data_warehouse')
    
    # Get last processed LSN
    last_lsn = get_last_lsn(dw_hook, 'customers')
    
    # Query CDC log
    query = """
        SELECT operation, customer_id, customer_name, ...
        FROM cdc.customers_log
        WHERE commit_timestamp > %s
        ORDER BY commit_timestamp;
    """
    
    changes = source_hook.get_records(query, parameters=(last_lsn,))
    
    for change in changes:
        if change[0] == 'INSERT':
            insert_customer(dw_hook, change[1:])
        elif change[0] == 'UPDATE':
            update_customer(dw_hook, change[1:])
        elif change[0] == 'DELETE':
            delete_customer(dw_hook, change[1:])
    
    # Update last LSN
    update_last_lsn(dw_hook, 'customers', changes[-1][-1])

with DAG('cdc_realtime_sync', schedule_interval='*/5 * * * *') as dag:
    cdc = PythonOperator(
        task_id='capture_customer_changes',
        python_callable=capture_customer_changes
    )
```

---

## 6. Data Loading Patterns (Batch vs Incremental)

### Overview

| Pattern | Description | Latency | Use Case |
|---------|-------------|---------|----------|
| **Batch** | Load all data at scheduled intervals | Hours | Daily reports, historical analytics |
| **Incremental** | Load only new/changed data | Minutes | Near-real-time dashboards |
| **CDC** | Capture changes automatically | Minutes | Real-time sync, audit trail |

### Batch Loading

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    BATCH LOADING PATTERN                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐         │
│  │  Source  │───►│  Extract │───►│Transform │───►│   Load   │         │
│  │  (OLTP) │    │ (All)    │    │ (All)    │    │ (All)    │         │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘         │
│                                                                         │
│  Schedule: Daily (2 AM)                                                 │
│  Data: ALL records (full load)                                          │
│  Latency: Hours                                                         │
│                                                                         │
│  EXAMPLE: Nightly extract from core_banking                             │
│  • Extract ALL customers, accounts, transactions                        │
│  • Transform and validate                                               │
│  • Load to staging tables                                               │
│                                                                         │
│  TOOL: Airflow DAGs                                                     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**When to Use Batch:**
- Data can tolerate hours of delay
- Simple to implement and maintain
- Large volumes that need bulk processing
- Historical data loads

### Incremental Loading (CDC)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    INCREMENTAL LOADING PATTERN (CDC)                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐         │
│  │  Source  │───►│ Debezium │───►│  Kafka   │───►│   DW     │         │
│  │  (OLTP) │    │ (CDC)    │    │ (Events) │    │ (Merge)  │         │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘         │
│                                                                         │
│  Schedule: Every 5 minutes                                              │
│  Data: Only NEW or CHANGED records                                      │
│  Latency: Minutes                                                       │
│                                                                         │
│  EXAMPLE: Near-real-time customer updates                               │
│  • Capture INSERT/UPDATE/DELETE from source                             │
│  • Transform new records                                                │
│  • Merge into staging tables (UPSERT)                                   │
│                                                                         │
│  TOOL: CDC Pipeline (Debezium + Airflow)                                │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**When to Use Incremental:**
- Need fresh data (minutes, not hours)
- Large tables where full reload is expensive
- Want to minimize source system load
- Real-time dashboards required

### Decision Guide

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    BATCH vs INCREMENTAL DECISION                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Do you need REAL-TIME data (< 1 second)?                              │
│  ├── YES → Use STREAMING (Kafka Streams, Flink)                        │
│  └── NO                                                                  │
│      │                                                                  │
│      ├── Do you need data within MINUTES?                               │
│      │   ├── YES → Use CDC (Debezium + Kafka)                          │
│      │   └── NO                                                         │
│      │       │                                                          │
│      │       ├── Can data wait until TOMORROW?                          │
│      │       │   ├── YES → Use BATCH (Airflow DAGs)                    │
│      │       │   └── NO → Reconsider requirements                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 7. When to Use Which Tool?

### Decision Matrix

| Scenario | Recommended | Why |
|----------|-------------|-----|
| **Daily batch ETL** | Airflow | Scheduling, dependencies, monitoring |
| **SQL transformations** | dbt | SQL-only, testing, documentation |
| **Real-time data sync** | CDC | Capture changes instantly |
| **Small team, SQL-heavy** | dbt | Easy to learn, SQL-only |
| **Complex workflows** | Airflow | Orchestration, dependencies |
| **Live dashboards** | CDC | Near real-time data |
| **Full production** | All three | Each for its strength |

### Use Case Examples

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    USE CASE → TOOL MAPPING                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  USE CASE: Daily Data Warehouse Refresh                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Airflow: Schedule extract → load dimensions → load facts       │   │
│  │  dbt: Transform staging → intermediate → marts                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  USE CASE: Real-time Customer Dashboard                                │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  CDC: Capture customer changes from source                     │   │
│  │  Airflow: Orchestrate CDC pipeline                              │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  USE CASE: Self-Service Analytics                                      │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  dbt: Analysts build their own transformations                  │   │
│  │  Airflow: Schedule dbt runs                                     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  USE CASE: Regulatory Reporting (Daily)                                │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Airflow: Schedule daily reports                                │   │
│  │  dbt: Generate compliance views                                 │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 8. Recommended Approach

### For This Banking Data Warehouse Project

| Recommendation | Reason |
|----------------|--------|
| **Primary: Airflow** | Orchestrate all pipelines, schedule, monitor |
| **Secondary: dbt** | SQL transformations, testing, documentation |
| **Tertiary: CDC** | Real-time sync for dashboards |

### Recommended Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    RECOMMENDED PRODUCTION ARCHITECTURE                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  LAYER 1: ORCHESTRATION (Airflow)                               │   │
│  │  • Schedule all pipelines                                       │   │
│  │  • Manage dependencies                                          │   │
│  │  • Monitor and alert                                            │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                 │
│                    ┌─────────────────┴─────────────────┐               │
│                    ▼                                   ▼               │
│  ┌─────────────────────────────┐   ┌─────────────────────────────┐   │
│  │  LAYER 2A: BATCH (dbt)      │   │  LAYER 2B: REAL-TIME (CDC)  │   │
│  │  • SQL transformations      │   │  • Capture changes          │   │
│  │  • Testing                  │   │  • Near real-time sync      │   │
│  │  • Documentation            │   │  • Audit trail              │   │
│  └─────────────────────────────┘   └─────────────────────────────┘   │
│                    │                                   │               │
│                    └─────────────────┬─────────────────┘               │
│                                      ▼                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  LAYER 3: DATA WAREHOUSE (PostgreSQL)                           │   │
│  │  • Star Schema (Dimensions + Facts)                             │   │
│  │  • Staging → Dimensions → Facts                                 │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                 │
│                                      ▼                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  LAYER 4: QUERY (pgAdmin / BI Tools)                            │   │
│  │  • Business intelligence                                        │   │
│  │  • Dashboards                                                   │   │
│  │  • Ad-hoc analysis                                              │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Why This Approach?

| Component | Role | Why |
|-----------|------|-----|
| **Airflow** | Orchestrator | Schedule, monitor, alert, dependencies |
| **dbt** | SQL Transformations | Easy, testable, documented, version-controlled |
| **CDC** | Real-time Sync | Capture changes instantly, low source impact |
| **PostgreSQL** | Data Warehouse | Star schema, ACID, fast queries |

---

## 9. Running the Pipelines

### Prerequisites

```bash
# Install dependencies
pip install apache-airflow
pip install dbt-postgres
pip install psycopg2-binary
```

### Running Airflow

```bash
# Start Airflow
airflow standalone

# Access UI
open http://localhost:8080

# Trigger DAG manually
airflow dags trigger extract_source_data
airflow dags trigger load_dimensions
airflow dags trigger load_facts
airflow dags trigger cdc_realtime_sync
```

### Running dbt

```bash
# Navigate to dbt project
cd 04-etl-pipelines/dbt

# Run all models
dbt run

# Run specific model
dbt run --select stg_customers

# Run tests
dbt test

# Generate docs
dbt docs generate
dbt docs serve
```

### Running CDC Pipeline

```bash
# Start Debezium connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @04-etl-pipelines/cdc/debezium/core-banking-connector.json

# Trigger CDC DAG
airflow dags trigger cdc_realtime_sync

# Monitor CDC
psql -h localhost -U postgres -d banking_dw \
  -c "SELECT * FROM cdc_metadata.vw_cdc_monitoring;"
```

### Complete Pipeline Execution

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    DAILY EXECUTION SCHEDULE                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  2:00 AM ───► extract_source_data                                      │
│                  │                                                      │
│                  └── Extract 7 tables from 3 OLTP databases            │
│                                                                         │
│  3:00 AM ───► load_dimensions                                          │
│                  │                                                      │
│                  └── Load 5 dimensions (SCD Type 1/2)                  │
│                                                                         │
│  4:00 AM ───► load_facts                                               │
│                  │                                                      │
│                  └── Load 3 fact tables                                │
│                                                                         │
│  6:00 AM ───► (Optional) dbt run                                       │
│                  │                                                      │
│                  └── Run dbt transformations                           │
│                                                                         │
│  Every 5 min ──► cdc_realtime_sync                                     │
│                  │                                                      │
│                  └── Capture real-time changes                         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Summary

| Pipeline | Files | Best For | Complexity | Latency |
|----------|-------|----------|------------|---------|
| **Airflow** | 4 DAGs | Orchestration, scheduling | Medium | Hours |
| **dbt** | 10 files | SQL transformations | Easy | Hours |
| **CDC** | 2 files | Real-time data sync | Medium | Minutes |

### Key Takeaways

1. **Airflow** = The Conductor (orchestrates everything)
2. **dbt** = The SQL Expert (transforms data with SQL)
3. **CDC** = The Real-time Sync (captures changes instantly)

### Final Recommendation

For a **banking Data Warehouse** project:

> **Use Airflow** as the primary orchestrator
> **Use dbt** for SQL transformations and testing
> **Use CDC** for real-time dashboards and audit trail

---

*Built with ❤️ for Data Engineers learning ETL pipelines and Banking Data Warehouse*
