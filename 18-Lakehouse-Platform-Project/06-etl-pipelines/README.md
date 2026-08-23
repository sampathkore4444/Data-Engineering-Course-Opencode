# 🔄 ETL Pipelines - Banking Data Platform

> **Five approaches to data transformation: Airflow, dbt, Spark, CDC, and Streaming**

---

## 📋 Table of Contents

1. [Overview](#1-overview)
2. [ETL Pipeline Comparison](#2-etl-pipeline-comparison)
3. [Pipeline 1: Apache Airflow](#3-pipeline-1-apache-airflow)
4. [Pipeline 2: dbt (Data Build Tool)](#4-pipeline-2-dbt-data-build-tool)
5. [Pipeline 3: Apache Spark](#5-pipeline-3-apache-spark)
6. [Pipeline 4: CDC (Change Data Capture)](#6-pipeline-4-cdc-change-data-capture)
7. [Pipeline 5: Real-time Streaming](#7-pipeline-5-real-time-streaming)
8. [How Each Pipeline Works](#8-how-each-pipeline-works-all-5)
9. [Data Loading Patterns](#9-data-loading-patterns-batch-vs-incremental)
10. [When to Use Which Tool?](#10-when-to-use-which-tool)
11. [Recommended Approach](#11-recommended-approach)
12. [Running the Pipelines](#12-running-the-pipelines)

---

## 1. Overview

This project includes **5 different ETL pipeline approaches** for educational purposes. Each approach has its own strengths and use cases.

### What is ETL?

| Term | Meaning | Example |
|------|---------|---------|
| **E**xtract | Get data from source systems | Read from Oracle, Mainframe |
| **T**ransform | Clean, validate, aggregate | Remove duplicates, calculate totals |
| **L**oad | Write to target system | Save to data lake, warehouse |

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

| Feature | Airflow | dbt | Spark |
|---------|---------|-----|-------|
| **Primary Use** | Orchestration | SQL Transformations | Big Data Processing |
| **Language** | Python | SQL | Python/Scala/SQL |
| **Best For** | Scheduling, dependencies | Analytics engineering | Large-scale processing |
| **Learning Curve** | Medium | Easy | Hard |
| **Community** | Large | Growing | Large |
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
│  ├── You're orchestrating multiple tools (Spark + dbt + custom)        │
│  └── You need retry logic and error handling                           │
│                                                                         │
│  USE dbt WHEN:                                                          │
│  ├── Transformations are mostly SQL (SELECT statements)                 │
│  ├── You want built-in testing and documentation                       │
│  ├── Multiple analysts need to understand the transformations          │
│  ├── You're building a data warehouse or analytics layer               │
│  └── You want version-controlled SQL transformations                   │
│                                                                         │
│  USE SPARK WHEN:                                                        │
│  ├── You have massive data (TB or PB scale)                            │
│  ├── Transformations are complex (ML, graph analytics)                  │
│  ├── You need real-time or near-real-time processing                   │
│  ├── You're processing unstructured data (logs, images)                │
│  └── You need distributed computing across clusters                    │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Pipeline 1: Apache Airflow

### What is Airflow?

**Apache Airflow** is a workflow orchestration platform that programmatically author, schedule, and monitor workflows.

### Files in This Project

```
06-etl-pipelines/airflow/
└── dags/
    ├── bronze_ingestion.py      # Ingest raw data from sources
    ├── silver_transform.py      # Clean and validate data
    ├── gold_aggregation.py      # Create business aggregations
    └── regulatory_reports.py    # Generate compliance reports
```

### What Each DAG Does

| DAG | Purpose | Schedule | Inputs | Outputs |
|-----|---------|----------|--------|---------|
| `bronze_ingestion.py` | Extract raw data from source systems | Hourly | Oracle, Mainframe, SQL Server | Bronze layer (raw) |
| `silver_transform.py` | Clean, validate, standardize data | Daily (2 AM) | Bronze layer | Silver layer (cleansed) |
| `gold_aggregation.py` | Create business-ready aggregations | Daily (4 AM) | Silver layer | Gold layer (business) |
| `regulatory_reports.py` | Generate SBV compliance reports | Daily (6 AM) | Gold layer | Regulatory reports |

### How Airflow Works

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    AIRFLOW WORKFLOW                                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐                                                   │
│  │ bronze_ingestion│  Schedule: Hourly                                 │
│  │ (Extract)       │  Run: Extract data from Oracle, Mainframe        │
│  └────────┬────────┘                                                   │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐                                                   │
│  │ silver_transform│  Schedule: Daily 2 AM                             │
│  │ (Transform)     │  Run: Clean, validate, deduplicate               │
│  └────────┬────────┘                                                   │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐                                                   │
│  │ gold_aggregation│  Schedule: Daily 4 AM                             │
│  │ (Load)          │  Run: Create Customer 360, aggregations          │
│  └────────┬────────┘                                                   │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐                                                   │
│  │regulatory_reports│ Schedule: Daily 6 AM                             │
│  │ (Report)        │  Run: Generate Basel III, AML reports            │
│  └─────────────────┘                                                   │
│                                                                         │
│  KEY FEATURES:                                                          │
│  • Automatic retries on failure                                         │
│  • Email alerts for failures                                            │
│  • Visual monitoring in Airflow UI                                      │
│  • Dependency management (A → B → C)                                    │
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
# bronze_ingestion.py - Extract data from sources

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract_core_banking():
    """Extract data from Oracle database"""
    import pandas as pd
    import sqlalchemy
    
    engine = sqlalchemy.create_engine('oracle://user:pass@host/COREBANK')
    df = pd.read_sql('SELECT * FROM ACCOUNTS', engine)
    df.to_parquet('s3://banking-lake/bronze/accounts/')
    return len(df)

with DAG('bronze_ingestion', schedule_interval='@hourly') as dag:
    extract = PythonOperator(
        task_id='extract_accounts',
        python_callable=extract_core_banking
    )
```

---

## 4. Pipeline 2: dbt (Data Build Tool)

### What is dbt?

**dbt (data build tool)** is a SQL-based transformation tool that enables analysts and engineers to transform data in the warehouse using SQL SELECT statements.

### Files in This Project

```
06-etl-pipelines/dbt/
├── dbt_project.yml
└── models/
    ├── sources.yml
    ├── staging/
    │   ├── stg_customers.sql
    │   ├── stg_accounts.sql
    │   ├── stg_transactions.sql
    │   ├── stg_cards.sql
    │   └── stg_loans.sql
    ├── intermediate/
    │   ├── int_customer_accounts.sql
    │   ├── int_customer_cards.sql
    │   └── int_customer_loans.sql
    └── marts/
        ├── mart_customer_360.sql
        ├── mart_daily_transactions.sql
        └── mart_credit_risk.sql
```

### What Each Model Does

| Layer | Model | Purpose |
|-------|-------|---------|
| **Staging** | `stg_customers` | Clean and standardize customer data |
| **Staging** | `stg_accounts` | Clean and validate account data |
| **Staging** | `stg_transactions` | Clean and enrich transaction data |
| **Staging** | `stg_cards` | Clean and mask credit card data |
| **Staging** | `stg_loans` | Clean and classify loan data |
| **Intermediate** | `int_customer_accounts` | Aggregate accounts per customer |
| **Intermediate** | `int_customer_cards` | Aggregate cards per customer |
| **Intermediate** | `int_customer_loans` | Aggregate loans per customer |
| **Mart** | `mart_customer_360` | Complete customer view (Gold layer) |
| **Mart** | `mart_daily_transactions` | Daily transaction summary |
| **Mart** | `mart_credit_risk` | Credit risk dashboard |

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
│  │  mart_customer  │  → Final output for BI tools                      │
│  │  mart_daily_txn │  → Materialized as VIEW                           │
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
            WHEN UPPER(gender) IN ('M', 'MALE') THEN 'MALE'
            WHEN UPPER(gender) IN ('F', 'FEMALE') THEN 'FEMALE'
            ELSE 'OTHER'
        END AS gender
    FROM source
    WHERE customer_id IS NOT NULL
)

SELECT * FROM cleaned
```

---

## 5. Pipeline 3: Apache Spark

### What is Spark?

**Apache Spark** is a unified analytics engine for large-scale data processing, with built-in modules for streaming, SQL, machine learning, and graph processing.

### Files in This Project

```
06-etl-pipelines/spark-jobs/
├── bronze-to-silver.py    # Transform Bronze → Silver
└── silver-to-gold.py      # Transform Silver → Gold
```

### What Each Spark Job Does

| Job | Purpose | Input | Output |
|-----|---------|-------|--------|
| `bronze-to-silver.py` | Clean and deduplicate raw data | Bronze layer | Silver layer |
| `silver-to-gold.py` | Create business aggregations | Silver layer | Gold layer |

### How Spark Works

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    SPARK PROCESSING FLOW                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐                                                   │
│  │  Bronze Layer   │  Raw data in Parquet files                        │
│  │  (MinIO/S3)     │  → /bronze/customers/, /bronze/accounts/          │
│  └────────┬────────┘                                                   │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────────────────────────────┐                           │
│  │  SPARK CLUSTER                           │                           │
│  │  ┌─────────────┐  ┌─────────────┐       │                           │
│  │  │ Executor 1  │  │ Executor 2  │       │                           │
│  │  │ (8 cores)   │  │ (8 cores)   │       │                           │
│  │  └─────────────┘  └─────────────┘       │                           │
│  │           │               │              │                           │
│  │           ▼               ▼              │                           │
│  │  ┌─────────────────────────────────┐    │                           │
│  │  │  Bronze → Silver Transformations│    │                           │
│  │  │  • Deduplication                │    │                           │
│  │  │  • Validation                   │    │                           │
│  │  │  • Standardization              │    │                           │
│  │  └─────────────────────────────────┘    │                           │
│  └─────────────────────────────────────────┘                           │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐                                                   │
│  │  Silver Layer   │  Cleansed data in Parquet                         │
│  │  (MinIO/S3)     │  → /silver/customers/, /silver/accounts/          │
│  └────────┬────────┘                                                   │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────────────────────────────┐                           │
│  │  SPARK CLUSTER                           │                           │
│  │  • Aggregations                          │                           │
│  │  • Joins                                 │                           │
│  │  • Window functions                      │                           │
│  └─────────────────────────────────────────┘                           │
│           │                                                            │
│           ▼                                                            │
│  ┌─────────────────┐                                                   │
│  │  Gold Layer     │  Business-ready data                              │
│  │  (MinIO/S3)     │  → /gold/customer_360/, /gold/daily_txn/          │
│  └─────────────────┘                                                   │
│                                                                         │
│  KEY FEATURES:                                                          │
│  • Distributed processing (scales to PB)                                │
│  • In-memory computing (10-100x faster than Hadoop)                     │
│  • Fault-tolerant (auto-recovery)                                       │
│  • Supports SQL, Python, Scala, R                                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Importance of Spark

| Importance | Why |
|------------|-----|
| **Scalability** | Process TB-PB of data efficiently |
| **Performance** | In-memory processing is extremely fast |
| **Flexibility** | SQL, Python, Scala, R support |
| **ML Integration** | Built-in machine learning library |
| **Streaming** | Real-time data processing |
| **Ecosystem** | Works with Kafka, Delta Lake, Iceberg |

### Spark Code Example

```python
# bronze-to-silver.py - Transform Bronze → Silver

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Bronze_to_Silver") \
    .getOrCreate()

# Read Bronze data
df_bronze = spark.read.parquet("s3a://banking-lake/bronze/customers/")

# Transform to Silver
df_silver = df_bronze \
    .dropDuplicates(["customer_id"]) \
    .filter(col("customer_id").isNotNull()) \
    .withColumn("customer_name", trim(upper(col("customer_name")))) \
    .withColumn("cleaned_at", current_timestamp())

# Write to Silver
df_silver.write.mode("overwrite").parquet("s3a://banking-lake/silver/customers/")
```

---

## 6. Pipeline 4: CDC (Change Data Capture)

### What is CDC?

**Change Data Capture (CDC)** captures changes made to data in source databases and streams them to downstream systems in real-time.

### Files in This Project

```
06-etl-pipelines/airflow/dags/
└── cdc_kafka_ingestion.py    # Consume CDC events from Kafka

07-cdc-setup/
├── debezium/                 # Debezium connectors
│   ├── core-banking-connector.json
│   ├── cards-connector.json
│   └── loans-connector.json
└── kafka/
    └── topics.json           # Kafka topic configuration
```

### How CDC Works

```\┌─────────────────────────────────────────────────────────────────────────┐
│                    CDC DATA FLOW                                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐         │
│  │  Source  │───►│ Debezium │───►│  Kafka   │───►│  Bronze  │         │
│  │  Database│    │  (CDC)   │    │  Topics  │    │  Layer   │         │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘         │
│                                                                         │
│  1. Application writes to database                                      │
│  2. Debezium reads database transaction log                             │
│  3. Change event published to Kafka topic                               │
│  4. Airflow consumes and writes to Bronze layer                         │
│                                                                         │
│  LATENCY: Seconds (near real-time)                                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Importance of CDC

| Importance | Why |
|------------|-----|
| **Real-time Sync** | Data synced within seconds, not hours |
| **Low Impact** | Reads transaction log, not database queries |
| **No Data Loss** | Captures all changes (INSERT, UPDATE, DELETE) |
| **Audit Trail** | Complete history of all changes |

---

## 7. Pipeline 5: Real-time Streaming

### What is Streaming?

**Real-time Streaming** processes data continuously as it arrives, enabling instant analytics and alerts.

### Files in This Project

```
06-etl-pipelines/streaming/
├── fraud_detection_stream.py      # Detect fraud in real-time
├── realtime_metrics_stream.py     # Live dashboard metrics
└── README.md                      # Streaming documentation
```

### How Streaming Works

```\┌─────────────────────────────────────────────────────────────────────────┐
│                    STREAMING DATA FLOW                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐         │
│  │  Source  │───►│  Kafka   │───►│  Flink   │───►│  Output  │         │
│  │ Events  │    │  Topics  │    │ (Process)│    │ (Alerts) │         │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘         │
│                                                                         │
│  1. Transaction event arrives                                           │
│  2. Published to Kafka topic                                            │
│  3. Flink processes in real-time (milliseconds)                         │
│  4. Fraud alert generated if score > threshold                          │
│                                                                         │
│  LATENCY: Milliseconds                                                  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Importance of Streaming

| Importance | Why |
|------------|-----|
| **Instant Detection** | Fraud caught in milliseconds |
| **Live Dashboards** | Real-time metrics for operations |
| **Immediate Alerts** | Instant notifications for critical events |
| **Pattern Detection** | Identify trends as they emerge |

---

## 8. How Each Pipeline Works (All 5)

### Pipeline Execution Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    PIPELINE EXECUTION COMPARISON                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  AIRFLOW (Orchestration):                                               │
│  ┌─────┐    ┌─────┐    ┌─────┐    ┌─────┐                             │
│  │  A  │───►│  B  │───►│  C  │───►│  D  │                             │
│  └─────┘    └─────┘    └─────┘    └─────┘                             │
│  Schedule: Hourly/Daily                                                 │
│  Feature: Dependency management, retries, alerts                        │
│                                                                         │
│  dbt (SQL Transformations):                                             │
│  ┌─────┐    ┌─────┐    ┌─────┐    ┌─────┐                             │
│  │ src │───►│stg  │───►│ int │───►│mart │                             │
│  └─────┘    └─────┘    └─────┘    └─────┘                             │
│  Trigger: `dbt run` or Airflow                                          │
│  Feature: SQL-only, tests, documentation                                │
│                                                                         │
│  SPARK (Big Data Processing):                                           │
│  ┌─────────────────────────────────────────┐                           │
│  │  Cluster: Master + Executors             │                           │
│  │  • Parallel processing                   │                           │
│  │  • In-memory computing                   │                           │
│  │  • Auto-fault tolerance                  │                           │
│  └─────────────────────────────────────────┘                           │
│  Trigger: `spark-submit` or Airflow                                     │
│  Feature: Distributed, scalable, fast                                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Transformation Comparison

| Transformation | Airflow | dbt | Spark |
|---------------|---------|-----|-------|
| **Clean data** | Python/SQL | SQL only | Python/SQL |
| **Deduplicate** | Python | SQL | Python/SQL |
| **Aggregate** | Python | SQL | Python/SQL |
| **Join tables** | Python | SQL | Python/SQL |
| **Validate** | Python | SQL tests | Python |
| **Schedule** | ✅ Built-in | ❌ Needs Airflow | ❌ Needs Airflow |
| **Test** | ❌ Manual | ✅ Built-in | ❌ Manual |
| **Document** | ❌ Manual | ✅ Auto | ❌ Manual |

---

## 9. Data Loading Patterns (Batch vs Incremental)

### Overview

| Pattern | Description | Latency | Use Case |
|---------|-------------|---------|----------|
| **Batch** | Load all data at scheduled intervals | Hours | Daily reports, historical analytics |
| **Incremental** | Load only new/changed data | Minutes | Near-real-time dashboards |
| **Real-time** | Process data as it arrives | Seconds/Milliseconds | Fraud detection, live alerts |

### Batch Loading

```\n┌─────────────────────────────────────────────────────────────────────────┐
│                    BATCH LOADING PATTERN                                 │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐         │
│  │  Source  │───►│  Extract │───►│Transform │───►│   Load   │         │
│  │  (DB)   │    │ (All)    │    │ (All)    │    │ (All)    │         │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘         │
│                                                                         │
│  Schedule: Daily (2 AM)                                                 │
│  Data: ALL records (full load)                                          │
│  Latency: Hours                                                         │
│                                                                         │
│  EXAMPLE: Nightly transaction summary                                   │
│  • Extract ALL transactions from last 24 hours                          │
│  • Transform and aggregate                                              │
│  • Load to Gold layer                                                   │
│                                                                         │
│  TOOL: Airflow + Spark (or dbt)                                         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**When to Use Batch:**
- Data can tolerate hours of delay
- Simple to implement and maintain
- Large volumes that need bulk processing
- Historical data loads

### Incremental Loading

```\n┌─────────────────────────────────────────────────────────────────────────┐
│                    INCREMENTAL LOADING PATTERN                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐         │
│  │  Source  │───►│  Extract │───►│Transform │───►│   Load   │         │
│  │  (DB)   │    │ (New/    │    │ (New/    │    │ (Merge)  │         │
│  │         │    │ Changed) │    │ Changed) │    │          │         │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘         │
│                                                                         │
│  Schedule: Every 5-15 minutes                                           │
│  Data: Only NEW or CHANGED records                                      │
│  Latency: Minutes                                                       │
│                                                                         │
│  EXAMPLE: Near-real-time dashboard                                      │
│  • Extract transactions since last run (watermark)                      │
│  • Transform new records                                                │
│  • Merge into existing data (UPSERT)                                    │
│                                                                         │
│  TOOLS: CDC (Debezium) or dbt incremental                               │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**When to Use Incremental:**
- Need fresh data (minutes, not hours)
- Large tables where full reload is expensive
- Want to reduce source system load
- Near-real-time dashboards required

### How Each Tool Handles Loading

| Tool | Batch Support | Incremental Support | How |
|------|---------------|---------------------|-----|
| **Airflow** | ✅ Yes | ✅ Yes | Schedule + watermark logic |
| **dbt** | ✅ Yes | ✅ Yes | `incremental` materialization |
| **Spark** | ✅ Yes | ✅ Yes | Delta Lake merge, Iceberg |
| **CDC** | ⚠️ N/A | ✅ Yes | Captures changes automatically |
| **Streaming** | ⚠️ N/A | ✅ Yes | Processes continuously |

### dbt Incremental Example

```sql
-- dbt incremental model (only processes new data)

{{ config(
    materialized='incremental',
    unique_key='txn_id',
    incremental_strategy='merge'
) }}

SELECT *
FROM {{ source('raw', 'transactions') }}

{% if is_incremental() %}
    -- Only load data since last run
    WHERE txn_timestamp > (SELECT MAX(txn_timestamp) FROM {{ this }})
{% endif %}
```

### Decision Guide

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    BATCH vs INCREMENTAL DECISION                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Do you need REAL-TIME data (< 1 second)?                              │
│  ├── YES → Use STREAMING (Flink)                                        │
│  └── NO                                                                  │
│      │                                                                  │
│      ├── Do you need data within MINUTES?                               │
│      │   ├── YES → Use INCREMENTAL (CDC or dbt incremental)            │
│      │   └── NO                                                         │
│      │       │                                                          │
│      │       ├── Can data wait until TOMORROW?                          │
│      │       │   ├── YES → Use BATCH (Airflow + Spark/dbt)             │
│      │       │   └── NO → Reconsider requirements                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 10. When to Use Which Tool?

### Decision Matrix

| Scenario | Recommended | Why |
|----------|-------------|-----|
| **Small team, SQL-heavy** | dbt | Easy to learn, SQL-only |
| **Complex workflows** | Airflow | Orchestration, dependencies |
| **Large data (TB+)** | Spark | Distributed processing |
| **Real-time streaming** | Spark Streaming | Low-latency processing |
| **Analytics project** | dbt | Testing, documentation |
| **Data engineering** | Airflow + Spark | Orchestration + processing |
| **Full production** | All three | Each for its strength |

### Use Case Examples

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    USE CASE → TOOL MAPPING                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  USE CASE: Daily ETL Pipeline                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Airflow: Schedule and orchestrate                              │   │
│  │  Spark: Process large data volumes                              │   │
│  │  dbt: Transform data in warehouse                               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  USE CASE: Real-time Fraud Detection                                   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Spark Streaming: Process transactions in real-time             │   │
│  │  Kafka: Message queue for transaction events                    │   │
│  │  Airflow: Orchestrate batch processing                          │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  USE CASE: Self-Service Analytics                                      │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  dbt: Analysts build their own transformations                  │   │
│  │  Airflow: Schedule dbt runs                                     │   │
│  │  Dremio: Query and visualize                                    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  USE CASE: Regulatory Reporting                                        │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Airflow: Schedule daily reports                                │   │
│  │  Spark: Aggregate large datasets                                │   │
│  │  dbt: Generate compliance reports                               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 11. Recommended Approach

### For This Banking Project

| Recommendation | Reason |
|----------------|--------|
| **Primary: Airflow + dbt** | Best for medium-scale banking data |
| **Secondary: Spark** | Use only if data > 1TB or complex ML needed |

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
│                                      ▼                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  LAYER 2: PROCESSING (Spark or dbt)                             │   │
│  │                                                                 │   │
│  │  Option A: dbt (if data < 1TB)                                  │   │
│  │  • SQL transformations                                          │   │
│  │  • Built-in testing                                             │   │
│  │  • Auto documentation                                           │   │
│  │                                                                 │   │
│  │  Option B: Spark (if data > 1TB)                                │   │
│  │  • Distributed processing                                       │   │
│  │  • In-memory computing                                          │   │
│  │  • ML integration                                               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                      │                                 │
│                                      ▼                                 │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  LAYER 3: QUERY (Dremio)                                        │   │
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
| **Airflow** | Orchestrator | Schedule, monitor, alert |
| **dbt** | SQL Transformations | Easy, testable, documented |
| **Spark** | Big Data (if needed) | Scale for large volumes |
| **Dremio** | Query Engine | Fast analytics with reflections |

---

## 12. Running the Pipelines

### Prerequisites

```bash
# Install dependencies
pip install apache-airflow
pip install dbt-postgres
pip install pyspark
```

### Running Airflow

```bash
# Start Airflow
airflow standalone

# Access UI
open http://localhost:8080

# Trigger DAG manually
dags trigger bronze_ingestion
```

### Running dbt

```bash
# Navigate to dbt project
cd 06-etl-pipelines/dbt

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

### Running Spark

```bash
# Submit Spark job
spark-submit \
  --master local[*] \
  06-etl-pipelines/spark-jobs/bronze-to-silver.py

# Submit to cluster
spark-submit \
  --master spark://master:7077 \
  --num-executors 4 \
  06-etl-pipelines/spark-jobs/bronze-to-silver.py
```

### Running All Pipelines (via Airflow)

```bash
# Airflow orchestrates everything
# 1. bronze_ingestion runs hourly
# 2. silver_transform runs daily at 2 AM
# 3. gold_aggregation runs daily at 4 AM
# 4. regulatory_reports runs daily at 6 AM
```

---

## 📊 Summary

| Pipeline | Files | Best For | Complexity |
|----------|-------|----------|------------|
| **Airflow** | 5 DAGs | Orchestration, scheduling | Medium |
| **dbt** | 15 files | SQL transformations | Easy |
| **Spark** | 2 files | Big data processing | Hard |
| **CDC** | 4 files | Real-time data sync | Medium |
| **Streaming** | 3 files | Real-time analytics | Hard |

### Key Takeaways

1. **Airflow** = The Conductor (orchestrates everything)
2. **dbt** = The SQL Expert (transforms data with SQL)
3. **Spark** = The Heavy Lifter (processes massive data)
4. **CDC** = The Real-time Sync (captures changes instantly)
5. **Streaming** = The Live Processor (analyzes data in motion)

### Final Recommendation

For a **banking data platform** with moderate data volume:

> **Use Airflow + dbt** as the primary approach
> Add Spark only when you need to process > 1TB or run complex ML
> Add CDC for real-time data sync
> Add Streaming for fraud detection

---

*Built with ❤️ for Data Engineers learning ETL pipelines and Banking Data Architecture*
