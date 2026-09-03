# 12 - Orchestration

## Table of Contents
1. [Apache Airflow](#1-apache-airflow)
2. [Other Orchestration Tools](#2-other-orchestration-tools)
3. [CI/CD for Data](#3-cicd-for-data)
4. [Real-World Scenarios](#4-real-world-scenarios)
   - [Scenario 1: Daily EOD Banking Pipeline](#scenario-1-daily-end-of-day-eod-banking-pipeline)
   - [Scenario 2: Real-Time Fraud Detection](#scenario-2-real-time-fraud-detection-pipeline)
   - [Scenario 3: Regulatory Reporting Automation](#scenario-3-regulatory-reporting-automation)
   - [Scenario 4: Customer Onboarding Pipeline](#scenario-4-customer-onboarding-pipeline)
   - [Scenario 5: ML Model Retraining](#scenario-5-ml-model-retraining-pipeline)
5. [Hands-On Exercises](#5-hands-on-exercises)
6. [Interview Questions](#6-interview-questions)

---

## 1. Apache Airflow

### Architecture

```
+--------------------------------------------------+
|                    WEB SERVER                     |
|  (UI for monitoring and management)              |
+--------------------------------------------------+
                        |
+--------------------------------------------------+
|                  METADATA DB                     |
|  (PostgreSQL/MySQL - stores DAGs, runs, logs)    |
+--------------------------------------------------+
                        |
+--------------------------------------------------+
|               SCHEDULER + WORKERS                 |
|  Scheduler: Triggers DAGs based on schedule      |
|  Workers: Execute tasks (Celery/Kubernetes)      |
|  +----------+  +----------+  +----------+        |
|  | Worker 1 |  | Worker 2 |  | Worker 3 |        |
|  +----------+  +----------+  +----------+        |
+--------------------------------------------------+
```

### Key Concepts

- **DAG (Directed Acyclic Graph):** Definition of task dependencies
- **Operator:** Template for a specific task (BashOperator, PythonOperator, etc.)
- **Task:** Instance of an operator within a DAG
- **Task Instance:** A specific run of a task
- **Connection:** Credentials for external systems
- **Variable:** Key-value pairs for configuration

### Airflow Tools & Platforms

| Tool | Type | Description |
|------|------|-------------|
| **Apache Airflow** | Open Source | Self-hosted orchestration |
| **MWAA (Managed Workflows)** | AWS Service | Managed Airflow on AWS |
| **Cloud Composer** | GCP Service | Managed Airflow on GCP |
| **Astronomer** | Enterprise | Enterprise Airflow platform |
| **Airflow Providers** | Extensions | 80+ provider packages |
| **Astronomer CLI** | CLI Tool | Local Airflow development |

### Example DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email': ['alerts@company.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_data(**context):
    """Extract data from source"""
    import pandas as pd
    df = pd.read_sql("SELECT * FROM orders WHERE date = %s", 
                     conn, params=[context['ds']])
    df.to_parquet(f"/tmp/orders_{context['ds']}.parquet")
    return len(df)

def transform_data(**context):
    """Transform extracted data"""
    import pandas as pd
    df = pd.read_parquet(f"/tmp/orders_{context['ds']}.parquet")
    df['profit'] = df['revenue'] - df['cost']
    df.to_parquet(f"/tmp/orders_transformed_{context['ds']}.parquet")

def validate_data(**context):
    """Validate transformed data"""
    import pandas as pd
    df = pd.read_parquet(f"/tmp/orders_transformed_{context['ds']}.parquet")
    assert len(df) > 0, "No data found"
    assert df['profit'].notna().all(), "Null profits found"
    assert (df['profit'] >= 0).all(), "Negative profits found"

with DAG(
    'daily_orders_etl',
    default_args=default_args,
    description='Daily orders ETL pipeline',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    catchup=False,
    tags=['production', 'orders'],
) as dag:

    extract = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id='transform_orders',
        python_callable=transform_data,
    )

    validate = PythonOperator(
        task_id='validate_orders',
        python_callable=validate_data,
    )

    load_to_s3 = BashOperator(
        task_id='upload_to_s3',
        bash_command='aws s3 cp /tmp/orders_transformed_{{ ds }}.parquet s3://data-lake/orders/{{ ds }}/',
    )

    load_to_redshift = S3ToRedshiftOperator(
        task_id='load_to_redshift',
        schema='public',
        table='fact_orders',
        s3_bucket='data-lake',
        s3_key='orders/{{ ds }}/',
        copy_options=['FORMAT AS PARQUET'],
        aws_conn_id='aws_default',
    )

    extract >> transform >> validate >> load_to_s3 >> load_to_redshift
```

### Sensors

```python
from airflow.sensors.s3 import S3KeySensor
from airflow.sensors.external_task import ExternalTaskSensor

# Wait for file to arrive
wait_for_file = S3KeySensor(
    task_id='wait_for_file',
    bucket_name='data-lake',
    bucket_key='raw/orders/{{ ds }}/orders.parquet',
    timeout=3600,
    poke_interval=60,
)

# Wait for another DAG to complete
wait_for_upstream = ExternalTaskSensor(
    task_id='wait_for_upstream_dag',
    external_dag_id='upstream_pipeline',
    external_task_id='load_complete',
    mode='reschedule',
)
```

### Hooks and Connections

```python
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook

# PostgreSQL
pg_hook = PostgresHook(postgres_conn_id='warehouse')
pg_hook.run("INSERT INTO fact_orders SELECT * FROM staging_orders")

# S3
s3_hook = S3Hook(aws_conn_id='aws_default')
s3_hook.load_file(filename='/tmp/data.parquet', 
                  bucket_name='data-lake', 
                  key='processed/data.parquet')
```

---

## 2. Other Orchestration Tools

### Orchestration Tools Comparison

| Tool | Type | Best For | Key Feature |
|------|------|----------|-------------|
| **Apache Airflow** | Open Source | Batch ETL, complex workflows | Mature ecosystem, scheduling |
| **Prefect** | Open Source/Cloud | Modern Python workflows | Dynamic, cloud-native |
| **Dagster** | Open Source | Asset-oriented data | Type system, testing |
| **Mage** | Open Source | Visual pipelines | Block-based, low-code |
| **Kestra** | Open Source | Event-driven workflows | YAML-based, declarative |
| **Argo Workflows** | Open Source | Kubernetes-native | GitOps, K8s integration |

### Apache Prefect

```python
from prefect import flow, task
from prefect.tasks import task_input_hash

@task(retries=3, cache_key_fn=task_input_hash)
def extract():
    return pd.read_sql("SELECT * FROM orders", conn)

@task
def transform(df):
    df['profit'] = df['revenue'] - df['cost']
    return df

@task
def load(df):
    df.to_sql("fact_orders", conn, if_exists="append")

@flow(name="orders-etl")
def orders_etl():
    raw_data = extract()
    transformed = transform(raw_data)
    load(transformed)

orders_etl()
```

### Dagster

```python
from dagster import job, op, OpDefinition

@op
def extract_orders():
    return pd.read_sql("SELECT * FROM orders", conn)

@op
def transform_orders(orders):
    orders['profit'] = orders['revenue'] - orders['cost']
    return orders

@op
def load_orders(transformed):
    transformed.to_sql("fact_orders", conn, if_exists="append")

@job
def orders_job():
    load_orders(transform_orders(extract_orders()))
```

### Kestra

```yaml
# Kestra uses YAML-based flow definitions
tasks:
  - id: extract
    type: io.kestra.plugin.scripts.python.Script
    script: |
      import pandas as pd
      df = pd.read_sql('SELECT * FROM orders', conn)
      df.to_parquet('/tmp/orders.parquet')
  
  - id: transform
    type: io.kestra.plugin.scripts.python.Script
    script: |
      import pandas as pd
      df = pd.read_parquet('/tmp/orders.parquet')
      df['profit'] = df['revenue'] - df['cost']
      df.to_parquet('/tmp/orders_transformed.parquet')
  
  - id: load
    type: io.kestra.plugin.scripts.python.Script
    script: |
      import pandas as pd
      df = pd.read_parquet('/tmp/orders_transformed.parquet')
      df.to_sql('fact_orders', conn, if_exists='append')
```

### Mage

```python
# Mage uses a block-based approach
# Block 1: Extract
@api
def extract():
    return execute_query('SELECT * FROM orders')

# Block 2: Transform
@api
def transform(df):
    df['profit'] = df['revenue'] - df['cost']
    return df

# Block 3: Load
@api  
def load(df):
    df.to_sql('fact_orders', conn, if_exists='append')
```

---

## 3. CI/CD for Data

### CI/CD Tools Comparison

| Tool | Type | Best For |
|------|------|----------|
| **GitHub Actions** | CI/CD | Git-native workflows |
| **GitLab CI** | CI/CD | GitLab-native workflows |
| **Jenkins** | CI/CD | Enterprise, customizable |
| **Terraform** | IaC | Infrastructure provisioning |
| **dbt Cloud** | CI/CD | dbt-specific deployment |
| **Pre-commit** | Git Hooks | Code quality checks |

### dbt CI/CD

```yaml
# .github/workflows/dbt-ci.yml
name: dbt CI/CD

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Install dbt
        run: pip install dbt-core dbt-postgres
      
      - name: Run dbt deps
        run: dbt deps
      
      - name: Run dbt build (test + build)
        run: dbt build --target dev
      
      - name: Run dbt docs generate
        run: dbt docs generate
      
      - name: Upload artifacts
        uses: actions/upload-artifact@v3
        with:
          name: dbt-docs
          path: target/

  deploy:
    runs-on: ubuntu-latest
    needs: test
    if: github.ref == 'refs/heads/main'
    steps:
      - name: Deploy to production
        run: dbt build --target prod
`

### Terraform for Infrastructure

```hcl
# main.tf
resource "aws_redshift_cluster" "warehouse" {
  cluster_identifier = "data-warehouse"
  database_name      = "analytics"
  master_username    = "admin"
  master_password    = var.redshift_password
  node_type          = "dc2.large"
  cluster_type       = "single-node"
  
  tags = {
    Environment = "production"
    Team        = "data-engineering"
  }
}

resource "aws_s3_bucket" "data_lake" {
  bucket = "company-data-lake-"
}
```

---

## 4. Real-World Scenarios

### Overview

This section presents **5 complete banking orchestration scenarios** that demonstrate how to build production-grade data pipelines using Apache Airflow and other tools. Each scenario includes the business context, problem, solution architecture, complete DAG code, and business outcomes.

---

### Scenario 1: Daily End-of-Day (EOD) Banking Pipeline

> **Business Context:** A bank must process millions of daily transactions, update account balances, generate reports, and prepare regulatory submissions every night.

#### The Problem

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CURRENT STATE (Before)                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ❌ Manual batch processing (operators run scripts manually)          │
│   ❌ No dependency tracking (steps fail silently)                       │
│   ❌ No retry mechanism (failures require manual intervention)         │
│   ❌ No alerting (issues discovered next morning)                      │
│   ❌ Processing takes 8+ hours (misses morning deadlines)             │
│                                                                         │
│   10:00 PM: Operator starts processing                                 │
│   2:00 AM:  Step 3 fails silently                                      │
│   6:00 AM:  Operator discovers failure, restarts from beginning        │
│   10:00 AM: Reports delayed, regulators unhappy                        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### The Solution: Automated EOD Pipeline with Airflow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                 EOD BANKING PIPELINE ARCHITECTURE                       │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   TRIGGER: Schedule (Daily at 10:00 PM) + Manual                       │
│                              │                                          │
│                              ▼                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    APACHE AIRFLOW DAG                            │  │
│   │                    "daily_eod_pipeline"                         │  │
│   │                                                                  │  │
│   │  ┌────────────┐                                                 │  │
│   │  │ extract_   │                                                 │  │
│   │  │ transactions│────┐                                            │  │
│   │  └────────────┘    │                                            │  │
│   │                    ▼                                            │  │
│   │            ┌────────────┐                                       │  │
│   │            │ validate_  │                                       │  │
│   │            │ data       │                                       │  │
│   │            └─────┬──────┘                                       │  │
│   │                  │                                              │  │
│   │         ┌────────┴────────┐                                     │  │
│   │         ▼                 ▼                                     │  │
│   │  ┌────────────┐   ┌────────────┐                                │  │
│   │  │ process_   │   │ process_   │                                │  │
│   │  │ cards      │   │ loans      │                                │  │
│   │  └─────┬──────┘   └─────┬──────┘                                │  │
│   │        │                │                                        │  │
│   │        └────────┬───────┘                                        │  │
│   │                 ▼                                                │  │
│   │          ┌────────────┐                                          │  │
│   │          │ update_    │                                          │  │
│   │          │ balances   │                                          │  │
│   │          └─────┬──────┘                                          │  │
│   │                │                                                 │  │
│   │       ┌────────┴────────┬────────────┐                          │  │
│   │       ▼                 ▼            ▼                           │  │
│   │ ┌──────────┐    ┌──────────┐   ┌──────────┐                     │  │
│   │ │ generate │    │ reconcile│   │ archive  │                     │  │
│   │ │ reports  │    │ accounts │   │ data     │                     │  │
│   │ └────┬─────┘    └────┬─────┘   └────┬─────┘                     │  │
│   │      │               │              │                           │  │
│   │      └───────────────┴──────────────┘                           │  │
│   │                      │                                           │  │
│   │              ┌───────▼───────┐                                   │  │
│   │              │ notify_team   │                                   │  │
│   │              └───────────────┘                                   │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Complete Airflow DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': True,  # Critical: don't run if previous day failed
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email': ['data-alerts@bank.com', 'ops@bank.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'retry_exponential_backoff': True,
    'execution_timeout': timedelta(hours=4),
}

# ============================================================
# TASK FUNCTIONS
# ============================================================

def extract_transactions(**context):
    """Extract daily transactions from core banking system."""
    from airflow.hooks.postgres_hook import PostgresHook
    import pandas as pd
    
    pg_hook = PostgresHook(postgres_conn_id='core_banking')
    
    # Extract today's transactions
    df = pg_hook.get_pandas_df("""
        SELECT 
            txn_id,
            account_id,
            txn_type,
            amount,
            currency,
            channel,
            status,
            created_at
        FROM transactions
        WHERE DATE(created_at) = %s
          AND status = 'COMPLETED'
    """, parameters=[context['ds']])
    
    # Save to staging area
    df.to_parquet(f"/tmp/staging/transactions_{context['ds']}.parquet", index=False)
    
    logger.info(f"Extracted {len(df)} transactions for {context['ds']}")
    context['ti'].xcom_push(key='transaction_count', value=len(df))
    return len(df)

def validate_data(**context):
    """Validate extracted data quality."""
    import pandas as pd
    
    df = pd.read_parquet(f"/tmp/staging/transactions_{context['ds']}.parquet")
    
    # Data quality checks
    errors = []
    
    # Check 1: No empty dataset
    if len(df) == 0:
        errors.append("No transactions found for the day")
    
    # Check 2: No duplicate transaction IDs
    if df['txn_id'].duplicated().any():
        errors.append(f"{df['txn_id'].duplicated().sum()} duplicate transaction IDs")
    
    # Check 3: No negative amounts
    if (df['amount'] < 0).any():
        errors.append(f"{(df['amount'] < 0).sum()} transactions with negative amounts")
    
    # Check 4: All required fields present
    required_cols = ['txn_id', 'account_id', 'txn_type', 'amount']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        errors.append(f"Missing columns: {missing_cols}")
    
    if errors:
        raise ValueError(f"Data validation failed: {'; '.join(errors)}")
    
    logger.info(f"Data validation passed: {len(df)} records")
    return True

def process_card_transactions(**context):
    """Process card transactions separately."""
    from airflow.hooks.postgres_hook import PostgresHook
    import pandas as pd
    
    pg_hook = PostgresHook(postgres_conn_id='cards_system')
    
    df = pg_hook.get_pandas_df("""
        SELECT 
            card_txn_id,
            card_number_masked,
            merchant_name,
            amount,
            currency,
            authorization_code,
            created_at
        FROM card_transactions
        WHERE DATE(created_at) = %s
    """, parameters=[context['ds']])
    
    df.to_parquet(f"/tmp/staging/card_txns_{context['ds']}.parquet", index=False)
    logger.info(f"Processed {len(df)} card transactions")
    return len(df)

def update_account_balances(**context):
    """Update account balances based on processed transactions."""
    from airflow.hooks.postgres_hook import PostgresHook
    
    pg_hook = PostgresHook(postgres_conn_id='warehouse')
    
    # Update balances using MERGE/UPSERT
    update_query = """
        INSERT INTO fact_daily_balances (account_id, balance_date, opening_balance, 
                                        total_credits, total_debits, closing_balance)
        SELECT 
            t.account_id,
            DATE(t.created_at) as balance_date,
            COALESCE(prev.closing_balance, 0) as opening_balance,
            SUM(CASE WHEN t.txn_type = 'CREDIT' THEN t.amount ELSE 0 END) as total_credits,
            SUM(CASE WHEN t.txn_type = 'DEBIT' THEN t.amount ELSE 0 END) as total_debits,
            COALESCE(prev.closing_balance, 0) + 
                SUM(CASE WHEN t.txn_type = 'CREDIT' THEN t.amount ELSE -t.amount END) as closing_balance
        FROM transactions t
        LEFT JOIN fact_daily_balances prev 
            ON t.account_id = prev.account_id 
            AND prev.balance_date = DATE(t.created_at) - INTERVAL '1 day'
        WHERE DATE(t.created_at) = %s
        GROUP BY t.account_id, DATE(t.created_at), prev.closing_balance
        ON CONFLICT (account_id, balance_date) 
        DO UPDATE SET 
            total_credits = EXCLUDED.total_credits,
            total_debits = EXCLUDED.total_debits,
            closing_balance = EXCLUDED.closing_balance;
    """
    
    pg_hook.run(update_query, parameters=[context['ds']])
    logger.info("Account balances updated successfully")

def reconcile_accounts(**context):
    """Reconcile internal vs external records."""
    from airflow.hooks.postgres_hook import PostgresHook
    
    pg_hook = PostgresHook(postgres_conn_id='warehouse')
    
    # Check for discrepancies
    result = pg_hook.get_first("""
        SELECT COUNT(*) as discrepancies
        FROM fact_daily_balances db
        JOIN bank_statements bs 
            ON db.account_id = bs.account_id 
            AND db.balance_date = bs.statement_date
        WHERE ABS(db.closing_balance - bs.balance) > 0.01
          AND db.balance_date = %s
    """, parameters=[context['ds']])
    
    discrepancies = result[0]
    if discrepancies > 0:
        raise ValueError(f"Reconciliation failed: {discrepancies} accounts have discrepancies")
    
    logger.info("Reconciliation passed: All accounts balanced")

def generate_reports(**context):
    """Generate daily regulatory and management reports."""
    from airflow.hooks.postgres_hook import PostgresHook
    
    pg_hook = PostgresHook(postgres_conn_id='warehouse')
    
    # Generate daily summary report
    report = pg_hook.get_pandas_df("""
        SELECT 
            balance_date,
            COUNT(DISTINCT account_id) as total_accounts,
            SUM(closing_balance) as total_deposits,
            SUM(total_credits) as total_credits,
            SUM(total_debits) as total_debits
        FROM fact_daily_balances
        WHERE balance_date = %s
        GROUP BY balance_date
    """, parameters=[context['ds']])
    
    report.to_csv(f"/tmp/reports/daily_summary_{context['ds']}.csv", index=False)
    logger.info(f"Report generated: {report.to_dict()}")

def notify_success(**context):
    """Send success notification."""
    from airflow.operators.email import EmailOperator
    
    txn_count = context['ti'].xcom_pull(task_ids='extract_transactions', key='transaction_count')
    
    message = f"""
    EOD Pipeline Completed Successfully
    
    Date: {context['ds']}
    Transactions Processed: {txn_count}
    Duration: {context['ti'].execution_date}
    
    Reports are available in /tmp/reports/
    """
    
    logger.info(message)

# ============================================================
# DAG DEFINITION
# ============================================================

with DAG(
    'daily_eod_pipeline',
    default_args=default_args,
    description='Daily End-of-Day Banking Pipeline',
    schedule_interval='0 22 * * *',  # 10:00 PM daily
    catchup=False,
    max_active_runs=1,  # Only one instance at a time
    tags=['banking', 'eod', 'production'],
) as dag:

    # Task 1: Extract transactions
    extract = PythonOperator(
        task_id='extract_transactions',
        python_callable=extract_transactions,
    )

    # Task 2: Validate data
    validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    # Task 3: Process card transactions (parallel)
    process_cards = PythonOperator(
        task_id='process_card_transactions',
        python_callable=process_card_transactions,
    )

    # Task 4: Update balances
    update_balances = PythonOperator(
        task_id='update_account_balances',
        python_callable=update_account_balances,
    )

    # Task 5: Reconcile accounts
    reconcile = PythonOperator(
        task_id='reconcile_accounts',
        python_callable=reconcile_accounts,
    )

    # Task 6: Generate reports
    reports = PythonOperator(
        task_id='generate_reports',
        python_callable=generate_reports,
    )

    # Task 7: Archive data
    archive = BashOperator(
        task_id='archive_data',
        bash_command='''
            aws s3 mv /tmp/staging/transactions_{{ ds }}.parquet \
                s3://data-warehouse/archive/{{ ds }}/transactions.parquet
            aws s3 mv /tmp/reports/daily_summary_{{ ds }}.csv \
                s3://reports/daily/{{ ds }}/summary.csv
        ''',
    )

    # Task 8: Notify
    notify = PythonOperator(
        task_id='notify_success',
        python_callable=notify_success,
    )

    # Define dependencies
    extract >> validate >> [update_balances, process_cards]
    [update_balances, process_cards] >> reconcile >> reports >> archive >> notify
```

#### Key Metrics & Outcomes

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Processing time | 8 hours | 2 hours | 75% faster |
| Failure rate | 15% | 2% | 87% reduction |
| Mean time to recovery | 4 hours | 10 minutes | 96% faster |
| Manual intervention | Daily | Weekly | 85% reduction |
| Report availability | 10:00 AM | 1:00 AM | 9 hours earlier |

---

### Scenario 2: Real-Time Fraud Detection Pipeline

> **Business Context:** A bank needs to detect fraudulent transactions in real-time and trigger alerts within seconds.

#### The Problem

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CURRENT STATE (Before)                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ❌ Fraud detection runs in BATCH (next-day analysis)                │
│   ❌ Average fraud detection time: 48 hours                           │
│   ❌ Monthly fraud losses: $2M+                                        │
│   ❌ High false positive rate (40%)                                    │
│   ❌ Manual investigation (3 days per case)                           │
│                                                                         │
│   Transaction → Database → Nightly Batch → Fraud Rules → Alert (Late!) │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### The Solution: Real-Time Streaming Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                 REAL-TIME FRAUD DETECTION PIPELINE                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   DATA SOURCES                                                         │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                │
│   │ Card          │  │ ATM          │  │ Mobile       │                │
│   │ Transactions  │  │ Transactions │  │ Payments     │                │
│   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                │
│          │                  │                  │                        │
│          └──────────────────┼──────────────────┘                        │
│                             │                                          │
│                    Apache Kafka                                        │
│                    (Event Streaming)                                   │
│                             │                                          │
│                             ▼                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    APACHE AIRFLOW                                │  │
│   │                    (Orchestration Layer)                         │  │
│   │                                                                  │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │ DAG: fraud_detection_realtime                             │ │  │
│   │  │                                                            │ │  │
│   │  │  ┌──────────┐    ┌──────────┐    ┌──────────┐            │ │  │
│   │  │  │ consume_ │    │ enrich_  │    │ score_   │            │ │  │
│   │  │  │ kafka    │───▶│ txn_data │───▶│ fraud    │            │ │  │
│   │  │  │ events   │    │          │    │ risk     │            │ │  │
│   │  │  └──────────┘    └──────────┘    └────┬─────┘            │ │  │
│   │  │                                       │                  │ │  │
│   │  │                    ┌──────────────────┼──────────────┐   │ │  │
│   │  │                    ▼                  ▼              ▼   │ │  │
│   │  │              ┌──────────┐       ┌──────────┐   ┌──────┐ │ │  │
│   │  │              │ block_   │       │ alert_   │   │ log_ │ │ │  │
│   │  │              │ txn_if   │       │ fraud_   │   │ to_  │ │ │  │
│   │  │              │ high_risk│       │ ops_team │   │ lake │ │ │  │
│   │  │              └──────────┘       └──────────┘   └──────┘ │ │  │
│   │  │                                                          │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                             │                                          │
│                             ▼                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                    DOWNSTREAM SYSTEMS                            │  │
│   │                                                                  │  │
│   │  ┌──────────┐    ┌──────────┐    ┌──────────┐                  │  │
│   │  │ Fraud    │    │ Case     │    │ ML Model │                  │  │
│   │  │ Dashboard│    │ Mgmt     │    │ Retrain  │                  │  │
│   │  │ (Live)   │    │ System   │    │ Pipeline │                  │  │
│   │  └──────────┘    └──────────┘    └──────────┘                  │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Complete Airflow DAG for Fraud Detection

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import json

default_args = {
    'owner': 'fraud-ops',
    'retries': 2,
    'retry_delay': timedelta(seconds=30),
    'execution_timeout': timedelta(minutes=5),
}

def consume_kafka_events(**context):
    """Consume transaction events from Kafka."""
    from kafka import KafkaConsumer
    import json
    
    consumer = KafkaConsumer(
        'card-transactions',
        bootstrap_servers=['kafka:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        consumer_timeout_ms=10000,  # 10 seconds
    )
    
    transactions = []
    for message in consumer:
        transactions.append(message.value)
        if len(transactions) >= 1000:  # Process in batches
            break
    
    consumer.close()
    context['ti'].xcom_push(key='transactions', value=transactions)
    return len(transactions)

def enrich_transaction_data(**context):
    """Enrich transactions with customer and historical data."""
    from airflow.hooks.postgres_hook import PostgresHook
    import pandas as pd
    
    transactions = context['ti'].xcom_pull(task_ids='consume_kafka_events', key='transactions')
    df = pd.DataFrame(transactions)
    
    pg_hook = PostgresHook(postgres_conn_id='warehouse')
    
    # Get customer profile
    customer_ids = df['customer_id'].unique().tolist()
    customers = pg_hook.get_pandas_df(
        "SELECT * FROM dim_customers WHERE customer_id IN %s",
        parameters=[tuple(customer_ids)]
    )
    
    # Get historical transaction patterns
    history = pg_hook.get_pandas_df("""
        SELECT 
            customer_id,
            AVG(amount) as avg_amount,
            STDDEV(amount) as std_amount,
            COUNT(*) as txn_count_30d
        FROM fact_transactions
        WHERE created_at >= NOW() - INTERVAL '30 days'
        GROUP BY customer_id
    """)
    
    # Merge
    df = df.merge(customers, on='customer_id', how='left')
    df = df.merge(history, on='customer_id', how='left')
    
    context['ti'].xcom_push(key='enriched_df', value=df.to_dict())
    return len(df)

def score_fraud_risk(**context):
    """Score transactions for fraud risk using ML model."""
    import pandas as pd
    import numpy as np
    from sklearn.ensemble import RandomForestClassifier
    import joblib
    
    enriched_data = context['ti'].xcom_pull(task_ids='enrich_transaction_data', key='enriched_df')
    df = pd.DataFrame(enriched_data)
    
    # Load pre-trained model
    model = joblib.load('/models/fraud_detector.pkl')
    
    # Feature engineering
    features = ['amount', 'avg_amount', 'std_amount', 'txn_count_30d']
    X = df[features].fillna(0)
    
    # Predict fraud probability
    df['fraud_probability'] = model.predict_proba(X)[:, 1]
    df['risk_level'] = pd.cut(
        df['fraud_probability'],
        bins=[0, 0.3, 0.7, 1.0],
        labels=['LOW', 'MEDIUM', 'HIGH']
    )
    
    # Flag high-risk transactions
    high_risk = df[df['risk_level'] == 'HIGH']
    
    context['ti'].xcom_push(key='high_risk_txns', value=high_risk.to_dict())
    return len(high_risk)

def block_and_alert(**context):
    """Block high-risk transactions and send alerts."""
    from airflow.hooks.postgres_hook import PostgresHook
    from airflow.models import Variable
    import requests
    
    high_risk = context['ti'].xcom_pull(task_ids='score_fraud_risk', key='high_risk_txns')
    df = pd.DataFrame(high_risk)
    
    pg_hook = PostgresHook(postgres_conn_id='cards_system')
    
    for _, txn in df.iterrows():
        # Block transaction
        pg_hook.run("""
            UPDATE card_transactions 
            SET status = 'BLOCKED', block_reason = 'FRAUD_SUSPECTED'
            WHERE txn_id = %s
        """, parameters=[txn['txn_id']])
        
        # Send alert to fraud ops
        slack_webhook = Variable.get('slack_fraud_webhook')
        requests.post(slack_webhook, json={
            'text': f"🚨 FRAUD ALERT: Transaction {txn['txn_id']} blocked. "
                    f"Amount: ${txn['amount']}, Customer: {txn['customer_id']}, "
                    f"Risk Score: {txn['fraud_probability']:.2f}"
        })
    
    return len(df)

with DAG(
    'fraud_detection_realtime',
    default_args=default_args,
    description='Real-time fraud detection pipeline',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=['fraud', 'realtime', 'critical'],
) as dag:

    consume = PythonOperator(
        task_id='consume_kafka_events',
        python_callable=consume_kafka_events,
    )

    enrich = PythonOperator(
        task_id='enrich_transaction_data',
        python_callable=enrich_transaction_data,
    )

    score = PythonOperator(
        task_id='score_fraud_risk',
        python_callable=score_fraud_risk,
    )

    block_alert = PythonOperator(
        task_id='block_and_alert',
        python_callable=block_and_alert,
    )

    consume >> enrich >> score >> block_alert
```

#### Key Metrics & Outcomes

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Fraud detection time | 48 hours | 5 minutes | 99.9% faster |
| Monthly fraud losses | $2M | $200K | 90% reduction |
| False positive rate | 40% | 10% | 75% reduction |
| Investigation time | 3 days | 2 hours | 97% faster |
| Customer alerts | None | Real-time | New capability |

---

### Scenario 3: Regulatory Reporting Automation

> **Business Context:** A bank must submit 50+ regulatory reports to the central bank (SBV/Fed) with strict deadlines.

#### The Problem

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CURRENT STATE (Before)                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ❌ 50+ reports, each with different data requirements               │
│   ❌ Manual data extraction from 10+ systems                           │
│   ❌ Reports prepared in Excel (error-prone)                          │
│   ❌ Missed deadlines (penalties: $100K+ per incident)                │
│   ❌ Audit trail incomplete (compliance risk)                         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### The Solution: Automated Regulatory Reporting Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│              AUTOMATED REGULATORY REPORTING PIPELINE                    │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                 APACHE AIRFLOW DAGS                              │  │
│   │                 (One DAG per report type)                        │  │
│   │                                                                  │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │ DAG: daily_basel_iii_report                               │ │  │
│   │  │ Schedule: Business days at 6:00 AM                        │ │  │
│   │  │                                                            │ │  │
│   │  │  ┌──────────┐    ┌──────────┐    ┌──────────┐            │ │  │
│   │  │  │ extract_ │    │ calculate│    │ validate_│            │ │  │
│   │  │  │ capital  │───▶│ ratios   │───▶│ report   │            │ │  │
│   │  │  │ data     │    │          │    │          │            │ │  │
│   │  │  └──────────┘    └──────────┘    └────┬─────┘            │ │  │
│   │  │                                       │                  │ │  │
│   │  │                    ┌──────────────────┼──────────────┐   │ │  │
│   │  │                    ▼                  ▼              ▼   │ │  │
│   │  │              ┌──────────┐       ┌──────────┐   ┌──────┐ │ │  │
│   │  │              │ generate │       │ get      │   │submit│ │ │  │
│   │  │              │ PDF      │       │ approval │   │to_sbv│ │ │  │
│   │  │              │ report   │       │          │   │      │ │ │  │
│   │  │              └──────────┘       └──────────┘   └──────┘ │ │  │
│   │  │                                                          │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                                                                  │  │
│   │  ┌────────────────────────────────────────────────────────────┐ │  │
│   │  │ DAG: monthly_aml_report                                    │ │  │
│   │  │ DAG: quarterly_financial_statements                        │ │  │
│   │  │ DAG: annual_audited_reports                                │ │  │
│   │  └────────────────────────────────────────────────────────────┘ │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Complete Airflow DAG for Basel III Report

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'regulatory-team',
    'depends_on_past': True,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email': ['regulatory@bank.com', 'compliance@bank.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
}

def extract_capital_data(**context):
    """Extract capital and risk-weighted assets data."""
    from airflow.hooks.postgres_hook import PostgresHook
    import pandas as pd
    
    pg_hook = PostgresHook(postgres_conn_id='warehouse')
    
    # Extract Tier 1 capital
    tier1 = pg_hook.get_pandas_df("""
        SELECT 
            SUM(common_equity) as cet1_capital,
            SUM(additional_tier1) as at1_capital,
            SUM(cet1_capital + additional_tier1) as tier1_capital
        FROM fact_capital_positions
        WHERE report_date = %s
    """, parameters=[context['ds']])
    
    # Extract Tier 2 capital
    tier2 = pg_hook.get_pandas_df("""
        SELECT SUM(eligible_amount) as tier2_capital
        FROM fact_subordinated_debt
        WHERE report_date = %s
    """, parameters=[context['ds']])
    
    # Extract risk-weighted assets
    rwa = pg_hook.get_pandas_df("""
        SELECT 
            asset_class,
            SUM(rwa_amount) as rwa
        FROM fact_risk_weighted_assets
        WHERE report_date = %s
        GROUP BY asset_class
    """, parameters=[context['ds']])
    
    # Combine
    capital = pd.concat([tier1, tier2], axis=1)
    capital['total_capital'] = capital['tier1_capital'] + capital['tier2_capital']
    capital['total_rwa'] = rwa['rwa'].sum()
    
    capital.to_parquet(f"/tmp/regulatory/basel_iii_{context['ds']}.parquet")
    return capital.to_dict()

def calculate_ratios(**context):
    """Calculate capital adequacy ratios."""
    import pandas as pd
    
    capital = pd.read_parquet(f"/tmp/regulatory/basel_iii_{context['ds']}.parquet")
    
    # Calculate ratios
    ratios = {
        'cet1_ratio': (capital['cet1_capital'].iloc[0] / capital['total_rwa'].iloc[0]) * 100,
        'tier1_ratio': (capital['tier1_capital'].iloc[0] / capital['total_rwa'].iloc[0]) * 100,
        'total_ratio': (capital['total_capital'].iloc[0] / capital['total_rwa'].iloc[0]) * 100,
    }
    
    # Check compliance
    ratios['cet1_compliant'] = ratios['cet1_ratio'] >= 4.5
    ratios['tier1_compliant'] = ratios['tier1_ratio'] >= 6.0
    ratios['total_compliant'] = ratios['total_ratio'] >= 8.0
    ratios['buffer_compliant'] = ratios['total_ratio'] >= 10.5  # +2.5% buffer
    
    if not ratios['buffer_compliant']:
        raise ValueError(f"Capital adequacy below required levels: {ratios}")
    
    context['ti'].xcom_push(key='ratios', value=ratios)
    return ratios

def generate_pdf_report(**context):
    """Generate PDF report for SBV submission."""
    from airflow.hooks.postgres_hook import PostgresHook
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    import pandas as pd
    
    ratios = context['ti'].xcom_pull(task_ids='calculate_ratios', key='ratios')
    
    # Generate PDF
    filename = f"/tmp/reports/basel_iii_{context['ds']}.pdf"
    c = canvas.Canvas(filename, pagesize=A4)
    
    c.setFont("Helvetica-Bold", 16)
    c.drawString(100, 800, "Basel III Capital Adequacy Report")
    c.drawString(100, 780, f"Report Date: {context['ds']}")
    
    c.setFont("Helvetica", 12)
    y = 740
    for key, value in ratios.items():
        c.drawString(100, y, f"{key}: {value}")
        y -= 20
    
    c.save()
    return filename

def submit_to_sbv(**context):
    """Submit report to State Bank of Vietnam."""
    import requests
    from airflow.models import Variable
    
    report_path = context['ti'].xcom_pull(task_ids='generate_pdf_report')
    
    sbv_api_url = Variable.get('sbv_reporting_api_url')
    sbv_api_key = Variable.get('sbv_reporting_api_key')
    
    with open(report_path, 'rb') as f:
        response = requests.post(
            sbv_api_url,
            headers={'Authorization': f'Bearer {sbv_api_key}'},
            files={'report': f},
            data={
                'report_type': 'BASEL_III',
                'report_date': context['ds'],
                'bank_code': 'BANK001'
            }
        )
    
    if response.status_code != 200:
        raise ValueError(f"SBV submission failed: {response.text}")
    
    return response.json()

with DAG(
    'daily_basel_iii_report',
    default_args=default_args,
    description='Daily Basel III Capital Adequacy Report',
    schedule_interval='0 6 * * 1-5',  # Business days at 6 AM
    catchup=False,
    tags=['regulatory', 'basel', 'sbv'],
) as dag:

    extract = PythonOperator(
        task_id='extract_capital_data',
        python_callable=extract_capital_data,
    )

    calculate = PythonOperator(
        task_id='calculate_ratios',
        python_callable=calculate_ratios,
    )

    generate_pdf = PythonOperator(
        task_id='generate_pdf_report',
        python_callable=generate_pdf_report,
    )

    submit = PythonOperator(
        task_id='submit_to_sbv',
        python_callable=submit_to_sbv,
    )

    extract >> calculate >> generate_pdf >> submit
```

#### Key Metrics & Outcomes

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Report preparation time | 5 days | 2 hours | 99% faster |
| Data accuracy | 95% | 99.9% | 99.9% accurate |
| Missed deadlines | 3-4 per year | 0 | 100% on-time |
| Audit findings | 20+ per audit | < 5 | 75% reduction |
| Regulatory penalties | $500K/year | $0 | Eliminated |

---

### Scenario 4: Customer Onboarding Pipeline

> **Business Context:** A bank needs to onboard new customers within 24 hours while meeting KYC/AML requirements.

#### The Problem

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CURRENT STATE (Before)                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ❌ Customer onboarding takes 5-7 days                               │
│   ❌ Manual KYC verification (human review required)                  │
│   ❌ High drop-off rate (40% abandon application)                     │
│   ❌ Compliance errors (incorrectly approved risky customers)         │
│   ❌ Customer complaints about slow process                           │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### The Solution: Automated Onboarding Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                 CUSTOMER ONBOARDING PIPELINE                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   TRIGGER: New Customer Application (API Event)                        │
│                              │                                          │
│                              ▼                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                 APACHE AIRFLOW DAG                               │  │
│   │                 "customer_onboarding"                            │  │
│   │                                                                  │  │
│   │  ┌──────────┐    ┌──────────┐    ┌──────────┐                  │  │
│   │  │ validate │    │ run_kyc  │    │ check_   │                  │  │
│   │  │ application│──▶│ aml     │──▶│ credit   │                  │  │
│   │  └──────────┘    └──────────┘    └────┬─────┘                  │  │
│   │                                       │                        │  │
│   │                    ┌──────────────────┼──────────────┐         │  │
│   │                    ▼                  ▼              ▼         │  │
│   │              ┌──────────┐       ┌──────────┐   ┌──────┐       │  │
│   │              │ APPROVE  │       │ MANUAL   │   │REJECT│       │  │
│   │              │ (auto)   │       │ REVIEW   │   │      │       │  │
│   │              └────┬─────┘       └────┬─────┘   └──────┘       │  │
│   │                   │                  │                        │  │
│   │                   └────────┬─────────┘                        │  │
│   │                            ▼                                   │  │
│   │                     ┌──────────┐                               │  │
│   │                     │ create_  │                               │  │
│   │                     │ account  │                               │  │
│   │                     └────┬─────┘                               │  │
│   │                          │                                     │  │
│   │              ┌───────────┼───────────┐                         │  │
│   │              ▼           ▼           ▼                          │  │
│   │        ┌──────────┐ ┌──────────┐ ┌──────────┐                 │  │
│   │        │ send_    │ │ setup_   │ │ notify_  │                 │  │
│   │        │ welcome  │ │ products │ │ rm_team  │                 │  │
│   │        └──────────┘ └──────────┘ └──────────┘                 │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Complete Airflow DAG for Customer Onboarding

```python
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'customer-onboarding',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['onboarding@bank.com'],
}

def validate_application(**context):
    """Validate customer application data."""
    import pandas as pd
    
    application = context['ti'].xcom_pull(task_ids='receive_application', key='application')
    
    errors = []
    
    # Required fields
    required = ['customer_name', 'email', 'phone', 'id_type', 'id_number']
    for field in required:
        if not application.get(field):
            errors.append(f"Missing required field: {field}")
    
    # Email validation
    if application.get('email') and '@' not in application['email']:
        errors.append("Invalid email format")
    
    if errors:
        raise ValueError(f"Application validation failed: {'; '.join(errors)}")
    
    return True

def run_kyc_aml_check(**context):
    """Run KYC and AML checks."""
    import requests
    from airflow.models import Variable
    
    application = context['ti'].xcom_pull(task_ids='receive_application', key='application')
    
    kyc_api_url = Variable.get('kyc_api_url')
    kyc_api_key = Variable.get('kyc_api_key')
    
    # KYC check
    kyc_response = requests.post(
        f"{kyc_api_url}/kyc/verify",
        json={
            'name': application['customer_name'],
            'id_type': application['id_type'],
            'id_number': application['id_number'],
        },
        headers={'Authorization': f'Bearer {kyc_api_key}'}
    )
    
    # AML check
    aml_response = requests.post(
        f"{kyc_api_url}/aml/screen",
        json={
            'name': application['customer_name'],
            'country': application.get('country', 'VN'),
        },
        headers={'Authorization': f'Bearer {kyc_api_key}'}
    )
    
    results = {
        'kyc_passed': kyc_response.json().get('verified', False),
        'aml_passed': aml_response.json().get('clear', False),
        'risk_score': aml_response.json().get('risk_score', 0),
    }
    
    context['ti'].xcom_push(key='kyc_aml_results', value=results)
    return results

def check_creditworthiness(**context):
    """Check customer credit score and risk profile."""
    from airflow.hooks.postgres_hook import PostgresHook
    import pandas as pd
    
    application = context['ti'].xcom_pull(task_ids='receive_application', key='application')
    kyc_results = context['ti'].xcom_pull(task_ids='run_kyc_aml_check', key='kyc_aml_results')
    
    # Decision logic
    if not kyc_results['kyc_passed']:
        return 'reject'
    
    if not kyc_results['aml_passed']:
        return 'reject'
    
    if kyc_results['risk_score'] > 0.7:
        return 'manual_review'
    
    return 'approve'

def decide_next_step(**context):
    """Branch based on decision."""
    decision = context['ti'].xcom_pull(task_ids='check_creditworthiness')
    
    if decision == 'approve':
        return 'create_account'
    elif decision == 'manual_review':
        return 'send_to_manual_review'
    else:
        return 'send_rejection_email'

def create_account(**context):
    """Create customer account in core banking system."""
    from airflow.hooks.postgres_hook import PostgresHook
    import uuid
    
    application = context['ti'].xcom_pull(task_ids='receive_application', key='application')
    
    pg_hook = PostgresHook(postgres_conn_id='core_banking')
    
    customer_id = f"CUST-{uuid.uuid4().hex[:8].upper()}"
    account_id = f"ACC-{uuid.uuid4().hex[:8].upper()}"
    
    pg_hook.run("""
        INSERT INTO customers (customer_id, name, email, phone, kyc_status, created_at)
        VALUES (%s, %s, %s, %s, 'VERIFIED', NOW())
    """, parameters=[customer_id, application['customer_name'], 
                      application['email'], application['phone']])
    
    pg_hook.run("""
        INSERT INTO accounts (account_id, customer_id, account_type, balance, created_at)
        VALUES (%s, %s, 'SAVINGS', 0, NOW())
    """, parameters=[account_id, customer_id])
    
    context['ti'].xcom_push(key='customer_id', value=customer_id)
    context['ti'].xcom_push(key='account_id', value=account_id)
    
    return {'customer_id': customer_id, 'account_id': account_id}

with DAG(
    'customer_onboarding',
    default_args=default_args,
    description='Customer onboarding pipeline',
    schedule_interval=None,  # Triggered by API event
    catchup=False,
    tags=['customer', 'onboarding', 'kyc'],
) as dag:

    validate = PythonOperator(
        task_id='validate_application',
        python_callable=validate_application,
    )

    kyc_aml = PythonOperator(
        task_id='run_kyc_aml_check',
        python_callable=run_kyc_aml_check,
    )

    credit = PythonOperator(
        task_id='check_creditworthiness',
        python_callable=check_creditworthiness,
    )

    branch = BranchPythonOperator(
        task_id='decide_next_step',
        python_callable=decide_next_step,
    )

    create = PythonOperator(
        task_id='create_account',
        python_callable=create_account,
    )

    manual_review = PythonOperator(
        task_id='send_to_manual_review',
        python_callable=lambda: None,  # Placeholder
    )

    reject = EmailOperator(
        task_id='send_rejection_email',
        to=['{{ ti.xcom_pull(task_ids="receive_application", key="application")["email"] }}'],
        subject='Application Update',
        html_content='Your application requires additional review.',
    )

    validate >> kyc_aml >> credit >> branch
    branch >> [create, manual_review, reject]
```

#### Key Metrics & Outcomes

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Onboarding time | 5-7 days | 4 hours | 97% faster |
| Application drop-off | 40% | 10% | 75% reduction |
| KYC accuracy | 90% | 99% | 99% accurate |
| Customer satisfaction | 60% | 90% | 50% increase |
| Compliance errors | 15% | 1% | 93% reduction |

---

### Scenario 5: ML Model Retraining Pipeline

> **Business Context:** A bank needs to retrain fraud detection models weekly with new data.

#### The Solution: Automated ML Pipeline

```
┌─────────────────────────────────────────────────────────────────────────┐
│                 ML MODEL RETRAINING PIPELINE                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   TRIGGER: Weekly Schedule (Sunday 2:00 AM)                            │
│                              │                                          │
│                              ▼                                          │
│   ┌──────────────────────────────────────────────────────────────────┐  │
│   │                 APACHE AIRFLOW DAG                               │  │
│   │                 "weekly_model_retrain"                           │  │
│   │                                                                  │  │
│   │  ┌──────────┐    ┌──────────┐    ┌──────────┐                  │  │
│   │  │ extract_ │    │ feature_ │    │ train_   │                  │  │
│   │  │ training │──▶│ engineer │──▶│ model    │                  │  │
│   │  │ data     │    │          │    │          │                  │  │
│   │  └──────────┘    └──────────┘    └────┬─────┘                  │  │
│   │                                       │                        │  │
│   │                    ┌──────────────────┼──────────────┐         │  │
│   │                    ▼                  ▼              ▼         │  │
│   │              ┌──────────┐       ┌──────────┐   ┌──────┐       │  │
│   │              │ evaluate │       │ compare  │   │deploy│       │  │
│   │              │ model    │       │ with     │   │ to   │       │  │
│   │              │ metrics  │       │ prod     │   │prod  │       │  │
│   │              └────┬─────┘       └────┬─────┘   └──────┘       │  │
│   │                   │                  │                        │  │
│   │                   └────────┬─────────┘                        │  │
│   │                            ▼                                   │  │
│   │                     ┌──────────┐                               │  │
│   │                     │ notify_  │                               │  │
│   │                     │ ml_team  │                               │  │
│   │                     └──────────┘                               │  │
│   │                                                                  │  │
│   └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Complete Airflow DAG for ML Retraining

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import mlflow

default_args = {
    'owner': 'ml-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'email_on_failure': True,
    'email': ['ml-alerts@bank.com'],
}

def extract_training_data(**context):
    """Extract training data from data lake."""
    from airflow.hooks.postgres_hook import PostgresHook
    import pandas as pd
    
    pg_hook = PostgresHook(postgres_conn_id='warehouse')
    
    # Extract last 90 days of transaction data
    df = pg_hook.get_pandas_df("""
        SELECT 
            txn_id,
            amount,
            customer_age,
            txn_hour,
            merchant_category,
            is_fraud
        FROM fact_fraud_training_data
        WHERE created_at >= NOW() - INTERVAL '90 days'
    """)
    
    df.to_parquet(f"/tmp/ml/training_data_{context['ds']}.parquet")
    return len(df)

def feature_engineering(**context):
    """Create features for model training."""
    import pandas as pd
    import numpy as np
    
    df = pd.read_parquet(f"/tmp/ml/training_data_{context['ds']}.parquet")
    
    # Feature engineering
    df['amount_log'] = np.log1p(df['amount'])
    df['hour_sin'] = np.sin(2 * np.pi * df['txn_hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['txn_hour'] / 24)
    
    # Encode categorical variables
    df = pd.get_dummies(df, columns=['merchant_category'])
    
    df.to_parquet(f"/tmp/ml/features_{context['ds']}.parquet")
    return df.shape[1]

def train_model(**context):
    """Train fraud detection model."""
    import pandas as pd
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    import mlflow
    import joblib
    
    df = pd.read_parquet(f"/tmp/ml/features_{context['ds']}.parquet")
    
    # Split data
    X = df.drop('is_fraud', axis=1)
    y = df['is_fraud']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
    
    # Train model
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # Log with MLflow
    mlflow.set_experiment('fraud_detection')
    with mlflow.start_run():
        mlflow.log_param('n_estimators', 100)
        mlflow.log_metric('accuracy', model.score(X_test, y_test))
        mlflow.sklearn.log_model(model, 'model')
    
    # Save model
    joblib.dump(model, f"/tmp/ml/model_{context['ds']}.pkl")
    return model.score(X_test, y_test)

def evaluate_model(**context):
    """Evaluate model performance."""
    import pandas as pd
    import joblib
    from sklearn.metrics import precision_score, recall_score, f1_score
    
    df = pd.read_parquet(f"/tmp/ml/features_{context['ds']}.parquet")
    model = joblib.load(f"/tmp/ml/model_{context['ds']}.pkl")
    
    X = df.drop('is_fraud', axis=1)
    y = df['is_fraud']
    
    y_pred = model.predict(X)
    
    metrics = {
        'precision': precision_score(y, y_pred),
        'recall': recall_score(y, y_pred),
        'f1': f1_score(y, y_pred),
    }
    
    # Check if model meets threshold
    if metrics['f1'] < 0.7:
        raise ValueError(f"Model performance below threshold: {metrics}")
    
    context['ti'].xcom_push(key='model_metrics', value=metrics)
    return metrics

def deploy_model(**context):
    """Deploy model to production."""
    import shutil
    import os
    
    # Copy model to production location
    source = f"/tmp/ml/model_{context['ds']}.pkl"
    destination = "/models/production/fraud_detector.pkl"
    
    shutil.copy(source, destination)
    
    # Update model metadata
    from datetime import datetime
    metadata = {
        'model_version': context['ds'],
        'deployed_at': datetime.now().isoformat(),
        'metrics': context['ti'].xcom_pull(task_ids='evaluate_model', key='model_metrics'),
    }
    
    import json
    with open('/models/production/model_metadata.json', 'w') as f:
        json.dump(metadata, f)
    
    return metadata

with DAG(
    'weekly_model_retrain',
    default_args=default_args,
    description='Weekly ML model retraining pipeline',
    schedule_interval='0 2 * * 0',  # Sunday at 2 AM
    catchup=False,
    tags=['ml', 'retrain', 'fraud'],
) as dag:

    extract = PythonOperator(
        task_id='extract_training_data',
        python_callable=extract_training_data,
    )

    features = PythonOperator(
        task_id='feature_engineering',
        python_callable=feature_engineering,
    )

    train = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
    )

    evaluate = PythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model,
    )

    deploy = PythonOperator(
        task_id='deploy_model',
        python_callable=deploy_model,
    )

    extract >> features >> train >> evaluate >> deploy
```

---

### Scenario Comparison Matrix

| Aspect | EOD Pipeline | Fraud Detection | Regulatory | Onboarding | ML Retraining |
|--------|--------------|-----------------|------------|------------|---------------|
| **Schedule** | Daily 10 PM | Every 5 min | Business days | Event-driven | Weekly |
| **Latency** | 2 hours | 5 minutes | 2 hours | 4 hours | 3 hours |
| **Complexity** | Medium | High | Medium | High | High |
| **Criticality** | High | Critical | Critical | High | Medium |
| **Key Tools** | Airflow, Spark | Kafka, Flink | Airflow, dbt | Airflow, APIs | MLflow, Spark |
| **Success Metric** | 99% on-time | 99.9% accuracy | 100% compliance | < 4 hours | F1 > 0.7 |

---

## 6. E-Commerce Examples

### Example 1: Multi-Source Data Pipeline

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

with DAG('ecommerce_etl', schedule_interval='@daily') as dag:

    extract_web = GlueJobOperator(
        task_id='extract_web_logs',
        job_name='extract-web-logs',
        script_location='s3://scripts/extract_web.py',
    )

    extract_pos = GlueJobOperator(
        task_id='extract_pos',
        job_name='extract-pos-data',
        script_location='s3://scripts/extract_pos.py',
    )

    extract_inventory = GlueJobOperator(
        task_id='extract_inventory',
        job_name='extract-inventory',
        script_location='s3://scripts/extract_inventory.py',
    )

    @task
    def merge_data(web, pos, inventory):
        return merge_all_sources(web, pos, inventory)

    load = GlueJobOperator(
        task_id='load_to_warehouse',
        job_name='load-warehouse',
    )

    [extract_web, extract_pos, extract_inventory] >> merge_data >> load
```

---

## 5. Hands-On Exercises

### Exercise 1: Build an Airflow DAG
```python
# Task: Create a complete ETL DAG with error handling

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd

# Default arguments with retry and alerting
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email': ['alerts@company.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
}

def extract(**context):
    """Extract data from source."""
    # Simulate extraction
    df = pd.DataFrame({
        'order_id': range(1, 101),
        'amount': [i * 10 for i in range(1, 101)],
        'status': ['completed'] * 80 + ['pending'] * 20
    })
    df.to_parquet(f"/tmp/raw_orders_{context['ds']}.parquet")
    print(f"Extracted {len(df)} records")
    return len(df)

def validate(**context):
    """Validate data quality."""
    df = pd.read_parquet(f"/tmp/raw_orders_{context['ds']}.parquet")
    
    # Quality checks
    assert len(df) > 0, "No data extracted"
    assert df['order_id'].is_unique, "Duplicate order IDs"
    assert df['amount'].notna().all(), "Null amounts"
    assert (df['amount'] > 0).all(), "Negative amounts"
    
    print("Validation passed!")
    return True

def transform(**context):
    """Transform data."""
    df = pd.read_parquet(f"/tmp/raw_orders_{context['ds']}.parquet")
    df['tax'] = df['amount'] * 0.1
    df['total'] = df['amount'] + df['tax']
    df.to_parquet(f"/tmp/processed_orders_{context['ds']}.parquet")
    print(f"Transformed {len(df)} records")

def load(**context):
    """Load to target."""
    df = pd.read_parquet(f"/tmp/processed_orders_{context['ds']}.parquet")
    # In production: load to database
    print(f"Loaded {len(df)} records to target")

with DAG(
    'etl_orders',
    default_args=default_args,
    description='ETL orders pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'orders'],
) as dag:

    t_extract = PythonOperator(task_id='extract', python_callable=extract)
    t_validate = PythonOperator(task_id='validate', python_callable=validate)
    t_transform = PythonOperator(task_id='transform', python_callable=transform)
    t_load = PythonOperator(task_id='load', python_callable=load)
    t_cleanup = BashOperator(
        task_id='cleanup',
        bash_command='rm -f /tmp/raw_orders_{{ ds }}.parquet /tmp/processed_orders_{{ ds }}.parquet'
    )

    t_extract >> t_validate >> t_transform >> t_load >> t_cleanup
```

### Exercise 2: Prefect Workflow
```python
# Task: Build a Prefect workflow with caching

from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def extract_orders():
    """Extract orders with caching."""
    print("Extracting orders...")
    return pd.DataFrame({
        'order_id': range(1, 51),
        'amount': [i * 20 for i in range(1, 51)]
    })

@task
def validate_orders(df):
    """Validate order data."""
    assert len(df) > 0, "No orders found"
    assert df['amount'].min() > 0, "Negative amounts"
    print(f"Validated {len(df)} orders")
    return df

@task
def transform_orders(df):
    """Transform orders."""
    df['tax'] = df['amount'] * 0.08
    df['total'] = df['amount'] + df['tax']
    print(f"Transformed {len(df)} orders")
    return df

@task
def load_orders(df):
    """Load to target."""
    # In production: load to database
    print(f"Loaded {len(df)} orders")
    return True

@flow(name="orders-pipeline")
def orders_pipeline():
    """Main pipeline flow."""
    raw = extract_orders()
    validated = validate_orders(raw)
    transformed = transform_orders(validated)
    load_orders(transformed)
    print("Pipeline completed!")

if __name__ == "__main__":
    orders_pipeline()
```

### Exercise 3: Dagster Asset Pipeline
```python
# Task: Build a Dagster asset-based pipeline

from dagster import asset, AssetExecutionContext, Definitions
import pandas as pd

@asset
def raw_orders(context: AssetExecutionContext):
    """Ingest raw orders."""
    df = pd.DataFrame({
        'order_id': range(1, 101),
        'customer_id': [f'C{i:03d}' for i in range(1, 101)],
        'amount': [i * 15 for i in range(1, 101)]
    })
    context.log.info(f"Ingested {len(df)} orders")
    return df

@asset
def validated_orders(raw_orders):
    """Validate and clean orders."""
    df = raw_orders.copy()
    df = df[df['amount'] > 0]  # Remove invalid
    df = df.drop_duplicates(subset=['order_id'])
    return df

@asset
def order_summary(validated_orders):
    """Create order summary."""
    summary = validated_orders.groupby('customer_id').agg(
        order_count=('order_id', 'count'),
        total_amount=('amount', 'sum'),
        avg_amount=('amount', 'mean')
    ).reset_index()
    return summary

# Define assets
defs = Definitions(
    assets=[raw_orders, validated_orders, order_summary],
)
```

### Exercise 4: Terraform Infrastructure
```hcl
# Task: Define data platform infrastructure

# main.tf

variable "environment" {
  description = "Environment name"
  default     = "dev"
}

variable "project" {
  description = "Project name"
  default     = "data-platform"
}

# S3 Bucket for Data Lake
resource "aws_s3_bucket" "data_lake" {
  bucket = "${var.project}-${var.environment}-datalake"
  
  tags = {
    Environment = var.environment
    Project     = var.project
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Redshift Cluster
resource "aws_redshift_cluster" "warehouse" {
  cluster_identifier = "${var.project}-${var.environment}-warehouse"
  database_name      = "analytics"
  master_username    = "admin"
  master_password    = var.redshift_password
  node_type          = "dc2.large"
  cluster_type       = "single-node"
  
  tags = {
    Environment = var.environment
    Project     = var.project
  }
}

# IAM Role for Redshift
resource "aws_iam_role" "redshift_role" {
  name = "${var.project}-${var.environment}-redshift-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "redshift.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "redshift_s3" {
  name = "redshift-s3-access"
  role = aws_iam_role.redshift_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake.arn,
          "${aws_s3_bucket.data_lake.arn}/*"
        ]
      }
    ]
  })
}
```

### Exercise 5: Monitoring and Alerting
```python
# Task: Implement pipeline monitoring

from datetime import datetime, timedelta
import json


class PipelineMonitor:
    def __init__(self, pipeline_name):
        self.pipeline_name = pipeline_name
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'records_processed': 0,
            'errors': [],
            'status': 'pending'
        }
    
    def start(self):
        """Mark pipeline start."""
        self.metrics['start_time'] = datetime.now().isoformat()
        self.metrics['status'] = 'running'
        print(f"Pipeline {self.pipeline_name} started")
    
    def record_batch(self, count):
        """Record batch processed."""
        self.metrics['records_processed'] += count
        print(f"Processed {count} records (total: {self.metrics['records_processed']})")
    
    def record_error(self, error_msg):
        """Record error."""
        self.metrics['errors'].append({
            'timestamp': datetime.now().isoformat(),
            'message': error_msg
        })
    
    def complete(self):
        """Mark pipeline complete."""
        self.metrics['end_time'] = datetime.now().isoformat()
        self.metrics['status'] = 'completed'
        
        # Calculate duration
        start = datetime.fromisoformat(self.metrics['start_time'])
        end = datetime.fromisoformat(self.metrics['end_time'])
        duration = (end - start).total_seconds()
        
        print(f"\nPipeline {self.pipeline_name} completed!")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Records processed: {self.metrics['records_processed']}")
        print(f"Errors: {len(self.metrics['errors'])}")
        
        return self.metrics
    
    def to_json(self):
        """Export metrics as JSON."""
        return json.dumps(self.metrics, indent=2)


# Test monitoring
def test_monitoring():
    monitor = PipelineMonitor('daily_orders_etl')
    
    monitor.start()
    monitor.record_batch(100)
    monitor.record_batch(150)
    monitor.record_batch(50)
    
    if len(monitor.metrics['errors']) == 0:
        monitor.complete()
    
    print("\nMetrics:")
    print(monitor.to_json())

test_monitoring()
```

---

## 6. Interview Questions

### Q1: Explain Airflow architecture and key concepts.

**Answer:** Airflow consists of: 

1) **Web Server:** UI for monitoring DAGs. 

2) **Scheduler:** Triggers DAG runs based on schedule (Cron). 

3) **Metadata Database:** Stores DAG definitions, run history, task states. 

4) **Workers:** Execute tasks (CeleryExecutor for distributed, LocalExecutor for single-machine). Key concepts: DAG (task dependencies), Operator (task template), Task (instance of operator), Sensor (waits for condition). Airflow is work-centric - it orchestrates compute, not stores data.

### Q2: How do you handle task failures and retries in Airflow?

**Answer:** Configure in default_args: 

1) **retries:** Number of retry attempts (typically 2-3). 

2) **retry_delay:** Time between retries (e.g., 5 minutes). 

3) **retry_exponential_backoff:** Increase delay exponentially. 

4) **on_failure_callback:** Send alerts to Slack/PagerDuty. 

5) **on_retry_callback:** Log retry attempts. 

6) **email_on_failure:** Email notification. 

7) **depends_on_past:** Don't run if previous run failed. Also implement idempotent tasks (safe to retry) and dead letter queues for failed data.

### Q3: Compare Airflow, Prefect, and Dagster.

**Answer:** 

**Airflow:** Mature, large community, Python-based DAGs, good for batch ETL, strong scheduling.缺点: Complex setup, rigid DAG model, limited streaming. 

**Prefect:** Modern, Python-native, dynamic workflows, better error handling, cloud-first. Similar to Airflow but easier to use. 

**Dagster:** Software-defined assets, type system, better testing, asset-centric model. Best for data engineering with strong software practices. Choose Airflow for existing infrastructure, Prefect for modern batch workflows, Dagster for asset-oriented data platforms.

### Q4: How do you implement CI/CD for data pipelines?

**Answer:** 

1) **Version control:** Store DAGs, dbt models, Terraform in Git. 

2) **Testing:** Unit tests for transformations, integration tests for pipelines. 

3) **Code review:** PR reviews for pipeline changes. 

4) **Staging environment:** Test in dev before production. 

5) **dbt:** dbt build --target dev in CI, dbt build --target prod in CD. 

6) **Airflow:** Use DAG versioning, test with irflow dags test. 

7) **Infrastructure:** Terraform for infrastructure as code. 

8) **Monitoring:** Deploy monitoring alongside pipelines.

### Q5: Design an idempotent data pipeline.

**Answer:** Idempotency means running the pipeline multiple times produces the same result. 

Strategies: 

1) **MERGE/UPSERT:** Instead of INSERT, use MERGE to handle duplicates. 

2) **Partition-based:** Overwrite entire partition (date-based). 

3) **Watermarking:** Track processed records, skip already processed. 

4) **Unique constraints:** Database-level uniqueness prevents duplicates. 

5) **Hash-based:** Compute hash of records, only process new hashes. 

6) **Checkpointing:** Store progress, resume from last checkpoint. 

Example: Instead of INSERT INTO target SELECT * FROM source, use MERGE INTO target USING source ON key MATCHED THEN UPDATE ELSE INSERT.

### Q6: How do you monitor and debug failing pipelines?

**Answer:**
1. **Logs:** Check task logs in Airflow UI/Prefect Cloud
2. **Alerts:** Configure email/Slack notifications on failure
3. **Metrics:** Track pipeline duration, record counts, error rates
4. **Dependencies:** Check if upstream tasks/sensors succeeded
5. **Resources:** Verify memory, CPU, network availability
6. **Data:** Validate input data quality and freshness
7. **Retries:** Check if retries exhausted
8. **Code:** Review recent changes (git diff)

Tools: Airflow UI, Datadog, Grafana, PagerDuty, custom dashboards

### Q7: What is the difference between orchestration and workflow automation?

**Answer:**
**Orchestration:**
- Manages complex data pipelines
- Handles scheduling, dependencies, retries
- Focuses on data movement and transformation
- Examples: Airflow, Prefect, Dagster

**Workflow Automation:**
- Automates business processes
- Human-in-the-loop approvals
- Focuses on business logic
- Examples: Zapier, Make, n8n

In data engineering, orchestration is the primary focus.

---

## Summary Checklist

### Apache Airflow
- [ ] Understand Airflow architecture (Web Server, Scheduler, Workers)
- [ ] Create DAGs with dependencies and error handling
- [ ] Use Sensors for event-driven triggers
- [ ] Configure retries and alerting

### Other Orchestration Tools
- [ ] Compare Airflow vs Prefect vs Dagster
- [ ] Know when to use each tool
- [ ] Understand asset-based vs task-based approaches

### CI/CD for Data
- [ ] Implement dbt CI/CD with testing
- [ ] Use Terraform for infrastructure as code
- [ ] Set up automated deployments

### Practical Skills
- [ ] Build ETL pipelines with error handling
- [ ] Implement monitoring and alerting
- [ ] Design idempotent pipelines
- [ ] Debug failing pipelines

---

*Next Section: [13 - Performance Optimization](../13-Performance-Optimization/README.md)*
