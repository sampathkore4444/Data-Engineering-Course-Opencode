# 12 - Orchestration

## Table of Contents
1. [Apache Airflow](#1-apache-airflow)
2. [Other Orchestration Tools](#2-other-orchestration-tools)
3. [CI/CD for Data](#3-cicd-for-data)
4. [Interview Questions](#4-interview-questions)

---

## 1. Apache Airflow

### Architecture

`
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
`

### Key Concepts

- **DAG (Directed Acyclic Graph):** Definition of task dependencies
- **Operator:** Template for a specific task (BashOperator, PythonOperator, etc.)
- **Task:** Instance of an operator within a DAG
- **Task Instance:** A specific run of a task
- **Connection:** Credentials for external systems
- **Variable:** Key-value pairs for configuration

### Example DAG

`python
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
`

### Sensors

`python
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
`

### Hooks and Connections

`python
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
`

---

## 2. Other Orchestration Tools

### Apache Prefect

`python
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
`

### Dagster

`python
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
`

### Mage

`python
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
`

---

## 3. CI/CD for Data

### dbt CI/CD

`yaml
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

`hcl
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
`

---

## 4. Real-World Scenarios

### Scenario 1: Financial Reporting Pipeline

`
Daily Pipeline Flow:
+--------+    +--------+    +--------+    +--------+    +--------+
|Extract | -> |Validate| -> |Transform| -> |  Load  | -> | Report |
| (6 AM) |    | (6:15) |    | (6:30)  |    | (7:00) |    | (7:30) |
+--------+    +--------+    +---------+    +--------+    +--------+
    |             |              |              |             |
    v             v              v              v             v
  Source       Quality        Business       Data         Regulatory
  Systems      Checks         Logic          Warehouse    Reports
`

---

## 5. Banking Examples

### Example 1: Daily Reconciliation Pipeline

`python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def reconcile_accounts(**context):
    """Compare internal vs external records"""
    pg_hook = PostgresHook(postgres_conn_id='warehouse')
    
    # Get internal balances
    internal = pg_hook.get_pandas_df("""
        SELECT account_id, SUM(debit) as total_debit, SUM(credit) as total_credit
        FROM gl_entries WHERE entry_date = %s
        GROUP BY account_id
    """, parameters=[context['ds']])
    
    # Get external balances (from bank statement)
    external = pg_hook.get_pandas_df("""
        SELECT account_id, balance
        FROM bank_statements WHERE statement_date = %s
    """, parameters=[context['ds']])
    
    # Compare
    merged = internal.merge(external, on='account_id')
    merged['variance'] = merged['total_credit'] - merged['total_debit'] - merged['balance']
    
    # Flag discrepancies
    discrepancies = merged[abs(merged['variance']) > 0.01]
    if len(discrepancies) > 0:
        raise ValueError(f"Reconciliation failed: {len(discrepancies)} discrepancies")
`

---

## 6. E-Commerce Examples

### Example 1: Multi-Source Data Pipeline

`python
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
`

---

## 7. Interview Questions

### Q1: Explain Airflow architecture and key concepts.

**Answer:** Airflow consists of: 1) **Web Server:** UI for monitoring DAGs. 2) **Scheduler:** Triggers DAG runs based on schedule (Cron). 3) **Metadata Database:** Stores DAG definitions, run history, task states. 4) **Workers:** Execute tasks (CeleryExecutor for distributed, LocalExecutor for single-machine). Key concepts: DAG (task dependencies), Operator (task template), Task (instance of operator), Sensor (waits for condition). Airflow is work-centric - it orchestrates compute, not stores data.

### Q2: How do you handle task failures and retries in Airflow?

**Answer:** Configure in default_args: 1) **retries:** Number of retry attempts (typically 2-3). 2) **retry_delay:** Time between retries (e.g., 5 minutes). 3) **retry_exponential_backoff:** Increase delay exponentially. 4) **on_failure_callback:** Send alerts to Slack/PagerDuty. 5) **on_retry_callback:** Log retry attempts. 6) **email_on_failure:** Email notification. 7) **depends_on_past:** Don't run if previous run failed. Also implement idempotent tasks (safe to retry) and dead letter queues for failed data.

### Q3: Compare Airflow, Prefect, and Dagster.

**Answer:** **Airflow:** Mature, large community, Python-based DAGs, good for batch ETL, strong scheduling.缺点: Complex setup, rigid DAG model, limited streaming. **Prefect:** Modern, Python-native, dynamic workflows, better error handling, cloud-first. Similar to Airflow but easier to use. **Dagster:** Software-defined assets, type system, better testing, asset-centric model. Best for data engineering with strong software practices. Choose Airflow for existing infrastructure, Prefect for modern batch workflows, Dagster for asset-oriented data platforms.

### Q4: How do you implement CI/CD for data pipelines?

**Answer:** 1) **Version control:** Store DAGs, dbt models, Terraform in Git. 2) **Testing:** Unit tests for transformations, integration tests for pipelines. 3) **Code review:** PR reviews for pipeline changes. 4) **Staging environment:** Test in dev before production. 5) **dbt:** dbt build --target dev in CI, dbt build --target prod in CD. 6) **Airflow:** Use DAG versioning, test with irflow dags test. 7) **Infrastructure:** Terraform for infrastructure as code. 8) **Monitoring:** Deploy monitoring alongside pipelines.

### Q5: Design an idempotent data pipeline.

**Answer:** Idempotency means running the pipeline multiple times produces the same result. Strategies: 1) **MERGE/UPSERT:** Instead of INSERT, use MERGE to handle duplicates. 2) **Partition-based:** Overwrite entire partition (date-based). 3) **Watermarking:** Track processed records, skip already processed. 4) **Unique constraints:** Database-level uniqueness prevents duplicates. 5) **Hash-based:** Compute hash of records, only process new hashes. 6) **Checkpointing:** Store progress, resume from last checkpoint. Example: Instead of INSERT INTO target SELECT * FROM source, use MERGE INTO target USING source ON key MATCHED THEN UPDATE ELSE INSERT.

---

*Next Section: [13 - Performance Optimization](../13-Performance-Optimization/README.md)*
