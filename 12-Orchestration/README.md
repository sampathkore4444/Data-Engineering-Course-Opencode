# 12 - Orchestration

## Table of Contents
1. [Apache Airflow](#1-apache-airflow)
2. [Other Orchestration Tools](#2-other-orchestration-tools)
3. [CI/CD for Data](#3-cicd-for-data)
4. [Real-World Scenarios](#4-real-world-scenarios)
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

### Scenario 1: Financial Reporting Pipeline

```
Daily Pipeline Flow:
+--------+    +--------+    +--------+    +--------+    +--------+
|Extract | -> |Validate| -> |Transform| -> |  Load  | -> | Report |
| (6 AM) |    | (6:15) |    | (6:30)  |    | (7:00) |    | (7:30) |
+--------+    +--------+    +---------+    +--------+    +--------+
    |             |              |              |             |
    v             v              v              v             v
  Source       Quality        Business       Data         Regulatory
  Systems      Checks         Logic          Warehouse    Reports
```

---

## 5. Banking Examples

### Example 1: Daily Reconciliation Pipeline

```python
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
```

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
