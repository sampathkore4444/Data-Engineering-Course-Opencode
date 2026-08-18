# 17 - Projects and Practice

## Table of Contents
1. [Beginner Projects](#1-beginner-projects)
2. [Intermediate Projects](#2-intermediate-projects)
3. [Advanced Projects](#3-advanced-projects)
4. [Capstone Projects](#4-capstone-projects)
5. [Portfolio Tips](#5-portfolio-tips)

---

## 1. Beginner Projects

### Project 1: E-Commerce Data Warehouse

**Objective:** Design and implement a data warehouse for an e-commerce company.

**Requirements:**
- Star schema design (fact and dimension tables)
- ETL pipeline from CSV/JSON sources
- SCD Type 2 for customer dimension
- Daily incremental loads
- Basic BI dashboards

**Data Sources:**
`
- orders.csv (order_id, customer_id, order_date, amount)
- customers.csv (customer_id, name, email, city, segment)
- products.csv (product_id, name, category, price)
- order_items.csv (order_id, product_id, quantity, unit_price)
`

**Implementation:**
`python
# Step 1: Create dimension tables
def create_dim_customer(df):
    df['customer_key'] = range(1, len(df) + 1)
    df['effective_date'] = pd.Timestamp.now()
    df['expiry_date'] = pd.Timestamp('9999-12-31')
    df['is_current'] = True
    return df

# Step 2: Create fact table
def create_fact_orders(orders_df, customers_df, products_df):
    fact = orders_df.merge(customers_df[['customer_id', 'customer_key']], on='customer_id')
    fact = fact.merge(products_df[['product_id', 'product_key']], on='product_id')
    return fact[['order_id', 'customer_key', 'product_key', 'order_date_key', 'amount']]

# Step 3: Incremental load
def incremental_load(new_data, existing_data):
    # SCD Type 2 logic
    changed = new_data.merge(existing_data, on='customer_id', suffixes=('', '_old'))
    changed = changed[changed['hash'] != changed['hash_old']]
    
    # Expire old records
    existing_data.loc[existing_data['customer_id'].isin(changed['customer_id']), 'is_current'] = False
    
    # Insert new versions
    new_versions = create_dim_customer(changed)
    return pd.concat([existing_data, new_versions])
`

**Deliverables:**
- [ ] Star schema ERD diagram
- [ ] ETL pipeline code (Python/SQL)
- [ ] Data quality checks
- [ ] README with setup instructions

---

### Project 2: Data Quality Framework

**Objective:** Build a reusable data quality checking framework.

**Requirements:**
- Schema validation
- Completeness checks
- Uniqueness checks
- Range validation
- Custom business rules
- Reporting dashboard

**Implementation:**
`python
import pandas as pd
from datetime import datetime

class DataQualityChecker:
    def __init__(self, df, table_name):
        self.df = df
        self.table_name = table_name
        self.results = []
    
    def check_completeness(self, columns, threshold=0.95):
        for col in columns:
            completeness = self.df[col].notna().mean()
            self.results.append({
                'check': 'completeness',
                'column': col,
                'value': completeness,
                'threshold': threshold,
                'status': 'PASS' if completeness >= threshold else 'FAIL'
            })
    
    def check_uniqueness(self, columns):
        duplicates = self.df.duplicated(subset=columns).sum()
        self.results.append({
            'check': 'uniqueness',
            'column': str(columns),
            'duplicates': duplicates,
            'status': 'PASS' if duplicates == 0 else 'FAIL'
        })
    
    def check_range(self, column, min_val, max_val):
        out_of_range = ((self.df[column] < min_val) | (self.df[column] > max_val)).sum()
        self.results.append({
            'check': 'range',
            'column': column,
            'out_of_range': out_of_range,
            'status': 'PASS' if out_of_range == 0 else 'FAIL'
        })
    
    def generate_report(self):
        report = pd.DataFrame(self.results)
        report['timestamp'] = datetime.now()
        report['table'] = self.table_name
        return report

# Usage
checker = DataQualityChecker(df, 'orders')
checker.check_completeness(['order_id', 'customer_id', 'amount'], threshold=0.99)
checker.check_uniqueness(['order_id'])
checker.check_range('amount', min_val=0, max_val=1000000)
report = checker.generate_report()
`

---

## 2. Intermediate Projects

### Project 3: Real-Time Inventory Dashboard

**Objective:** Build a real-time inventory monitoring system.

**Architecture:**
`
Source Systems --> Kafka --> Flink --> Redis --> Dashboard
                   |
                   +--> Data Lake (S3)
`

**Requirements:**
- Kafka producer for inventory events
- Flink consumer for real-time processing
- Redis for low-latency reads
- Simple web dashboard

**Implementation:**
`python
# Kafka Producer
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def publish_inventory_event(product_id, warehouse_id, quantity_change):
    event = {
        'product_id': product_id,
        'warehouse_id': warehouse_id,
        'quantity_change': quantity_change,
        'timestamp': datetime.now().isoformat()
    }
    producer.send('inventory-events', key=str(product_id).encode(), value=json.dumps(event).encode())

# Flink Processing (SQL)
# CREATE TABLE inventory_events (
#     product_id STRING,
#     warehouse_id STRING,
#     quantity_change INT,
#     event_time TIMESTAMP(3),
#     WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
# ) WITH (
#     'connector' = 'kafka',
#     'topic' = 'inventory-events',
#     'properties.bootstrap.servers' = 'localhost:9092',
#     'format' = 'json'
# );
# 
# SELECT 
#     product_id,
#     SUM(quantity_change) as net_change,
#     TUMBLE_START(event_time, INTERVAL '1' MINUTE) as window_start
# FROM inventory_events
# GROUP BY product_id, TUMBLE(event_time, INTERVAL '1' MINUTE);
`

---

### Project 4: dbt Analytics Engineering

**Objective:** Implement a complete dbt project for analytics.

**Project Structure:**
`
my_dbt_project/
+-- models/
|   +-- staging/
|   |   +-- stg_orders.sql
|   |   +-- stg_customers.sql
|   |   +-- schema.yml
|   +-- marts/
|   |   +-- fact_orders.sql
|   |   +-- dim_customers.sql
|   |   +-- schema.yml
+-- tests/
+-- dbt_project.yml
+-- profiles.yml
`

**Implementation:**
`yaml
# dbt_project.yml
name: 'my_analytics'
version: '1.0.0'
config-version: 2

profile: 'my_analytics'

models:
  my_analytics:
    staging:
      +materialized: view
    marts:
      +materialized: table

# models/staging/schema.yml
version: 2
models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id
`

`sql
-- models/marts/fact_orders.sql
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    o.order_id,
    o.order_date,
    c.customer_name,
    c.segment,
    o.amount,
    o.amount - o.cost as profit
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
`

---

## 3. Advanced Projects

### Project 5: Data Lakehouse with Delta Lake

**Objective:** Build a production data lakehouse with ACID transactions.

**Architecture:**
`
Raw Data --> S3 (Bronze) --> Spark --> Delta (Silver) --> Spark --> Delta (Gold)
                                          |
                                    Schema enforcement
                                    ACID transactions
                                    Time travel
`

**Implementation:**
`python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataLakehouse") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Bronze: Raw ingestion
df_bronze = spark.read.json("/raw/orders/")
df_bronze.write.format("delta").mode("append").partitionBy("year", "month").save("/bronze/orders")

# Silver: Deduplicated and validated
df_silver = spark.read.format("delta").load("/bronze/orders") \
    .dropDuplicates(["order_id"]) \
    .filter("amount > 0 AND customer_id IS NOT NULL")

# Create Delta table with schema enforcement
DeltaTable.createIfNotExists(spark) \
    .tableName("silver.orders") \
    .add("order_id", "STRING") \
    .add("customer_id", "STRING") \
    .add("amount", "DECIMAL(10,2)") \
    .add("order_date", "DATE") \
    .partitionedBy("order_date") \
    .execute()

# Gold: Business aggregates
df_gold = spark.sql("""
    SELECT 
        DATE_TRUNC('day', order_date) as order_day,
        customer_id,
        COUNT(*) as order_count,
        SUM(amount) as total_amount
    FROM silver.orders
    GROUP BY 1, 2
""")
df_gold.write.format("delta").mode("overwrite").save("/gold/customer_daily")

# Time travel
df_yesterday = spark.read.format("delta").option("versionAsOf", 5).load("/silver/orders")
`

---

### Project 6: Apache Airflow Pipeline

**Objective:** Build a production ETL pipeline with Airflow.

**Implementation:**
`python
from airflow import DAG
from airflow.operators.python import PythonOperator
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

def extract_orders(**context):
    import pandas as pd
    from sqlalchemy import create_engine
    
    engine = create_engine('postgresql://user:pass@source-db/orders')
    df = pd.read_sql(
        "SELECT * FROM orders WHERE order_date = %s",
        engine,
        params=[context['ds']]
    )
    df.to_parquet(f"/tmp/orders_{context['ds']}.parquet")
    return len(df)

def validate_orders(**context):
    import pandas as pd
    df = pd.read_parquet(f"/tmp/orders_{context['ds']}.parquet")
    
    assert len(df) > 0, "No orders found"
    assert df['order_id'].is_unique, "Duplicate order IDs"
    assert (df['amount'] > 0).all(), "Negative amounts"

with DAG(
    'daily_orders_etl',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders,
    )

    validate = PythonOperator(
        task_id='validate_orders',
        python_callable=validate_orders,
    )

    load_to_s3 = BashOperator(
        task_id='upload_to_s3',
        bash_command='aws s3 cp /tmp/orders_{{ ds }}.parquet s3://data-lake/orders/{{ ds }}/',
    )

    load_to_redshift = S3ToRedshiftOperator(
        task_id='load_to_redshift',
        schema='public',
        table='fact_orders',
        s3_bucket='data-lake',
        s3_key='orders/{{ ds }}/',
        copy_options=['FORMAT AS PARQUET'],
    )

    extract >> validate >> load_to_s3 >> load_to_redshift
`

---

## 4. Capstone Projects

### Project 7: End-to-End Data Platform

**Objective:** Build a complete data platform with all components.

**Architecture:**
`
+--------------------------------------------------+
|              DATA PLATFORM CAPSTONE              |
+--------------------------------------------------+
|                                                  |
|  Sources:  MySQL | API | Files | Streaming      |
|              |      |      |          |          |
|              v      v      v          v          |
|  Ingestion: Kafka | Airbyte | S3 Event           |
|              |      |      |          |          |
|              v      v      v          v          |
|  Storage: S3 (Bronze) --> Delta (Silver/Gold)    |
|                                                  |
|  Processing: Spark | dbt | Flink                 |
|                                                  |
|  Warehouse: Snowflake / Redshift                 |
|                                                  |
|  Orchestration: Airflow                          |
|                                                  |
|  Quality: Great Expectations                     |
|                                                  |
|  Catalog: OpenMetadata                           |
|                                                  |
|  BI: Superset / Tableau                          |
+--------------------------------------------------+
`

**Deliverables:**
- [ ] Architecture diagram
- [ ] Infrastructure as Code (Terraform)
- [ ] ETL pipelines (Airflow DAGs)
- [ ] Data models (dbt)
- [ ] Quality checks (Great Expectations)
- [ ] Documentation
- [ ] Monitoring dashboards

---

## 5. Portfolio Tips

### GitHub Repository Structure

`
data-engineering-portfolio/
+-- README.md
+-- project-1-ecommerce-dw/
|   +-- README.md
|   +-- sql/
|   +-- python/
|   +-- diagrams/
+-- project-2-realtime-inventory/
|   +-- README.md
|   +-- kafka/
|   +-- flink/
+-- project-3-data-lakehouse/
|   +-- README.md
|   +-- spark/
|   +-- delta/
+-- project-4-airflow-pipeline/
|   +-- README.md
|   +-- dags/
|   +-- tests/
`

### Project README Template

`markdown
# Project Name

## Overview
Brief description of the project and its purpose.

## Architecture
![Architecture](diagrams/architecture.png)

## Technologies
- Apache Spark
- Delta Lake
- Apache Airflow
- PostgreSQL

## Setup
### Prerequisites
- Python 3.9+
- Docker

### Installation
`ash
pip install -r requirements.txt
docker-compose up -d
`

## Usage
`ash
python run_pipeline.py
`

## Results
- Processed 1M+ records
- Reduced processing time by 75%
- Achieved 99.9% data quality score

## Lessons Learned
- Key insights and challenges faced
`

### Skills to Highlight

| Skill | Project Evidence |
|-------|-----------------|
| SQL | Complex queries, window functions |
| Python | ETL scripts, data processing |
| Spark | Distributed processing |
| dbt | Analytics engineering |
| Airflow | Pipeline orchestration |
| Kafka | Real-time streaming |
| Cloud (AWS/GCP) | Cloud data services |
| Data Modeling | Star schema, Data Vault |
| Data Quality | Great Expectations |

---

## Interview Preparation

### Common Interview Topics

1. **SQL:** Window functions, CTEs, optimization
2. **Data Modeling:** Star schema, SCD, normalization
3. **Spark:** Transformations, actions, optimization
4. **Kafka:** Architecture, exactly-once semantics
5. **Airflow:** DAGs, operators, best practices
6. **Data Warehouse:** Redshift/Snowflake/BigQuery
7. **Data Lake:** Delta Lake, Iceberg, Hudi
8. **System Design:** End-to-end pipeline design

### Practice Resources

| Resource | Purpose |
|----------|---------|
| LeetCode (SQL) | SQL practice |
| HackerRank (SQL) | SQL challenges |
| StrataScratch | Real interview questions |
| DataLemur | SQL interview prep |
| GitHub | Open source contributions |

---

*Congratulations on completing the Data Engineering course! Remember: the best way to learn is by building projects and solving real problems.*
