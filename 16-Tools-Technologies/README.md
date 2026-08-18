# 16 - Tools and Technologies Ecosystem

## Table of Contents
1. [Programming Languages](#1-programming-languages)
2. [Data Processing Frameworks](#2-data-processing-frameworks)
3. [Data Storage Formats](#3-data-storage-formats)
4. [Data Integration Platforms](#4-data-integration-platforms)
5. [Data Visualization](#5-data-visualization)

---

## 1. Programming Languages

### SQL

The most important language for data engineering. Used in virtually every data tool.

`sql
-- Window functions for analytics
SELECT 
    customer_id,
    order_date,
    amount,
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) as running_total,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC) as rank
FROM orders;

-- CTEs for complex logic
WITH monthly_metrics AS (
    SELECT 
        DATE_TRUNC('month', order_date) as month,
        SUM(amount) as revenue
    FROM orders
    GROUP BY 1
)
SELECT 
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) as prev_month,
    revenue - LAG(revenue) OVER (ORDER BY month) as mom_change
FROM monthly_metrics;
`

### Python

Essential for data engineering - used in ETL, data processing, and pipeline development.

`python
# pandas for data manipulation
import pandas as pd

df = pd.read_parquet('data/orders.parquet')
df_clean = df.dropna(subset=['customer_id'])
df_aggregated = df.groupby('customer_id')['amount'].sum().reset_index()

# pyspark for large-scale processing
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("ETL").getOrCreate()
df = spark.read.parquet("/data/orders")
result = df.filter(col("amount") > 0).groupBy("customer_id").agg(sum("amount"))

# SQLAlchemy for database operations
from sqlalchemy import create_engine
engine = create_engine('postgresql://user:pass@host/db')
df.to_sql('fact_orders', engine, if_exists='append', index=False)
`

### Scala

Used with Apache Spark (Spark is written in Scala).

`scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("ETL").getOrCreate()

val df = spark.read.parquet("/data/orders")
val result = df
  .filter(col("amount") > 0)
  .groupBy("customer_id")
  .agg(sum("amount").alias("total_amount"))

result.write.mode("overwrite").parquet("/output/customer_totals")
`

---

## 2. Data Processing Frameworks

### Apache Spark

Distributed processing engine for large-scale data processing.

`
Use Cases:
- ETL pipelines (batch)
- Data transformation
- Machine learning
- SQL analytics

When to Use:
- Data > 100GB
- Complex transformations
- Need distributed processing
- ML workloads
`

### Apache Flink

Stream-first processing framework.

`
Use Cases:
- Real-time event processing
- Complex event processing
- Stateful stream processing
- Low-latency applications

When to Use:
- Sub-second latency required
- Event-time processing
- Complex windowing
- Exactly-once semantics needed
`

### dbt (Data Build Tool)

SQL-based transformation tool for analytics engineering.

`sql
-- models/marts/fact_orders.sql
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),
products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    o.order_id,
    o.order_date,
    c.customer_name,
    c.segment,
    p.product_name,
    p.category,
    o.quantity,
    o.amount,
    o.amount - (o.quantity * p.unit_cost) as profit
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
`

---

## 3. Data Storage Formats

### Format Comparison

| Format | Type | Compression | Column Pruning | Schema Evolution | Best For |
|--------|------|-------------|---------------|-----------------|----------|
| CSV | Text | None | No | No | Data exchange |
| JSON | Text | Low | No | Partial | APIs, configs |
| Parquet | Columnar | Excellent | Yes | Yes | Analytics |
| ORC | Columnar | Excellent | Yes | Yes | Hive |
| Avro | Row | Good | No | Yes | Streaming |
| Protobuf | Row | Excellent | No | Yes | APIs |

### Parquet Deep Dive

`
Parquet File Structure:
+------------------------------------------+
| Row Group 1                              |
| +--------------------------------------+ |
| | Column 1: [val1, val2, val3, ...]   | |
| | Column 2: [val1, val2, val3, ...]   | |
| | Column 3: [val1, val2, val3, ...]   | |
| +--------------------------------------+ |
| Statistics (min/max/count)               |
+------------------------------------------+
| Row Group 2                              |
| +--------------------------------------+ |
| | Column 1: [val1, val2, val3, ...]   | |
| +--------------------------------------+ |
+------------------------------------------+
| Footer (metadata, offsets)               |
+------------------------------------------+

Benefits:
- Only reads required columns (column pruning)
- Compression per column (similar values compress well)
- Predicate pushdown (skip entire row groups)
- Schema evolution (add/remove columns)
`

---

## 4. Data Integration Platforms

### Fivetran vs Airbyte vs Meltano

| Feature | Fivetran | Airbyte | Meltano |
|---------|----------|---------|---------|
| Type | Managed SaaS | Open-source + Cloud | Open-source |
| Connectors | 300+ | 300+ | 200+ |
| Setup | Zero config | Config required | Config required |
| Cost | Higher (per row) | Lower | Free (self-host) |
| Custom | Limited | Yes (CDK) | Yes (SDK) |
| Best for | Enterprise | Teams wanting control | Budget-conscious |

### Airbyte Example

`python
# Airbyte connector config
{
    "source": {
        "type": "postgres",
        "config": {
            "host": "source-db.example.com",
            "port": 5432,
            "database": "production",
            "user": "airbyte",
            "password": "secret"
        }
    },
    "destination": {
        "type": "snowflake",
        "config": {
            "host": "account.snowflakecomputing.com",
            "database": "DATA_LAKE",
            "schema": "RAW",
            "warehouse": "COMPUTE_WH",
            "role": "AIRBYTE_ROLE"
        }
    },
    "sync": {
        "catalog": {
            "streams": [{
                "name": "orders",
                "syncMode": "incremental",
                "cursorField": ["updated_at"],
                "destinationSyncMode": "append_dedup"
            }]
        }
    }
}
`

---

## 5. Data Visualization

### BI Tools Comparison

| Tool | Type | Best For | Cost |
|------|------|----------|------|
| Tableau | Commercial | Enterprise BI, advanced viz | High |
| Power BI | Commercial | Microsoft ecosystem | Medium |
| Looker | Commercial | Google ecosystem, code-based | High |
| Superset | Open-source | Self-hosted, SQL-based | Free |
| Metabase | Open-source | Simple dashboards, embedding | Free |

### Apache Superset

`sql
-- Superset uses SQL for chart definitions
SELECT 
    DATE_TRUNC('month', order_date) as month,
    product_category,
    SUM(amount) as revenue
FROM fact_orders
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
`

### Grafana for Data Monitoring

`json
{
    "dashboard": {
        "title": "Data Pipeline Dashboard",
        "panels": [
            {
                "title": "Pipeline Runs",
                "type": "stat",
                "targets": [{
                    "expr": "sum(pipeline_runs_total{status='success'})",
                    "legendFormat": "Successful"
                }]
            }
        ]
    }
}
`

---

## 6. Technology Decision Matrix

| Need | Recommended Tools |
|------|-------------------|
| Cloud Data Warehouse | Snowflake, BigQuery, Redshift |
| Data Lake Storage | S3, GCS, ADLS |
| Batch Processing | Spark, dbt, Glue |
| Stream Processing | Kafka, Flink, Kinesis |
| Orchestration | Airflow, Prefect, Dagster |
| Data Quality | Great Expectations, dbt tests |
| Data Catalog | DataHub, OpenMetadata |
| BI/Visualization | Tableau, Power BI, Superset |
| ETL/ELT | dbt, Fivetran, Airbyte |
| Infrastructure | Terraform, CloudFormation |

---

## 7. Interview Questions

### Q1: When would you choose Spark over pandas?

**Answer:** Choose **pandas** when: data fits in memory (< 10GB), single machine is sufficient, rapid prototyping, complex operations that are easier in pandas. Choose **Spark** when: data exceeds memory, need distributed processing, production ETL pipelines, need fault tolerance, data is already in distributed storage (S3, HDFS). Spark has higher overhead but scales horizontally; pandas is simpler but limited by single-machine resources.

### Q2: Compare Parquet vs ORC. When to use each?

**Answer:** **Parquet:** Best for general analytics, cloud data lakes, Spark/Beam workloads, and organizations wanting ecosystem flexibility. Better compression with Snappy. **ORC:** Best for Hive-based analytics, when you need built-in indexes (bloom filters, min/max), and ACID transactions with Hive. Both are excellent columnar formats; Parquet is more widely supported outside Hadoop ecosystem. Most modern data lakes use Parquet.

### Q3: How do you choose between managed and open-source tools?

**Answer:** **Choose managed (Fivetran, Snowflake, Databricks):** When you want less operational overhead, faster time-to-value, and have budget. Best for teams without dedicated platform engineering. **Choose open-source (Airbyte, Spark, Airflow):** When you want full control, lower long-term costs, and have engineering resources. Best for organizations with strong platform teams. **Hybrid:** Use open-source for core processing, managed for specialized services (e.g., Spark open-source + Snowflake managed warehouse).

### Q4: Explain the modern data stack.

**Answer:** The modern data stack typically consists: 1) **Ingestion:** Fivetran/Airbyte for SaaS, Kafka for streaming. 2) **Storage:** S3/GCS data lake (raw + processed zones). 3) **Warehouse:** Snowflake/BigQuery/Redshift for analytics. 4) **Transformation:** dbt for SQL transformations. 5) **Orchestration:** Airflow/Prefect for scheduling. 6) **BI:** Tableau/Power BI/Looker for visualization. 7) **Quality:** Great Expectations for validation. 8) **Catalog:** DataHub for discovery. Key principle: each tool does one thing well, assembled like LEGO blocks.

### Q5: What emerging technologies should data engineers learn?

**Answer:** Key trends: 1) **Apache Iceberg/Hudi:** Open table formats replacing proprietary formats. 2) **dbt:** Becoming standard for analytics engineering. 3) **Data mesh:** Organizational pattern for scaling data teams. 4) **Real-time everything:** Streaming-first architectures. 5) **DataOps/MLOps:** CI/CD for data and ML. 6) **Serverless:** BigQuery, Snowflake, Lambda for reduced ops. 7) **AI/ML integration:** Feature stores, ML pipelines. 8) **Data contracts:** Formal agreements between producers and consumers.

---

*Next Section: [17 - Projects and Practice](../17-Projects-Practice/README.md)*
