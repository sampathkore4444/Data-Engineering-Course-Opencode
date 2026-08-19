# 16 - Tools and Technologies Ecosystem

## Table of Contents
1. [Programming Languages](#1-programming-languages)
2. [Data Processing Frameworks](#2-data-processing-frameworks)
3. [Data Storage Formats](#3-data-storage-formats)
4. [Data Integration Platforms](#4-data-integration-platforms)
5. [Data Visualization](#5-data-visualization)
6. [Hands-On Exercises](#6-hands-on-exercises)
7. [Interview Questions](#7-interview-questions)

---

## 1. Programming Languages

### SQL

The most important language for data engineering. Used in virtually every data tool.

```sql
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
```

### Python

Essential for data engineering - used in ETL, data processing, and pipeline development.

```python
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
```

### Other Languages

| Language | Use Case | Tools |
|----------|----------|-------|
| **Java** | Spark, Kafka, Flink | Big data ecosystem |
| **R** | Statistical analysis | RStudio, Shiny |
| **Julia** | High-performance computing | Flux (ML) |
| **Go** | Infrastructure, CLI tools | Terraform, Docker |
| **Rust** | Performance-critical tools | Polars, DataFusion |

### Scala

Used with Apache Spark (Spark is written in Scala).

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder.appName("ETL").getOrCreate()

val df = spark.read.parquet("/data/orders")
val result = df
  .filter(col("amount") > 0)
  .groupBy("customer_id")
  .agg(sum("amount").alias("total_amount"))

result.write.mode("overwrite").parquet("/output/customer_totals")
```

---

## 2. Data Processing Frameworks

### Apache Spark

Distributed processing engine for large-scale data processing.

```
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
```

### Apache Flink

Stream-first processing framework.

```
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
```

### Data Processing Tools Comparison

| Tool | Type | Best For | Language |
|------|------|----------|----------|
| **Apache Spark** | Distributed processing | Large-scale batch ETL | Python, Scala, SQL |
| **Apache Flink** | Stream processing | Real-time event processing | Java, SQL |
| **dbt** | SQL transformation | Analytics engineering | SQL, Python |
| **Apache Beam** | Unified batch+stream | Portable pipelines | Java, Python, Go |
| **Polars** | DataFrame library | Fast single-machine processing | Python, Rust |
| **DuckDB** | Analytical database | Embedded analytics | SQL |

### dbt (Data Build Tool)

SQL-based transformation tool for analytics engineering.

```sql
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
```

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

### Table Format Comparison

| Format | ACID | Time Travel | Schema Evolution | Best For |
|--------|------|-------------|------------------|----------|
| **Delta Lake** | Yes | Yes | Yes | Databricks ecosystem |
| **Apache Iceberg** | Yes | Yes | Yes | Multi-engine support |
| **Apache Hudi** | Yes | Yes | Yes | Incremental processing |

### Parquet Deep Dive

```
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
```

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

```python
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
```

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

```sql
-- Superset uses SQL for chart definitions
SELECT 
    DATE_TRUNC('month', order_date) as month,
    product_category,
    SUM(amount) as revenue
FROM fact_orders
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
```

### Other Visualization Tools

| Tool | Type | Best For |
|------|------|----------|
| **Plotly** | Python library | Interactive dashboards |
| **Streamlit** | Python framework | ML app deployment |
| **Dash** | Python framework | Analytical web apps |
| **Jupyter** | Notebook | Exploration, collaboration |
| **Observable** | JavaScript | Interactive data stories |

### Grafana for Data Monitoring

```json
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
```

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

## 6. Hands-On Exercises

### Exercise 1: Python Data Processing
```python
# Task: Process and analyze sales data with pandas

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Create sample data
def create_sales_data(n_rows=10000):
    dates = [datetime(2024, 1, 1) + timedelta(days=i % 365) for i in range(n_rows)]
    return pd.DataFrame({
        'order_id': range(1, n_rows + 1),
        'customer_id': np.random.randint(1, 1001, n_rows),
        'product_id': np.random.randint(1, 101, n_rows),
        'order_date': dates,
        'amount': np.random.uniform(10, 1000, n_rows).round(2),
        'quantity': np.random.randint(1, 10, n_rows)
    })

df = create_sales_data()

# Solution: Analyze sales by customer
def analyze_sales(df):
    """Perform sales analysis."""
    # Customer summary
    customer_summary = df.groupby('customer_id').agg(
        total_orders=('order_id', 'count'),
        total_amount=('amount', 'sum'),
        avg_order_value=('amount', 'mean'),
        first_order=('order_date', 'min'),
        last_order=('order_date', 'max')
    ).reset_index()
    
    # Monthly trends
    df['month'] = pd.to_datetime(df['order_date']).dt.to_period('M')
    monthly_sales = df.groupby('month').agg(
        orders=('order_id', 'count'),
        revenue=('amount', 'sum')
    ).reset_index()
    
    # Top customers
    top_customers = customer_summary.nlargest(10, 'total_amount')
    
    return {
        'customer_summary': customer_summary,
        'monthly_sales': monthly_sales,
        'top_customers': top_customers
    }

# Run analysis
results = analyze_sales(df)
print("Top 10 Customers:")
print(results['top_customers'].head())
print("\nMonthly Sales:")
print(results['monthly_sales'].head())
```

### Exercise 2: Spark Data Processing
```python
# Task: Process large dataset with PySpark

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark
spark = SparkSession.builder \
    .appName("SalesAnalysis") \
    .master("local[*]") \
    .getOrCreate()

# Create sample data
data = [(i, f'C{i%100:03d}', f'P{i%10:02d}', 
         f'2024-{(i%12)+1:02d}-{(i%28)+1:02d}', 
         float(i * 10), i % 5 + 1)
        for i in range(1, 10001)]

df = spark.createDataFrame(data, 
    ["order_id", "customer_id", "product_id", "order_date", "amount", "quantity"])

# Solution: Window functions and aggregations
window_spec = Window.partitionBy("customer_id").orderBy("order_date")

result = df \
    .withColumn("running_total", sum("amount").over(window_spec)) \
    .withColumn("rank", row_number().over(window_spec)) \
    .groupBy("customer_id") \
    .agg(
        count("order_id").alias("order_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    ) \
    .orderBy(desc("total_amount"))

result.show(10)

# Write to Parquet
result.write.mode("overwrite").parquet("/output/customer_summary")

spark.stop()
```

### Exercise 3: SQL Analytics
```sql
-- Task: Write analytical queries for sales data

-- Create sample tables
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(10,2),
    status VARCHAR(20)
);

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(100),
    segment VARCHAR(20),
    city VARCHAR(50)
);

-- Solution 1: Customer segmentation with RFM
WITH rfm AS (
    SELECT 
        customer_id,
        DATEDIFF(day, MAX(order_date), CURRENT_DATE) as recency,
        COUNT(DISTINCT order_id) as frequency,
        SUM(amount) as monetary
    FROM orders
    WHERE order_date >= DATEADD(year, -1, CURRENT_DATE)
    GROUP BY customer_id
),
rfm_scored AS (
    SELECT 
        *,
        NTILE(5) OVER (ORDER BY recency DESC) as r_score,
        NTILE(5) OVER (ORDER BY frequency) as f_score,
        NTILE(5) OVER (ORDER BY monetary) as m_score
    FROM rfm
)
SELECT 
    customer_id,
    recency,
    frequency,
    monetary,
    r_score + f_score + m_score as rfm_total,
    CASE 
        WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal'
        WHEN r_score >= 4 AND f_score <= 2 THEN 'New'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
        ELSE 'Other'
    END as segment
FROM rfm_scored
ORDER BY rfm_total DESC;

-- Solution 2: Month-over-month growth
WITH monthly AS (
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
    ROUND((revenue - LAG(revenue) OVER (ORDER BY month)) * 100.0 / 
          LAG(revenue) OVER (ORDER BY month), 2) as growth_pct
FROM monthly
ORDER BY month;
```

### Exercise 4: Data Format Conversion
```python
# Task: Convert between data formats

import pandas as pd
import json
import os

# Create sample data
def create_sample_data():
    return pd.DataFrame({
        'order_id': range(1, 1001),
        'customer_id': [f'C{i%100:03d}' for i in range(1, 1001)],
        'amount': [i * 1.5 for i in range(1, 1001)],
        'status': ['completed'] * 800 + ['pending'] * 200
    })

df = create_sample_data()

# Solution: Convert between formats
def convert_formats(df, output_dir='/tmp/formats'):
    """Convert DataFrame to different formats."""
    os.makedirs(output_dir, exist_ok=True)
    
    # CSV
    df.to_csv(f'{output_dir}/orders.csv', index=False)
    print(f"CSV: {os.path.getsize(f'{output_dir}/orders.csv') / 1024:.2f} KB")
    
    # JSON
    df.to_json(f'{output_dir}/orders.json', orient='records', indent=2)
    print(f"JSON: {os.path.getsize(f'{output_dir}/orders.json') / 1024:.2f} KB")
    
    # Parquet with different compression
    for compression in ['snappy', 'gzip', 'zstd']:
        filename = f'{output_dir}/orders_{compression}.parquet'
        df.to_parquet(filename, compression=compression)
        size = os.path.getsize(filename) / 1024
        print(f"Parquet ({compression}): {size:.2f} KB")
    
    # Compare sizes
    print("\nSize Comparison:")
    for fmt in ['csv', 'json']:
        size = os.path.getsize(f'{output_dir}/orders.{fmt}') / 1024
        print(f"  {fmt.upper()}: {size:.2f} KB")

convert_formats(df)
```

---

## 7. Interview Questions

### Q1: When would you choose Spark over pandas?

**Answer:** 

Choose **pandas** when: data fits in memory (< 10GB), single machine is sufficient, rapid prototyping, complex operations that are easier in pandas. 

Choose **Spark** when: data exceeds memory, need distributed processing, production ETL pipelines, need fault tolerance, data is already in distributed storage (S3, HDFS). Spark has higher overhead but scales horizontally; pandas is simpler but limited by single-machine resources.

### Q2: Compare Parquet vs ORC. When to use each?

**Answer:** 

**Parquet:** Best for general analytics, cloud data lakes, Spark/Beam workloads, and organizations wanting ecosystem flexibility. Better compression with Snappy. 

**ORC:** Best for Hive-based analytics, when you need built-in indexes (bloom filters, min/max), and ACID transactions with Hive. Both are excellent columnar formats; Parquet is more widely supported outside Hadoop ecosystem. Most modern data lakes use Parquet.

### Q3: How do you choose between managed and open-source tools?

**Answer:** 

**Choose managed (Fivetran, Snowflake, Databricks):** When you want less operational overhead, faster time-to-value, and have budget. Best for teams without dedicated platform engineering. 

**Choose open-source (Airbyte, Spark, Airflow):** When you want full control, lower long-term costs, and have engineering resources. Best for organizations with strong platform teams. **Hybrid:** Use open-source for core processing, managed for specialized services (e.g., Spark open-source + Snowflake managed warehouse).

### Q4: Explain the modern data stack.

**Answer:** 

The modern data stack typically consists: 

1) **Ingestion:** Fivetran/Airbyte for SaaS, Kafka for streaming. 

2) **Storage:** S3/GCS data lake (raw + processed zones). 

3) **Warehouse:** Snowflake/BigQuery/Redshift for analytics. 

4) **Transformation:** dbt for SQL transformations. 

5) **Orchestration:** Airflow/Prefect for scheduling. 

6) **BI:** Tableau/Power BI/Looker for visualization. 

7) **Quality:** Great Expectations for validation. 

8) **Catalog:** DataHub for discovery. Key principle: each tool does one thing well, assembled like LEGO blocks.

### Q5: What emerging technologies should data engineers learn?

**Answer:** 

Key trends: 

1) **Apache Iceberg/Hudi:** Open table formats replacing proprietary formats. 

2) **dbt:** Becoming standard for analytics engineering. 

3) **Data mesh:** Organizational pattern for scaling data teams. 

4) **Real-time everything:** Streaming-first architectures. 

5) **DataOps/MLOps:** CI/CD for data and ML. 

6) **Serverless:** BigQuery, Snowflake, Lambda for reduced ops. 

7) **AI/ML integration:** Feature stores, ML pipelines. 

8) **Data contracts:** Formal agreements between producers and consumers.

### Q6: What are the must-have tools for a data engineer?

**Answer:** Essential tools:
1. **SQL:** Universal language for data
2. **Python:** ETL, scripting, data processing
3. **Git:** Version control
4. **dbt:** SQL transformations
5. **Airflow/Prefect:** Orchestration
6. **Spark:** Large-scale processing
7. **Cloud platform:** AWS/GCP/Azure
8. **BI tool:** Tableau/Power BI/Superset

Nice to have:
- Kafka for streaming
- Terraform for infrastructure
- Docker for containerization
- Kubernetes for orchestration

### Q7: How do you evaluate new data tools?

**Answer:** Evaluation criteria:
1. **Community:** Size, activity, documentation
2. **Integration:** Works with existing stack
3. **Scalability:** Handles current and future needs
4. **Cost:** License + operational costs
5. **Learning curve:** Team can adopt quickly
6. **Vendor lock-in:** Portability and migration
7. **Performance:** Benchmarks and real-world tests
8. **Support:** Enterprise support options

Process: POC with real use case → measure ROI → team feedback → production pilot

---

## Summary Checklist

### Programming Languages
- [ ] Master SQL (window functions, CTEs, optimization)
- [ ] Learn Python (pandas, PySpark, SQLAlchemy)
- [ ] Understand Scala basics (for Spark)

### Data Processing Frameworks
- [ ] Know when to use Spark vs Flink vs dbt
- [ ] Understand batch vs stream processing
- [ ] Implement transformations with dbt

### Data Storage Formats
- [ ] Compare Parquet vs ORC vs Avro
- [ ] Understand columnar storage benefits
- [ ] Know table formats (Delta Lake, Iceberg, Hudi)

### Data Integration
- [ ] Compare Fivetran vs Airbyte vs Meltano
- [ ] Set up data ingestion pipelines
- [ ] Handle incremental syncs

### Data Visualization
- [ ] Choose appropriate BI tool
- [ ] Build dashboards and reports
- [ ] Understand data storytelling

### Practical Skills
- [ ] Process data with Python and Spark
- [ ] Write analytical SQL queries
- [ ] Convert between data formats
- [ ] Evaluate and select tools for use cases

---

*Next Section: [17 - Projects and Practice](../17-Projects-Practice/README.md)*
