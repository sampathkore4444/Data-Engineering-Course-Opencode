# 08 - Cloud Data Platforms

## Table of Contents
1. [AWS Data Services](#1-aws-data-services)
2. [Google Cloud Platform](#2-google-cloud-platform)
3. [Microsoft Azure](#3-microsoft-azure)
4. [Snowflake](#4-snowflake)
5. [Real-World Scenarios](#5-real-world-scenarios)
6. [Hands-On Exercises](#6-hands-on-exercises)
7. [Interview Questions](#7-interview-questions)

---

## 1. AWS Data Services

### Amazon S3 (Simple Storage Service)

Object storage service for data lakes.

**Key Features:**
- Unlimited storage
- 99.999999999% durability
- Multiple storage classes
- Versioning and lifecycle policies

**Tools & Clients:** AWS CLI, AWS Console, boto3 SDK, s3cmd, AWS DataSync

**S3 Operations:**
```python
import boto3

s3 = boto3.client('s3')

# Upload file
s3.upload_file('local_file.csv', 'my-bucket', 'data/file.csv')

# Download file
s3.download_file('my-bucket', 'data/file.csv', 'local_file.csv')

# List objects
response = s3.list_objects_v2(Bucket='my-bucket', Prefix='data/')

# S3 Select (query in place)
response = s3.select_object_content(
    Bucket='my-bucket',
    Key='data/orders.parquet',
    Expression="SELECT * FROM s3object WHERE amount > 1000",
    ExpressionType='SQL',
    InputSerialization={'Parquet': {}},
    OutputSerialization={'CSV': {}}
)
```

### Amazon Athena

Serverless interactive query service for S3 data.

**Key Features:**
- Query data in S3 using standard SQL
- Pay per query
- No infrastructure management
- Supports Parquet, ORC, JSON, CSV

**Tools:** AWS Console, JDBC/ODBC drivers, AWS CLI

### Amazon Redshift

Cloud data warehouse for analytics.

**Architecture:**
```
+--------------------------------------------------+
|                  Redshift Cluster                |
|  +----------+  +----------+  +----------+        |
|  |Leader Node|  |Compute 1 |  |Compute 2 |        |
|  |(Query    |  |Node      |  |Node      |        |
|  |Planning) |  |          |  |          |        |
|  +----------+  +----------+  +----------+        |
|       |              |              |             |
|  +----v--------------v--------------v----+        |
|  |         Distributed Storage          |        |
|  |    (Columnar, Compressed, Encrypted) |        |
|  +--------------------------------------+        |
+--------------------------------------------------+
```

**SQL Examples:**
```sql
-- Create table with distribution and sort keys
CREATE TABLE fact_sales (
    sale_id BIGINT,
    customer_key INT,
    product_key INT,
    sale_date DATE,
    amount DECIMAL(12,2)
)
DISTSTYLE KEY
DISTKEY(customer_key)
COMPOUND SORTKEY(sale_date, customer_key);

-- Copy from S3
COPY fact_sales FROM 's3://my-bucket/data/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftRole'
FORMAT AS PARQUET;

-- Query with predicate pushdown
SELECT customer_key, SUM(amount)
FROM fact_sales
WHERE sale_date BETWEEN '2024-01-01' AND '2024-03-31'
GROUP BY customer_key;

-- Create materialized view
CREATE MATERIALIZED VIEW mv_monthly_sales AS
SELECT 
    DATE_TRUNC('month', sale_date) as month,
    product_key,
    SUM(amount) as total_sales
FROM fact_sales
GROUP BY 1, 2;
```

### AWS Glue

Serverless ETL service.

**Tools:** Glue Studio (visual ETL), Glue DataBrew (data prep), Lake Formation (governance)

```python
import boto3

glue = boto3.client('glue')

# Create database
glue.create_database(DatabaseInput={'Name': 'data_lake'})

# Create table
glue.create_table(
    DatabaseName='data_lake',
    TableInput={
        'Name': 'orders',
        'StorageDescriptor': {
            'Columns': [
                {'Name': 'order_id', 'Type': 'bigint'},
                {'Name': 'customer_id', 'Type': 'string'},
                {'Name': 'amount', 'Type': 'decimal(10,2)'}
            ],
            'Location': 's3://my-bucket/orders/',
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
            }
        },
        'PartitionKeys': [
            {'Name': 'year', 'Type': 'int'},
            {'Name': 'month', 'Type': 'int'}
        ],
        'TableType': 'EXTERNAL_TABLE'
    }
)

# Start ETL job
glue.start_job_run(JobName='process_orders')
```

### Amazon Kinesis

Real-time data streaming service.

**Tools:** Kinesis Data Analytics, Kinesis Data Firehose, CloudWatch (monitoring)

```python
import boto3

kinesis = boto3.client('kinesis')

# Put record
kinesis.put_record(
    StreamName='order-stream',
    Data=json.dumps({'order_id': '001', 'amount': 150.00}),
    PartitionKey='order-001'
)

# Get records
response = kinesis.get_shard_iterator(
    StreamName='order-stream',
    ShardId='shardId-000000000000',
    ShardIteratorType='LATEST'
)
records = kinesis.get_records(ShardIterator=response['ShardIterator'])
```

---

## 2. Google Cloud Platform

### Google BigQuery

Serverless, highly scalable data warehouse.

**Key Features:**
- No infrastructure management
- Pay-per-query or flat-rate pricing
- BigQuery ML integration
- BI Engine for dashboards
- Time travel (7 days)
- External tables

**Tools:** BigQuery Console, bq CLI, DBeaver, Looker Studio

**SQL Examples:**
```sql
-- Create partitioned and clustered table
CREATE TABLE project.dataset.orders
(
    order_id INT64,
    customer_id STRING,
    order_date DATE,
    amount FLOAT64,
    category STRING
)
PARTITION BY order_date
CLUSTER BY customer_id, category;

-- Query with partition pruning
SELECT 
    DATE_TRUNC(order_date, MONTH) as month,
    category,
    SUM(amount) as total_sales
FROM project.dataset.orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-03-31'
  AND category = 'Electronics'
GROUP BY 1, 2;

-- Load from Cloud Storage
LOAD DATA INTO project.dataset.orders
FROM FILES (
    format = 'PARQUET',
    uris = ['gs://my-bucket/orders/*.parquet']
);

-- BigQuery ML
CREATE MODEL project.dataset.sales_forecast
OPTIONS(model_type='ARIMA_PLUS', time_series_timestamp_col='date', time_series_data_col='sales')
AS SELECT date, sales FROM project.dataset.daily_sales;
```

### Google Cloud Dataflow

Apache Beam-based serverless stream/batch processing.

**Tools:** Dataflow UI, Apache Beam SDK, Google Cloud Monitoring

```python
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions([
    '--project', 'my-project',
    '--runner', 'DataflowRunner',
    '--region', 'us-central1',
    '--staging_location', 'gs://my-bucket/staging',
    '--temp_location', 'gs://my-bucket/temp'
])

with beam.Pipeline(options=options) as p:
    orders = (
        p
        | 'Read' >> beam.io.ReadFromBigQuery(query='SELECT * FROM orders')
        | 'Filter' >> beam.Filter(lambda x: x['amount'] > 100)
        | 'Transform' >> beam.Map(lambda x: {**x, 'tax': x['amount'] * 0.1})
        | 'Write' >> beam.io.WriteToBigQuery(
            'project.dataset.processed_orders',
            schema='order_id:STRING, amount:FLOAT, tax:FLOAT',
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
```

### Google Cloud Pub/Sub

Message queuing service (similar to Kafka).

**Tools:** Console, gcloud CLI, client libraries (Python, Java, Go)

```python
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()

# Publish
future = publisher.publish(
    'projects/my-project/topics/orders',
    data=json.dumps({'order_id': '001'}).encode('utf-8'),
    category='electronics'
)
print(future.result())

# Subscribe
def callback(message):
    print(f'Received: {message.data.decode()}')
    message.ack()

subscription = subscriber.subscribe(
    'projects/my-project/subscriptions/orders-sub',
    callback=callback
)
```

---

## 3. Microsoft Azure

### Azure Synapse Analytics

Unified analytics platform combining data warehousing and big data.

**Tools:** Synapse Studio, Azure Data Studio, SQL Server Management Studio

```sql
-- Create dedicated SQL pool
CREATE DATABASE SalesDW;

-- Create table with distribution
CREATE TABLE dbo.factSales
(
    SaleDate DATE,
    ProductKey INT,
    CustomerKey INT,
    StoreKey INT,
    SalesAmount DECIMAL(12,2)
)
WITH
(
    DISTRIBUTION = HASH(ProductKey),
    CLUSTERED COLUMNSTORE INDEX,
    PARTITION (SaleDate RANGE RIGHT FOR VALUES ('2024-01-01', '2024-04-01'))
);

-- Load data
COPY INTO dbo.factSales
FROM 'https://myaccount.dfs.core.windows.net/data/factSales/'
WITH
(
    FILE_TYPE = 'PARQUET',
    IDENTITY_INSERT = 'OFF'
);
```

### Azure Data Factory

Cloud ETL/ELT service for data integration.

**Tools:** ADF Studio, Azure CLI, PowerShell, ARM Templates

**Pipeline Activity (JSON):**
```json
{
    "name": "CopyOrders",
    "properties": {
        "activities": [
            {
                "name": "CopyFromSource",
                "type": "Copy",
                "inputs": [
                    {"referenceName": "AzureBlobDataset", "type": "DatasetReference"}
                ],
                "outputs": [
                    {"referenceName": "SynapseTable", "type": "DatasetReference"}
                ],
                "typeProperties": {
                    "source": {"type": "ParquetSource"},
                    "sink": {"type": "SqlSink"}
                }
            }
        ]
    }
}
```

### Azure Data Lake Storage

Hierarchical namespace-enabled storage for big data analytics.

**Tools:** Azure Storage Explorer, AzCopy, Azure CLI

```
+--------------------------------------------------+
|              Data Lake Storage Gen2              |
+--------------------------------------------------+
|  Raw Zone         |  Curated Zone  |  Serving    |
|  /raw/            |  /curated/     |  /serving/  |
|  - landing/       |  - cleansed/   |  - marts/   |
|  - validated/     |  - enriched/   |  - reports/ |
+--------------------------------------------------+
```

---

## 4. Snowflake

### Architecture

```
+--------------------------------------------------+
|              SERVICES LAYER                      |
|  Query Processing, Metadata, Security,           |
|  Optimization, Data Sharing                      |
+--------------------------------------------------+
              |
+--------------------------------------------------+
|              COMPUTE LAYER                       |
|  +----------+  +----------+  +----------+        |
|  |Warehouse |  |Warehouse |  |Warehouse |        |
|  |   XS     |  |   S      |  |   M      |        |
|  | (BI)     |  | (ETL)    |  | (ML)     |        |
|  +----------+  +----------+  +----------+        |
|  Independent compute clusters                    |
+--------------------------------------------------+
              |
+--------------------------------------------------+
|              STORAGE LAYER                       |
|  Micro-partitions, Columnar, Compressed          |
|  Automatically managed, pay-per-use              |
+--------------------------------------------------+
```

### Key Features

```sql
-- Time travel
SELECT * FROM orders AT (TIMESTAMP => '2024-01-15 10:00:00');

-- Clone table
CREATE TABLE orders_clone CLONE orders;

-- Zero-copy cloning for dev/test
CREATE TABLE dev_orders CLONE prod.orders;

-- Data sharing
CREATE SHARE my_share;
GRANT USAGE ON DATABASE my_db TO SHARE my_share;
GRANT USAGE ON SCHEMA my_db.public TO SHARE my_share;
GRANT SELECT ON TABLE my_db.public.orders TO SHARE my_share;

-- Streams (CDC)
CREATE STREAM orders_stream ON TABLE orders SHOW_INITIAL_ROWS = TRUE;

-- Tasks (scheduling)
CREATE TASK refresh_orders
    WAREHOUSE = compute_wh
    SCHEDULE = 'USING CRON 0 2 * * * America/New_York'
AS
    INSERT INTO orders_refreshed SELECT * FROM orders_stream;

-- Snowpark (Python UDFs)
CREATE OR REPLACE FUNCTION calculate_tax(amount FLOAT)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'tax_calculation'
AS 
def tax_calculation(amount):
    return amount * 0.08
;
```

---

## 5. Real-World Scenarios

### Scenario 1: Multi-Cloud Data Architecture

```
AWS (Primary)              GCP (Analytics)            Azure (BI)
+------------------+      +------------------+      +------------------+
| S3 (Data Lake)   |      | BigQuery (DW)    |      | Power BI         |
| Redshift (EDW)   | ---->|                  | ---->| (Dashboards)     |
| Glue (ETL)       |      | Dataflow (Stream)|      |                  |
| Kinesis (Stream) |      | Pub/Sub          |      |                  |
+------------------+      +------------------+      +------------------+
```

### Scenario 2: Data Lakehouse Architecture

```
Raw Data --> S3/ADLS/GCS --> Delta Lake/Iceberg --> Spark --> BI Tools
              |                    |                 |
              v                    v                 v
         Bronze Layer        Silver Layer        Gold Layer
         (Raw)               (Cleaned)          (Business)
```

---

## 6. Hands-On Exercises

### Exercise 1: S3 Data Lake Setup (AWS)
```python
import boto3
import json
from datetime import datetime

# Task: Create a data lake structure on S3

def create_data_lake_structure(bucket_name):
    """Create medallion architecture data lake."""
    s3 = boto3.client('s3')
    
    # Create bucket
    try:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")
    except Exception as e:
        print(f"Bucket may already exist: {e}")
    
    # Create folder structure
    zones = [
        'bronze/raw/',
        'bronze/landing/',
        'silver/cleansed/',
        'silver/enriched/',
        'gold/marts/',
        'gold/reports/',
        'archive/'
    ]
    
    for zone in zones:
        s3.put_object(Bucket=bucket_name, Key=zone, Body=b'')
        print(f"Created: {zone}")
    
    return True

def upload_to_bronze(bucket_name, file_path, source_name):
    """Upload raw data to bronze zone."""
    s3 = boto3.client('s3')
    timestamp = datetime.now().strftime('%Y/%m/%d')
    key = f"bronze/raw/{source_name}/{timestamp}/{file_path.split('/')[-1]}"
    
    s3.upload_file(file_path, bucket_name, key)
    print(f"Uploaded to: s3://{bucket_name}/{key}")
    return key

# Test
create_data_lake_structure('my-data-lake-2024')
```

### Exercise 2: Redshift Query Optimization
```sql
-- Task: Optimize a slow Redshift query

-- Create sample tables
CREATE TABLE fact_orders (
    order_id BIGINT,
    customer_key INT,
    product_key INT,
    order_date DATE,
    amount DECIMAL(12,2)
)
DISTSTYLE KEY
DISTKEY(customer_key)
COMPOUND SORTKEY(order_date, customer_key);

-- Create dimension table
CREATE TABLE dim_customer (
    customer_key INT,
    customer_name VARCHAR(100),
    segment VARCHAR(20)
)
DISTSTYLE ALL;

-- BEFORE: Slow query (full table scan)
-- SELECT c.customer_name, SUM(o.amount)
-- FROM fact_orders o
-- JOIN dim_customer c ON o.customer_key = c.customer_key
-- WHERE o.order_date >= '2024-01-01'
-- GROUP BY c.customer_name;

-- AFTER: Optimized with distribution and sort keys
EXPLAIN
SELECT c.customer_name, SUM(o.amount)
FROM fact_orders o
JOIN dim_customer c ON o.customer_key = c.customer_key
WHERE o.order_date BETWEEN '2024-01-01' AND '2024-03-31'
GROUP BY c.customer_name
ORDER BY SUM(o.amount) DESC;

-- Create materialized view for common aggregations
CREATE MATERIALIZED VIEW mv_monthly_customer_sales AS
SELECT 
    DATE_TRUNC('month', order_date) as month,
    customer_key,
    SUM(amount) as total_sales,
    COUNT(*) as order_count
FROM fact_orders
GROUP BY 1, 2;

-- Query the materialized view (much faster)
SELECT * FROM mv_monthly_customer_sales
WHERE month >= '2024-01-01';

-- Refresh materialized view
REFRESH MATERIALIZED VIEW mv_monthly_customer_sales;
```

### Exercise 3: BigQuery Partitioning and Clustering
```sql
-- Task: Optimize BigQuery tables for cost and performance

-- Create partitioned and clustered table
CREATE TABLE `project.dataset.orders_optimized`
(
    order_id INT64,
    customer_id STRING,
    order_date DATE,
    amount FLOAT64,
    category STRING,
    region STRING
)
PARTITION BY order_date
CLUSTER BY category, region;

-- Insert sample data
INSERT INTO `project.dataset.orders_optimized`
SELECT 
    order_id,
    customer_id,
    order_date,
    amount,
    category,
    region
FROM `project.dataset.orders_raw`;

-- Query with partition pruning and clustering
SELECT 
    category,
    region,
    SUM(amount) as total_sales
FROM `project.dataset.orders_optimized`
WHERE order_date BETWEEN '2024-01-01' AND '2024-03-31'  -- Partition pruning
  AND category = 'Electronics'  -- Clustering helps
GROUP BY 1, 2;

-- Check query statistics
-- Look at 'Bytes scanned' in the query results
-- Partitioned+clustered table should scan much less data
```

### Exercise 4: Snowflake Time Travel
```sql
-- Task: Use Snowflake time travel for data recovery

-- Create and populate table
CREATE TABLE orders (
    order_id INT,
    amount DECIMAL(10,2),
    status VARCHAR(20)
);

INSERT INTO orders VALUES (1, 100.00, 'completed');
INSERT INTO orders VALUES (2, 200.00, 'completed');

-- Oops! Someone deleted data
DELETE FROM orders WHERE order_id = 1;

-- Check current state
SELECT * FROM orders;  -- Only order 2 exists

-- Query historical data (time travel)
SELECT * FROM orders AT (OFFSET => -60*5);  -- 5 minutes ago

-- Recover the deleted data
CREATE TABLE orders_recovered AS
SELECT * FROM orders AT (OFFSET => -60*5)  -- 5 minutes ago
WHERE order_id = 1;

-- Insert recovered data
INSERT INTO orders SELECT * FROM orders_recovered;

-- Verify recovery
SELECT * FROM orders;  -- Both orders exist

-- Clone table for dev/test (zero-copy)
CREATE TABLE orders_dev CLONE orders;

SELECT * FROM orders_dev;
```

### Exercise 5: Multi-Cloud Data Pipeline
```python
# Task: Design a pipeline that works across clouds

import boto3  # AWS
from google.cloud import bigquery  # GCP
from azure.storage.blob import BlobServiceClient  # Azure

class MultiCloudPipeline:
    def __init__(self):
        self.aws_s3 = boto3.client('s3')
        self.gcp_bq = bigquery.Client()
        self.azure_blob = BlobServiceClient.from_connection_string(
            "DefaultEndpointsProtocol=https;AccountName=..."
        )
    
    def extract_from_aws(self, bucket, key):
        """Extract data from AWS S3."""
        response = self.aws_s3.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    
    def load_to_gcp(self, dataset, table, data):
        """Load data to BigQuery."""
        table_ref = self.gcp_bq.dataset(dataset).table(table)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET
        )
        job = self.gcp_bq.load_table_from_json(
            data, table_ref, job_config=job_config
        )
        job.result()  # Wait for completion
        print(f"Loaded {len(data)} rows to {dataset}.{table}")
    
    def load_to_azure(self, container, blob_name, data):
        """Load data to Azure Blob Storage."""
        blob_client = self.azure_blob.get_blob_client(
            container=container, blob=blob_name
        )
        blob_client.upload_blob(data, overwrite=True)
        print(f"Uploaded to Azure: {container}/{blob_name}")
    
    def run_pipeline(self):
        """Run multi-cloud pipeline."""
        # Extract from AWS
        data = self.extract_from_aws('my-bucket', 'data/orders.parquet')
        
        # Transform (simplified)
        processed_data = self.transform(data)
        
        # Load to GCP (for analytics)
        self.load_to_gcp('analytics', 'orders', processed_data)
        
        # Load to Azure (for BI)
        self.load_to_azure('reports', 'orders/processed.parquet', data)
        
        print("Multi-cloud pipeline completed!")
    
    def transform(self, data):
        """Transform data."""
        # Add transformation logic here
        return data

# Note: In production, use managed services like:
# - AWS Glue, Google Dataflow, Azure Data Factory
# - Or use Fivetran, Airbyte for cross-cloud ingestion
```

---

## 7. Interview Questions

### Q1: Compare Redshift, BigQuery, and Snowflake.

**Answer:** 

**Redshift:** Best for AWS ecosystem, predictable workloads, and organizations needing SQL familiarity. Uses DC2/RA3 nodes, requires cluster management. 

**BigQuery:** Best for serverless operations, ML integration (BQL ML), and organizations wanting zero management. Pay-per-query or flat-rate. 

**Snowflake:** Best for multi-workload environments, data sharing, and time travel features. Unique separation of storage/compute. All support standard SQL; choice depends on cloud ecosystem, management preference, and pricing model.

### Q2: How would you design a data lake on AWS?

**Answer:** 

Use S3 as the foundation with a multi-zone architecture: 

**Raw Zone:** Landing area for source data (original format). 

**Cleaned Zone:** Validated, deduplicated data (Parquet). 

**Curated Zone:** Business-ready data (star schemas). Use AWS Glue for cataloging, Lake Formation for governance, EMR for processing, and Athena/Redshift for querying. Implement lifecycle policies to move old data to cheaper storage (Glacier). Use partitioning (by date) and compression (Snappy) for query performance.

### Q3: What is the advantage of Snowflake's architecture?

**Answer:** 

Snowflake's key advantage is the 

**separation of storage and compute**. You can scale compute independently (virtual warehouses) without affecting storage. 

This means: 

1) Different workloads (BI, ETL, ML) can run on separate warehouses with different sizes. 

2) Concurrency scaling adds warehouses automatically during peak loads. 

3) Time travel and cloning are essentially free (storage-based). 

4) Data sharing is seamless across organizations. The micro-partition format automatically optimizes data layout without manual tuning.

### Q4: When would you use Dataflow vs Spark on Dataproc?

**Answer:** 

**Dataflow** (Apache Beam): Best for serverless stream/batch unification, when you don't want to manage clusters, and for event-driven pipelines. Auto-scaling and auto-healing. 

**Dataproc** (Spark): Best for existing Spark workloads, when you need more control over cluster configuration, and for cost-sensitive batch processing. Use preemptible VMs for lower cost. Choose Dataflow for new streaming projects; choose Dataproc for migrating existing Spark applications or when you need fine-grained cluster control.

### Q5: How do you optimize costs in cloud data platforms?

**Answer:** 

Strategies: 

1) **Right-size compute:** Monitor utilization and adjust warehouse/instance sizes. 

2) **Auto-suspend:** Set warehouses to auto-suspend when idle. 

3) **Spot/preemptible instances:** Use for fault-tolerant batch workloads (60-90% savings). 

4) **Reserved capacity:** Commit to 1-3 years for predictable workloads. 

5) **Storage tiering:** Move old data to cheaper storage classes. 

6) **Query optimization:** Partition tables, use materialized views, avoid SELECT *. 

7) **Monitor costs:** Set up alerts and budgets. 

8) **Use serverless where possible:** Pay only for what you use.

### Q6: What is the medallion architecture and why use it?

**Answer:** The medallion architecture (Bronze-Silver-Gold) organizes data into layers:
- **Bronze (Raw):** Ingested data in original format, no transformations. Full history preserved.
- **Silver (Cleansed):** Validated, deduplicated, schema-enforced data. Intermediate transformations.
- **Gold (Business):** Aggregated, business-ready data for analytics and reporting.

Benefits: Data lineage, quality gates at each layer, separate raw from processed data, enable reprocessing from any layer.

### Q7: How do you handle data governance in cloud platforms?

**Answer:** Key components:
1. **Data Catalog:** AWS Glue Data Catalog, Azure Purview, Google Data Catalog
2. **Lineage:** Track data flow from source to destination
3. **Access Control:** IAM roles, column-level security, row-level security
4. **Encryption:** At rest (KMS) and in transit (TLS)
5. **Monitoring:** Audit logs, access logs, data quality metrics
6. **Compliance:** GDPR, HIPAA, SOC2 compliance tools

---

## Summary Checklist

### AWS Data Services
- [ ] Store and manage data in S3 with lifecycle policies
- [ ] Optimize Redshift with distribution and sort keys
- [ ] Use Glue for serverless ETL and data cataloging
- [ ] Stream data with Kinesis or MSK

### Google Cloud Platform
- [ ] Query data in BigQuery with partitioning and clustering
- [ ] Build streaming pipelines with Dataflow
- [ ] Use Pub/Sub for event-driven architectures

### Microsoft Azure
- [ ] Design Synapse Analytics solutions
- [ ] Build ETL pipelines with Data Factory
- [ ] Store data in Data Lake Storage Gen2

### Snowflake
- [ ] Leverage storage/compute separation
- [ ] Use time travel for data recovery
- [ ] Implement data sharing across organizations

### Cost Optimization
- [ ] Right-size compute resources
- [ ] Use spot/preemptible instances for batch workloads
- [ ] Implement storage tiering
- [ ] Monitor and optimize query costs

### Practical Skills
- [ ] Set up data lake architecture (medallion)
- [ ] Build multi-cloud data pipelines
- [ ] Implement data governance and security
- [ ] Choose the right platform for your use case

---

*Next Section: [09 - Data Streaming](../09-Data-Streaming/README.md)*
