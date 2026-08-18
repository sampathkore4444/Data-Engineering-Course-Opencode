# 08 - Cloud Data Platforms

## Table of Contents
1. [AWS Data Services](#1-aws-data-services)
2. [Google Cloud Platform](#2-google-cloud-platform)
3. [Microsoft Azure](#3-microsoft-azure)
4. [Snowflake](#4-snowflake)
5. [Interview Questions](#5-interview-questions)

---

## 1. AWS Data Services

### Amazon S3 (Simple Storage Service)

Object storage service for data lakes.

**Key Features:**
- Unlimited storage
- 99.999999999% durability
- Multiple storage classes
- Versioning and lifecycle policies

**S3 Operations:**
`python
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
`

### Amazon Redshift

Cloud data warehouse for analytics.

**Architecture:**
`
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
`

**SQL Examples:**
`sql
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
`

### AWS Glue

Serverless ETL service.

`python
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
`

### Amazon Kinesis

Real-time data streaming service.

`python
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
`

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

**SQL Examples:**
`sql
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
`

### Google Cloud Dataflow

Apache Beam-based serverless stream/batch processing.

`python
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
`

### Google Cloud Pub/Sub

Message queuing service (similar to Kafka).

`python
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
`

---

## 3. Microsoft Azure

### Azure Synapse Analytics

Unified analytics platform combining data warehousing and big data.

`sql
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
`

### Azure Data Factory

Cloud ETL/ELT service for data integration.

**Pipeline Activity (JSON):**
`json
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
`

### Azure Data Lake Storage

Hierarchical namespace-enabled storage for big data analytics.

`
+--------------------------------------------------+
|              Data Lake Storage Gen2              |
+--------------------------------------------------+
|  Raw Zone         |  Curated Zone  |  Serving    |
|  /raw/            |  /curated/     |  /serving/  |
|  - landing/       |  - cleansed/   |  - marts/   |
|  - validated/     |  - enriched/   |  - reports/ |
+--------------------------------------------------+
`

---

## 4. Snowflake

### Architecture

`
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
`

### Key Features

`sql
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
`

---

## 5. Interview Questions

### Q1: Compare Redshift, BigQuery, and Snowflake.

**Answer:** **Redshift:** Best for AWS ecosystem, predictable workloads, and organizations needing SQL familiarity. Uses DC2/RA3 nodes, requires cluster management. **BigQuery:** Best for serverless operations, ML integration (BQL ML), and organizations wanting zero management. Pay-per-query or flat-rate. **Snowflake:** Best for multi-workload environments, data sharing, and time travel features. Unique separation of storage/compute. All support standard SQL; choice depends on cloud ecosystem, management preference, and pricing model.

### Q2: How would you design a data lake on AWS?

**Answer:** Use S3 as the foundation with a multi-zone architecture: **Raw Zone:** Landing area for source data (original format). **Cleaned Zone:** Validated, deduplicated data (Parquet). **Curated Zone:** Business-ready data (star schemas). Use AWS Glue for cataloging, Lake Formation for governance, EMR for processing, and Athena/Redshift for querying. Implement lifecycle policies to move old data to cheaper storage (Glacier). Use partitioning (by date) and compression (Snappy) for query performance.

### Q3: What is the advantage of Snowflake's architecture?

**Answer:** Snowflake's key advantage is the **separation of storage and compute**. You can scale compute independently (virtual warehouses) without affecting storage. This means: 1) Different workloads (BI, ETL, ML) can run on separate warehouses with different sizes. 2) Concurrency scaling adds warehouses automatically during peak loads. 3) Time travel and cloning are essentially free (storage-based). 4) Data sharing is seamless across organizations. The micro-partition format automatically optimizes data layout without manual tuning.

### Q4: When would you use Dataflow vs Spark on Dataproc?

**Answer:** **Dataflow** (Apache Beam): Best for serverless stream/batch unification, when you don't want to manage clusters, and for event-driven pipelines. Auto-scaling and auto-healing. **Dataproc** (Spark): Best for existing Spark workloads, when you need more control over cluster configuration, and for cost-sensitive batch processing. Use preemptible VMs for lower cost. Choose Dataflow for new streaming projects; choose Dataproc for migrating existing Spark applications or when you need fine-grained cluster control.

### Q5: How do you optimize costs in cloud data platforms?

**Answer:** Strategies: 1) **Right-size compute:** Monitor utilization and adjust warehouse/instance sizes. 2) **Auto-suspend:** Set warehouses to auto-suspend when idle. 3) **Spot/preemptible instances:** Use for fault-tolerant batch workloads (60-90% savings). 4) **Reserved capacity:** Commit to 1-3 years for predictable workloads. 5) **Storage tiering:** Move old data to cheaper storage classes. 6) **Query optimization:** Partition tables, use materialized views, avoid SELECT *. 7) **Monitor costs:** Set up alerts and budgets. 8) **Use serverless where possible:** Pay only for what you use.

---

*Next Section: [09 - Data Streaming](../09-Data-Streaming/README.md)*
