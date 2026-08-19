# 07 - Big Data Technologies

## Table of Contents
1. [Apache Hadoop Ecosystem](#1-apache-hadoop-ecosystem)
2. [Apache Spark](#2-apache-spark)
3. [Apache Kafka](#3-apache-kafka)
4. [Apache Flink](#4-apache-flink)
5. [Real-World Scenarios](#5-real-world-scenarios)
6. [Hands-On Exercises](#6-hands-on-exercises)
7. [Interview Questions](#7-interview-questions)

---

## 1. Apache Hadoop Ecosystem

### Modern Big Data Tools

| Category | Tools | Description |
|----------|-------|-------------|
| **Cloud Storage** | AWS S3, Azure Data Lake, Google Cloud Storage | Scalable object storage |
| **Managed Spark** | Databricks, AWS EMR, Google Dataproc | Managed Spark clusters |
| **Managed Kafka** | Confluent Cloud, AWS MSK, Azure Event Hubs | Managed streaming |
| **Stream Processing** | Amazon Kinesis, Google Dataflow, Azure Stream Analytics | Cloud-native streaming |
| **Data Catalog** | AWS Glue, DataHub, Amundsen | Metadata management |
| **Monitoring** | Prometheus, Grafana, Datadog | Cluster monitoring |

### HDFS (Hadoop Distributed File System)

HDFS is a distributed file system designed to store very large datasets across multiple machines.

**Architecture:**
```
+--------------------------------------------------+
|                   CLIENT                         |
|  (Read/Write operations)                         |
+--------------------------------------------------+
                      |
        +-------------+-------------+
        |                           |
+-------v-------+         +-------v-------+
|  NameNode     |         |  Secondary    |
|  (Master)     |         |  NameNode     |
|  - Metadata   |         |  (Backup)     |
|  - File sys   |         |               |
+-------+-------+         +---------------+
        |
   +----+----+----+----+
   |    |    |    |    |
+--v--+ +--v--+ +--v--+ +--v--+
|DN-1 | |DN-2 | |DN-3 | |DN-4 |
|(Data| |(Data| |(Data| |(Data|
|Node)| |Node)| |Node)| |Node)|
+-----+ +-----+ +-----+ +-----+
Blocks  Blocks  Blocks  Blocks
```

**Key Concepts:**
- Block size: 128MB or 256MB (vs 4KB in traditional FS)
- Replication factor: Default 3 (each block stored on 3 nodes)
- Write-once-read-many model

**HDFS Commands:**
```ash
# List files
hdfs dfs -ls /data/orders

# Copy file to HDFS
hdfs dfs -put local_file.csv /data/orders/

# Read file
hdfs dfs -cat /data/orders/file.csv

# Copy from HDFS to local
hdfs dfs -get /data/orders/file.csv ./local_file.csv

# Check file size
hdfs dfs -du -h /data/orders/

# Delete file
hdfs dfs -rm /data/orders/old_file.csv
```

### YARN (Yet Another Resource Negotiator)

Resource management layer for Hadoop.

```
+--------------------------------------------------+
|                   YARN                           |
+--------------------------------------------------+
|  ResourceManager (Master)                       |
|  +----------+  +----------+                     |
|  |Scheduler |  |AppsMgmt  |                     |
|  +----------+  +----------+                     |
+--------------------------------------------------+
        |
   +----+----+----+
   |    |    |    |
+--v--+ +--v--+ +--v--+
|NM-1 | |NM-2 | |NM-3 |
|(Node| |(Node| |(Node|
|Mgr) | |Mgr) | |Mgr) |
+--+--+ +--+--+ +--+--+
   |       |       |
+--v--+ +--v--+ +--v--+
|AM-1 | |AM-2 | |AM-3 |
|(App | |(App | |(App |
|Mgr) | |Mgr) | |Mgr) |
+-----+ +-----+ +-----+
```

### Apache Hive

SQL-like interface for querying data stored in HDFS.

**Tools:** Hive CLI, Beeline, Apache Atlas (metadata), Apache Ranger (security)

```sql
-- Create external table
CREATE EXTERNAL TABLE orders (
    order_id BIGINT,
    customer_id STRING,
    order_date STRING,
    amount DECIMAL(10,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION '/data/orders';

-- Query
SELECT customer_id, SUM(amount) as total
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY customer_id
ORDER BY total DESC;

-- Partitioned table
CREATE TABLE orders_partitioned (
    order_id BIGINT,
    customer_id STRING,
    amount DECIMAL(10,2)
)
PARTITIONED BY (order_year INT, order_month INT)
STORED AS PARQUET;

ALTER TABLE orders_partitioned ADD PARTITION (order_year=2024, order_month=1);
```

---

## 2. Apache Spark

### Architecture

```
+--------------------------------------------------+
|                  DRIVER                          |
|  (SparkContext / SparkSession)                   |
+--------------------------------------------------+
        |
   +----+----+
   |         |
+--v---+ +--v---+
|Exec-1| |Exec-2|
|(JVM) | |(JVM) |
+--+---+ +--+---+
   |         |
+--v---+ +--v---+
|Task-1| |Task-2|
+------+ +------+
```

### Managed Spark Platforms

| Platform | Features | Best For |
|----------|----------|----------|
| **Databricks** | Unity Catalog, AutoML, Delta Lake | Enterprise ML + Analytics |
| **AWS EMR** | EC2-based, EMR Serverless, S3 integration | AWS ecosystem |
| **Google Dataproc** | Serverless, GCP integration, Spark on GKE | GCP ecosystem |
| **Azure Synapse** | Spark pools, SQL pools, Power BI integration | Azure ecosystem |

### Spark SQL and DataFrames

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("DataEngineering") \
    .config("spark.sql.shuffle.partitions", 200) \
    .getOrCreate()

# Read data
df = spark.read.parquet("/data/orders")
df = spark.read.json("/data/customers")
df = spark.read.jdbc("jdbc:postgresql://host/db", "orders", properties={"user": "me"})

# Transformations
result = df \
    .filter(col("order_date") >= "2024-01-01") \
    .groupBy("customer_id") \
    .agg(
        count("order_id").alias("order_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    ) \
    .orderBy(desc("total_amount"))

# Write
result.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet("/output/customer_summary")

# SQL interface
df.createOrReplaceTempView("orders")
spark.sql("""
    SELECT customer_id, SUM(amount) as total
    FROM orders
    GROUP BY customer_id
""")
```

### Advanced Spark Operations

```python
# Window functions
from pyspark.sql.window import Window

window_spec = Window.partitionBy("customer_id").orderBy("order_date")
df = df.withColumn("running_total", sum("amount").over(window_spec))
df = df.withColumn("row_number", row_number().over(window_spec))

# Broadcast join (small table fits in memory)
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "customer_id")

# Repartition and coalesce
df = df.repartition(100, "customer_id")  # Increase partitions
df = df.coalesce(10)  # Decrease partitions

# Cache/persist
df.cache()  # or df.persist(StorageLevel.MEMORY_AND_DISK)

# UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def categorize_amount(amount):
    if amount > 1000: return 'High'
    elif amount > 100: return 'Medium'
    else: return 'Low'

df = df.withColumn("category", categorize_amount(col("amount")))
```

---

## 3. Apache Kafka

### Architecture

```
+--------+     +--------+     +--------+
|Producer|     |Producer|     |Producer|
+---+----+     +---+----+     +---+----+
    |              |              |
    v              v              v
+--------------------------------------------------+
|              KAFKA CLUSTER                        |
|  +----------+  +----------+  +----------+        |
|  | Broker 1 |  | Broker 2 |  | Broker 3 |        |
|  | +-------+|  | +-------+|  | +-------+|        |
|  | |Topic A||  | |Topic A||  | |Topic A||        |
|  | |Part 0 ||  | |Part 1 ||  | |Part 2 ||        |
|  | +-------+|  | +-------+|  | +-------+|        |
|  +----------+  +----------+  +----------+        |
+--------------------------------------------------+
    |              |              |
    v              v              v
+--------+     +--------+     +--------+
|Consumer|     |Consumer|     |Consumer|
|Group   |     |Group   |     |Group   |
+--------+     +--------+     +--------+
```

### Kafka Commands

```ash
# Create topic
kafka-topics.sh --create --topic orders \
    --bootstrap-server localhost:9092 \
    --partitions 6 \
    --replication-factor 3

# List topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Produce messages
kafka-console-producer.sh --topic orders \
    --bootstrap-server localhost:9092

# Consume messages
kafka-console-consumer.sh --topic orders \
    --bootstrap-server localhost:9092 \
    --from-beginning

# Describe topic
kafka-topics.sh --describe --topic orders \
    --bootstrap-server localhost:9092
```

### Managed Kafka Platforms

| Platform | Features | Best For |
|----------|----------|----------|
| **Confluent Cloud** | Fully managed, Schema Registry, ksqlDB | Enterprise streaming |
| **AWS MSK** | Managed Kafka, IAM integration | AWS ecosystem |
| **Azure Event Hubs** | Kafka-compatible, serverless | Azure ecosystem |
| **Aiven for Kafka** | Multi-cloud, open source | Multi-cloud deployments |

### Kafka Connect

```json
// Source connector (MySQL -> Kafka)
{
  "name": "mysql-source",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql-host",
    "database.port": "3306",
    "database.user": "kafka",
    "database.password": "secret",
    "database.server.id": "1",
    "database.include.list": "orders_db",
    "table.include.list": "orders_db.orders",
    "topic.prefix": "mysql"
  }
}

// Sink connector (Kafka -> Elasticsearch)
{
  "name": "elasticsearch-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "topics": "orders",
    "connection.url": "http://elasticsearch:9200",
    "type.name": "_doc",
    "key.ignore": "false"
  }
}
`

### Kafka Streams

```java
StreamsBuilder builder = new StreamsBuilder();

KStream<String, Order> orders = builder.stream("orders");

KStream<String, Order> enrichedOrders = orders
    .filter((key, order) -> order.getAmount() > 0)
    .mapValues(order -> {
        order.setTax(order.getAmount() * 0.1);
        return order;
    });

enrichedOrders.to("enriched-orders");

KGroupedStream<String, Order> grouped = orders
    .groupBy((key, order) -> order.getCustomerId());

KTable<String, Double> totalByCustomer = grouped
    .aggregate(
        () -> 0.0,
        (key, order, total) -> total + order.getAmount()
    );

totalByCustomer.toStream().to("customer-totals");
```

---

## 4. Apache Flink

### Architecture

```
+--------------------------------------------------+
|                  CLIENT                          |
+--------------------------------------------------+
        |
+-------v-------+
|  JobManager   |
|  (Master)     |
+-------+-------+
        |
   +----+----+----+
   |    |    |    |
+--v--+ +--v--+ +--v--+
|TM-1 | |TM-2 | |TM-3 |
|(Task| |(Task| |(Task|
|Mgr) | |Mgr) | |Mgr) |
+--+--+ +--+--+ +--+--+
   |       |       |
+--v--+ +--v--+ +--v--+
|Task | |Task | |Task |
|Slot  | |Slot | |Slot |
+-----+ +-----+ +-----+
```

### Flink Streaming

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<Order> orders = env.addSource(new KafkaSource<>("orders"));

DataStream<Order> processed = orders
    .filter(order -> order.getAmount() > 0)
    .keyBy(Order::getCustomerId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .aggregate(new OrderAggregator());

processed.addSink(new ElasticsearchSink<>(...));

env.execute("Order Processing Job");
```

### Managed Flink Platforms

| Platform | Features | Best For |
|----------|----------|----------|
| **Amazon Kinesis Data Analytics** | Managed Flink, serverless | AWS ecosystem |
| **Google Dataflow** | Unified batch+streaming, serverless | GCP ecosystem |
| **Ververica Cloud** | Managed Flink, enterprise support | Pure Flink workloads |

### Flink SQL

```sql
CREATE TABLE orders (
    order_id STRING,
    customer_id STRING,
    amount DECIMAL(10,2),
    order_time TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json'
);

SELECT 
    TUMBLE_START(order_time, INTERVAL '5' MINUTE) as window_start,
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM orders
GROUP BY TUMBLE(order_time, INTERVAL '5' MINUTE), customer_id;
```

---

## 5. Real-World Scenarios

### Scenario 1: Real-Time E-Commerce Analytics

```
Web App --> Kafka --> Flink --> PostgreSQL --> Grafana
   |           |        |          |           |
   v           v        v          v           v
Events    Stream    Aggregate   Store     Dashboard
          Buffer    & Enrich    (OLTP)    & Alerts
```

### Scenario 2: Batch ETL Pipeline

```
Source --> Airbyte --> S3 (Parquet) --> Spark/EMR --> Redshift --> Looker
   |          |            |              |             |           |
   v          v            v              v             v           v
Extract    Ingest      Raw Data      Transform     Warehouse   Reports
```

---

## 6. Hands-On Exercises

### Exercise 1: Spark DataFrame Operations
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Task: Process sales data with Spark

spark = SparkSession.builder \
    .appName("SalesAnalysis") \
    .master("local[*]") \
    .getOrCreate()

# Create sample data
sales_data = [(1, "2024-01-01", "Electronics", 1000),
              (2, "2024-01-01", "Clothing", 500),
              (3, "2024-01-02", "Electronics", 1500),
              (4, "2024-01-02", "Food", 200),
              (5, "2024-01-03", "Clothing", 800)]

df = spark.createDataFrame(sales_data, 
    ["order_id", "order_date", "category", "amount"])

# Solution: Analyze sales by category
result = df \
    .withColumn("order_date", to_date("order_date")) \
    .groupBy("category") \
    .agg(
        count("order_id").alias("order_count"),
        sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value"),
        max("amount").alias("max_order")
    ) \
    .orderBy(desc("total_revenue"))

result.show()

# Write to Parquet
df.write.mode("overwrite").parquet("/output/sales")
```

### Exercise 2: Kafka Producer-Consumer
```python
from kafka import KafkaProducer, KafkaConsumer
import json
from datetime import datetime

# Task: Build a simple Kafka producer-consumer pair

# Producer
def create_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def send_order_event(producer, topic, order):
    """Send order event to Kafka."""
    order['timestamp'] = datetime.now().isoformat()
    producer.send(topic, value=order)
    producer.flush()
    print(f"Sent order: {order['order_id']}")

# Consumer
def create_consumer(group_id):
    return KafkaConsumer(
        'orders',
        bootstrap_servers='localhost:9092',
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest'
    )

def process_orders(consumer):
    """Process orders from Kafka."""
    for message in consumer:
        order = message.value
        print(f"Processing order: {order['order_id']}")
        print(f"  Amount: ${order.get('amount', 0)}")
        print(f"  Timestamp: {order.get('timestamp')}")

# Test
def test_kafka():
    producer = create_producer()
    
    # Send test orders
    for i in range(3):
        send_order_event(producer, 'orders', {
            'order_id': f'ORD-{i:04d}',
            'customer_id': f'CUST-{i:03d}',
            'amount': (i + 1) * 100
        })
    
    producer.close()
    print("\nOrders sent successfully!")

# Uncomment to test (requires running Kafka)
# test_kafka()
```

### Exercise 3: Flink SQL Windowing
```sql
-- Task: Create a streaming aggregation with Flink SQL

-- Create orders table (Kafka source)
CREATE TABLE orders (
    order_id STRING,
    customer_id STRING,
    amount DECIMAL(10,2),
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'orders',
    'properties.bootstrap.servers' = 'localhost:9092',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

-- Solution 1: Tumbling window (5-minute windows)
SELECT 
    TUMBLE_START(event_time, INTERVAL '5' MINUTE) as window_start,
    TUMBLE_END(event_time, INTERVAL '5' MINUTE) as window_end,
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM orders
GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE), customer_id;

-- Solution 2: Sliding window (10-minute window, slide every 5 minutes)
SELECT 
    HOP_START(event_time, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE) as window_start,
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM orders
GROUP BY HOP(event_time, INTERVAL '5' MINUTE, INTERVAL '10' MINUTE), customer_id;

-- Solution 3: Session window (30-minute inactivity timeout)
SELECT 
    SESSION_START(event_time, INTERVAL '30' MINUTE) as session_start,
    SESSION_END(event_time, INTERVAL '30' MINUTE) as session_end,
    customer_id,
    COUNT(*) as order_count,
    SUM(amount) as session_total
FROM orders
GROUP BY SESSION(event_time, INTERVAL '30' MINUTE), customer_id;
```

### Exercise 4: Hive Data Modeling
```sql
-- Task: Create a partitioned Hive table for sales data

-- Create external table
CREATE EXTERNAL TABLE sales_raw (
    order_id BIGINT,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    order_date STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS PARQUET
LOCATION '/data/sales/raw';

-- Create partitioned table for analytics
CREATE TABLE sales_partitioned (
    order_id BIGINT,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    revenue DECIMAL(12,2)
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET;

-- Insert data with partitioning
INSERT OVERWRITE TABLE sales_partitioned
PARTITION (year, month, day)
SELECT 
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    quantity * unit_price as revenue,
    YEAR(order_date) as year,
    MONTH(order_date) as month,
    DAY(order_date) as day
FROM sales_raw;

-- Query with partition pruning
SELECT 
    year, month,
    SUM(revenue) as total_revenue,
    COUNT(DISTINCT customer_id) as unique_customers
FROM sales_partitioned
WHERE year = 2024 AND month = 1
GROUP BY year, month;
```

### Exercise 5: Data Skew Handling
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import random

# Task: Handle data skew in Spark

spark = SparkSession.builder \
    .appName("DataSkewDemo") \
    .master("local[*]") \
    .getOrCreate()

# Create skewed data (90% of orders from one customer)
data = []
for i in range(1000):
    if random.random() < 0.9:  # 90% from customer C001
        data.append((f"C001", f"ORD-{i:04d}", random.uniform(10, 100)))
    else:
        data.append((f"C{i%10+2:03d}", f"ORD-{i:04d}", random.uniform(10, 100)))

df = spark.createDataFrame(data, ["customer_id", "order_id", "amount"])

# Problem: Skewed aggregation
print("Skewed data distribution:")
df.groupBy("customer_id").count().show()

# Solution 1: Salting technique
def add_salt(df, num_salt_buckets=10):
    """Add random salt to handle skew."""
    return df.withColumn(
        "salt", 
        (rand() * num_salt_buckets).cast("int")
    )

# Solution 2: Broadcast join for small tables
# If one table is small, broadcast it
small_df = spark.createDataFrame([
    ("C001", "Alice"),
    ("C002", "Bob")
], ["customer_id", "customer_name"])

# Broadcast small table to avoid shuffle
result = df.join(broadcast(small_df), "customer_id")
result.show(5)

# Solution 3: Repartition before aggregation
df_repartitioned = df.repartition(100, "customer_id")
result = df_repartitioned.groupBy("customer_id").agg(
    count("order_id").alias("order_count"),
    sum("amount").alias("total_amount")
)
result.show()
```

---

## 7. Interview Questions

### Q1: Explain the difference between MapReduce and Spark.

**Answer:** 

**MapReduce** writes intermediate results to disk after each Map and Reduce phase, making it slow for iterative algorithms. 

**Spark** keeps data in memory across operations (RDDs/DataFrames), only writing to disk when necessary. Spark is 10-100x faster than MapReduce for most workloads. Spark also provides higher-level abstractions (DataFrames, SQL), ML libraries, and streaming capabilities. MapReduce is only preferred for extremely large datasets that don't fit in memory across the cluster.

### Q2: What is a Kafka partition and why is it important?

**Answer:** 

A **partition** is an ordered, immutable sequence of records within a topic. Partitions enable: 

1) **Parallelism:** Multiple consumers can read from different partitions simultaneously. 

2) **Scalability:** Add more partitions to handle more throughput. 

3) **Ordering:** Guarantees order within a partition (not across partitions). 

4) **Fault tolerance:** Each partition can be replicated across multiple brokers. The number of partitions determines the maximum consumer parallelism. Choose partition count based on throughput requirements and consumer group size.

### Q3: When would you use Spark vs Flink?

**Answer:** 

**Spark** is better for: batch processing, ETL workloads, ML pipelines, SQL analytics, and when you need a unified batch+streaming framework with high-level APIs. 

**Flink** is better for: true event-by-event streaming, low-latency requirements (< 100ms), complex event processing, stateful streaming with exactly-once guarantees, and event-time processing. Flink's checkpointing mechanism provides stronger fault tolerance guarantees for streaming. Choose Spark for batch-first workloads; choose Flink for streaming-first workloads.

### Q4: Explain the Hadoop ecosystem components and their roles.

**Answer:** 

**HDFS:** Distributed file system for storing data across commodity hardware. 

**YARN:** Resource manager that allocates cluster resources to applications. 

**MapReduce:** Programming model for distributed computation (largely replaced by Spark). 

**Hive:** SQL-like query engine for data stored in HDFS. 

**HBase:** NoSQL database on top of HDFS for random read/write. 

**ZooKeeper:** Coordination service for distributed systems. 

**Oozie/Airflow:** Workflow orchestration. 

**Sqoop:** Data import/export between RDBMS and HDFS. Modern Hadoop stacks are often replaced by cloud-native services (S3, EMR, etc.).

### Q5: What is data skew and how do you handle it in Spark?

**Answer:** 

**Data skew** occurs when some partitions have significantly more data than others, causing some tasks to take much longer. 

Symptoms: one task takes 10x longer, shuffle write/read imbalance. 

Solutions: 

1) **Salting:** Add random prefix to skewed key, process separately, then combine. 

2) **Broadcast join:** If one table is small, broadcast it to avoid shuffle. 

3) **Adaptive query execution:** Spark 3.0+ auto-detects and optimizes skew. 

4) **Custom partitioning:** Repartition data before processing. 

5) **Increase partitions:** More parallelism can help. 

6) **Pre-aggregate:** Reduce data before joins.

### Q6: What is the difference between Spark Streaming and Structured Streaming?

**Answer:**
**Spark Streaming (DStream):**
- Micro-batch processing
- RDD-based API
- Lower-level abstraction
- Less fault tolerance guarantees

**Structured Streaming:**
- Continuous or micro-batch processing
- DataFrame/Dataset API
- Event-time processing with watermarks
- Exactly-once semantics
- Better integration with Spark SQL

Structured Streaming is the recommended approach for new projects.

### Q7: How do you monitor a Kafka cluster in production?

**Answer:**
Key metrics to monitor:
- **Broker metrics:** Under-replicated partitions, request latency, disk usage
- **Consumer metrics:** Consumer lag, consumption rate
- **Producer metrics:** Request latency, error rate

Tools:
- **Confluent Control Center:** Commercial monitoring
- **Kafka Manager:** Open source cluster management
- **Prometheus + JMX Exporter:** Custom metrics collection
- **Grafana:** Visualization dashboards
- **Datadog/Splunk:** Enterprise monitoring

---

## Summary Checklist

### Hadoop Ecosystem
- [ ] Understand HDFS architecture (NameNode, DataNodes, blocks)
- [ ] Know YARN resource management concepts
- [ ] Write Hive queries for data in HDFS

### Apache Spark
- [ ] Create and manipulate DataFrames
- [ ] Use Spark SQL for analytics
- [ ] Implement window functions and aggregations
- [ ] Handle data skew with salting and broadcast joins
- [ ] Know when to use cache/persist

### Apache Kafka
- [ ] Understand topic, partition, and consumer group concepts
- [ ] Create producers and consumers
- [ ] Configure Kafka Connect for data integration
- [ ] Know managed Kafka platforms (Confluent, MSK)

### Apache Flink
- [ ] Understand streaming vs batch processing
- [ ] Write Flink SQL with windowing
- [ ] Know when to use Flink vs Spark

### Modern Tools
- [ ] Know cloud platforms (Databricks, EMR, Dataproc)
- [ ] Understand managed Kafka services
- [ ] Familiar with monitoring tools (Prometheus, Grafana)

### Practical Skills
- [ ] Build Spark ETL pipelines
- [ ] Implement Kafka producer-consumer patterns
- [ ] Handle data skew in distributed processing
- [ ] Monitor streaming pipelines

---

*Next Section: [08 - Cloud Data Platforms](../08-Cloud-Data-Platforms/README.md)*
