# 07 - Big Data Technologies

## Table of Contents
1. [Apache Hadoop Ecosystem](#1-apache-hadoop-ecosystem)
2. [Apache Spark](#2-apache-spark)
3. [Apache Kafka](#3-apache-kafka)
4. [Apache Flink](#4-apache-flink)
5. [Interview Questions](#5-interview-questions)

---

## 1. Apache Hadoop Ecosystem

### HDFS (Hadoop Distributed File System)

HDFS is a distributed file system designed to store very large datasets across multiple machines.

**Architecture:**
`
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
`

**Key Concepts:**
- Block size: 128MB or 256MB (vs 4KB in traditional FS)
- Replication factor: Default 3 (each block stored on 3 nodes)
- Write-once-read-many model

**HDFS Commands:**
`ash
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
`

### YARN (Yet Another Resource Negotiator)

Resource management layer for Hadoop.

`
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
`

### Apache Hive

SQL-like interface for querying data stored in HDFS.

`sql
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
`

---

## 2. Apache Spark

### Architecture

`
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
`

### Spark SQL and DataFrames

`python
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
`

### Advanced Spark Operations

`python
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
`

---

## 3. Apache Kafka

### Architecture

`
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
`

### Kafka Commands

`ash
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
`

### Kafka Connect

`json
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

`java
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
`

---

## 4. Apache Flink

### Architecture

`
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
`

### Flink Streaming

`java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<Order> orders = env.addSource(new KafkaSource<>("orders"));

DataStream<Order> processed = orders
    .filter(order -> order.getAmount() > 0)
    .keyBy(Order::getCustomerId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .aggregate(new OrderAggregator());

processed.addSink(new ElasticsearchSink<>(...));

env.execute("Order Processing Job");
`

### Flink SQL

`sql
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
`

---

## 5. Interview Questions

### Q1: Explain the difference between MapReduce and Spark.

**Answer:** **MapReduce** writes intermediate results to disk after each Map and Reduce phase, making it slow for iterative algorithms. **Spark** keeps data in memory across operations (RDDs/DataFrames), only writing to disk when necessary. Spark is 10-100x faster than MapReduce for most workloads. Spark also provides higher-level abstractions (DataFrames, SQL), ML libraries, and streaming capabilities. MapReduce is only preferred for extremely large datasets that don't fit in memory across the cluster.

### Q2: What is a Kafka partition and why is it important?

**Answer:** A **partition** is an ordered, immutable sequence of records within a topic. Partitions enable: 1) **Parallelism:** Multiple consumers can read from different partitions simultaneously. 2) **Scalability:** Add more partitions to handle more throughput. 3) **Ordering:** Guarantees order within a partition (not across partitions). 4) **Fault tolerance:** Each partition can be replicated across multiple brokers. The number of partitions determines the maximum consumer parallelism. Choose partition count based on throughput requirements and consumer group size.

### Q3: When would you use Spark vs Flink?

**Answer:** **Spark** is better for: batch processing, ETL workloads, ML pipelines, SQL analytics, and when you need a unified batch+streaming framework with high-level APIs. **Flink** is better for: true event-by-event streaming, low-latency requirements (< 100ms), complex event processing, stateful streaming with exactly-once guarantees, and event-time processing. Flink's checkpointing mechanism provides stronger fault tolerance guarantees for streaming. Choose Spark for batch-first workloads; choose Flink for streaming-first workloads.

### Q4: Explain the Hadoop ecosystem components and their roles.

**Answer:** **HDFS:** Distributed file system for storing data across commodity hardware. **YARN:** Resource manager that allocates cluster resources to applications. **MapReduce:** Programming model for distributed computation (largely replaced by Spark). **Hive:** SQL-like query engine for data stored in HDFS. **HBase:** NoSQL database on top of HDFS for random read/write. **ZooKeeper:** Coordination service for distributed systems. **Oozie/Airflow:** Workflow orchestration. **Sqoop:** Data import/export between RDBMS and HDFS. Modern Hadoop stacks are often replaced by cloud-native services (S3, EMR, etc.).

### Q5: What is data skew and how do you handle it in Spark?

**Answer:** **Data skew** occurs when some partitions have significantly more data than others, causing some tasks to take much longer. Symptoms: one task takes 10x longer, shuffle write/read imbalance. Solutions: 1) **Salting:** Add random prefix to skewed key, process separately, then combine. 2) **Broadcast join:** If one table is small, broadcast it to avoid shuffle. 3) **Adaptive query execution:** Spark 3.0+ auto-detects and optimizes skew. 4) **Custom partitioning:** Repartition data before processing. 5) **Increase partitions:** More parallelism can help. 6) **Pre-aggregate:** Reduce data before joins.

---

*Next Section: [08 - Cloud Data Platforms](../08-Cloud-Data-Platforms/README.md)*
