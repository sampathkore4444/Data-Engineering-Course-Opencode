# 06 - Database Systems

## Table of Contents
1. [Relational Databases](#1-relational-databases)
2. [Columnar Databases](#2-columnar-databases)
3. [NoSQL Databases](#3-nosql-databases)
4. [NewSQL Databases](#4-newsql-databases)
5. [Interview Questions](#5-interview-questions)

---

## 1. Relational Databases (RDBMS)

### PostgreSQL

The most advanced open-source RDBMS with extensive feature set.

**Key Features:**
- ACID compliance
- JSON/JSONB support
- Full-text search
- Table partitioning
- Materialized views
- Extensible (custom types, functions)

**Advanced Features:**
`sql
-- Window functions
SELECT customer_id, order_date, amount,
    SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_date) as running_total
FROM orders;

-- JSONB queries
SELECT * FROM orders WHERE metadata @> '{"payment_method": "credit_card"}';

-- Partitioning
CREATE TABLE orders_2024 PARTITION OF orders
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

-- CTEs with recursive
WITH RECURSIVE category_tree AS (
    SELECT id, name, parent_id, 1 as level
    FROM categories WHERE parent_id IS NULL
    UNION ALL
    SELECT c.id, c.name, c.parent_id, ct.level + 1
    FROM categories c JOIN category_tree ct ON c.parent_id = ct.id
)
SELECT * FROM category_tree;
`

### MySQL/MariaDB

Widely used open-source RDBMS, popular for web applications.

**Key Features:**
- Replication (master-slave, master-master)
- Storage engines (InnoDB, MyISAM)
- Partitioning
- Full-text search

### Oracle Database

Enterprise-grade RDBMS with advanced features.

**Key Features:**
- Real Application Clusters (RAC)
- Advanced compression
- Partitioning
- Advanced security
- Exadata integration

### Amazon Aurora

Cloud-native relational database compatible with MySQL/PostgreSQL.

**Key Features:**
- 5x MySQL performance, 3x PostgreSQL performance
- Auto-scaling storage
- Read replicas
- Global database
- Serverless option

---

## 2. Columnar Databases

### Amazon Redshift

Cloud data warehouse optimized for analytics.

**Key Features:**
- Columnar storage
- Massively parallel processing (MPP)
- Compression encoding
- Distribution styles (KEY, EVEN, ALL, AUTO)
- Sort keys (Compound, Interleaved)
- Concurrency scaling
- Spectrum (query S3 directly)

**Optimization:**
`sql
-- Distribution style
CREATE TABLE fact_sales (
    sale_id BIGINT,
    customer_key INT,
    product_key INT,
    amount DECIMAL(12,2)
)
DISTSTYLE KEY
DISTKEY(customer_key)
COMPOUND SORTKEY(order_date, customer_key);

-- Vacuum and analyze
VACUUM fact_sales;
ANALYZE fact_sales;

-- Query with result caching
SET enable_result_cache_for_session = ON;
`

### Google BigQuery

Serverless, highly scalable data warehouse.

**Key Features:**
- Serverless (no infrastructure management)
- Columnar storage
- BigQuery ML
- BI Engine
- External tables
- Time-travel queries

**Optimization:**
`sql
-- Partitioned table
CREATE TABLE dataset.orders
PARTITION BY DATE(order_timestamp)
CLUSTER BY customer_id, product_category
AS SELECT * FROM source_orders;

-- Materialized view
CREATE MATERIALIZED VIEW dataset.monthly_sales
AS SELECT 
    DATE_TRUNC('month', order_date) as month,
    product_category,
    SUM(amount) as total_sales
FROM dataset.orders
GROUP BY 1, 2;

-- Query optimization
SELECT * FROM dataset.orders
WHERE _PARTITIONDATE BETWEEN '2024-01-01' AND '2024-03-31'
  AND customer_id = 12345;
`

### Snowflake

Cloud data platform with unique architecture.

**Key Features:**
- Separation of storage and compute
- Virtual warehouses (independent compute clusters)
- Time travel (up to 90 days)
- Cloning
- Data sharing
- Snowpark (Python/Java/Scala UDFs)
- Streams and tasks

**Architecture:**
`
+--------------------------------------------------+
|                   SERVICES LAYER                  |
|  (Metadata, Optimization, Security, Query)       |
+--------------------------------------------------+
|                   COMPUTE LAYER                   |
|  +----------+  +----------+  +----------+        |
|  |Warehouse |  |Warehouse |  |Warehouse |        |
|  |   XS     |  |   S      |  |   M      |        |
|  | (BI)     |  | (ETL)    |  | (ML)     |        |
|  +----------+  +----------+  +----------+        |
+--------------------------------------------------+
|                  STORAGE LAYER                    |
|  (Micro-partitions, Columnar, Compressed)        |
+--------------------------------------------------+
`

---

## 3. NoSQL Databases

### Document Stores (MongoDB)

**Use Case:** Flexible schemas, nested documents, rapid development.

`javascript
// Insert document
db.orders.insertOne({
    order_id: "ORD-001",
    customer: {
        name: "John Smith",
        email: "john@email.com"
    },
    items: [
        { product: "Laptop", quantity: 1, price: 999.99 },
        { product: "Mouse", quantity: 2, price: 29.99 }
    ],
    total: 1059.97,
    status: "completed"
});

// Query
db.orders.find({
    "customer.email": "john@email.com",
    "items.product": "Laptop"
});
`

### Key-Value Stores (Redis, DynamoDB)

**Use Case:** Caching, session storage, high-speed lookups.

`python
# Redis
import redis
r = redis.Redis()
r.set("session:123", json.dumps({"user_id": 456, "role": "admin"}))
r.get("session:123")

# DynamoDB
import boto3
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Sessions')
table.put_item(Item={'session_id': '123', 'user_id': 456})
table.get_item(Key={'session_id': '123'})
`

### Column-Family Stores (Cassandra)

**Use Case:** Time-series data, high write throughput, distributed systems.

`sql
-- CQL (Cassandra Query Language)
CREATE TABLE sensor_data (
    sensor_id UUID,
    timestamp TIMESTAMP,
    temperature FLOAT,
    humidity FLOAT,
    PRIMARY KEY (sensor_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

INSERT INTO sensor_data (sensor_id, timestamp, temperature, humidity)
VALUES (uuid(), toTimestamp(now()), 22.5, 45.0);

SELECT * FROM sensor_data 
WHERE sensor_id = ? 
AND timestamp > '2024-01-01';
`

### Time-Series Databases (InfluxDB, TimescaleDB)

**Use Case:** IoT data, metrics, monitoring.

`sql
-- TimescaleDB (PostgreSQL extension)
CREATE TABLE metrics (
    time TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    cpu_usage DOUBLE PRECISION,
    memory_usage DOUBLE PRECISION
);

SELECT create_hypertable('metrics', 'time');

-- Query with time bucketing
SELECT 
    time_bucket('1 hour', time) as bucket,
    device_id,
    AVG(cpu_usage) as avg_cpu
FROM metrics
WHERE time > NOW() - INTERVAL '24 hours'
GROUP BY bucket, device_id
ORDER BY bucket;
`

---

## 4. NewSQL Databases

Combine NoSQL scalability with SQL ACID guarantees.

### CockroachDB
- Distributed SQL database
- Serializable isolation
- Geo-partitioning
- Automatic sharding

### Google Spanner
- Global distributed database
- External consistency (stronger than linearizable)
- SQL interface
- Automatic sharding

---

## 5. Interview Questions

### Q1: When would you choose NoSQL over RDBMS?

**Answer:** Choose **NoSQL** when: 1) Data structure is flexible/evolving (document store), 2) Extreme write throughput needed (column-family), 3) Massive scale with simple queries (key-value), 4) Time-series data with high ingestion (time-series DB). Choose **RDBMS** when: 1) Data has clear relationships (joins needed), 2) ACID transactions are critical (financial data), 3) Schema is stable and well-defined, 4) Complex queries with aggregations are common. Most modern systems use polyglot persistence - different databases for different use cases.

### Q2: Explain Redshift distribution styles and when to use each.

**Answer:** **KEY:** Rows with same distkey value on same node. Use for large fact tables joined on a specific column (e.g., customer_key). **EVEN:** Rows distributed round-robin. Use for tables without clear join patterns or when data is uniformly accessed. **ALL:** Every node has full copy. Use for small dimension tables that join with all fact tables. **AUTO:** Redshift picks based on table size and query patterns. Default for new tables. Use KEY for large facts, ALL for small dimensions, EVEN for staging tables.

### Q3: What is the CAP theorem and why does it matter for database selection?

**Answer:** CAP theorem states a distributed system can guarantee only two of three: **Consistency** (all nodes see same data), **Availability** (every request gets response), **Partition Tolerance** (works despite network failures). Since partitions are unavoidable, the choice is CP vs AP. **CP databases** (PostgreSQL, MongoDB with majority write concern) prioritize consistency - use for financial data. **AP databases** (Cassandra, DynamoDB) prioritize availability - use for social media feeds where eventual consistency is acceptable.

### Q4: Compare Snowflake, Redshift, and BigQuery.

**Answer:** **Snowflake:** Best for multi-workload environments with independent scaling. Unique separation of storage/compute. Best data sharing features. Time travel up to 90 days. **Redshift:** Best for organizations already in AWS ecosystem. Best price-performance for predictable workloads. Concurrency scaling for bursty loads. **BigQuery:** Best for serverless operations with zero management. Best for ML integration (BigQuery ML). Flat-rate or on-demand pricing. All three are excellent; choice depends on cloud ecosystem, pricing model preference, and specific workload patterns.

### Q5: How do you choose between different NoSQL databases?

**Answer:** Decision matrix: **Need ACID transactions + flexibility?** Document store (MongoDB). **Need extreme write throughput + time-series?** Column-family (Cassandra/ScyllaDB). **Need sub-millisecond lookups + caching?** Key-value (Redis/DynamoDB). **Need time-series analytics?** TimescaleDB or InfluxDB. **Need graph relationships?** Graph DB (Neo4j). **Need vector similarity search?** Vector DB (Pinecone, Weaviate). Consider: query patterns, consistency requirements, scale needs, operational complexity, and team expertise.

---

*Next Section: [07 - Big Data Technologies](../07-Big-Data-Technologies/README.md)*
