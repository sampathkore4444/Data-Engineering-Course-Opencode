# 06 - Database Systems

## Table of Contents
1. [Relational Databases](#1-relational-databases)
2. [Columnar Databases](#2-columnar-databases)
3. [NoSQL Databases](#3-nosql-databases)
4. [NewSQL Databases](#4-newsql-databases)
5. [Real-World Scenarios](#5-real-world-scenarios)
6. [Hands-On Exercises](#6-hands-on-exercises)
7. [Interview Questions](#7-interview-questions)

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

**Tools & Clients:** pgAdmin, DBeaver, DataGrip, Postico, pgcli

**Advanced Features:**
```sql
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
```

### MySQL/MariaDB

Widely used open-source RDBMS, popular for web applications.

**Key Features:**
- Replication (master-slave, master-master)
- Storage engines (InnoDB, MyISAM)
- Partitioning
- Full-text search

**Tools & Clients:** MySQL Workbench, DBeaver, HeidiSQL, Sequel Pro

### Oracle Database

Enterprise-grade RDBMS with advanced features.

**Key Features:**
- Real Application Clusters (RAC)
- Advanced compression
- Partitioning
- Advanced security
- Exadata integration

**Tools & Clients:** SQL Developer, Toad, DBeaver

### Amazon Aurora

Cloud-native relational database compatible with MySQL/PostgreSQL.

**Key Features:**
- 5x MySQL performance, 3x PostgreSQL performance
- Auto-scaling storage
- Read replicas
- Global database
- Serverless option

**Cloud Tools:** AWS RDS Console, AWS CLI, CloudFormation

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

**Tools & Clients:** Redshift Query Editor, DBeaver, Tableau, Looker, dbt

**Optimization:**
```sql
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
```

### Google BigQuery

Serverless, highly scalable data warehouse.

**Key Features:**
- Serverless (no infrastructure management)
- Columnar storage
- BigQuery ML
- BI Engine
- External tables
- Time-travel queries

**Tools & Clients:** BigQuery Console, bq CLI, DBeaver, Metabase

**Optimization:**
```sql
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
```

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

**Tools & Clients:** Snowsight, SnowSQL, DBeaver, Sigma, ThoughtSpot

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

**Tools:** MongoDB Compass, MongoDB Atlas, Studio 3T, Robo 3T

```javascript
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
```

### Key-Value Stores (Redis, DynamoDB)

**Use Case:** Caching, session storage, high-speed lookups.

**Tools:** RedisInsight, AWS Console, DBeaver

```python
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
```

### Column-Family Stores (Cassandra)

**Use Case:** Time-series data, high write throughput, distributed systems.

**Tools:** Cassandra Web, DevCenter, DBeaver

```sql
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
```

### Time-Series Databases (InfluxDB, TimescaleDB)

**Use Case:** IoT data, metrics, monitoring.

**Tools:** InfluxDB UI, Grafana, Chronograf

```sql
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
```

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

## 5. Real-World Scenarios

### Scenario 1: E-Commerce Database Architecture

```
Transactional (OLTP)         Analytical (OLAP)
+------------------+         +------------------+
| PostgreSQL       |  CDC    | Snowflake        |
| - Orders         | ------> | - Fact: Sales    |
| - Customers      |         | - Dim: Products  |
| - Products       |         | - Dim: Customers |
| - Inventory      |         | - Aggregations   |
+------------------+         +------------------+
        |                              |
        v                              v
+------------------+         +------------------+
| MongoDB          |         | Redis            |
| - Product Catalog|         | - Session Cache  |
| - Reviews        |         | - Cart Data      |
| - User Profiles  |         | - Rate Limiting  |
+------------------+         +------------------+
```

### Scenario 2: IoT Time-Series Platform

```
Devices --> Kafka --> Flink --> TimescaleDB --> Grafana
              |               |                     |
              v               v                     v
          S3 (Parquet)   Alerting              Dashboards
          for analytics  System                & Reports
```

---

## 6. Hands-On Exercises

### Exercise 1: PostgreSQL Advanced Queries
```sql
-- Task: Create and query a partitioned table

-- Create partitioned orders table
CREATE TABLE orders (
    order_id SERIAL,
    customer_id INT,
    amount DECIMAL(10,2),
    order_date DATE NOT NULL,
    PRIMARY KEY (order_id, order_date)
) PARTITION BY RANGE (order_date);

-- Create partitions
CREATE TABLE orders_2023 PARTITION OF orders
    FOR VALUES FROM ('2023-01-01') TO ('2024-01-01');

CREATE TABLE orders_2024 PARTITION OF orders
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

-- Insert sample data
INSERT INTO orders (customer_id, amount, order_date) VALUES
(1, 100.00, '2023-06-15'),
(2, 250.00, '2023-12-20'),
(3, 150.00, '2024-01-10'),
(4, 300.00, '2024-03-05');

-- Query with partition pruning
EXPLAIN ANALYZE
SELECT * FROM orders WHERE order_date >= '2024-01-01';

-- JSONB query
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    attributes JSONB
);

INSERT INTO products (name, attributes) VALUES
('Laptop', '{"brand": "Dell", "ram": 16, "storage": 512}'),
('Phone', '{"brand": "Apple", "storage": 256, "color": "black"}');

-- Query JSONB
SELECT name, attributes->>'brand' as brand,
       (attributes->>'ram')::int as ram
FROM products
WHERE attributes @> '{"brand": "Dell"}';
```

### Exercise 2: MongoDB Document Operations
```javascript
// Task: Create and query a product catalog

// Insert nested documents
db.products.insertMany([
    {
        name: "Laptop Pro",
        category: "Electronics",
        specs: {
            cpu: "M2 Pro",
            ram: 16,
            storage: 512
        },
        reviews: [
            { user: "Alice", rating: 5, comment: "Great laptop!" },
            { user: "Bob", rating: 4, comment: "Good value" }
        ],
        price: 1999.99
    },
    {
        name: "Wireless Mouse",
        category: "Electronics",
        specs: {
            dpi: 16000,
            wireless: true
        },
        reviews: [
            { user: "Charlie", rating: 4, comment: "Works well" }
        ],
        price: 79.99
    }
]);

// Query with nested conditions
db.products.find({
    category: "Electronics",
    "specs.ram": { $gte: 16 },
    price: { $lt: 2000 }
});

// Aggregate: Average rating per product
db.products.aggregate([
    { $unwind: "$reviews" },
    { $group: {
        _id: "$name",
        avg_rating: { $avg: "$reviews.rating" },
        review_count: { $sum: 1 }
    }}
]);
```

### Exercise 3: Redis Caching Pattern
```python
import redis
import json
from datetime import timedelta

# Task: Implement cache-aside pattern

class ProductCache:
    def __init__(self):
        self.redis = redis.Redis(host='localhost', port=6379, db=0)
        self.ttl = timedelta(minutes=30)
    
    def get_product(self, product_id):
        """Get product from cache or database."""
        cache_key = f"product:{product_id}"
        
        # Try cache first
        cached = self.redis.get(cache_key)
        if cached:
            print("Cache HIT")
            return json.loads(cached)
        
        # Cache miss - fetch from DB
        print("Cache MISS - fetching from DB")
        product = self.fetch_from_db(product_id)
        
        # Store in cache with TTL
        self.redis.setex(
            cache_key,
            self.ttl,
            json.dumps(product)
        )
        
        return product
    
    def invalidate_product(self, product_id):
        """Remove product from cache."""
        self.redis.delete(f"product:{product_id}")
    
    def fetch_from_db(self, product_id):
        """Simulate database fetch."""
        # In real app, this would query the database
        return {
            'id': product_id,
            'name': f'Product {product_id}',
            'price': 99.99
        }

# Test the cache
def test_cache():
    cache = ProductCache()
    
    # First call - cache miss
    product = cache.get_product(123)
    print(f"Product: {product}")
    
    # Second call - cache hit
    product = cache.get_product(123)
    print(f"Product: {product}")
    
    # Invalidate and fetch again
    cache.invalidate_product(123)
    product = cache.get_product(123)
    print(f"After invalidation: {product}")

test_cache()
```

### Exercise 4: Database Selection Decision Tree
```python
# Task: Build a database selection helper

def select_database(requirements):
    """
    Select appropriate database based on requirements.
    
    Args:
        requirements: dict with keys:
            - data_type: 'structured', 'semi-structured', 'unstructured'
            - scale: 'small', 'medium', 'large', 'massive'
            - latency: 'real-time', 'low', 'moderate'
            - consistency: 'strong', 'eventual'
            - use_case: 'oltp', 'olap', 'caching', 'time-series', 'document'
    """
    recommendations = []
    
    # OLTP use case
    if requirements.get('use_case') == 'oltp':
        if requirements.get('consistency') == 'strong':
            recommendations.append('PostgreSQL')
            recommendations.append('MySQL')
        if requirements.get('scale') == 'massive':
            recommendations.append('CockroachDB')
            recommendations.append('Google Spanner')
    
    # OLAP use case
    elif requirements.get('use_case') == 'olap':
        if requirements.get('scale') in ['large', 'massive']:
            recommendations.append('Snowflake')
            recommendations.append('BigQuery')
            recommendations.append('Redshift')
    
    # Caching use case
    elif requirements.get('use_case') == 'caching':
        recommendations.append('Redis')
        recommendations.append('Memcached')
    
    # Time-series use case
    elif requirements.get('use_case') == 'time-series':
        recommendations.append('TimescaleDB')
        recommendations.append('InfluxDB')
    
    # Document use case
    elif requirements.get('use_case') == 'document':
        recommendations.append('MongoDB')
        recommendations.append('DynamoDB')
    
    return recommendations

# Test the function
test_cases = [
    {'use_case': 'oltp', 'consistency': 'strong', 'scale': 'medium'},
    {'use_case': 'olap', 'scale': 'large', 'data_type': 'structured'},
    {'use_case': 'caching', 'latency': 'real-time'},
    {'use_case': 'time-series', 'scale': 'massive'},
    {'use_case': 'document', 'data_type': 'semi-structured'}
]

for test in test_cases:
    print(f"\nRequirements: {test}")
    print(f"Recommendations: {select_database(test)}")
```

### Exercise 5: Schema Comparison Challenge
```sql
-- Task: Compare star schema vs Data Vault for same business

-- Star Schema (Kimball)
CREATE TABLE dim_customer_star (
    customer_key INT PRIMARY KEY,
    customer_id VARCHAR(20),
    name VARCHAR(100),
    email VARCHAR(100),
    segment VARCHAR(20),
    city VARCHAR(50)
);

CREATE TABLE fact_sales_star (
    sale_key BIGINT PRIMARY KEY,
    customer_key INT REFERENCES dim_customer_star,
    product_key INT,
    date_key INT,
    amount DECIMAL(12,2)
);

-- Data Vault (Inmon-style)
CREATE TABLE hub_customer (
    customer_hk VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(20),
    load_dts TIMESTAMP,
    record_source VARCHAR(50)
);

CREATE TABLE sat_customer_details (
    customer_hk VARCHAR(64),
    load_dts TIMESTAMP,
    name VARCHAR(100),
    email VARCHAR(100),
    segment VARCHAR(20),
    PRIMARY KEY (customer_hk, load_dts)
);

CREATE TABLE link_sale_customer (
    link_hk VARCHAR(64) PRIMARY KEY,
    sale_hk VARCHAR(64),
    customer_hk VARCHAR(64),
    load_dts TIMESTAMP
);

-- Query comparison
-- Star Schema: Simple, fast
SELECT c.name, SUM(s.amount)
FROM fact_sales_star s
JOIN dim_customer_star c ON s.customer_key = c.customer_key
GROUP BY c.name;

-- Data Vault: More complex, but full history
SELECT c.name, SUM(l.amount)
FROM link_sale_customer lk
JOIN sat_customer_details c ON lk.customer_hk = c.customer_hk
WHERE c.load_dts = (SELECT MAX(load_dts) FROM sat_customer_details WHERE customer_hk = c.customer_hk)
GROUP BY c.name;
```

---

## 7. Interview Questions

### Q1: When would you choose NoSQL over RDBMS?

**Answer:** 

Choose **NoSQL** when: 
1) Data structure is flexible/evolving (document store), 
2) Extreme write throughput needed (column-family), 
3) Massive scale with simple queries (key-value), 
4) Time-series data with high ingestion (time-series DB). 

Choose **RDBMS** when: 

1) Data has clear relationships (joins needed), 
2) ACID transactions are critical (financial data), 
3) Schema is stable and well-defined, 
4) Complex queries with aggregations are common. Most modern systems use polyglot persistence - different databases for different use cases.

### Q2: Explain Redshift distribution styles and when to use each.

**Answer:** 

**KEY:** Rows with same distkey value on same node. Use for large fact tables joined on a specific column (e.g., customer_key). 

**EVEN:** Rows distributed round-robin. Use for tables without clear join patterns or when data is uniformly accessed. 

**ALL:** Every node has full copy. Use for small dimension tables that join with all fact tables. 

**AUTO:** Redshift picks based on table size and query patterns. Default for new tables. Use KEY for large facts, ALL for small dimensions, EVEN for staging tables.

### Q3: What is the CAP theorem and why does it matter for database selection?

**Answer:** CAP theorem states a distributed system can guarantee only two of three: 

**Consistency** (all nodes see same data), 

**Availability** (every request gets response), 

**Partition Tolerance** (works despite network failures). Since partitions are unavoidable, the choice is CP vs AP. 

**CP databases** (PostgreSQL, MongoDB with majority write concern) prioritize consistency - use for financial data. 

**AP databases** (Cassandra, DynamoDB) prioritize availability - use for social media feeds where eventual consistency is acceptable.

### Q4: Compare Snowflake, Redshift, and BigQuery.

**Answer:** 

**Snowflake:** Best for multi-workload environments with independent scaling. Unique separation of storage/compute. Best data sharing features. Time travel up to 90 days. 

**Redshift:** Best for organizations already in AWS ecosystem. Best price-performance for predictable workloads. Concurrency scaling for bursty loads. 

**BigQuery:** Best for serverless operations with zero management. Best for ML integration (BigQuery ML). Flat-rate or on-demand pricing. All three are excellent; choice depends on cloud ecosystem, pricing model preference, and specific workload patterns.

### Q5: How do you choose between different NoSQL databases?

**Answer:** 
Decision matrix: 

**Need ACID transactions + flexibility?** Document store (MongoDB). 

**Need extreme write throughput + time-series?** Column-family (Cassandra/ScyllaDB). 

**Need sub-millisecond lookups + caching?** Key-value (Redis/DynamoDB). 

**Need time-series analytics?** TimescaleDB or InfluxDB. 

**Need graph relationships?** Graph DB (Neo4j). 

**Need vector similarity search?** Vector DB (Pinecone, Weaviate). 

Consider: query patterns, consistency requirements, scale needs, operational complexity, and team expertise.

### Q6: What is polyglot persistence and when would you use it?

**Answer:** Polyglot persistence means using different database technologies for different use cases within the same application. Example:
- **PostgreSQL** for transactional data (orders, customers)
- **Redis** for caching and sessions
- **MongoDB** for product catalog (flexible schema)
- **Elasticsearch** for search functionality
- **Snowflake** for analytics and reporting

Use it when different data access patterns require different optimizations. The trade-off is increased operational complexity.

### Q7: How do you handle database migrations in production?

**Answer:**
1. **Version control:** Store migrations in Git (Flyway, Liquibase)
2. **Backward compatible:** Make changes that don't break existing code
3. **Blue-green deployment:** Deploy new version alongside old, switch traffic
4. **Feature flags:** Control which users see new schema
5. **Backfill data:** Migrate historical data separately
6. **Test thoroughly:** Run migrations on staging first
7. **Rollback plan:** Have a rollback migration ready

---

## Summary Checklist

### Relational Databases
- [ ] Know PostgreSQL advanced features (JSONB, partitioning, CTEs)
- [ ] Understand MySQL replication and storage engines
- [ ] Compare cloud databases (Aurora, Cloud SQL)

### Columnar Databases
- [ ] Optimize Redshift (distribution styles, sort keys, VACUUM)
- [ ] Use BigQuery partitioning and clustering
- [ ] Understand Snowflake architecture (storage/compute separation)

### NoSQL Databases
- [ ] Choose appropriate NoSQL type for use case
- [ ] Query MongoDB documents with nested conditions
- [ ] Implement Redis caching patterns
- [ ] Design Cassandra data models for time-series

### Database Selection
- [ ] Apply CAP theorem to database choices
- [ ] Use decision matrix for NoSQL selection
- [ ] Consider polyglot persistence strategies

### Practical Skills
- [ ] Write advanced SQL queries (CTEs, window functions)
- [ ] Implement partitioning strategies
- [ ] Build cache-aside patterns with Redis
- [ ] Select databases based on requirements

---

*Next Section: [07 - Big Data Technologies](../07-Big-Data-Technologies/README.md)*
