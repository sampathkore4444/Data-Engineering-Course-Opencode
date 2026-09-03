# 13 - Performance Optimization

## Table of Contents
1. [Query Performance](#1-query-performance)
2. [Data Warehouse Optimization](#2-data-warehouse-optimization)
3. [Storage Optimization](#3-storage-optimization)
4. [Cost Optimization](#4-cost-optimization)
5. [Real-World Scenarios](#5-real-world-scenarios)
   - [Scenario 1: Banking DW Query Optimization](#scenario-1-banking-data-warehouse-query-optimization)
   - [Scenario 2: BigQuery Cost Optimization](#scenario-2-bigquery-cost-optimization)
   - [Scenario 3: Real-Time Transaction Processing](#scenario-3-real-time-transaction-processing-optimization)
   - [Scenario 4: Regulatory Report Generation](#scenario-4-regulatory-report-generation-optimization)
   - [Scenario 5: Multi-Tenant Analytics](#scenario-5-multi-tenant-analytics-cost-optimization)
6. [Hands-On Exercises](#6-hands-on-exercises)
7. [Interview Questions](#7-interview-questions)

---

## 1. Query Performance

### Performance Monitoring Tools

| Tool | Type | Description |
|------|------|-------------|
| **pgBadger** | PostgreSQL | Query log analyzer |
| **EXPLAIN Visualizer** | Web Tool | Visualize query plans |
| **Redshift Query Editor** | AWS | Built-in query analysis |
| **BigQuery Execution Details** | GCP | Query performance insights |
| **Snowflake Query Profile** | Snowflake | Visual query analysis |
| **Datadog** | Monitoring | Database performance monitoring |
| **New Relic** | APM | Application performance monitoring |

### Understanding Execution Plans

```sql
-- PostgreSQL
EXPLAIN ANALYZE
SELECT customer_id, SUM(amount) as total
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY customer_id;

-- BigQuery
SELECT * FROM INFORMATION_SCHEMA.JOBS
WHERE job_id = 'your-job-id';

-- Redshift
EXPLAIN SELECT customer_id, SUM(amount) FROM orders GROUP BY customer_id;
```

### Index Optimization

```sql
-- B-Tree index for equality/range queries
CREATE INDEX idx_orders_date ON orders(order_date);

-- Composite index for multi-column filters
CREATE INDEX idx_orders_date_customer ON orders(order_date, customer_id);

-- Covering index (includes all needed columns)
CREATE INDEX idx_orders_covering ON orders(order_date) INCLUDE (customer_id, amount);

-- Partial index (for specific conditions)
CREATE INDEX idx_orders_pending ON orders(order_date) 
WHERE status = 'pending';

-- Bitmap index for low-cardinality columns
CREATE BITMAP INDEX idx_orders_region ON orders(region_id);
```

### Query Rewriting

```sql
-- BAD: Correlated subquery
SELECT * FROM orders o
WHERE amount > (SELECT AVG(amount) FROM orders WHERE customer_id = o.customer_id);

-- GOOD: Window function
SELECT * FROM (
    SELECT *, AVG(amount) OVER (PARTITION BY customer_id) as avg_amount
    FROM orders
) WHERE amount > avg_amount;

-- BAD: OR condition (prevents index usage)
SELECT * FROM orders WHERE customer_id = 1 OR customer_id = 2;

-- GOOD: IN clause (uses index)
SELECT * FROM orders WHERE customer_id IN (1, 2);

-- BAD: Function on indexed column
SELECT * FROM orders WHERE YEAR(order_date) = 2024;

-- GOOD: Range condition
SELECT * FROM orders WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01';
```

---

## 2. Data Warehouse Optimization

### Redshift Optimization

```sql
-- Distribution styles
-- KEY: Rows with same distkey on same node
CREATE TABLE fact_sales DISTSTYLE KEY DISTKEY(customer_key) SORTKEY(order_date);

-- ALL: Every node has full copy (for small dimension tables)
CREATE TABLE dim_product DISTSTYLE ALL SORTKEY(product_key);

-- EVEN: Round-robin distribution
CREATE TABLE staging_orders DISTSTYLE EVEN SORTKEY(order_date);

-- Sort keys
-- Compound: Best for queries filtering on leading sort key columns
CREATE TABLE fact_sales COMPOUND SORTKEY(order_date, customer_key);

-- Interleaved: Equal weight to all sort key columns
CREATE TABLE fact_sales INTERLEAVED SORTKEY(order_date, customer_key);

-- Vacuum and analyze
VACUUM fact_sales;
ANALYZE fact_sales;

-- Result caching
SET enable_result_cache_for_session = ON;

-- Concurrency scaling
ALTER WAREHOUSE compute_wh SET concurrency_scaling = 'AUTO';
```

### BigQuery Optimization

```sql
-- Partitioning
CREATE TABLE dataset.orders
PARTITION BY DATE(order_timestamp)
CLUSTER BY customer_id, product_category;

-- Materialized views
CREATE MATERIALIZED VIEW dataset.monthly_sales
AS SELECT 
    DATE_TRUNC('month', order_date) as month,
    product_category,
    SUM(amount) as total_sales
FROM dataset.orders
GROUP BY 1, 2;

-- BI Engine (in-memory acceleration)
SELECT * FROM dataset.orders
WHERE order_date >= '2024-01-01'  -- BI Engine accelerates this
LIMIT 1000;

-- Approximate aggregation
SELECT 
    APPROX_COUNT_DISTINCT(customer_id) as approx_customers,
    APPROX_QUANTILES(amount, 100)[OFFSET(50)] as median_amount
FROM orders;
```

### Snowflake Optimization

```sql
-- Virtual warehouse sizing
CREATE WAREHOUSE analytics_wh
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 5
    SCALING_POLICY = 'ECONOMY';

-- Clustering
ALTER TABLE orders CLUSTER BY (order_date, customer_id);

-- Result caching (automatic)
SELECT * FROM orders WHERE customer_id = 123;

-- Query profile
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY()) 
ORDER BY START_TIME DESC;
```

---

## 3. Storage Optimization

### Storage Optimization Tools

| Tool | Type | Description |
|------|------|-------------|
| **Apache Parquet** | File Format | Columnar storage for analytics |
| **Delta Lake** | Table Format | ACID transactions on data lakes |
| **Apache Iceberg** | Table Format | Open table format for large datasets |
| **Apache Hudi** | Table Format | Incremental data processing |
| **LakeFS** | Version Control | Git-like versioning for data lakes |

### File Format Comparison

```
CSV:     No compression, no column pruning, no predicate pushdown
JSON:    Low compression, schema-on-read, nested data support
Parquet: Excellent compression, column pruning, predicate pushdown
ORC:     Excellent compression, built-in indexes (Hive optimized)
Avro:    Row-oriented, good compression, schema evolution (streaming)
```

### Compression Optimization

```python
# Parquet with different compression
df.to_parquet("data.parquet", compression="snappy")  # Fast, 2-3x
df.to_parquet("data.parquet", compression="gzip")    # Better, 5-10x
df.to_parquet("data.parquet", compression="zstd")    # Best balance, 3-5x
df.to_parquet("data.parquet", compression="lz4")     # Fastest decompress
```

### Partition Design

```python
# Good: Partition by date (common filter)
df.write.partitionBy("year", "month").parquet("/data/orders/")

# Bad: Partition by high-cardinality column (too many small files)
df.write.partitionBy("customer_id").parquet("/data/orders/")

# Optimal: Check file sizes
# Target: 128MB - 1GB per file
```

### Bucketing/Clustering

```python
# BigQuery clustering
CREATE TABLE dataset.orders
CLUSTER BY customer_id, product_category
AS SELECT * FROM source_orders;

# Spark bucketing
df.write.bucketBy(100, "customer_id").saveAsTable("orders_bucketed")
```

---

## 4. Cost Optimization

### Cost Optimization Tools

| Tool | Cloud | Description |
|------|-------|-------------|
| **AWS Cost Explorer** | AWS | Cost visualization and forecasting |
| **AWS Budgets** | AWS | Cost alerts and budgets |
| **GCP Cost Management** | GCP | Cost monitoring and optimization |
| **Azure Cost Management** | Azure | Cost analysis and optimization |
| **Snowflake Account Usage** | Snowflake | Query and storage costs |
| **Databricks Cost Dashboard** | Databricks | DBU usage and costs |

### Cloud Cost Strategies

| Strategy | Savings | Implementation |
|----------|---------|----------------|
| Reserved instances | 30-70% | Commit 1-3 years |
| Spot/preemptible | 60-90% | For fault-tolerant batch |
| Auto-suspend | 20-50% | Suspend idle warehouses |
| Right-sizing | 10-40% | Monitor utilization |
| Storage tiering | 40-80% | Move old data to cold storage |
| Query optimization | 20-60% | Partition, materialized views |

### Monitoring Costs

```sql
-- Redshift: Query execution costs
SELECT 
    query,
    starttime,
    elapsed / 1000000 as seconds,
    rows,
    bytes / 1024 / 1024 as mb_scanned
FROM stl_query
WHERE starttime > DATEADD(day, -7, GETDATE())
ORDER BY elapsed DESC;

-- BigQuery: Query costs
SELECT 
    query,
    creation_time,
    total_bytes_processed / 1024 / 1024 / 1024 as gb_processed,
    total_bytes_processed * 0.00000000625 as estimated_cost
FROM 
egion-us.INFORMATION_SCHEMA.JOBS
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY total_bytes_processed DESC;
```

---

## 5. Real-World Scenarios

### Overview

This section presents **5 complete banking performance optimization scenarios** that demonstrate how to identify, diagnose, and resolve real-world performance issues. Each scenario includes the problem, diagnosis steps, solution implementation, and measurable outcomes.

---

### Scenario 1: Banking Data Warehouse Query Optimization

> **Business Context:** A bank's analytics team reports that critical regulatory queries are taking 10+ minutes, causing missed reporting deadlines.

#### The Problem

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CURRENT STATE (Before)                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ❌ Regulatory reports take 10-15 minutes to generate                │
│   ❌ Dashboard queries timeout frequently                              │
│   ❌ Ad-hoc analyst queries block each other                           │
│   ❌ Redshift cluster at 90% CPU utilization                          │
│   ❌ Data refresh delays (2-3 hours behind source)                    │
│                                                                         │
│   Query: "Get daily transaction summary for last 30 days"             │
│   Time: 12 minutes 45 seconds                                          │
│   Rows scanned: 2.5 billion                                            │
│   Method: Sequential scan (no indexes)                                 │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Diagnosis Steps

```sql
-- Step 1: Check current query performance
SELECT 
    query_id,
    query_text,
    execution_time_ms,
    rows_scanned,
    bytes_scanned / 1024 / 1024 / 1024 AS gb_scanned,
    cpu_time_ms
FROM query_history
WHERE query_text LIKE '%transaction_summary%'
  AND start_time > CURRENT_DATE - 7
ORDER BY execution_time_ms DESC
LIMIT 10;

-- Result: Average execution time 12 minutes, scanning 2.5B rows

-- Step 2: Analyze execution plan
EXPLAIN ANALYZE
SELECT 
    transaction_date,
    transaction_type,
    COUNT(*) as txn_count,
    SUM(amount) as total_amount
FROM fact_transactions
WHERE transaction_date >= CURRENT_DATE - 30
GROUP BY transaction_date, transaction_type;

-- Problem identified: Seq Scan on fact_transactions (2.5B rows)
-- No distribution key set
-- No sort key on transaction_date

-- Step 3: Check table statistics
SELECT 
    schemaname,
    tablename,
    n_live_tup,
    n_dead_tup,
    last_vacuum,
    last_analyze
FROM pg_stat_user_tables
WHERE tablename = 'fact_transactions';

-- Problem: No vacuum since 3 months, dead tuples = 45%
```

#### Solution Implementation

```sql
-- Solution 1: Add distribution key (for joins)
ALTER TABLE fact_transactions 
    ADD DISTKEY(customer_key);

-- Solution 2: Add sort key (for date range queries)
ALTER TABLE fact_transactions 
    ADD SORTKEY(transaction_date);

-- Solution 3: Vacuum and analyze
VACUUM fact_transactions;
ANALYZE fact_transactions;

-- Solution 4: Create materialized view for common aggregation
CREATE MATERIALIZED VIEW mv_daily_transaction_summary AS
SELECT 
    transaction_date,
    transaction_type,
    COUNT(*) as txn_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM fact_transactions
GROUP BY transaction_date, transaction_type;

-- Solution 5: Add indexes for common filters
CREATE INDEX idx_fact_txn_date ON fact_transactions(transaction_date);
CREATE INDEX idx_fact_txn_type ON fact_transactions(transaction_type);

-- Solution 6: Update statistics
ANALYZE fact_transactions;
```

#### Performance Improvement

```sql
-- After optimization: Same query
EXPLAIN ANALYZE
SELECT 
    transaction_date,
    transaction_type,
    COUNT(*) as txn_count,
    SUM(amount) as total_amount
FROM fact_transactions
WHERE transaction_date >= CURRENT_DATE - 30
GROUP BY transaction_date, transaction_type;

-- Result:
-- Before: Seq Scan on 2.5B rows (12 minutes 45 seconds)
-- After: Index Scan + Dist Key (5 seconds)
-- Improvement: 99.3% faster (150x speedup)
```

#### Key Metrics & Outcomes

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Query execution time | 12 minutes 45 seconds | 5 seconds | 99.3% faster |
| Rows scanned | 2.5 billion | 1.2 million | 99.95% reduction |
| Dashboard load time | 30+ seconds | 2 seconds | 93% faster |
| Cluster CPU utilization | 90% | 45% | 50% reduction |
| Data freshness | 2-3 hours | 15 minutes | 90% faster |

---

### Scenario 2: BigQuery Cost Optimization

> **Business Context:** A bank's BigQuery costs increased 3x in one month due to unoptimized queries and missing partitioning.

#### The Problem

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CURRENT STATE (Before)                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ❌ Monthly BigQuery costs: $45,000 (was $15,000)                    │
│   ❌ Full table scans on 500GB+ tables daily                          │
│   ❌ SELECT * used in 80% of queries                                   │
│   ❌ No partitioning on transaction tables                             │
│   ❌ No materialized views for dashboards                             │
│                                                                         │
│   Cost breakdown:                                                       │
│   • Storage: $2,000 (reasonable)                                       │
│   • Query processing: $43,000 (excessive!)                             │
│   • Most expensive: SELECT * FROM transactions (500GB scan)            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Diagnosis Steps

```sql
-- Step 1: Identify most expensive queries
SELECT 
    query,
    user_email,
    creation_time,
    total_bytes_processed / 1024 / 1024 / 1024 AS gb_processed,
    total_bytes_processed * 0.00000000625 AS estimated_cost_usd,
    query_rewrite_enabled
FROM `region-us`.INFORMATION_SCHEMA.JOBS
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND total_bytes_processed > 0
ORDER BY total_bytes_processed DESC
LIMIT 20;

-- Result: Top query scans 500GB daily, costing $3.12 per run

-- Step 2: Check table partitioning
SELECT 
    table_name,
    partitioning_type,
    clustering_columns,
    total_logical_bytes / 1024 / 1024 / 1024 AS size_gb
FROM `region-us`.INFORMATION_SCHEMA.TABLE_OPTIONS
WHERE table_name = 'transactions';

-- Problem: No partitioning on 500GB table

-- Step 3: Analyze query patterns
SELECT 
    query,
    COUNT(*) as execution_count,
    AVG(total_bytes_processed) as avg_bytes
FROM `region-us`.INFORMATION_SCHEMA.JOBS
WHERE query LIKE '%transactions%'
GROUP BY query
HAVING COUNT(*) > 10
ORDER BY avg_bytes DESC;
```

#### Solution Implementation

```sql
-- Solution 1: Add partitioning to transactions table
CREATE TABLE `project.dataset.transactions_partitioned`
PARTITION BY DATE(transaction_timestamp)
CLUSTER BY customer_id, transaction_type
AS SELECT * FROM `project.dataset.transactions`;

-- Solution 2: Create materialized view for dashboard
CREATE MATERIALIZED VIEW `project.dataset.mv_daily_transactions`
AS SELECT 
    DATE(transaction_timestamp) as transaction_date,
    transaction_type,
    customer_id,
    COUNT(*) as txn_count,
    SUM(amount) as total_amount
FROM `project.dataset.transactions_partitioned`
GROUP BY 1, 2, 3;

-- Solution 3: Replace SELECT * with specific columns
-- BEFORE:
-- SELECT * FROM transactions WHERE transaction_date = '2024-01-15'
-- (Scans 500GB)

-- AFTER:
SELECT 
    transaction_id,
    customer_id,
    amount,
    transaction_type
FROM `project.dataset.transactions_partitioned`
WHERE DATE(transaction_timestamp) = '2024-01-15'
-- (Scans only 1.5GB - 99.7% reduction)

-- Solution 4: Use approximate functions for dashboards
SELECT 
    APPROX_COUNT_DISTINCT(customer_id) as approx_customers,
    APPROX_QUANTILES(amount, 100)[OFFSET(50)] as median_amount
FROM `project.dataset.transactions_partitioned`
WHERE DATE(transaction_timestamp) = '2024-01-15';
```

#### Performance Improvement

```sql
-- Before optimization:
-- Query: SELECT * FROM transactions WHERE transaction_date = '2024-01-15'
-- Bytes processed: 500GB
-- Cost: $3.12 per execution
-- Daily executions: 50
-- Daily cost: $156

-- After optimization:
-- Query: SELECT transaction_id, customer_id, amount FROM transactions_partitioned WHERE DATE(transaction_timestamp) = '2024-01-15'
-- Bytes processed: 1.5GB (99.7% reduction)
-- Cost: $0.009 per execution
-- Daily executions: 50
-- Daily cost: $0.45 (99.7% reduction)
```

#### Key Metrics & Outcomes

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Monthly cost | $45,000 | $12,000 | 73% reduction |
| Average bytes per query | 50GB | 2GB | 96% reduction |
| Dashboard load time | 45 seconds | 5 seconds | 89% faster |
| Data freshness | Real-time | Real-time | No change |
| Query count capacity | 100/day | 500/day | 5x increase |

---

### Scenario 3: Real-Time Transaction Processing Optimization

> **Business Context:** A bank's card authorization system is experiencing latency spikes during peak hours, causing transaction timeouts.

#### The Problem

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CURRENT STATE (Before)                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ❌ Card authorization latency: 2-5 seconds (target: < 500ms)        │
│   ❌ Transaction timeouts: 5% during peak hours (12-2 PM)             │
│   ❌ Customer complaints: 500+ per day during lunch rush              │
│   ❌ Revenue loss: $50K/day from failed transactions                  │
│                                                                         │
│   Transaction flow:                                                     │
│   Card Swipe → Auth Request → [2-5s delay] → Response → POS          │
│                                                                         │
│   Root cause: Database queries scanning full customer table            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Diagnosis Steps

```sql
-- Step 1: Check authorization query performance
EXPLAIN ANALYZE
SELECT 
    c.customer_id,
    c.card_status,
    c.daily_limit,
    c.current_daily_usage,
    c.fraud_score
FROM customers c
JOIN cards ca ON c.customer_id = ca.customer_id
WHERE ca.card_number = '****-****-****-1234'
  AND c.card_status = 'ACTIVE';

-- Problem: Seq Scan on customers (5M rows) + Seq Scan on cards (8M rows)
-- Execution time: 2.3 seconds

-- Step 2: Check table statistics
SELECT 
    schemaname,
    tablename,
    n_live_tup,
    n_dead_tup,
    last_vacuum
FROM pg_stat_user_tables
WHERE tablename IN ('customers', 'cards');

-- Problem: No indexes on card_number, no vacuum since 2 months

-- Step 3: Check connection pool
SHOW max_connections;
SHOW superuser_reserved_connections;

-- Problem: Connection pool exhausted during peak hours
```

#### Solution Implementation

```sql
-- Solution 1: Add indexes for fast card lookup
CREATE INDEX idx_cards_number ON cards(card_number) INCLUDE (customer_id);
CREATE INDEX idx_customers_status ON customers(card_status) INCLUDE (customer_id);

-- Solution 2: Create composite index for authorization query
CREATE INDEX idx_auth_query ON cards(card_number, customer_id)
WHERE card_status = 'ACTIVE';

-- Solution 3: Add materialized view for hot data
CREATE MATERIALIZED VIEW mv_active_cards AS
SELECT 
    ca.card_number,
    c.customer_id,
    c.daily_limit,
    c.current_daily_usage,
    c.fraud_score
FROM cards ca
JOIN customers c ON ca.customer_id = c.customer_id
WHERE ca.card_status = 'ACTIVE';

-- Solution 4: Implement Redis cache for card lookups
-- (See application code below)

-- Solution 5: Vacuum and analyze tables
VACUUM customers;
VACUUM cards;
ANALYZE customers;
ANALYZE cards;
```

#### Application-Level Cache Implementation

```python
import redis
import psycopg2
import json
from datetime import datetime, timedelta

class CardAuthorizationService:
    def __init__(self):
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
        self.db_pool = psycopg2.pool.ThreadedConnectionPool(10, 100, dsn)
    
    def authorize_transaction(self, card_number: str, amount: float) -> dict:
        """Authorize card transaction with caching."""
        # Step 1: Check Redis cache first (microseconds)
        cache_key = f"card:{card_number}:auth"
        cached = self.redis_client.get(cache_key)
        
        if cached:
            return json.loads(cached)
        
        # Step 2: Query database if not cached (milliseconds)
        conn = self.db_pool.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        c.customer_id,
                        c.card_status,
                        c.daily_limit,
                        c.current_daily_usage,
                        c.fraud_score
                    FROM cards ca
                    JOIN customers c ON ca.customer_id = c.customer_id
                    WHERE ca.card_number = %s
                      AND c.card_status = 'ACTIVE'
                """, (card_number,))
                
                result = cur.fetchone()
                if not result:
                    return {'approved': False, 'reason': 'Card not found'}
                
                auth_data = {
                    'customer_id': result[0],
                    'card_status': result[1],
                    'daily_limit': float(result[2]),
                    'current_daily_usage': float(result[3]),
                    'fraud_score': float(result[4]),
                }
                
                # Step 3: Cache for 5 minutes (expires before daily reset)
                self.redis_client.setex(
                    cache_key,
                    timedelta(minutes=5),
                    json.dumps(auth_data)
                )
                
                return auth_data
        finally:
            self.db_pool.putconn(conn)
    
    def approve_transaction(self, card_number: str, amount: float) -> bool:
        """Approve transaction if within limits."""
        auth_data = self.authorize_transaction(card_number, amount)
        
        if not auth_data.get('card_status') == 'ACTIVE':
            return False
        
        # Check daily limit
        remaining = auth_data['daily_limit'] - auth_data['current_daily_usage']
        if amount > remaining:
            return False
        
        # Check fraud score
        if auth_data['fraud_score'] > 0.7:
            return False
        
        # Update usage (async)
        self.update_usage_async(card_number, amount)
        
        return True

# Usage:
service = CardAuthorizationService()
result = service.approve_transaction('****-****-****-1234', 150.00)
print(f"Transaction approved: {result}")
```

#### Performance Improvement

```sql
-- Before optimization:
-- Card lookup: 2.3 seconds (full table scan)
-- Peak hour timeout rate: 5%
-- Customer complaints: 500/day

-- After optimization:
-- Card lookup: 15ms (Redis cache hit)
-- Card lookup: 120ms (database with index)
-- Peak hour timeout rate: 0.1%
-- Customer complaints: < 10/day
```

#### Key Metrics & Outcomes

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Authorization latency | 2-5 seconds | 15-120ms | 98% faster |
| Transaction timeouts | 5% | 0.1% | 98% reduction |
| Customer complaints | 500/day | < 10/day | 98% reduction |
| Daily revenue loss | $50K | $1K | 98% reduction |
| Peak hour capacity | 10K TPS | 50K TPS | 5x increase |

---

### Scenario 4: Regulatory Report Generation Optimization

> **Business Context:** A bank must generate 50+ regulatory reports daily, but the current process takes 8 hours and misses deadlines.

#### The Problem

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CURRENT STATE (Before)                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ❌ Report generation time: 8 hours (deadline: 4 hours)              │
│   ❌ 50+ reports, each with different data requirements               │
│   ❌ Sequential processing (one report at a time)                     │
│   ❌ Data extracted multiple times (redundant I/O)                    │
│   ❌ Manual intervention required for failures                        │
│                                                                         │
│   Timeline:                                                             │
│   10:00 PM: Start processing                                          │
│   6:00 AM:  Reports complete (2 hours late!)                          │
│   8:00 AM:  Deadline (reports not ready)                              │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Diagnosis Steps

```sql
-- Step 1: Analyze report generation bottlenecks
SELECT 
    report_name,
    start_time,
    end_time,
    EXTRACT(EPOCH FROM (end_time - start_time)) as duration_seconds,
    rows_processed,
    bytes_scanned
FROM report_execution_log
WHERE execution_date = CURRENT_DATE - 1
ORDER BY duration_seconds DESC
LIMIT 10;

-- Result: Basel III report takes 45 minutes, AML report takes 30 minutes

-- Step 2: Check for redundant data extraction
SELECT 
    source_table,
    COUNT(*) as extraction_count,
    SUM(bytes_scanned) as total_bytes
FROM report_execution_log
WHERE execution_date = CURRENT_DATE - 1
GROUP BY source_table
ORDER BY extraction_count DESC;

-- Problem: fact_transactions extracted 15 times (once per report)

-- Step 3: Analyze query patterns
SELECT 
    report_name,
    query_text,
    COUNT(*) as execution_count
FROM report_queries
WHERE execution_date = CURRENT_DATE - 1
GROUP BY report_name, query_text
HAVING COUNT(*) > 1;

-- Problem: Same base query duplicated across reports
```

#### Solution Implementation

```sql
-- Solution 1: Create common data mart for shared data
CREATE TABLE mart_daily_transaction_summary AS
SELECT 
    transaction_date,
    transaction_type,
    customer_segment,
    product_category,
    COUNT(*) as txn_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    COUNT(DISTINCT customer_id) as unique_customers
FROM fact_transactions
WHERE transaction_date >= CURRENT_DATE - 90
GROUP BY 1, 2, 3, 4;

-- Solution 2: Create pre-aggregated tables for common reports
CREATE TABLE mart_basel_iii_daily AS
SELECT 
    transaction_date,
    risk_category,
    COUNT(*) as exposure_count,
    SUM(amount) as exposure_amount,
    SUM(amount * risk_weight) as risk_weighted_amount
FROM fact_transactions t
JOIN dim_risk_categories r ON t.risk_category_id = r.risk_category_id
GROUP BY 1, 2;

-- Solution 3: Implement parallel report generation
-- (See Airflow DAG below)

-- Solution 4: Add indexes for report queries
CREATE INDEX idx_mart_txn_date ON mart_daily_transaction_summary(transaction_date);
CREATE INDEX idx_mart_txn_type ON mart_daily_transaction_summary(transaction_type);

-- Solution 5: Implement incremental processing
CREATE TABLE report_incremental_log (
    report_name VARCHAR(100),
    last_processed_date DATE,
    last_processed_timestamp TIMESTAMP,
    PRIMARY KEY (report_name)
);
```

#### Parallel Report Generation DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import multiprocessing

default_args = {
    'owner': 'regulatory-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': True,
    'email': ['regulatory@bank.com'],
}

def generate_basel_iii_report(**context):
    """Generate Basel III Capital Adequacy Report."""
    from airflow.hooks.postgres_hook import PostgresHook
    
    pg_hook = PostgresHook(postgres_conn_id='warehouse')
    
    # Use pre-aggregated mart instead of scanning raw data
    report = pg_hook.get_pandas_df("""
        SELECT 
            transaction_date,
            SUM(exposure_amount) as total_exposure,
            SUM(risk_weighted_amount) as total_rwa,
            SUM(risk_weighted_amount) / SUM(exposure_amount) * 100 as capital_ratio
        FROM mart_basel_iii_daily
        WHERE transaction_date = %s
        GROUP BY transaction_date
    """, parameters=[context['ds']])
    
    report.to_csv(f"/tmp/reports/basel_iii_{context['ds']}.csv", index=False)
    return len(report)

def generate_aml_report(**context):
    """Generate AML Monitoring Report."""
    from airflow.hooks.postgres_hook import PostgresHook
    
    pg_hook = PostgresHook(postgres_conn_id='warehouse')
    
    # Use pre-aggregated mart
    report = pg_hook.get_pandas_df("""
        SELECT 
            transaction_date,
            transaction_type,
            COUNT(*) as suspicious_count,
            SUM(amount) as suspicious_amount
        FROM mart_daily_transaction_summary
        WHERE transaction_date = %s
          AND transaction_type IN ('WIRE_TRANSFER', 'CASH_DEPOSIT')
          AND amount > 10000
        GROUP BY transaction_date, transaction_type
    """, parameters=[context['ds']])
    
    report.to_csv(f"/tmp/reports/aml_{context['ds']}.csv", index=False)
    return len(report)

def generate_customer_360_report(**context):
    """Generate Customer 360 Report."""
    from airflow.hooks.postgres_hook import PostgresHook
    
    pg_hook = PostgresHook(postgres_conn_id='warehouse')
    
    report = pg_hook.get_pandas_df("""
        SELECT 
            customer_segment,
            COUNT(DISTINCT customer_id) as customer_count,
            SUM(total_amount) as total_relationship_value
        FROM mart_daily_transaction_summary
        WHERE transaction_date = %s
        GROUP BY customer_segment
    """, parameters=[context['ds']])
    
    report.to_csv(f"/tmp/reports/customer_360_{context['ds']}.csv", index=False)
    return len(report)

with DAG(
    'parallel_report_generation',
    default_args=default_args,
    description='Parallel regulatory report generation',
    schedule_interval='0 22 * * *',  # 10 PM daily
    catchup=False,
    max_active_runs=1,
    tags=['regulatory', 'reports', 'parallel'],
) as dag:

    # Generate reports in parallel
    basel_iii = PythonOperator(
        task_id='generate_basel_iii',
        python_callable=generate_basel_iii_report,
    )

    aml = PythonOperator(
        task_id='generate_aml',
        python_callable=generate_aml_report,
    )

    customer_360 = PythonOperator(
        task_id='generate_customer_360',
        python_callable=generate_customer_360_report,
    )

    # All reports run in parallel
    [basel_iii, aml, customer_360]
```

#### Performance Improvement

```sql
-- Before optimization:
-- Report generation time: 8 hours (sequential)
-- Data extraction: 15 times (redundant)
-- Manual intervention: Daily

-- After optimization:
-- Report generation time: 1.5 hours (parallel)
-- Data extraction: 1 time (shared mart)
-- Manual intervention: Weekly
```

#### Key Metrics & Outcomes

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Report generation time | 8 hours | 1.5 hours | 81% faster |
| Data extraction count | 15 times | 1 time | 93% reduction |
| Data scanned | 50TB | 5TB | 90% reduction |
| Manual intervention | Daily | Weekly | 85% reduction |
| Deadline compliance | 70% | 100% | 30% improvement |

---

### Scenario 5: Multi-Tenant Analytics Cost Optimization

> **Business Context:** A bank provides analytics to 200+ branches, but costs are spiraling due to inefficient resource allocation.

#### The Problem

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    CURRENT STATE (Before)                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   ❌ Monthly analytics cost: $120,000                                 │
│   ❌ 200 branches, each running similar queries                        │
│   ❌ No resource isolation (noisy neighbor problem)                   │
│   ❌ Small branches consuming same resources as large branches        │
│   ❌ No cost attribution (can't charge back to branches)              │
│                                                                         │
│   Cost breakdown:                                                       │
│   • Compute: $80,000 (67%)                                             │
│   • Storage: $25,000 (21%)                                             │
│   • Network: $15,000 (12%)                                             │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

#### Diagnosis Steps

```sql
-- Step 1: Analyze query costs by branch
SELECT 
    branch_id,
    COUNT(*) as query_count,
    SUM(bytes_processed) / 1024 / 1024 / 1024 AS gb_processed,
    SUM(bytes_processed) * 0.00000000625 AS cost_usd,
    AVG(execution_time_ms) as avg_query_time_ms
FROM query_history
WHERE execution_date >= CURRENT_DATE - 30
GROUP BY branch_id
ORDER BY cost_usd DESC
LIMIT 20;

-- Problem: Top 10 branches consume 70% of costs

-- Step 2: Check for duplicate queries
SELECT 
    query_text,
    COUNT(*) as execution_count,
    SUM(bytes_processed) / 1024 / 1024 / 1024 AS total_gb_processed
FROM query_history
WHERE execution_date >= CURRENT_DATE - 30
GROUP BY query_text
HAVING COUNT(*) > 10
ORDER BY total_gb_processed DESC;

-- Problem: Same dashboard query executed 500 times by different branches

-- Step 3: Analyze resource utilization
SELECT 
    branch_id,
    MAX(concurrent_queries) as peak_concurrency,
    AVG(cpu_utilization) as avg_cpu,
    AVG(memory_utilization) as avg_memory
FROM resource_usage
WHERE usage_date >= CURRENT_DATE - 7
GROUP BY branch_id
HAVING AVG(cpu_utilization) < 30;

-- Problem: Many branches have low utilization (< 30%)
```

#### Solution Implementation

```sql
-- Solution 1: Create branch-specific materialized views
CREATE MATERIALIZED VIEW mv_branch_daily_summary AS
SELECT 
    branch_id,
    transaction_date,
    COUNT(*) as txn_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM fact_transactions
GROUP BY branch_id, transaction_date;

-- Solution 2: Implement query caching with Redis
-- (See application code below)

-- Solution 3: Create resource pools per branch tier
CREATE RESOURCE POOL branch_tier1_pool
    WITH (concurrency=50, memory='8GB', cpu=4);

CREATE RESOURCE POOL branch_tier2_pool
    WITH (concurrency=20, memory='4GB', cpu=2);

CREATE RESOURCE POOL branch_tier3_pool
    WITH (concurrency=10, memory='2GB', cpu=1);

-- Solution 4: Implement cost attribution
CREATE TABLE branch_cost_allocation (
    branch_id VARCHAR(10),
    query_id BIGINT,
    cost_usd DECIMAL(10,4),
    allocation_date DATE,
    PRIMARY KEY (branch_id, query_id)
);

-- Solution 5: Optimize storage with compression
ALTER TABLE fact_transactions SET (compression = 'zstd');
ALTER TABLE dim_customers SET (compression = 'snappy');
```

#### Application-Level Query Caching

```python
import redis
import hashlib
import json
from datetime import timedelta

class BranchAnalyticsCache:
    def __init__(self):
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
    
    def get_cached_result(self, branch_id: str, query: str) -> dict:
        """Get cached query result."""
        cache_key = self._generate_cache_key(branch_id, query)
        cached = self.redis_client.get(cache_key)
        
        if cached:
            return json.loads(cached)
        return None
    
    def cache_result(self, branch_id: str, query: str, result: dict, ttl_hours: int = 4):
        """Cache query result."""
        cache_key = self._generate_cache_key(branch_id, query)
        self.redis_client.setex(
            cache_key,
            timedelta(hours=ttl_hours),
            json.dumps(result)
        )
    
    def _generate_cache_key(self, branch_id: str, query: str) -> str:
        """Generate cache key from branch and query."""
        query_hash = hashlib.md5(query.encode()).hexdigest()
        return f"analytics:{branch_id}:{query_hash}"

# Usage:
cache = BranchAnalyticsCache()

# Check cache first
cached = cache.get_cached_result('BR001', 'SELECT SUM(amount) FROM ...')
if cached:
    print("Cache hit!")
    result = cached
else:
    # Execute query
    result = execute_query('SELECT SUM(amount) FROM ...')
    # Cache for 4 hours
    cache.cache_result('BR001', 'SELECT SUM(amount) FROM ...', result, ttl_hours=4)
```

#### Performance Improvement

```sql
-- Before optimization:
-- Monthly cost: $120,000
-- Average query time: 45 seconds
-- Cache hit rate: 0%

-- After optimization:
-- Monthly cost: $45,000 (63% reduction)
-- Average query time: 8 seconds (82% faster)
-- Cache hit rate: 75%
```

#### Key Metrics & Outcomes

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Monthly cost | $120,000 | $45,000 | 63% reduction |
| Average query time | 45 seconds | 8 seconds | 82% faster |
| Cache hit rate | 0% | 75% | New capability |
| Resource utilization | 30-90% | 60-80% | Balanced |
| Cost per branch | $600 | $225 | 63% reduction |

---

### Scenario Comparison Matrix

| Aspect | Query Optimization | BigQuery Cost | Transaction Processing | Reports | Multi-Tenant |
|--------|-------------------|---------------|------------------------|---------|--------------|
| **Primary Issue** | Slow queries | High costs | Latency spikes | Missed deadlines | Cost allocation |
| **Root Cause** | Missing indexes | Full table scans | No caching | Sequential processing | No resource isolation |
| **Solution Type** | Database tuning | Partitioning + MV | Redis cache | Parallel processing | Query caching |
| **Implementation Time** | 1 day | 3 days | 1 week | 2 weeks | 1 week |
| **Cost Impact** | $0 (time only) | $33K/month savings | $49K/day savings | $0 (time only) | $75K/month savings |
| **Risk Level** | Low | Medium | High | Medium | Low |

---

## 6. Hands-On Exercises

### Exercise 1: Query Plan Analysis
```sql
-- Task: Analyze and optimize a slow query

-- Create sample tables
CREATE TABLE orders (
    order_id BIGINT,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(10,2),
    status VARCHAR(20)
);

CREATE TABLE customers (
    customer_id INT,
    customer_name VARCHAR(100),
    segment VARCHAR(20)
);

-- Insert sample data (1M rows)
INSERT INTO orders 
SELECT 
    generate_series(1, 1000000),
    (random() * 10000)::int,
    '2024-01-01'::date + (random() * 365)::int,
    random() * 1000,
    CASE WHEN random() > 0.1 THEN 'completed' ELSE 'pending' END;

INSERT INTO customers 
SELECT generate_series(1, 10000), 'Customer ' || g, 
       CASE WHEN g % 3 = 0 THEN 'Premium' ELSE 'Standard' END
FROM generate_series(1, 10000) g;

-- BEFORE: Slow query
EXPLAIN ANALYZE
SELECT c.segment, COUNT(*), SUM(o.amount)
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01'
GROUP BY c.segment;

-- Solution: Add indexes
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_orders_customer ON orders(customer_id);

-- AFTER: Check improvement
EXPLAIN ANALYZE
SELECT c.segment, COUNT(*), SUM(o.amount)
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01'
GROUP BY c.segment;
```

### Exercise 2: Partitioning Strategy
```python
# Task: Implement optimal partitioning

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Create sample data
def create_sample_data(n_rows=1000000):
    dates = [datetime(2024, 1, 1) + timedelta(days=i % 365) for i in range(n_rows)]
    return pd.DataFrame({
        'order_id': range(n_rows),
        'customer_id': np.random.randint(1, 10001, n_rows),
        'order_date': dates,
        'amount': np.random.uniform(10, 1000, n_rows),
        'category': np.random.choice(['Electronics', 'Clothing', 'Food'], n_rows)
    })

df = create_sample_data()

# Good partitioning: by date (low cardinality, common filter)
df.to_parquet(
    '/data/orders/date_partitioned/',
    partition_cols=['order_date']
)
print("Date partitioned: Good for date range queries")

# Check file sizes
import os
total_size = 0
file_count = 0
for root, dirs, files in os.walk('/data/orders/date_partitioned/'):
    for file in files:
        if file.endswith('.parquet'):
            total_size += os.path.getsize(os.path.join(root, file))
            file_count += 1

print(f"Files: {file_count}, Avg size: {total_size/file_count/1024/1024:.2f} MB")

# Bad partitioning: by customer_id (high cardinality, too many files)
# df.to_parquet(
#     '/data/orders/customer_partitioned/',
#     partition_cols=['customer_id']
# )  # Don't do this!
```

### Exercise 3: Materialized View Design
```sql
-- Task: Create materialized views for common aggregations

-- Create base table
CREATE TABLE fact_sales (
    sale_id BIGINT,
    sale_date DATE,
    customer_key INT,
    product_key INT,
    store_key INT,
    amount DECIMAL(12,2),
    quantity INT
);

-- Materialized view 1: Daily sales by product
CREATE MATERIALIZED VIEW mv_daily_sales AS
SELECT 
    sale_date,
    product_key,
    SUM(amount) as total_sales,
    SUM(quantity) as total_quantity,
    COUNT(DISTINCT customer_key) as unique_customers
FROM fact_sales
GROUP BY sale_date, product_key;

-- Materialized view 2: Monthly sales by store
CREATE MATERIALIZED VIEW mv_monthly_store_sales AS
SELECT 
    DATE_TRUNC('month', sale_date) as month,
    store_key,
    SUM(amount) as total_sales,
    COUNT(*) as transaction_count
FROM fact_sales
GROUP BY 1, 2;

-- Query the materialized view (much faster than base table)
SELECT * FROM mv_daily_sales 
WHERE sale_date >= '2024-01-01' AND product_key = 123;

-- Refresh when base data changes
REFRESH MATERIALIZED VIEW mv_daily_sales;
REFRESH MATERIALIZED VIEW mv_monthly_store_sales;
```

### Exercise 4: Compression Analysis
```python
# Task: Compare compression algorithms

import pandas as pd
import os

# Create sample data
def create_test_data(n_rows=1000000):
    return pd.DataFrame({
        'id': range(n_rows),
        'name': [f'customer_{i % 10000}' for i in range(n_rows)],
        'amount': [i * 1.5 for i in range(n_rows)],
        'status': ['active'] * (n_rows // 2) + ['inactive'] * (n_rows // 2)
    })

df = create_test_data()
print(f"DataFrame size: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

# Test different compression algorithms
compressions = ['snappy', 'gzip', 'zstd', 'lz4']
results = []

for compression in compressions:
    filename = f'/tmp/test_{compression}.parquet'
    df.to_parquet(filename, compression=compression, index=False)
    size = os.path.getsize(filename) / 1024 / 1024
    results.append({'compression': compression, 'size_mb': size})
    print(f"{compression}: {size:.2f} MB")

# Compare results
results_df = pd.DataFrame(results)
print("\nCompression Comparison:")
print(results_df.to_string(index=False))

# Find best compression
best = results_df.loc[results_df['size_mb'].idxmin()]
print(f"\nBest: {best['compression']} ({best['size_mb']:.2f} MB)")
```

### Exercise 5: Cost Monitoring Dashboard
```python
# Task: Build a cost monitoring dashboard

import pandas as pd
from datetime import datetime, timedelta


class CostMonitor:
    def __init__(self):
        self.costs = []
    
    def log_query_cost(self, query_id, bytes_processed, query_time_ms):
        """Log query execution cost."""
        self.costs.append({
            'timestamp': datetime.now(),
            'query_id': query_id,
            'bytes_processed': bytes_processed,
            'cost_usd': bytes_processed * 0.00000000625,  # BigQuery pricing
            'query_time_ms': query_time_ms
        })
    
    def get_daily_summary(self):
        """Get daily cost summary."""
        df = pd.DataFrame(self.costs)
        df['date'] = pd.to_datetime(df['timestamp']).dt.date
        
        summary = df.groupby('date').agg(
            query_count=('query_id', 'count'),
            total_bytes=('bytes_processed', 'sum'),
            total_cost=('cost_usd', 'sum'),
            avg_query_time=('query_time_ms', 'mean')
        ).reset_index()
        
        summary['total_gb'] = summary['total_bytes'] / 1024 / 1024 / 1024
        return summary
    
    def find_expensive_queries(self, top_n=5):
        """Find most expensive queries."""
        df = pd.DataFrame(self.costs)
        return df.nlargest(top_n, 'cost_usd')
    
    def alert_on_budget(self, daily_budget=100):
        """Check if daily cost exceeds budget."""
        summary = self.get_daily_summary()
        today = datetime.now().date()
        today_cost = summary[summary['date'] == today]['total_cost'].sum()
        
        if today_cost > daily_budget:
            print(f"ALERT: Today's cost (${today_cost:.2f}) exceeds budget (${daily_budget})")
            return True
        return False


# Test the monitor
def test_cost_monitor():
    monitor = CostMonitor()
    
    # Simulate query costs
    for i in range(10):
        monitor.log_query_cost(
            query_id=f'Q{i:04d}',
            bytes_processed=(i + 1) * 1024 * 1024 * 100,  # 100MB - 1GB
            query_time_ms=(i + 1) * 1000
        )
    
    # Get summary
    print("Daily Summary:")
    print(monitor.get_daily_summary())
    
    # Find expensive queries
    print("\nTop 3 Expensive Queries:")
    print(monitor.find_expensive_queries(3))
    
    # Check budget
    print("\nBudget Check:")
    monitor.alert_on_budget(daily_budget=0.01)  # Low budget to trigger alert

test_cost_monitor()
```

---

## 7. Interview Questions

### Q1: How do you identify and fix slow SQL queries?

**Answer:** 
Systematic approach: 

1) **EXPLAIN ANALYZE** to see execution plan. 

2) Look for: sequential scans, nested loops on large tables, sort operations on large datasets. 

3) **Add indexes** on filtered/joined columns. 

4) **Rewrite queries:** Replace correlated subqueries with JOINs, avoid functions on indexed columns. 

5) **Partition tables** by date for range queries. 

6) **Use materialized views** for complex aggregations. 

7) **Update statistics** with ANALYZE. 

8) **Check for lock contention.** 

9) **Consider denormalization** for frequently joined tables. 

10) **Profile the application** to identify hot queries.

### Q2: Explain Redshift distribution keys and sort keys.

**Answer:** 

**Distribution keys (DISTKEY):** Determine how data is distributed across nodes. Rows with same DISTKEY value are stored on same node. Use the column most frequently used in JOINs (e.g., customer_key). Key distributes evenly across nodes; ALL copies entire table to every node (for small dimensions); EVEN uses round-robin. 

**Sort keys (SORTKEY):** Determine physical sort order on disk. Compound: Best when queries filter on leading columns. 

Interleaved: Equal weight to all sort columns. Together, proper DISTKEY/SORTKEY can improve query performance by 10-100x.

### Q3: How do you optimize Spark jobs?

**Answer:** 

Key optimizations: 

1) **Avoid shuffle:** Use broadcast joins for small tables, filter early. 

2) **Partition appropriately:** Set spark.sql.shuffle.partitions (default 200 is often too high). 

3) **Cache frequently accessed DataFrames.** 

4) **Use DataFrame/Dataset API** instead of RDDs (Catalyst optimizer). 

5) **Avoid UDFs:** Use built-in functions (10-100x faster). 

6) **Coalesce/repartition** before write to control file count. 

7) **Use columnar formats** (Parquet/ORC) for intermediate data. 

8) **Enable adaptive query execution** (Spark 3.0+). 

9) **Tune memory settings** based on data size.

### Q4: What is the difference between partitioning and bucketing?

**Answer:** 

**Partitioning:** Divides data into separate directories/files based on column values. Best for low-to-medium cardinality columns used in WHERE clauses (e.g., date). Enables partition pruning (only scan relevant partitions). 

**Bucketing:** Divides data into fixed number of files based on hash of column. Best for high-cardinality columns used in JOINs (e.g., customer_id). Ensures same key always goes to same bucket, enabling bucket joins. Use partitioning for time-based queries, bucketing for join optimization.

### Q5: How do you optimize costs in a data warehouse?

**Answer:** 

Multi-faceted approach: 

1) **Right-size compute:** Monitor query load and adjust warehouse size. 

2) **Auto-suspend:** Set aggressive suspend timeouts (5-15 min idle). 

3) **Concurrency scaling:** Use for peak loads instead of over-provisioning. 

4) **Materialized views:** Pre-compute expensive aggregations. 

5) **Partitioning:** Reduce data scanned per query. 

6) **Query optimization:** Eliminate SELECT *, use proper filters. 

7) **Storage tiering:** Move old data to cheaper storage. 

8) **Monitor and alert:** Track costs per team/query. 

9) **Reserved capacity:** Commit to base workload. 

10) **Use serverless options** for unpredictable workloads.

### Q6: How do you tune Spark job performance?

**Answer:**
1. **Shuffle Optimization:** Use broadcast joins, reduce shuffle partitions
2. **Memory Management:** Tune spark.sql.shuffle.partitions, spark.memory.fraction
3. **Caching:** Cache frequently reused DataFrames
4. **Data Skew:** Salt keys, use adaptive query execution
5. **File Formats:** Use Parquet/ORC for columnar storage
6. **Partitioning:** Repartition by join/filter keys
7. **Avoid UDFs:** Use built-in functions (Catalyst optimization)
8. **Speculative Execution:** Enable for slow tasks
9. **Monitoring:** Use Spark UI to identify bottlenecks

### Q7: What is the difference between materialized views and pre-aggregated tables?

**Answer:**
**Materialized Views:**
- Database manages refresh automatically
- Optimized for specific query patterns
- May not be real-time (depends on refresh strategy)
- Database can rewrite queries to use MV

**Pre-Aggregated Tables:**
- Manual refresh via ETL pipeline
- More flexible aggregation logic
- Can be real-time or near-real-time
- Requires explicit query rewriting

Best Practice: Use materialized views for simple aggregations, pre-aggregated tables for complex business logic.

---

## Summary Checklist

### Query Performance
- [ ] Read and interpret EXPLAIN ANALYZE plans
- [ ] Optimize indexes (B-Tree, Composite, Covering, Partial)
- [ ] Rewrite slow queries (correlated subqueries, OR conditions)

### Data Warehouse Optimization
- [ ] Configure Redshift distribution and sort keys
- [ ] Implement BigQuery partitioning and clustering
- [ ] Optimize Snowflake warehouse sizing and clustering

### Storage Optimization
- [ ] Choose appropriate file formats (Parquet, ORC, Avro)
- [ ] Optimize compression (Snappy, Gzip, Zstd)
- [ ] Design partitioning strategies
- [ ] Implement bucketing for join optimization

### Cost Optimization
- [ ] Monitor and analyze query costs
- [ ] Implement auto-suspend for idle resources
- [ ] Use reserved/spot instances for predictable workloads
- [ ] Optimize queries to reduce bytes processed

### Practical Skills
- [ ] Tune Spark job performance
- [ ] Design materialized views
- [ ] Build cost monitoring dashboards
- [ ] Investigate and resolve performance issues

---

*Next Section: [14 - Data Security](../14-Data-Security/README.md)*
