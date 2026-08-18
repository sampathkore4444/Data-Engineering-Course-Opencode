# 13 - Performance Optimization

## Table of Contents
1. [Query Performance](#1-query-performance)
2. [Data Warehouse Optimization](#2-data-warehouse-optimization)
3. [Storage Optimization](#3-storage-optimization)
4. [Cost Optimization](#4-cost-optimization)
5. [Interview Questions](#5-interview-questions)

---

## 1. Query Performance

### Understanding Execution Plans

`sql
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
`

### Index Optimization

`sql
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
`

### Query Rewriting

`sql
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
`

---

## 2. Data Warehouse Optimization

### Redshift Optimization

`sql
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
`

### BigQuery Optimization

`sql
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
`

### Snowflake Optimization

`sql
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
`

---

## 3. Storage Optimization

### File Format Comparison

`
CSV:     No compression, no column pruning, no predicate pushdown
JSON:    Low compression, schema-on-read, nested data support
Parquet: Excellent compression, column pruning, predicate pushdown
ORC:     Excellent compression, built-in indexes (Hive optimized)
Avro:    Row-oriented, good compression, schema evolution (streaming)
`

### Compression Optimization

`python
# Parquet with different compression
df.to_parquet("data.parquet", compression="snappy")  # Fast, 2-3x
df.to_parquet("data.parquet", compression="gzip")    # Better, 5-10x
df.to_parquet("data.parquet", compression="zstd")    # Best balance, 3-5x
df.to_parquet("data.parquet", compression="lz4")     # Fastest decompress
`

### Partition Design

`python
# Good: Partition by date (common filter)
df.write.partitionBy("year", "month").parquet("/data/orders/")

# Bad: Partition by high-cardinality column (too many small files)
df.write.partitionBy("customer_id").parquet("/data/orders/")

# Optimal: Check file sizes
# Target: 128MB - 1GB per file
`

### Bucketing/Clustering

`python
# BigQuery clustering
CREATE TABLE dataset.orders
CLUSTER BY customer_id, product_category
AS SELECT * FROM source_orders;

# Spark bucketing
df.write.bucketBy(100, "customer_id").saveAsTable("orders_bucketed")
`

---

## 4. Cost Optimization

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

`sql
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
FROM egion-us.INFORMATION_SCHEMA.JOBS
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY total_bytes_processed DESC;
`

---

## 5. Interview Questions

### Q1: How do you identify and fix slow SQL queries?

**Answer:** Systematic approach: 1) **EXPLAIN ANALYZE** to see execution plan. 2) Look for: sequential scans, nested loops on large tables, sort operations on large datasets. 3) **Add indexes** on filtered/joined columns. 4) **Rewrite queries:** Replace correlated subqueries with JOINs, avoid functions on indexed columns. 5) **Partition tables** by date for range queries. 6) **Use materialized views** for complex aggregations. 7) **Update statistics** with ANALYZE. 8) **Check for lock contention.** 9) **Consider denormalization** for frequently joined tables. 10) **Profile the application** to identify hot queries.

### Q2: Explain Redshift distribution keys and sort keys.

**Answer:** **Distribution keys (DISTKEY):** Determine how data is distributed across nodes. Rows with same DISTKEY value are stored on same node. Use the column most frequently used in JOINs (e.g., customer_key). Key distributes evenly across nodes; ALL copies entire table to every node (for small dimensions); EVEN uses round-robin. **Sort keys (SORTKEY):** Determine physical sort order on disk. Compound: Best when queries filter on leading columns. Interleaved: Equal weight to all sort columns. Together, proper DISTKEY/SORTKEY can improve query performance by 10-100x.

### Q3: How do you optimize Spark jobs?

**Answer:** Key optimizations: 1) **Avoid shuffle:** Use broadcast joins for small tables, filter early. 2) **Partition appropriately:** Set spark.sql.shuffle.partitions (default 200 is often too high). 3) **Cache frequently accessed DataFrames.** 4) **Use DataFrame/Dataset API** instead of RDDs (Catalyst optimizer). 5) **Avoid UDFs:** Use built-in functions (10-100x faster). 6) **Coalesce/repartition** before write to control file count. 7) **Use columnar formats** (Parquet/ORC) for intermediate data. 8) **Enable adaptive query execution** (Spark 3.0+). 9) **Tune memory settings** based on data size.

### Q4: What is the difference between partitioning and bucketing?

**Answer:** **Partitioning:** Divides data into separate directories/files based on column values. Best for low-to-medium cardinality columns used in WHERE clauses (e.g., date). Enables partition pruning (only scan relevant partitions). **Bucketing:** Divides data into fixed number of files based on hash of column. Best for high-cardinality columns used in JOINs (e.g., customer_id). Ensures same key always goes to same bucket, enabling bucket joins. Use partitioning for time-based queries, bucketing for join optimization.

### Q5: How do you optimize costs in a data warehouse?

**Answer:** Multi-faceted approach: 1) **Right-size compute:** Monitor query load and adjust warehouse size. 2) **Auto-suspend:** Set aggressive suspend timeouts (5-15 min idle). 3) **Concurrency scaling:** Use for peak loads instead of over-provisioning. 4) **Materialized views:** Pre-compute expensive aggregations. 5) **Partitioning:** Reduce data scanned per query. 6) **Query optimization:** Eliminate SELECT *, use proper filters. 7) **Storage tiering:** Move old data to cheaper storage. 8) **Monitor and alert:** Track costs per team/query. 9) **Reserved capacity:** Commit to base workload. 10) **Use serverless options** for unpredictable workloads.

---

*Next Section: [14 - Data Security](../14-Data-Security/README.md)*
