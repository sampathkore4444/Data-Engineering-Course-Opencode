# 13 - Performance Optimization

## Table of Contents
1. [Query Performance](#1-query-performance)
2. [Data Warehouse Optimization](#2-data-warehouse-optimization)
3. [Storage Optimization](#3-storage-optimization)
4. [Cost Optimization](#4-cost-optimization)
5. [Real-World Scenarios](#5-real-world-scenarios)
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

### Scenario 1: Redshift Performance Tuning

```
Problem: Queries taking 10+ minutes

Diagnosis:
1. EXPLAIN shows sequential scan on 1B rows
2. No distribution key set
3. No sort key

Solution:
1. Add DISTKEY(customer_key) for joins
2. Add SORTKEY(order_date) for date filters
3. VACUUM to sort data
4. ANALYZE to update statistics

Result: Queries now run in 5 seconds
```

### Scenario 2: Cost Spike Investigation

```
Problem: BigQuery costs increased 3x

Diagnosis:
1. Query history shows full table scans
2. No partitioning on large tables
3. SELECT * used frequently

Solution:
1. Add partitioning by date
2. Add clustering by customer_id
3. Replace SELECT * with specific columns
4. Use materialized views for dashboards

Result: Costs reduced by 60%
```

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
