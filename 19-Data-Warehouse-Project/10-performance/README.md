# Performance Optimization Guide

## Overview

This folder contains performance optimization strategies for the banking data warehouse, including indexing, partitioning, and query tuning.

---

## Optimization Strategies

### 1. Indexing

```sql
-- Primary Key Indexes (auto-created)
-- Dimension tables have surrogate key indexes

-- Foreign Key Indexes (for JOIN performance)
CREATE INDEX idx_fact_txn_date ON dw.fact_transactions(date_key);
CREATE INDEX idx_fact_txn_customer ON dw.fact_transactions(customer_sk);
CREATE INDEX idx_fact_txn_account ON dw.fact_transactions(account_sk);
CREATE INDEX idx_fact_txn_branch ON dw.fact_transactions(branch_sk);

-- Composite Indexes (for common query patterns)
CREATE INDEX idx_fact_txn_date_type ON dw.fact_transactions(date_key, txn_type);
CREATE INDEX idx_fact_txn_date_branch ON dw.fact_transactions(date_key, branch_sk);

-- Partial Indexes (for filtered queries)
CREATE INDEX idx_fact_txn_high_value ON dw.fact_transactions(transaction_amount)
WHERE is_high_value = TRUE;

-- Covering Indexes (include all columns needed by query)
CREATE INDEX idx_fact_txn_covering ON dw.fact_transactions(date_key, customer_sk)
INCLUDE (transaction_amount, txn_type);
```

### 2. Partitioning

```sql
-- Range Partitioning by Date (for fact tables)
CREATE TABLE dw.fact_transactions (
    transaction_sk SERIAL,
    date_key INT,
    ...
) PARTITION BY RANGE (date_key);

-- Create partitions for each year
CREATE TABLE dw.fact_transactions_2023
    PARTITION OF dw.fact_transactions
    FOR VALUES FROM (20230101) TO (20240101);

CREATE TABLE dw.fact_transactions_2024
    PARTITION OF dw.fact_transactions
    FOR VALUES FROM (20240101) TO (20250101);

-- List Partitioning by Region (for branch analysis)
CREATE TABLE dw.fact_transactions_south
    PARTITION OF dw.fact_transactions
    FOR VALUES IN ('SOUTH');
```

### 3. Materialized Views

```sql
-- Pre-aggregated daily summary
CREATE MATERIALIZED VIEW mv_daily_transaction_summary AS
SELECT
    date_key,
    customer_segment,
    account_type,
    branch_region,
    COUNT(*) AS transaction_count,
    SUM(transaction_amount) AS total_amount,
    AVG(transaction_amount) AS avg_amount
FROM dw.fact_transactions t
JOIN dw.dim_customer c ON t.customer_sk = c.customer_sk
JOIN dw.dim_account a ON t.account_sk = a.account_sk
JOIN dw.dim_branch b ON t.branch_sk = b.branch_sk
GROUP BY date_key, customer_segment, account_type, branch_region;

-- Refresh strategy
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_transaction_summary;
```

### 4. Query Optimization

```sql
-- Use EXPLAIN ANALYZE to check query performance
EXPLAIN ANALYZE
SELECT * FROM vw_customer_360
WHERE customer_segment = 'CORPORATE';

-- Avoid SELECT * in production
-- Bad:
SELECT * FROM fact_transactions;

-- Good:
SELECT date_key, transaction_amount FROM fact_transactions;

-- Use WHERE clauses that match indexes
-- Good:
SELECT * FROM fact_transactions WHERE date_key = 20240115;

-- Bad (full table scan):
SELECT * FROM fact_transactions WHERE EXTRACT(MONTH FROM txn_date) = 1;
```

### 5. Connection Pooling

```yaml
# PgBouncer configuration for connection pooling
[databases]
banking_dw = host=postgres-dw port=5432 dbname=banking_dw

[pgbouncer]
pool_mode = transaction
max_client_conn = 100
default_pool_size = 20
```

---

## Performance Checklist

| Check | Status | Notes |
|-------|--------|-------|
| Primary keys on all tables | ✅ | Auto-increment surrogate keys |
| Foreign key indexes | ✅ | Indexes on all FK columns |
| Composite indexes | ✅ | For common query patterns |
| Partitioning on fact tables | ⚠️ | Optional for large datasets |
| Materialized views | ✅ | For pre-aggregated summaries |
| Connection pooling | ⚠️ | Add PgBouncer for production |
| Query limits | ✅ | Use LIMIT for exploratory queries |
| EXPLAIN ANALYZE | ✅ | Check before deploying queries |

---

## Benchmarking

```sql
-- Test query performance
\timing on

-- Simple query
SELECT COUNT(*) FROM fact_transactions;

-- Aggregation query
SELECT
    d.month_name,
    COUNT(*) AS txn_count,
    SUM(t.transaction_amount) AS total_amount
FROM fact_transactions t
JOIN dim_date d ON t.date_key = d.date_key
GROUP BY d.month_name, d.month_number
ORDER BY d.month_number;

-- Complex join query
SELECT * FROM vw_customer_360
WHERE customer_segment = 'CORPORATE'
ORDER BY total_deposit_balance DESC
LIMIT 10;
```

---

*Part of: [Data Warehouse Project](../README.md)*
