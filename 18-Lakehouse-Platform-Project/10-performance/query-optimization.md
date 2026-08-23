# SQL Query Optimization Guide for Banking

## Overview
This guide covers query optimization techniques for banking workloads in Dremio.

## Common Query Patterns and Optimizations

### 1. Customer 360° Query Optimization

**Before Optimization:**
```sql
SELECT * FROM banking_gold.customer_360 
WHERE customer_id = 'CUST-12345';
```

**After Optimization:**
```sql
-- Use specific columns instead of SELECT *
SELECT 
    customer_id,
    customer_name,
    total_balance,
    total_card_outstanding,
    total_loan_outstanding,
    net_relationship_value
FROM banking_gold.customer_360 
WHERE customer_id = 'CUST-12345';

-- Enable reflection for this view
ALTER VIEW banking_gold.customer_360 
CREATE RAW REFLECTION 
PARTITION BY (customer_id)
DISPLAY BY (customer_name, total_balance, total_card_outstanding);
```

### 2. Transaction History Query Optimization

**Before Optimization:**
```sql
SELECT * FROM banking_cleansed.core_banking_transactions 
WHERE account_id = 'ACC-12345'
ORDER BY txn_date DESC;
```

**After Optimization:**
```sql
-- Add date filter to reduce data scanned
SELECT 
    txn_id,
    txn_date,
    txn_timestamp,
    txn_type_standardized,
    amount,
    channel_standardized,
    description
FROM banking_cleansed.core_banking_transactions 
WHERE account_id = 'ACC-12345'
  AND txn_date >= DATEADD(MONTH, -3, CURRENT_DATE)  -- Last 3 months only
ORDER BY txn_date DESC, txn_timestamp DESC
LIMIT 100;  -- Limit results

-- Create index on account_id and txn_date
CREATE INDEX idx_txn_account_date 
ON banking_cleansed.core_banking_transactions (account_id, txn_date);
```

### 3. Fraud Detection Query Optimization

**Before Optimization:**
```sql
SELECT * FROM gold.fraud_score 
WHERE fraud_score > 50
ORDER BY fraud_score DESC;
```

**After Optimization:**
```sql
-- Use specific columns and add time filter
SELECT 
    txn_id,
    card_number,
    customer_id,
    merchant_name,
    amount,
    fraud_score,
    recommended_action,
    evaluated_at
FROM gold.fraud_score 
WHERE fraud_score > 50
  AND evaluated_at >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())  -- Last 24 hours only
ORDER BY fraud_score DESC
LIMIT 1000;  -- Limit for dashboard

-- Enable reflection with real-time refresh
ALTER VIEW gold.fraud_score 
CREATE RAW REFLECTION 
PARTITION BY (card_number)
DISPLAY BY (customer_id, merchant_name, amount, fraud_score)
ORDER BY (fraud_score DESC);
```

### 4. Regulatory Report Query Optimization

**Before Optimization:**
```sql
SELECT * FROM gold.call_report 
WHERE report_date = CURRENT_DATE;
```

**After Optimization:**
```sql
-- Use specific columns
SELECT 
    report_date,
    total_assets,
    total_deposits,
    total_loans,
    cet1_capital,
    tier1_capital,
    total_capital,
    risk_weighted_assets,
    cet1_ratio,
    tier1_ratio,
    car_ratio,
    npl_ratio
FROM gold.call_report 
WHERE report_date = CURRENT_DATE;

-- Enable AGGREGATE reflection for regulatory reports
ALTER VIEW gold.call_report 
CREATE AGGREGATE REFLECTION 
GROUP BY (report_date)
MEASURE (total_assets, total_deposits, total_loans, 
         cet1_capital, tier1_capital, total_capital);
```

## Query Performance Analysis

### 1. EXPLAIN PLAN Analysis

```sql
-- Analyze query execution plan
EXPLAIN PLAN FOR
SELECT customer_id, customer_name, total_balance
FROM banking_gold.customer_360 
WHERE customer_id = 'CUST-12345';

-- Key metrics to look for:
-- - Rows scanned vs returned
-- - Join strategy (hash, sort-merge, nested loop)
-- - Filter pushdown
-- - Reflection usage
```

### 2. Query Statistics

```sql
-- Check query performance metrics
SELECT 
    query_id,
    query_text,
    start_time,
    end_time,
    DATEDIFF(MILLISECOND, start_time, end_time) AS duration_ms,
    rows_scanned,
    rows_returned,
    bytes_scanned,
    reflection_name,
    reflection_hit
FROM sys."query" 
WHERE start_time >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
ORDER BY duration_ms DESC
LIMIT 20;
```

### 3. Slow Query Detection

```sql
-- Find slow queries
SELECT 
    query_id,
    SUBSTRING(query_text, 1, 100) AS query_preview,
    start_time,
    DATEDIFF(MILLISECOND, start_time, end_time) AS duration_ms,
    rows_scanned,
    rows_returned,
    CASE 
        WHEN DATEDIFF(MILLISECOND, start_time, end_time) > 30000 THEN 'CRITICAL'
        WHEN DATEDIFF(MILLISECOND, start_time, end_time) > 10000 THEN 'SLOW'
        ELSE 'NORMAL'
    END AS performance_status
FROM sys."query" 
WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE)
  AND DATEDIFF(MILLISECOND, start_time, end_time) > 10000
ORDER BY duration_ms DESC;
```

## Performance Best Practices

### 1. Query Writing Best Practices

| Practice | Bad Example | Good Example |
|----------|-------------|--------------|
| **Select specific columns** | `SELECT * FROM table` | `SELECT col1, col2 FROM table` |
| **Filter early** | `SELECT * FROM table WHERE col > 100` | `SELECT * FROM table WHERE date >= '2024-01-01' AND col > 100` |
| **Limit results** | `SELECT * FROM table` | `SELECT * FROM table LIMIT 1000` |
| **Use proper JOINs** | `SELECT * FROM a, b WHERE a.id = b.id` | `SELECT * FROM a JOIN b ON a.id = b.id` |
| **Avoid subqueries** | `SELECT * FROM a WHERE id IN (SELECT id FROM b)` | `SELECT * FROM a JOIN b ON a.id = b.id` |

### 2. Index Strategy

```sql
-- Create indexes for common query patterns
CREATE INDEX idx_customer_id ON banking_cleansed.core_banking_customers (customer_id);
CREATE INDEX idx_account_customer ON banking_cleansed.core_banking_accounts (customer_id, account_id);
CREATE INDEX idx_txn_account_date ON banking_cleansed.core_banking_transactions (account_id, txn_date);
CREATE INDEX idx_card_customer ON banking_cleansed.credit_cards (customer_id, card_number);
CREATE INDEX idx_loan_customer ON banking_cleansed.loan_accounts (customer_id, loan_id);
```

### 3. Partitioning Strategy

```sql
-- Partition large tables by date
ALTER TABLE banking_cleansed.core_banking_transactions 
PARTITION BY (txn_date);

-- Partition by customer for customer-specific queries
ALTER TABLE banking_gold.customer_360 
PARTITION BY (customer_id);
```

## Monitoring and Alerting

### 1. Performance Dashboard

```sql
-- Query performance dashboard
SELECT 
    DATE(start_time) AS query_date,
    COUNT(*) AS total_queries,
    AVG(DATEDIFF(MILLISECOND, start_time, end_time)) AS avg_duration_ms,
    MAX(DATEDIFF(MILLISECOND, start_time, end_time)) AS max_duration_ms,
    COUNT(CASE WHEN DATEDIFF(MILLISECOND, start_time, end_time) > 10000 THEN 1 END) AS slow_queries,
    SUM(rows_scanned) AS total_rows_scanned,
    SUM(bytes_scanned) / 1024 / 1024 / 1024 AS total_gb_scanned
FROM sys."query" 
WHERE start_time >= DATEADD(DAY, -7, CURRENT_DATE)
GROUP BY DATE(start_time)
ORDER BY query_date DESC;
```

### 2. Reflection Hit Rate Monitoring

```sql
-- Monitor reflection effectiveness
SELECT 
    reflection_name,
    query_count,
    hit_count,
    ROUND(hit_count * 100.0 / NULLIF(query_count, 0), 2) AS hit_rate_pct,
    size_bytes / 1024 / 1024 AS size_mb,
    CASE 
        WHEN hit_rate_pct > 80 THEN 'EXCELLENT'
        WHEN hit_rate_pct > 50 THEN 'GOOD'
        WHEN hit_rate_pct > 20 THEN 'FAIR'
        ELSE 'POOR'
    END AS effectiveness
FROM sys."reflection"
WHERE query_count > 0
ORDER BY hit_rate_pct DESC;
```

### 3. Cost Analysis

```sql
-- Calculate query cost
SELECT 
    user_id,
    COUNT(*) AS query_count,
    SUM(rows_scanned) AS total_rows_scanned,
    SUM(bytes_scanned) / 1024 / 1024 / 1024 AS total_gb_scanned,
    AVG(DATEDIFF(MILLISECOND, start_time, end_time)) AS avg_duration_ms,
    SUM(bytes_scanned) / 1024 / 1024 / 1024 * 0.023 AS estimated_cost_usd  -- $0.023/GB
FROM sys."query" 
WHERE start_time >= DATEADD(DAY, -30, CURRENT_DATE)
GROUP BY user_id
ORDER BY total_gb_scanned DESC;
```
