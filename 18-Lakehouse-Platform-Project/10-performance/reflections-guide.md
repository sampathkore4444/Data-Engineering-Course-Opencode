# Dremio Reflections Optimization Guide

## Overview
Reflections are Dremio's materialized accelerations that pre-compute and store query results for faster performance. This guide covers reflection strategy for banking workloads.

## Reflection Types

| Type | Description | Best For | Refresh Cost |
|------|-------------|----------|--------------|
| **RAW** | Stores full dataset with layout optimization | Frequent full-table scans | High |
| **AGGREGATE** | Pre-aggregates data | Aggregation queries | Medium |
| **EXTERNAL** | References external materialization | Iceberg/Delta tables | Low |

## Banking Reflection Strategy

### 1. Customer 360° Reflection (RAW)

```sql
-- Enable RAW reflection for Customer 360
ALTER VIEW banking_gold.customer_360 
CREATE RAW REFLECTION 
PARTITION BY (customer_id)
DISPLAY BY (customer_name, city)
ORDER BY (customer_id);

-- Expected performance improvement: 10x
-- Refresh schedule: Every 2 hours
-- Storage cost: ~2x source data size
```

### 2. Daily Transaction Summary (AGGREGATE)

```sql
-- Enable AGGREGATE reflection for transaction summary
ALTER VIEW banking_gold.daily_transaction_summary 
CREATE AGGREGATE REFLECTION 
GROUP BY (txn_date, channel_standardized, txn_type_standardized)
MEASURE (transaction_count, total_amount, avg_amount, unique_accounts)
ORDER BY (txn_date DESC);

-- Expected performance improvement: 50x for aggregation queries
-- Refresh schedule: Every hour
-- Storage cost: ~0.1x source data size
```

### 3. Credit Risk Dashboard (RAW)

```sql
-- Enable RAW reflection for risk dashboard
ALTER VIEW banking_gold.credit_risk_dashboard 
CREATE RAW REFLECTION 
PARTITION BY (customer_id)
DISPLAY BY (customer_name, loan_type, risk_classification)
ORDER BY (risk_classification, principal_outstanding DESC);

-- Expected performance improvement: 8x
-- Refresh schedule: Every 4 hours
-- Storage cost: ~1.5x source data size
```

### 4. Fraud Score (RAW)

```sql
-- Enable RAW reflection for fraud scoring
ALTER VIEW gold.fraud_score 
CREATE RAW REFLECTION 
PARTITION BY (card_number)
DISPLAY BY (customer_id, merchant_name, amount, fraud_score, recommended_action)
ORDER BY (fraud_score DESC);

-- Expected performance improvement: 15x (critical for real-time)
-- Refresh schedule: Real-time (streaming)
-- Storage cost: ~1x source data size
```

## Reflection Management

### Monitor Reflection Status

```sql
-- Check reflection status
SELECT 
    reflection_name,
    reflection_type,
    status,
    refresh_time,
    size_bytes,
    display_time
FROM sys."reflection"
ORDER BY refresh_time DESC;

-- Check reflection hit rate
SELECT 
    reflection_name,
    query_count,
    hit_count,
    ROUND(hit_count * 100.0 / NULLIF(query_count, 0), 2) AS hit_rate_pct
FROM sys."reflection"
WHERE query_count > 0
ORDER BY hit_rate_pct DESC;
```

### Refresh Reflections

```sql
-- Manual refresh (emergency)
ALTER REFLECTION customer_360_raw REFRESH;

-- Refresh all reflections
ALTER SYSTEM REFRESH ALL REFLECTIONS;

-- Check refresh progress
SELECT 
    reflection_name,
    status,
    refresh_start,
    refresh_end,
    DATEDIFF(MINUTE, refresh_start, refresh_end) AS refresh_duration_min
FROM sys."reflection"
WHERE status = 'REFRESHING';
```

### Drop Unused Reflections

```sql
-- Find reflections with low hit rate
SELECT 
    reflection_name,
    query_count,
    hit_count,
    ROUND(hit_count * 100.0 / NULLIF(query_count, 0), 2) AS hit_rate_pct,
    size_bytes / 1024 / 1024 AS size_mb
FROM sys."reflection"
WHERE query_count < 100  -- Less than 100 queries
  AND hit_count < 10     -- Less than 10 hits
ORDER BY size_bytes DESC;

-- Drop unused reflection
ALTER VIEW banking_gold.customer_360 DROP REFLECTION customer_360_raw;
```

## Performance Tuning

### Reflection Layout Optimization

```sql
-- Optimize layout for common query patterns
ALTER VIEW banking_gold.customer_360 
CREATE RAW REFLECTION 
PARTITION BY (customer_id, city)  -- Partition by high-cardinality columns
DISPLAY BY (customer_name, total_balance, total_cards)  -- Include frequently accessed columns
ORDER BY (customer_id);  -- Sort for range queries
```

### Reflection Refresh Optimization

```sql
-- Use incremental refresh for large datasets
ALTER VIEW banking_gold.daily_transaction_summary 
CREATE AGGREGATE REFLECTION 
REFRESH STRATEGY INCREMENTAL
REFRESH INCREMENTAL SELECT * FROM banking_gold.daily_transaction_summary 
WHERE txn_date >= DATEADD(DAY, -1, CURRENT_DATE);

-- Schedule refresh during off-peak hours
ALTER SYSTEM SET "reflection.refresh.schedule" = "0 2 * * *";  -- 2 AM daily
```

## Cost Optimization

### Storage Cost Analysis

```sql
-- Calculate reflection storage cost
SELECT 
    reflection_name,
    reflection_type,
    size_bytes / 1024 / 1024 / 1024 AS size_gb,
    size_bytes / 1024 / 1024 / 1024 * 0.023 AS monthly_cost_usd,  -- $0.023/GB
    query_count,
    hit_count,
    ROUND(hit_count * 100.0 / NULLIF(query_count, 0), 2) AS hit_rate_pct,
    CASE 
        WHEN hit_count > 1000 AND size_bytes / 1024 / 1024 / 1024 < 10 
        THEN 'KEEP'
        WHEN hit_count < 100 
        THEN 'CONSIDER_DROPPING'
        ELSE 'REVIEW'
    END AS recommendation
FROM sys."reflection"
ORDER BY size_bytes DESC;
```

### Reflection Refresh Cost

```sql
-- Calculate refresh cost
SELECT 
    reflection_name,
    refresh_strategy,
    DATEDIFF(MINUTE, refresh_start, refresh_end) AS refresh_duration_min,
    refresh_duration_min * 0.5 AS estimated_cost_usd,  -- $0.50/min
    refresh_count,
    refresh_count * estimated_cost_usd AS monthly_refresh_cost
FROM sys."reflection"
WHERE refresh_start IS NOT NULL
ORDER BY monthly_refresh_cost DESC;
```

## Best Practices

### 1. Reflection Selection

| Query Pattern | Recommended Reflection | Why |
|---------------|----------------------|-----|
| `SELECT * FROM table WHERE id = ?` | RAW | Full row access |
| `SELECT col1, col2 FROM table` | RAW | Column selection |
| `SELECT SUM(col) FROM table GROUP BY col` | AGGREGATE | Pre-aggregated |
| `SELECT * FROM table JOIN other` | RAW on both | Join optimization |
| `SELECT * FROM table ORDER BY col LIMIT 10` | RAW | Sorted data |

### 2. Refresh Strategy

| Dataset Size | Refresh Strategy | Frequency |
|-------------|-----------------|-----------|
| < 1 GB | FULL | Real-time |
| 1-100 GB | INCREMENTAL | Hourly |
| > 100 GB | INCREMENTAL | Daily |

### 3. Partitioning Strategy

| Query Pattern | Partition By | Why |
|---------------|-------------|-----|
| Customer queries | customer_id | Filter optimization |
| Time-series queries | date | Range pruning |
| Geographic queries | region/city | Location-based access |

## Monitoring Dashboard

```sql
-- Reflection health dashboard
SELECT 
    reflection_name,
    reflection_type,
    status,
    size_bytes / 1024 / 1024 AS size_mb,
    query_count,
    hit_count,
    ROUND(hit_count * 100.0 / NULLIF(query_count, 0), 2) AS hit_rate_pct,
    refresh_time,
    DATEDIFF(MINUTE, refresh_time, CURRENT_TIMESTAMP) AS age_minutes,
    CASE 
        WHEN status = 'ACTIVE' AND DATEDIFF(MINUTE, refresh_time, CURRENT_TIMESTAMP) < 60 
        THEN 'HEALTHY'
        WHEN status = 'ACTIVE' AND DATEDIFF(MINUTE, refresh_time, CURRENT_TIMESTAMP) < 240 
        THEN 'STALE'
        ELSE 'NEEDS_REFRESH'
    END AS health_status
FROM sys."reflection"
ORDER BY health_status, size_bytes DESC;
```
