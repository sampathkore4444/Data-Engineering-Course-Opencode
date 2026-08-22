-- Query Optimization Techniques
-- Banking Data Warehouse

-- =====================================================
-- 1. ANALYZE QUERY PERFORMANCE
-- =====================================================

-- Enable query tracking
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Find slow queries
SELECT 
    query,
    calls,
    total_time,
    mean_time,
    rows
FROM pg_stat_statements
ORDER BY mean_time DESC
LIMIT 10;

-- =====================================================
-- 2. OPTIMIZE COMMON QUERIES
-- =====================================================

-- BAD: Full table scan
SELECT * FROM gold.fact_transactions 
WHERE transaction_date = '2024-01-15';

-- GOOD: Use index
SELECT transaction_id, amount 
FROM gold.fact_transactions 
WHERE transaction_date_sk = 20240115;

-- =====================================================
-- 3. MATERIALIZED VIEWS FOR COMMON AGGREGATIONS
-- =====================================================

-- Create materialized view for daily summary
CREATE MATERIALIZED VIEW gold.mv_daily_transaction_summary AS
SELECT 
    transaction_date_sk,
    transaction_type,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
FROM gold.fact_transactions
GROUP BY transaction_date_sk, transaction_type;

-- Refresh daily
REFRESH MATERIALIZED VIEW gold.mv_daily_transaction_summary;

-- =====================================================
-- 4. VACUUM AND ANALYZE
-- =====================================================

-- Analyze table statistics
ANALYZE gold.fact_transactions;
ANALYZE gold.dim_customer;

-- Vacuum to reclaim space
VACUUM ANALYZE gold.fact_transactions;

-- =====================================================
-- 5. CONNECTION POOLING
-- =====================================================

-- Check active connections
SELECT 
    state,
    COUNT(*) as connection_count
FROM pg_stat_activity
WHERE datname = 'banking_dw'
GROUP BY state;
