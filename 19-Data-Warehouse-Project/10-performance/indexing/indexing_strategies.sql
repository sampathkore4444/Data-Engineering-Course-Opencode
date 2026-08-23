-- Indexing Strategies for Data Warehouse
-- Banking Data Warehouse

-- =====================================================
-- 1. DIMENSION TABLE INDEXES
-- =====================================================

-- dim_customer: Primary lookup by customer_id
CREATE INDEX idx_dim_customer_id ON gold.dim_customer(customer_id);
CREATE INDEX idx_dim_customer_segment ON gold.dim_customer(customer_segment);
CREATE INDEX idx_dim_customer_type ON gold.dim_customer(customer_type);

-- dim_account: Lookups by account_id and customer_id
CREATE INDEX idx_dim_account_id ON gold.dim_account(account_id);
CREATE INDEX idx_dim_account_customer ON gold.dim_account(customer_id);
CREATE INDEX idx_dim_account_type ON gold.dim_account(account_type);

-- dim_branch: Lookups by branch_id and region
CREATE INDEX idx_dim_branch_id ON gold.dim_branch(branch_id);
CREATE INDEX idx_dim_branch_region ON gold.dim_branch(region);

-- dim_date: Lookups by date components
CREATE INDEX idx_dim_date_full ON gold.dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON gold.dim_date(year, month);

-- =====================================================
-- 2. FACT TABLE INDEXES (Partition-aware)
-- =====================================================

-- fact_transactions: Most queried table
CREATE INDEX idx_fact_txn_account ON gold.fact_transactions(account_sk);
CREATE INDEX idx_fact_txn_date ON gold.fact_transactions(transaction_date_sk);
CREATE INDEX idx_fact_txn_type ON gold.fact_transactions(transaction_type);
CREATE INDEX idx_fact_txn_amount ON gold.fact_transactions(amount);

-- Composite index for common query pattern
CREATE INDEX idx_fact_txn_account_date ON gold.fact_transactions(account_sk, transaction_date_sk);

-- fact_account_balance: Daily snapshots
CREATE INDEX idx_fact_balance_account ON gold.fact_account_balance(account_sk);
CREATE INDEX idx_fact_balance_date ON gold.fact_account_balance(snapshot_date_sk);
CREATE INDEX idx_fact_balance_branch ON gold.fact_account_balance(branch_sk);

-- =====================================================
-- 3. PARTIAL INDEXES (for specific use cases)
-- =====================================================

-- Only active accounts
CREATE INDEX idx_dim_account_active ON gold.dim_account(account_id) 
WHERE status = 'ACTIVE';

-- Large transactions only
CREATE INDEX idx_fact_txn_large ON gold.fact_transactions(amount, transaction_date_sk) 
WHERE amount > 10000000;

-- =====================================================
-- 4. INDEX MAINTENANCE
-- =====================================================

-- Check index usage
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as times_used,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes
WHERE schemaname = 'gold'
ORDER BY idx_scan DESC;

-- Find unused indexes
SELECT 
    schemaname,
    tablename,
    indexname,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size
FROM pg_stat_user_indexes
WHERE idx_scan = 0
  AND schemaname = 'gold';

-- Reindex if needed
-- REINDEX INDEX gold.idx_fact_txn_account;
