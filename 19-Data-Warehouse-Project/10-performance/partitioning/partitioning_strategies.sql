-- Partitioning Strategies for Data Warehouse
-- Banking Data Warehouse

-- =====================================================
-- 1. RANGE PARTITIONING (by date)
-- =====================================================

-- Partition fact_transactions by transaction_date
CREATE TABLE gold.fact_transactions_partitioned (
    transaction_id VARCHAR(50),
    account_sk INT,
    transaction_date_sk INT,
    transaction_type VARCHAR(20),
    amount DECIMAL(15,2),
    transaction_date DATE
) PARTITION BY RANGE (transaction_date);

-- Create monthly partitions
CREATE TABLE gold.fact_transactions_2024_01 
    PARTITION OF gold.fact_transactions_partitioned
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');

CREATE TABLE gold.fact_transactions_2024_02 
    PARTITION OF gold.fact_transactions_partitioned
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');

-- Create partition for current month
CREATE TABLE gold.fact_transactions_current 
    PARTITION OF gold.fact_transactions_partitioned
    FOR VALUES FROM (DATE_TRUNC('month', CURRENT_DATE)) 
                  TO (DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 month');

-- =====================================================
-- 2. LIST PARTITIONING (by category)
-- =====================================================

-- Partition by transaction type
CREATE TABLE gold.fact_transactions_by_type (
    transaction_id VARCHAR(50),
    account_sk INT,
    transaction_type VARCHAR(20),
    amount DECIMAL(15,2)
) PARTITION BY LIST (transaction_type);

-- Create partitions for each type
CREATE TABLE gold.fact_txn_debit 
    PARTITION OF gold.fact_transactions_by_type
    FOR VALUES IN ('DEBIT');

CREATE TABLE gold.fact_txn_credit 
    PARTITION OF gold.fact_transactions_by_type
    FOR VALUES IN ('CREDIT');

CREATE TABLE gold.fact_txn_transfer 
    PARTITION OF gold.fact_transactions_by_type
    FOR VALUES IN ('TRANSFER');

-- =====================================================
-- 3. MANAGING PARTITIONS
-- =====================================================

-- View partitions
SELECT 
    schemaname,
    tablename,
    partitionname,
    partitionboundary
FROM pg_partitions
WHERE tablename = 'fact_transactions_partitioned';

-- Drop old partition (archive first!)
-- ALTER TABLE gold.fact_transactions_partitioned 
-- DETACH PARTITION gold.fact_transactions_2023_01;

-- =====================================================
-- 4. QUERYING PARTITIONED TABLES
-- =====================================================

-- Query automatically uses partition pruning
SELECT * 
FROM gold.fact_transactions_partitioned
WHERE transaction_date >= '2024-01-01' 
  AND transaction_date < '2024-02-01';
-- Only scans 2024_01 partition!

-- =====================================================
-- 5. PARTITION MAINTENANCE
-- =====================================================

-- Create future partitions (run monthly via cron)
DO $$
DECLARE
    start_date DATE := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '1 month');
    end_date DATE := start_date + INTERVAL '1 month';
    partition_name TEXT;
BEGIN
    partition_name := 'fact_transactions_' || TO_CHAR(start_date, 'YYYY_MM');
    
    EXECUTE FORMAT(
        'CREATE TABLE IF NOT EXISTS gold.%I PARTITION OF gold.fact_transactions_partitioned FOR VALUES FROM (%L) TO (%L)',
        partition_name, start_date, end_date
    );
END $$;
