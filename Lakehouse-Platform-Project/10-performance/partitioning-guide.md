# Data Partitioning Guide for Banking

## Overview
Partitioning divides large tables into smaller, more manageable pieces. This guide covers partitioning strategies for banking data.

## Partitioning Strategies

### 1. Date-Based Partitioning (Most Common)

**Best for:** Transaction data, time-series data, daily reports

```sql
-- Partition transactions by date
CREATE TABLE banking_cleansed.core_banking_transactions (
    txn_id BIGINT,
    account_id VARCHAR(20),
    amount DECIMAL(18,2),
    txn_date DATE,
    txn_timestamp TIMESTAMP,
    txn_type VARCHAR(20),
    channel VARCHAR(20)
)
PARTITION BY (txn_date);

-- Query optimization: Dremio automatically prunes partitions
SELECT * FROM banking_cleansed.core_banking_transactions
WHERE txn_date BETWEEN '2024-01-01' AND '2024-01-31';
-- Only scans January 2024 partitions
```

### 2. Customer-Based Partitioning

**Best for:** Customer data, account data, customer-specific queries

```sql
-- Partition accounts by customer
CREATE TABLE banking_cleansed.core_banking_accounts (
    account_id VARCHAR(20),
    customer_id VARCHAR(20),
    balance DECIMAL(18,2),
    account_type VARCHAR(20),
    status VARCHAR(20)
)
PARTITION BY (customer_id);

-- Query optimization: Customer-specific queries are fast
SELECT * FROM banking_cleansed.core_banking_accounts
WHERE customer_id = 'CUST-12345';
-- Only scans that customer's partition
```

### 3. Branch-Based Partitioning

**Best for:** Branch-level reporting, regional analysis

```sql
-- Partition by branch for branch managers
CREATE TABLE banking_cleansed.core_banking_transactions (
    txn_id BIGINT,
    account_id VARCHAR(20),
    branch_code VARCHAR(10),
    amount DECIMAL(18,2),
    txn_date DATE
)
PARTITION BY (branch_code);

-- Branch-specific query is fast
SELECT * FROM banking_cleansed.core_banking_transactions
WHERE branch_code = 'BR001';
```

### 4. Composite Partitioning

**Best for:** Complex query patterns, multi-dimensional access

```sql
-- Partition by date and customer
CREATE TABLE banking_cleansed.core_banking_transactions (
    txn_id BIGINT,
    account_id VARCHAR(20),
    customer_id VARCHAR(20),
    amount DECIMAL(18,2),
    txn_date DATE
)
PARTITION BY (txn_date, customer_id);

-- Query 1: Date range (prunes by date)
SELECT * FROM banking_cleansed.core_banking_transactions
WHERE txn_date = '2024-01-15';

-- Query 2: Customer + date (prunes by both)
SELECT * FROM banking_cleansed.core_banking_transactions
WHERE customer_id = 'CUST-12345' AND txn_date = '2024-01-15';
```

## Banking-Specific Partitioning

### 1. Transaction Data Partitioning

```sql
-- Optimal partitioning for transactions
CREATE TABLE banking_cleansed.core_banking_transactions (
    txn_id BIGINT,
    account_id VARCHAR(20),
    customer_id VARCHAR(20),
    amount DECIMAL(18,2),
    txn_date DATE,
    txn_timestamp TIMESTAMP,
    txn_type VARCHAR(20),
    channel VARCHAR(20),
    branch_code VARCHAR(10)
)
PARTITION BY (txn_date);

-- Add monthly partition for historical data
ALTER TABLE banking_cleansed.core_banking_transactions 
ADD PARTITION (txn_date = '2024-01-01');
```

### 2. Card Transaction Partitioning

```sql
-- Partition card transactions by date and card
CREATE TABLE banking_cleansed.credit_card_transactions (
    txn_id BIGINT,
    card_number VARCHAR(20),
    amount DECIMAL(18,2),
    merchant_category VARCHAR(50),
    txn_date DATE,
    txn_timestamp TIMESTAMP
)
PARTITION BY (txn_date);

-- Fraud detection query (recent transactions)
SELECT * FROM banking_cleansed.credit_card_transactions
WHERE card_number = 'XXXX-XXXX-XXXX-1234'
  AND txn_date >= DATEADD(DAY, -7, CURRENT_DATE);
```

### 3. Loan Data Partitioning

```sql
-- Partition loans by status and type
CREATE TABLE banking_cleansed.loan_accounts (
    loan_id VARCHAR(20),
    customer_id VARCHAR(20),
    loan_type VARCHAR(20),
    principal_outstanding DECIMAL(18,2),
    status VARCHAR(20),
    disbursement_date DATE
)
PARTITION BY (loan_type);

-- Risk report query (by loan type)
SELECT * FROM banking_cleansed.loan_accounts
WHERE loan_type = 'HOME_LOAN' AND status = 'ACTIVE';
```

## Partition Management

### 1. Create Partitions

```sql
-- Create new monthly partition
ALTER TABLE banking_cleansed.core_banking_transactions 
ADD PARTITION (txn_date = '2024-02-01');

-- Create partition for new customer
ALTER TABLE banking_cleansed.core_banking_accounts 
ADD PARTITION (customer_id = 'CUST-NEW-001');
```

### 2. Drop Old Partitions

```sql
-- Drop partition older than 7 years (SBV requirement)
ALTER TABLE banking_cleansed.core_banking_transactions 
DROP PARTITION (txn_date = '2017-01-01');

-- Archive before dropping
INSERT INTO banking_archive.transactions_2017
SELECT * FROM banking_cleansed.core_banking_transactions
WHERE txn_date = '2017-01-01';

ALTER TABLE banking_cleansed.core_banking_transactions 
DROP PARTITION (txn_date = '2017-01-01');
```

### 3. Merge Partitions

```sql
-- Merge small partitions
ALTER TABLE banking_cleansed.core_banking_transactions 
MERGE PARTITIONS (txn_date = '2024-01-01', txn_date = '2024-01-02');
```

## Performance Monitoring

### 1. Partition Statistics

```sql
-- Check partition distribution
SELECT 
    txn_date,
    COUNT(*) AS row_count,
    SUM(LENGTH(txn_id)) / 1024 / 1024 AS size_mb
FROM banking_cleansed.core_banking_transactions
GROUP BY txn_date
ORDER BY txn_date DESC;
```

### 2. Partition Pruning Verification

```sql
-- Verify partition pruning in query plan
EXPLAIN PLAN FOR
SELECT * FROM banking_cleansed.core_baking_transactions
WHERE txn_date = '2024-01-15';

-- Look for "Partition Pruning" in the plan
-- Should show only relevant partitions being scanned
```

### 3. Skew Detection

```sql
-- Detect partition skew
SELECT 
    txn_date,
    COUNT(*) AS row_count,
    AVG(COUNT(*)) OVER () AS avg_rows,
    COUNT(*) / AVG(COUNT(*)) OVER () AS skew_ratio
FROM banking_cleansed.core_baking_transactions
GROUP BY txn_date
HAVING COUNT(*) / AVG(COUNT(*)) OVER () > 2;  -- More than 2x average
```

## Best Practices

### 1. Partition Key Selection

| Query Pattern | Recommended Partition Key | Why |
|---------------|--------------------------|-----|
| Daily reports | txn_date | Date range queries |
| Customer analytics | customer_id | Customer-specific queries |
| Branch reporting | branch_code | Branch-level access |
| Fraud detection | card_number | Card-specific queries |
| Regulatory reports | report_date | Report generation |

### 2. Partition Size Guidelines

| Table Size | Recommended Partitions | Partition Size |
|------------|----------------------|----------------|
| < 1 GB | 1-10 | 100 MB - 1 GB |
| 1-100 GB | 10-100 | 1-10 GB |
| 100 GB - 1 TB | 100-1000 | 1-10 GB |
| > 1 TB | 1000+ | 1-10 GB |

### 3. Partition Maintenance

```sql
-- Daily partition maintenance script
CREATE PROCEDURE banking_maintenance.maintain_partitions()
AS
BEGIN
    -- Add new partition for tomorrow
    ALTER TABLE banking_cleansed.core_baking_transactions 
    ADD PARTITION (txn_date = DATEADD(DAY, 1, CURRENT_DATE));
    
    -- Drop partition older than 7 years
    ALTER TABLE banking_cleansed.core_baking_transactions 
    DROP PARTITION (txn_date = DATEADD(YEAR, -7, CURRENT_DATE));
    
    -- Log maintenance
    INSERT INTO banking_maintenance.partition_log (
        action, table_name, partition_key, partition_value
    ) VALUES (
        'MAINTENANCE', 'core_baking_transactions', 
        'txn_date', CURRENT_DATE
    );
END;
```

## Common Pitfalls

### 1. Too Many Partitions

```sql
-- Bad: Partitioning by timestamp (creates millions of partitions)
CREATE TABLE transactions (
    txn_id BIGINT,
    txn_timestamp TIMESTAMP
)
PARTITION BY (txn_timestamp);  -- BAD: Too many partitions

-- Good: Partition by date (creates manageable partitions)
CREATE TABLE transactions (
    txn_id BIGINT,
    txn_timestamp TIMESTAMP,
    txn_date DATE
)
PARTITION BY (txn_date);  -- GOOD: Daily partitions
```

### 2. Skewed Partitions

```sql
-- Bad: Partitioning by status (uneven distribution)
CREATE TABLE loans (
    loan_id VARCHAR(20),
    status VARCHAR(20)
)
PARTITION BY (status);  -- BAD: Most loans are ACTIVE

-- Good: Partition by loan type (more even distribution)
CREATE TABLE loans (
    loan_id VARCHAR(20),
    loan_type VARCHAR(20),
    status VARCHAR(20)
)
PARTITION BY (loan_type);  -- GOOD: More even distribution
```

### 3. Missing Partition Key in Queries

```sql
-- Bad: Query without partition key (scans all partitions)
SELECT * FROM transactions WHERE account_id = 'ACC-12345';

-- Good: Query with partition key (prunes partitions)
SELECT * FROM transactions 
WHERE txn_date = '2024-01-15' AND account_id = 'ACC-12345';
```
