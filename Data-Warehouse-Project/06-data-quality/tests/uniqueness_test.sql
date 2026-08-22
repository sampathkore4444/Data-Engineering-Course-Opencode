-- Uniqueness Test
-- Check for duplicate records in dimension tables

-- Test 1: dim_customer - customer_id should be unique
SELECT 
    customer_id,
    COUNT(*) as duplicate_count
FROM gold.dim_customer
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Test 2: dim_account - account_id should be unique
SELECT 
    account_id,
    COUNT(*) as duplicate_count
FROM gold.dim_account
GROUP BY account_id
HAVING COUNT(*) > 1;

-- Test 3: fact_transactions - transaction_id should be unique
SELECT 
    transaction_id,
    COUNT(*) as duplicate_count
FROM gold.fact_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;
