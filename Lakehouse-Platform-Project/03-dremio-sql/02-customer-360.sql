-- Dremio SQL Scripts: Customer 360 View & Banking Analytics
-- ==========================================================

-- Query 1: Get Complete Customer 360° View
-- This single query joins data from Core Banking, Cards, and Loans
SELECT 
    customer_id,
    customer_name,
    customer_type,
    city,
    state,
    kyc_status,
    risk_category,
    customer_since,
    
    -- Banking Summary
    total_accounts,
    savings_balance,
    current_balance,
    
    -- Cards Summary
    total_cards,
    total_card_outstanding,
    total_credit_limit,
    
    -- Loans Summary
    total_loans,
    total_loan_outstanding,
    total_monthly_emi,
    credit_risk,
    
    -- Computed: Total Relationship Value
    (savings_balance + current_balance + total_card_outstanding + total_loan_outstanding) 
        AS total_relationship_value,
    
    -- Computed: Customer Tier
    CASE 
        WHEN (savings_balance + current_balance + total_card_outstanding + total_loan_outstanding) > 10000000 
            THEN 'PLATINUM'
        WHEN (savings_balance + current_balance + total_card_outstanding + total_loan_outstanding) > 5000000 
            THEN 'GOLD'
        WHEN (savings_balance + current_balance + total_card_outstanding + total_loan_outstanding) > 1000000 
            THEN 'SILVER'
        ELSE 'BRONZE'
    END AS customer_tier
    
FROM "banking-vault"."virtual.customer_360"
WHERE customer_id = 'CUST-001';

-- Query 2: Get Customer Transaction History (All Channels)
-- Shows transactions from both Core Banking and Cards
SELECT 
    'CORE_BANKING' AS channel,
    transaction_id,
    customer_id,
    account_id AS reference_id,
    transaction_type,
    amount,
    balance_after,
    description,
    transaction_date,
    status
FROM "banking-vault"."virtual.transaction_analytics"
WHERE customer_id = 'CUST-001'

UNION ALL

SELECT 
    'CREDIT_CARD' AS channel,
    transaction_id,
    customer_id,
    card_id AS reference_id,
    transaction_type,
    transaction_amount AS amount,
    NULL AS balance_after,
    merchant_name AS description,
    transaction_date,
    status
FROM "banking-vault"."virtual.card_transaction_analytics"
WHERE customer_id = 'CUST-001'

ORDER BY transaction_date DESC;

-- Query 3: Customer Portfolio Analysis
-- Shows complete financial portfolio for a customer
SELECT 
    c.customer_id,
    c.customer_name,
    
    -- Accounts
    a.account_id,
    a.account_type,
    a.balance AS account_balance,
    
    -- Cards
    cc.card_id,
    cc.card_type,
    cc.outstanding AS card_outstanding,
    cc.credit_limit AS card_limit,
    
    -- Loans
    l.loan_id,
    l.loan_type,
    l.principal_outstanding AS loan_outstanding,
    l.emi_amount AS monthly_emi,
    l.npa_classification AS loan_status
    
FROM "banking-postgres".core_banking.customers c
LEFT JOIN "banking-vault"."virtual.customer_accounts" a ON c.customer_id = a.customer_id
LEFT JOIN "banking-vault"."virtual.customer_cards" cc ON c.customer_id = cc.customer_id
LEFT JOIN "banking-vault"."virtual.customer_loans" l ON c.customer_id = l.customer_id
WHERE c.customer_id = 'CUST-001';

-- Query 4: High-Value Customers Analysis
-- Find customers with total relationship > 1 Crore
SELECT 
    customer_id,
    customer_name,
    customer_type,
    city,
    savings_balance,
    current_balance,
    total_card_outstanding,
    total_loan_outstanding,
    (savings_balance + current_balance + total_card_outstanding + total_loan_outstanding) 
        AS total_relationship_value
FROM "banking-vault"."virtual.customer_360"
WHERE (savings_balance + current_balance + total_card_outstanding + total_loan_outstanding) > 10000000
ORDER BY total_relationship_value DESC;

-- Query 5: Customer Segmentation by Product Usage
SELECT 
    customer_type,
    COUNT(DISTINCT customer_id) AS customer_count,
    AVG(savings_balance) AS avg_savings,
    AVG(total_card_outstanding) AS avg_card_outstanding,
    AVG(total_loan_outstanding) AS avg_loan_outstanding,
    AVG(total_relationship_value) AS avg_total_relationship
FROM "banking-vault"."virtual.customer_360"
GROUP BY customer_type
ORDER BY avg_total_relationship DESC;

-- Query 6: Geographic Distribution of Customers
SELECT 
    state,
    city,
    COUNT(DISTINCT customer_id) AS customer_count,
    SUM(savings_balance + current_balance) AS total_deposits,
    SUM(total_loan_outstanding) AS total_loans,
    SUM(total_relationship_value) AS total_relationship
FROM "banking-vault"."virtual.customer_360"
GROUP BY state, city
ORDER BY total_relationship DESC;

-- Query 7: Risk Analysis - Customers with High Credit Exposure
SELECT 
    c.customer_id,
    c.customer_name,
    c.risk_category,
    
    -- Total Exposure
    (a.balance + cc.outstanding + l.principal_outstanding) AS total_exposure,
    
    -- Account Details
    a.balance AS account_balance,
    
    -- Card Details
    cc.outstanding AS card_outstanding,
    cc.credit_limit AS card_limit,
    (cc.outstanding / cc.credit_limit * 100) AS card_utilization_pct,
    
    -- Loan Details
    l.loan_type,
    l.principal_outstanding AS loan_outstanding,
    l.days_past_due,
    l.npa_classification
    
FROM "banking-postgres".core_banking.customers c
LEFT JOIN "banking-vault"."virtual.customer_accounts" a ON c.customer_id = a.customer_id
LEFT JOIN "banking-vault"."virtual.customer_cards" cc ON c.customer_id = cc.customer_id
LEFT JOIN "banking-vault"."virtual.customer_loans" l ON c.customer_id = l.customer_id
WHERE c.risk_category IN ('HIGH', 'VERY_HIGH')
   OR l.days_past_due > 0
   OR (cc.outstanding / cc.credit_limit) > 0.80
ORDER BY total_exposure DESC;

-- Query 8: Cross-Sell Opportunities
-- Find customers who have only one product type
SELECT 
    customer_id,
    customer_name,
    customer_type,
    city,
    total_accounts,
    total_cards,
    total_loans,
    total_relationship_value,
    
    CASE 
        WHEN total_accounts > 0 AND total_cards = 0 AND total_loans = 0 
            THEN 'POTENTIAL_CARDS_CUSTOMER'
        WHEN total_accounts > 0 AND total_cards > 0 AND total_loans = 0 
            THEN 'POTENTIAL_LOAN_CUSTOMER'
        WHEN total_accounts > 0 AND total_cards = 0 AND total_loans > 0 
            THEN 'POTENTIAL_CARD_CUSTOMER'
        ELSE 'CROSS_SELL'
    END AS opportunity_type
    
FROM "banking-vault"."virtual.customer_360"
WHERE (total_accounts > 0 AND total_cards = 0) 
   OR (total_accounts > 0 AND total_loans = 0)
ORDER BY total_relationship_value DESC;

-- Query 9: Real-Time Customer Lookup (For Call Center)
-- Optimized for fast response
SELECT 
    c.customer_id,
    c.customer_name,
    c.phone,
    c.email,
    c.kyc_status,
    
    -- Quick Account Summary
    a.balance AS primary_account_balance,
    a.account_type,
    
    -- Quick Card Summary
    cc.card_number,
    cc.card_type,
    cc.outstanding AS card_outstanding,
    
    -- Quick Loan Summary
    l.loan_type,
    l.principal_outstanding AS loan_balance,
    l.emi_amount AS next_emi,
    
    -- Last Transaction
    t.transaction_date AS last_transaction_date,
    t.amount AS last_transaction_amount,
    t.description AS last_transaction_description
    
FROM "banking-postgres".core_banking.customers c
LEFT JOIN "banking-vault"."virtual.customer_accounts" a ON c.customer_id = a.customer_id
LEFT JOIN "banking-vault"."virtual.customer_cards" cc ON c.customer_id = cc.customer_id
LEFT JOIN "banking-vault"."virtual.customer_loans" l ON c.customer_id = l.customer_id
LEFT JOIN (
    SELECT customer_id, transaction_date, amount, description,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY transaction_date DESC) AS rn
    FROM "banking-vault"."virtual.transaction_analytics"
) t ON c.customer_id = t.customer_id AND t.rn = 1
WHERE c.customer_id = 'CUST-001';

-- Query 10: Customer Activity Summary (Last 30 Days)
SELECT 
    c.customer_id,
    c.customer_name,
    
    -- Transaction Summary
    COUNT(DISTINCT t.transaction_id) AS total_transactions,
    SUM(CASE WHEN t.transaction_type = 'CREDIT' THEN t.amount ELSE 0 END) AS total_credits,
    SUM(CASE WHEN t.transaction_type = 'DEBIT' THEN t.amount ELSE 0 END) AS total_debits,
    
    -- Card Activity
    COUNT(DISTINCT ct.transaction_id) AS card_transactions,
    SUM(ct.transaction_amount) AS card_spend,
    
    -- Active Days
    COUNT(DISTINCT DATE(t.transaction_date)) AS active_days
    
FROM "banking-postgres".core_banking.customers c
LEFT JOIN "banking-vault"."virtual.transaction_analytics" t 
    ON c.customer_id = t.customer_id 
    AND t.transaction_date >= CURRENT_DATE() - INTERVAL '30' DAY
LEFT JOIN "banking-vault"."virtual.card_transaction_analytics" ct 
    ON c.customer_id = ct.customer_id 
    AND ct.transaction_date >= CURRENT_DATE() - INTERVAL '30' DAY
GROUP BY c.customer_id, c.customer_name
ORDER BY total_credits + total_debits + card_spend DESC;