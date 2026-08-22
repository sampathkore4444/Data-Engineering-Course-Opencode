-- Dremio SQL Scripts: Source Connections & Virtual Datasets
-- =========================================================

-- Step 1: Create Source Connections in Dremio
-- (These are typically done via Dremio UI, but shown here as SQL)

-- PostgreSQL Connection (Core Banking + Loans)
-- Connection Name: banking-postgres
-- Host: postgres-core-banking
-- Port: 5432
-- Database: core_banking

-- MySQL Connection (Credit Cards)
-- Connection Name: banking-mysql
-- Host: mysql-credit-cards
-- Port: 3306
-- Database: credit_cards

-- Step 2: Create Physical Datasets (Source Tables)

-- Core Banking Tables
CREATE DATASET IF NOT EXISTS "banking-postgres".core_banking.customers
AS SELECT * FROM "banking-postgres".core_banking.customers;

CREATE DATASET IF NOT EXISTS "banking-postgres".core_banking.accounts
AS SELECT * FROM "banking-postgres".core_banking.accounts;

CREATE DATASET IF NOT EXISTS "banking-postgres".core_banking.transactions
AS SELECT * FROM "banking-postgres".core_banking.transactions;

-- Credit Cards Tables
CREATE DATASET IF NOT EXISTS "banking-mysql".credit_cards.credit_cards
AS SELECT * FROM "banking-mysql".credit_cards.credit_cards;

CREATE DATASET IF NOT EXISTS "banking-mysql".credit_cards.card_transactions
AS SELECT * FROM "banking-mysql".credit_cards.card_transactions;

-- Loans Tables
CREATE DATASET IF NOT EXISTS "banking-postgres".core_banking.loan_accounts
AS SELECT * FROM "banking-postgres".core_banking.loan_accounts;

CREATE DATASET IF NOT EXISTS "banking-postgres".core_banking.loan_payments
AS SELECT * FROM "banking-postgres".core_banking.loan_payments;

-- Step 3: Create Virtual Datasets (Business Views)

-- Virtual Dataset: Customer Master
CREATE OR REPLACE VDS "banking-vault"."virtual.customer_master" AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_type,
    c.email,
    c.phone,
    c.address,
    c.city,
    c.state,
    c.pincode,
    c.pan_number,
    c.aadhaar_number,
    c.kyc_status,
    c.customer_since,
    c.risk_category
FROM "banking-postgres".core_banking.customers c;

-- Virtual Dataset: Customer Accounts
CREATE OR REPLACE VDS "banking-vault"."virtual.customer_accounts" AS
SELECT 
    a.account_id,
    a.customer_id,
    a.account_type,
    a.balance,
    a.status,
    c.customer_name,
    c.customer_type
FROM "banking-postgres".core_banking.accounts a
JOIN "banking-postgres".core_banking.customers c ON a.customer_id = c.customer_id;

-- Virtual Dataset: Customer Credit Cards
CREATE OR REPLACE VDS "banking-vault"."virtual.customer_cards" AS
SELECT 
    cc.card_id,
    cc.customer_id,
    cc.card_number,
    cc.card_type,
    cc.credit_limit,
    cc.outstanding,
    cc.card_status,
    cc.reward_points,
    c.customer_name,
    c.customer_type
FROM "banking-mysql".credit_cards.credit_cards cc
JOIN "banking-postgres".core_banking.customers c ON cc.customer_id = c.customer_id;

-- Virtual Dataset: Customer Loans
CREATE OR REPLACE VDS "banking-vault"."virtual.customer_loans" AS
SELECT 
    l.loan_id,
    l.customer_id,
    l.loan_type,
    l.loan_amount,
    l.principal_outstanding,
    l.interest_rate,
    l.emi_amount,
    l.loan_status,
    l.npa_classification,
    l.days_past_due,
    c.customer_name,
    c.customer_type
FROM "banking-postgres".core_banking.loan_accounts l
JOIN "banking-postgres".core_banking.customers c ON l.customer_id = c.customer_id;

-- Step 4: Create Business-Level Virtual Datasets

-- Virtual Dataset: 360 Customer View
CREATE OR REPLACE VDS "banking-vault"."virtual.customer_360" AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_type,
    c.email,
    c.phone,
    c.city,
    c.state,
    c.kyc_status,
    c.risk_category,
    c.customer_since,
    
    -- Account Summary
    COUNT(DISTINCT a.account_id) AS total_accounts,
    SUM(CASE WHEN a.account_type = 'SAVINGS' THEN a.balance ELSE 0 END) AS savings_balance,
    SUM(CASE WHEN a.account_type = 'CURRENT' THEN a.balance ELSE 0 END) AS current_balance,
    
    -- Cards Summary
    COUNT(DISTINCT cc.card_id) AS total_cards,
    SUM(cc.outstanding) AS total_card_outstanding,
    SUM(cc.credit_limit) AS total_credit_limit,
    
    -- Loans Summary
    COUNT(DISTINCT l.loan_id) AS total_loans,
    SUM(l.principal_outstanding) AS total_loan_outstanding,
    SUM(l.emi_amount) AS total_monthly_emi,
    CASE WHEN SUM(l.days_past_due) > 90 THEN 'HIGH' 
         WHEN SUM(l.days_past_due) > 30 THEN 'MEDIUM' 
         ELSE 'LOW' END AS credit_risk
    
FROM "banking-postgres".core_banking.customers c
LEFT JOIN "banking-vault"."virtual.customer_accounts" a ON c.customer_id = a.customer_id
LEFT JOIN "banking-vault"."virtual.customer_cards" cc ON c.customer_id = cc.customer_id
LEFT JOIN "banking-vault"."virtual.customer_loans" l ON c.customer_id = l.customer_id
GROUP BY 
    c.customer_id, c.customer_name, c.customer_type, 
    c.email, c.phone, c.city, c.state, 
    c.kyc_status, c.risk_category, c.customer_since;

-- Virtual Dataset: Transaction Analytics
CREATE OR REPLACE VDS "banking-vault"."virtual.transaction_analytics" AS
SELECT 
    t.transaction_id,
    t.customer_id,
    c.customer_name,
    t.account_id,
    a.account_type,
    t.transaction_type,
    t.amount,
    t.balance_after,
    t.description,
    t.transaction_date,
    t.status,
    DATE_TRUNC('day', t.transaction_date) AS transaction_day,
    DATE_TRUNC('week', t.transaction_date) AS transaction_week,
    DATE_TRUNC('month', t.transaction_date) AS transaction_month,
    EXTRACT(DAY FROM t.transaction_date) AS day_of_month,
    EXTRACT(HOUR FROM t.transaction_date) AS hour_of_day,
    CASE 
        WHEN t.amount < 10000 THEN 'SMALL'
        WHEN t.amount < 100000 THEN 'MEDIUM'
        WHEN t.amount < 1000000 THEN 'LARGE'
        ELSE 'VERY LARGE'
    END AS amount_category
FROM "banking-postgres".core_banking.transactions t
JOIN "banking-postgres".core_banking.accounts a ON t.account_id = a.account_id
JOIN "banking-postgres".core_banking.customers c ON t.customer_id = c.customer_id;

-- Virtual Dataset: Card Transaction Analytics
CREATE OR REPLACE VDS "banking-vault"."virtual.card_transaction_analytics" AS
SELECT 
    ct.transaction_id,
    ct.card_id,
    cc.customer_id,
    c.customer_name,
    ct.transaction_amount,
    ct.transaction_type,
    ct.merchant_name,
    ct.merchant_category,
    ct.transaction_date,
    ct.status,
    cc.card_type,
    cc.credit_limit,
    ct.transaction_amount / cc.credit_limit * 100 AS utilization_pct
FROM "banking-mysql".credit_cards.card_transactions ct
JOIN "banking-mysql".credit_cards.credit_cards cc ON ct.card_id = cc.card_id
JOIN "banking-postgres".core_banking.customers c ON cc.customer_id = c.customer_id;

-- Virtual Dataset: Loan Performance
CREATE OR REPLACE VDS "banking-vault"."virtual.loan_performance" AS
SELECT 
    l.loan_id,
    l.customer_id,
    c.customer_name,
    l.loan_type,
    l.loan_amount,
    l.principal_outstanding,
    l.interest_rate,
    l.emi_amount,
    l.loan_status,
    l.npa_classification,
    l.days_past_due,
    l.disbursement_date,
    l.maturity_date,
    DATEDIFF(month, l.disbursement_date, CURRENT_DATE()) AS months_elapsed,
    (l.loan_amount - l.principal_outstanding) / l.loan_amount * 100 AS repayment_pct,
    CASE 
        WHEN l.days_past_due = 0 THEN 'CURRENT'
        WHEN l.days_past_due <= 30 THEN 'DELINQUENT_30'
        WHEN l.days_past_due <= 60 THEN 'DELINQUENT_60'
        WHEN l.days_past_due <= 90 THEN 'DELINQUENT_90'
        ELSE 'NPA'
    END AS delinquency_bucket
FROM "banking-postgres".core_banking.loan_accounts l
JOIN "banking-postgres".core_banking.customers c ON l.customer_id = c.customer_id;