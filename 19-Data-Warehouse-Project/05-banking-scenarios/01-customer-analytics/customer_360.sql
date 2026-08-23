-- =============================================================================
-- CUSTOMER 360 ANALYTICS
-- =============================================================================
-- Purpose: Unified view of customer across all banking products
-- Use Case: Relationship managers, call center, customer service
-- =============================================================================

-- Customer 360 View
CREATE OR REPLACE VIEW vw_customer_360 AS
SELECT
    -- Customer Information
    c.customer_sk,
    c.customer_id,
    c.customer_name,
    c.city,
    c.state,
    c.customer_type,
    c.customer_segment,
    c.age,
    c.age_group,

    -- Account Summary
    COUNT(DISTINCT a.account_sk) AS total_accounts,
    SUM(CASE WHEN a.account_type = 'SAVINGS' THEN 1 ELSE 0 END) AS savings_accounts,
    SUM(CASE WHEN a.account_type = 'CURRENT' THEN 1 ELSE 0 END) AS current_accounts,
    SUM(CASE WHEN a.account_type = 'FIXED_DEPOSIT' THEN 1 ELSE 0 END) AS fd_accounts,

    -- Balance Summary
    SUM(CASE WHEN a.account_type IN ('SAVINGS', 'FIXED_DEPOSIT') THEN a.current_balance ELSE 0 END) AS total_deposit_balance,
    SUM(CASE WHEN a.account_type = 'CURRENT' THEN a.current_balance ELSE 0 END) AS total_current_balance,

    -- Transaction Summary (last 30 days)
    COUNT(DISTINCT t.transaction_sk) AS total_transactions_30d,
    SUM(t.transaction_amount) AS total_transaction_amount_30d,

    -- Loan Summary
    COUNT(DISTINCT lp.loan_id) AS total_loans,
    SUM(lp.principal_outstanding) AS total_loan_outstanding,

    -- Risk Indicators
    MAX(CASE WHEN lp.is_npa THEN 1 ELSE 0 END) AS has_npa,
    SUM(CASE WHEN lp.days_past_due > 30 THEN 1 ELSE 0 END) AS overdue_payments

FROM dw.dim_customer c
LEFT JOIN dw.dim_account a ON c.customer_id = a.customer_id AND a.is_active = TRUE
LEFT JOIN dw.fact_transactions t ON c.customer_sk = t.customer_sk
LEFT JOIN dw.fact_loan_payment lp ON c.customer_sk = lp.customer_sk
WHERE c.is_current = TRUE
GROUP BY c.customer_sk, c.customer_id, c.customer_name, c.city, c.state,
         c.customer_type, c.customer_segment, c.age, c.age_group;

-- Customer Segmentation Query
CREATE OR REPLACE VIEW vw_customer_segmentation AS
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    AVG(total_deposit_balance) AS avg_deposit_balance,
    AVG(total_transactions_30d) AS avg_monthly_transactions,
    SUM(total_loan_outstanding) AS total_loan_exposure
FROM vw_customer_360
GROUP BY customer_segment
ORDER BY total_loan_exposure DESC;

-- Top Customers by Balance
SELECT
    customer_id,
    customer_name,
    city,
    customer_segment,
    total_accounts,
    total_deposit_balance,
    total_loan_outstanding
FROM vw_customer_360
ORDER BY total_deposit_balance DESC
LIMIT 10;

-- Customer Activity Analysis
SELECT
    c.customer_segment,
    d.month_name,
    d.year,
    COUNT(DISTINCT t.transaction_sk) AS transaction_count,
    SUM(t.transaction_amount) AS total_amount
FROM dw.fact_transactions t
JOIN dw.dim_customer c ON t.customer_sk = c.customer_sk
JOIN dw.dim_date d ON t.date_key = d.date_key
WHERE c.is_current = TRUE
GROUP BY c.customer_segment, d.month_name, d.year, d.month_number
ORDER BY d.year, d.month_number, c.customer_segment;
