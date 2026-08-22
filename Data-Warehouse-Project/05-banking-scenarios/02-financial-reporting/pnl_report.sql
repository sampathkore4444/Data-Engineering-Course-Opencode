-- =============================================================================
-- PROFIT & LOSS REPORT
-- =============================================================================
-- Purpose: Generate P&L statement for management reporting
-- Use Case: Finance team, executive dashboards
-- =============================================================================

-- Monthly P&L Summary
CREATE OR REPLACE VIEW vw_monthly_pnl AS
SELECT
    d.year,
    d.month_name,
    d.quarter_name,

    -- Interest Income (from loans)
    SUM(CASE WHEN lp.loan_type = 'HOME_LOAN' THEN lp.interest_amount ELSE 0 END) AS home_loan_interest,
    SUM(CASE WHEN lp.loan_type = 'PERSONAL_LOAN' THEN lp.interest_amount ELSE 0 END) AS personal_loan_interest,
    SUM(CASE WHEN lp.loan_type = 'CAR_LOAN' THEN lp.interest_amount ELSE 0 END) AS car_loan_interest,
    SUM(CASE WHEN lp.loan_type = 'BUSINESS_LOAN' THEN lp.interest_amount ELSE 0 END) AS business_loan_interest,
    SUM(lp.interest_amount) AS total_interest_income,

    -- Fee Income (from transactions)
    SUM(t.fee_amount) AS total_fee_income,

    -- Total Revenue
    SUM(lp.interest_amount) + SUM(t.fee_amount) AS total_revenue,

    -- Transaction Volume
    COUNT(DISTINCT t.transaction_sk) AS total_transactions,
    SUM(t.transaction_amount) AS total_transaction_volume

FROM dw.dim_date d
LEFT JOIN dw.fact_loan_payment lp ON d.date_key = lp.date_key
LEFT JOIN dw.fact_transactions t ON d.date_key = t.date_key
WHERE d.is_business_day = TRUE
GROUP BY d.year, d.month_name, d.quarter_name, d.month_number
ORDER BY d.year, d.month_number;

-- Regional P&L
CREATE OR REPLACE VIEW vw_regional_pnl AS
SELECT
    b.region,
    b.branch_name,
    d.quarter_name,

    -- Interest Income
    SUM(lp.interest_amount) AS interest_income,

    -- Transaction Volume
    COUNT(DISTINCT t.transaction_sk) AS transaction_count,
    SUM(t.transaction_amount) AS transaction_volume,

    -- Loan Portfolio
    COUNT(DISTINCT lp.loan_id) AS active_loans,
    SUM(lp.principal_outstanding) AS loan_portfolio

FROM dw.dim_branch b
LEFT JOIN dw.fact_loan_payment lp ON b.branch_sk = lp.branch_sk
LEFT JOIN dw.fact_transactions t ON b.branch_sk = t.branch_sk
LEFT JOIN dw.dim_date d ON lp.date_key = d.date_key OR t.date_key = d.date_key
GROUP BY b.region, b.branch_name, d.quarter_name
ORDER BY b.region, interest_income DESC;

-- Product Performance
CREATE OR REPLACE VIEW vw_product_performance AS
SELECT
    p.product_category,
    p.product_name,
    COUNT(DISTINCT a.account_sk) AS account_count,
    SUM(a.current_balance) AS total_balance,
    AVG(a.interest_rate) AS avg_interest_rate
FROM dw.dim_product p
LEFT JOIN dw.dim_account a ON p.product_subcategory = a.account_type
WHERE a.is_active = TRUE
GROUP BY p.product_category, p.product_name
ORDER BY total_balance DESC;

-- Executive Summary Query
SELECT
    'Total Revenue' AS metric,
    SUM(total_revenue) AS value
FROM vw_monthly_pnl
WHERE year = 2024
UNION ALL
SELECT
    'Total Transactions',
    SUM(total_transactions)
FROM vw_monthly_pnl
WHERE year = 2024
UNION ALL
SELECT
    'Total Loan Portfolio',
    SUM(principal_outstanding)
FROM dw.fact_loan_payment;
