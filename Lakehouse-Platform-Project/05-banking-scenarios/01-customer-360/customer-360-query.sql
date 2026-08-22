-- =============================================================================
-- CUSTOMER 360° QUERY - Complete Customer View
-- =============================================================================
-- Purpose: Provide a single view of customer across all banking products
-- Tool:    Dremio SQL
-- Layer:   Gold (Business-Ready)
-- =============================================================================

-- =============================================================================
-- 1. BASIC CUSTOMER 360° QUERY
-- =============================================================================

SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    c.phone,
    c.city,
    c.state,
    
    -- Account Summary
    COUNT(DISTINCT a.account_id) AS total_accounts,
    COUNT(DISTINCT CASE WHEN a.account_type_standardized = 'SAVINGS' THEN a.account_id END) AS savings_accounts,
    COUNT(DISTINCT CASE WHEN a.account_type_standardized = 'CURRENT' THEN a.account_id END) AS current_accounts,
    COALESCE(SUM(a.current_balance), 0) AS total_balance,
    COALESCE(SUM(a.available_balance), 0) AS total_available_balance,
    
    -- Credit Card Summary
    COUNT(DISTINCT cc.card_number) AS total_cards,
    COALESCE(SUM(cc.card_limit), 0) AS total_card_limit,
    COALESCE(SUM(cc.credit_used), 0) AS total_card_outstanding,
    ROUND(
        CASE 
            WHEN SUM(cc.card_limit) > 0 
            THEN (SUM(cc.credit_used) / SUM(cc.card_limit)) * 100 
            ELSE 0 
        END, 2
    ) AS avg_card_utilization_pct,
    
    -- Loan Summary
    COUNT(DISTINCT l.loan_id) AS total_loans,
    COALESCE(SUM(l.principal_amount), 0) AS total_loan_amount,
    COALESCE(SUM(l.principal_outstanding), 0) AS total_loan_outstanding,
    COALESCE(SUM(l.emi_amount), 0) AS total_monthly_emi,
    
    -- Transaction Summary (Last 30 days)
    COUNT(DISTINCT t.txn_id) AS txn_count_30d,
    COALESCE(SUM(CASE WHEN t.txn_type_standardized = 'CREDIT' THEN t.amount END), 0) AS total_credits_30d,
    COALESCE(SUM(CASE WHEN t.txn_type_standardized = 'DEBIT' THEN t.amount END), 0) AS total_debits_30d,
    
    -- Risk Indicators
    CASE 
        WHEN COALESCE(SUM(l.principal_outstanding), 0) > 10000000000 THEN 'HIGH'
        WHEN COALESCE(SUM(l.principal_outstanding), 0) > 5000000000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS credit_exposure_band,
    
    -- Customer Value
    (COALESCE(SUM(a.current_balance), 0) 
     + COALESCE(SUM(cc.card_limit), 0) 
     - COALESCE(SUM(l.principal_outstanding), 0)) AS net_relationship_value,
    
    -- Engagement Score
    CASE 
        WHEN COUNT(DISTINCT t.txn_id) > 100 THEN 'HIGH'
        WHEN COUNT(DISTINCT t.txn_id) > 20 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS engagement_level

FROM banking-cleansed.core_banking_customers c
LEFT JOIN banking-cleansed.core_banking_accounts a 
    ON c.customer_id = a.customer_id
LEFT JOIN banking-cleansed.core_banking_transactions t 
    ON a.account_id = t.account_id
    AND t.txn_date >= DATEADD(DAY, -30, CURRENT_DATE)
LEFT JOIN banking-cleansed.credit_cards cc 
    ON c.customer_id = cc.customer_id
LEFT JOIN banking-cleansed.loan_accounts l 
    ON c.customer_id = l.customer_id
GROUP BY 
    c.customer_id, c.customer_name, c.email, c.phone, c.city, c.state;


-- =============================================================================
-- 2. SINGLE CUSTOMER QUERY (For Relationship Manager)
-- =============================================================================

-- Get complete view for one customer
SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    c.phone,
    c.city,
    
    -- Accounts
    LISTAGG(DISTINCT CONCAT(a.account_id, ' (', a.account_type_standardized, '): ', 
        FORMAT_NUMBER(a.current_balance, 'VND')), '; ') 
        WITHIN GROUP (ORDER BY a.account_id) AS accounts_summary,
    COALESCE(SUM(a.current_balance), 0) AS total_balance,
    
    -- Cards
    LISTAGG(DISTINCT CONCAT('XXXX-', RIGHT(cc.card_number, 4), ' (', cc.card_brand, '): ',
        FORMAT_NUMBER(cc.credit_used, 'VND')), '; ')
        WITHIN GROUP (ORDER BY cc.card_number) AS cards_summary,
    COALESCE(SUM(cc.credit_used), 0) AS total_card_outstanding,
    
    -- Loans
    LISTAGG(DISTINCT CONCAT(l.loan_id, ' (', l.loan_type_standardized, '): ',
        FORMAT_NUMBER(l.principal_outstanding, 'VND')), '; ')
        WITHIN GROUP (ORDER BY l.loan_id) AS loans_summary,
    COALESCE(SUM(l.principal_outstanding), 0) AS total_loan_outstanding,
    
    -- Recent Transactions
    (SELECT LISTAGG(CONCAT(txn_date, ': ', txn_type_standardized, ' ', FORMAT_NUMBER(amount, 'VND')), '; ')
     FROM banking-cleansed.core_banking_transactions t2
     WHERE t2.account_id = a.account_id
       AND t2.txn_date >= DATEADD(DAY, -7, CURRENT_DATE)
    ) AS recent_transactions,
    
    -- Net Value
    (COALESCE(SUM(a.current_balance), 0) 
     + COALESCE(SUM(cc.card_limit), 0) 
     - COALESCE(SUM(l.principal_outstanding), 0)) AS net_relationship_value

FROM banking-cleansed.core_banking_customers c
LEFT JOIN banking-cleansed.core_banking_accounts a 
    ON c.customer_id = a.customer_id
LEFT JOIN banking-cleansed.credit_cards cc 
    ON c.customer_id = cc.customer_id
LEFT JOIN banking-cleansed.loan_accounts l 
    ON c.customer_id = l.customer_id
WHERE c.customer_id = 'CUST-12345'  -- ← Replace with actual customer ID
GROUP BY 
    c.customer_id, c.customer_name, c.email, c.phone, c.city;


-- =============================================================================
-- 3. CUSTOMER SEGMENTATION QUERY
-- =============================================================================

-- Segment customers by relationship value
SELECT 
    CASE 
        WHEN net_relationship_value >= 10000000000 THEN 'PLATINUM (≥10B VND)'
        WHEN net_relationship_value >= 5000000000 THEN 'GOLD (5-10B VND)'
        WHEN net_relationship_value >= 1000000000 THEN 'SILVER (1-5B VND)'
        WHEN net_relationship_value >= 100000000 THEN 'BRONZE (100M-1B VND)'
        ELSE 'STANDARD (<100M VND)'
    END AS customer_segment,
    COUNT(*) AS customer_count,
    SUM(total_balance) AS segment_total_balance,
    SUM(total_card_outstanding) AS segment_card_outstanding,
    SUM(total_loan_outstanding) AS segment_loan_outstanding,
    SUM(net_relationship_value) AS segment_net_value,
    ROUND(AVG(avg_card_utilization_pct), 2) AS avg_utilization
FROM banking-gold.customer_360
GROUP BY 
    CASE 
        WHEN net_relationship_value >= 10000000000 THEN 'PLATINUM (≥10B VND)'
        WHEN net_relationship_value >= 5000000000 THEN 'GOLD (5-10B VND)'
        WHEN net_relationship_value >= 1000000000 THEN 'SILVER (1-5B VND)'
        WHEN net_relationship_value >= 100000000 THEN 'BRONZE (100M-1B VND)'
        ELSE 'STANDARD (<100M VND)'
    END
ORDER BY segment_net_value DESC;


-- =============================================================================
-- 4. CUSTOMER LIFETIME VALUE QUERY
-- =============================================================================

-- Calculate CLV based on fees, interest, and balances
SELECT 
    c.customer_id,
    c.customer_name,
    
    -- Tenure
    DATEDIFF(MONTH, c.created_date, CURRENT_DATE) AS tenure_months,
    
    -- Revenue Contributors
    COALESCE(SUM(CASE WHEN t.txn_type_standardized = 'DEBIT' 
        AND t.description LIKE '%FEE%' THEN t.amount END), 0) AS total_fees_paid,
    
    COALESCE(SUM(CASE WHEN t.txn_type_standardized = 'DEBIT' 
        AND t.description LIKE '%INTEREST%' THEN t.amount END), 0) AS total_interest_paid,
    
    -- Balance-Based Revenue
    AVG(a.current_balance) AS avg_daily_balance,
    
    -- Simple CLV Estimate (Annual)
    (COALESCE(SUM(CASE WHEN t.txn_type_standardized = 'DEBIT' 
        AND t.description LIKE '%FEE%' THEN t.amount END), 0) * 12.0 /
     GREATEST(DATEDIFF(MONTH, c.created_date, CURRENT_DATE), 1)) AS estimated_annual_fee_revenue,
    
    -- Risk Score
    CASE 
        WHEN l.principal_outstanding > 10000000000 THEN 'HIGH_RISK'
        WHEN l.principal_outstanding > 5000000000 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS risk_score

FROM banking-cleansed.core_banking_customers c
LEFT JOIN banking-cleansed.core_banking_accounts a ON c.customer_id = a.customer_id
LEFT JOIN banking-cleansed.core_banking_transactions t ON a.account_id = t.account_id
LEFT JOIN banking-cleansed.loan_accounts l ON c.customer_id = l.customer_id
GROUP BY 
    c.customer_id, c.customer_name, c.created_date, l.principal_outstanding
HAVING DATEDIFF(MONTH, c.created_date, CURRENT_DATE) > 12  -- At least 1 year customer
ORDER BY estimated_annual_fee_revenue DESC
LIMIT 100;
