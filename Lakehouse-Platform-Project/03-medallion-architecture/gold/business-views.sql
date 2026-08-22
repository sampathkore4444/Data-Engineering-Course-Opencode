-- =============================================================================
-- GOLD LAYER: Business-Ready Aggregations for Banking
-- =============================================================================
-- Purpose: Pre-aggregated, business logic applied, ready for BI/ML
-- Input:   Silver layer (cleansed, validated)
-- Output:  Gold layer (star schema, materialized views)
-- =============================================================================

-- =============================================================================
-- 1. CUSTOMER 360° VIEW (Aggregated)
-- =============================================================================

CREATE OR REPLACE VIEW gold.customer_360 AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.date_of_birth,
    c.gender,
    c.nationality,
    c.email,
    c.phone,
    c.city,
    c.state,
    c.pin_code,
    
    -- Account Summary
    COUNT(DISTINCT a.account_id) AS total_accounts,
    COUNT(DISTINCT CASE WHEN a.account_type_standardized = 'SAVINGS' THEN a.account_id END) AS savings_accounts,
    COUNT(DISTINCT CASE WHEN a.account_type_standardized = 'CURRENT' THEN a.account_id END) AS current_accounts,
    COALESCE(SUM(a.current_balance), 0) AS total_balance,
    COALESCE(SUM(a.available_balance), 0) AS total_available_balance,
    
    -- Transaction Summary (Last 30 days)
    COUNT(DISTINCT t.txn_id) AS txn_count_30d,
    COALESCE(SUM(CASE WHEN t.txn_type_standardized = 'CREDIT' THEN t.amount END), 0) AS total_credits_30d,
    COALESCE(SUM(CASE WHEN t.txn_type_standardized = 'DEBIT' THEN t.amount END), 0) AS total_debits_30d,
    
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
    
    -- Risk Indicators
    CASE 
        WHEN COALESCE(SUM(l.principal_outstanding), 0) > 10000000 THEN 'HIGH'
        WHEN COALESCE(SUM(l.principal_outstanding), 0) > 5000000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS credit_exposure_band,
    
    -- Customer Value
    (COALESCE(SUM(a.current_balance), 0) 
     + COALESCE(SUM(cc.card_limit), 0) 
     - COALESCE(SUM(l.principal_outstanding), 0)) AS net_relationship_value,
    
    -- Engagement Score (simple)
    CASE 
        WHEN COUNT(DISTINCT t.txn_id) > 100 THEN 'HIGH'
        WHEN COUNT(DISTINCT t.txn_id) > 20 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS engagement_level,
    
    c.last_updated AS profile_last_updated

FROM silver.core_banking_customers c
LEFT JOIN silver.core_banking_accounts a ON c.customer_id = a.customer_id
LEFT JOIN silver.core_banking_transactions t ON a.account_id = t.account_id
    AND t.txn_date >= DATEADD(DAY, -30, CURRENT_DATE)
LEFT JOIN silver.credit_cards cc ON c.customer_id = cc.customer_id
LEFT JOIN silver.loan_accounts l ON c.customer_id = l.customer_id
GROUP BY 
    c.customer_id, c.customer_name, c.date_of_birth, c.gender,
    c.nationality, c.email, c.phone, c.city, c.state, c.pin_code,
    c.last_updated;


-- =============================================================================
-- 2. DAILY TRANSACTION SUMMARY (For Dashboards)
-- =============================================================================

CREATE OR REPLACE VIEW gold.daily_transaction_summary AS
SELECT 
    txn_date,
    channel_standardized AS channel,
    txn_type_standardized AS txn_type,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    MIN(amount) AS min_amount,
    MAX(amount) AS max_amount,
    COUNT(DISTINCT account_id) AS unique_accounts,
    SUM(CASE WHEN amount > 100000 THEN 1 ELSE 0 END) AS high_value_count,
    SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS weekend_count
FROM silver.core_banking_transactions
GROUP BY txn_date, channel_standardized, txn_type_standardized;


-- =============================================================================
-- 3. CREDIT RISK DASHBOARD
-- =============================================================================

CREATE OR REPLACE VIEW gold.credit_risk_dashboard AS
SELECT 
    l.customer_id,
    c.customer_name,
    l.loan_id,
    l.loan_type_standardized AS loan_type,
    l.principal_amount,
    l.principal_outstanding,
    l.interest_rate,
    l.interest_rate_band,
    l.emi_amount,
    l.disbursement_date,
    l.maturity_date,
    l.loan_status,
    
    -- Payment History
    COUNT(p.payment_id) AS total_payments_made,
    COUNT(CASE WHEN p.status_standardized = 'SUCCESS' THEN 1 END) AS successful_payments,
    COUNT(CASE WHEN p.status_standardized = 'FAILED' THEN 1 END) AS failed_payments,
    ROUND(
        CASE 
            WHEN COUNT(p.payment_id) > 0 
            THEN (COUNT(CASE WHEN p.status_standardized = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(p.payment_id))
            ELSE 0 
        END, 2
    ) AS payment_success_rate,
    
    -- Days Past Due (DPD)
    CASE 
        WHEN l.loan_status = 'ACTIVE' AND MAX(p.payment_date) < DATEADD(DAY, -30, CURRENT_DATE) THEN 30
        WHEN l.loan_status = 'ACTIVE' AND MAX(p.payment_date) < DATEADD(DAY, -60, CURRENT_DATE) THEN 60
        WHEN l.loan_status = 'ACTIVE' AND MAX(p.payment_date) < DATEADD(DAY, -90, CURRENT_DATE) THEN 90
        WHEN l.loan_status = 'ACTIVE' THEN 0
        ELSE NULL
    END AS estimated_dpd,
    
    -- Risk Classification
    CASE 
        WHEN l.loan_status = 'ACTIVE' AND MAX(p.payment_date) < DATEADD(DAY, -90, CURRENT_DATE) THEN 'NPA'
        WHEN l.loan_status = 'ACTIVE' AND MAX(p.payment_date) < DATEADD(DAY, -60, CURRENT_DATE) THEN 'SUB_STANDARD'
        WHEN l.loan_status = 'ACTIVE' AND MAX(p.payment_date) < DATEADD(DAY, -30, CURRENT_DATE) THEN 'SPECIAL_MENTION'
        WHEN l.loan_status = 'ACTIVE' THEN 'STANDARD'
        ELSE 'CLOSED'
    END AS risk_classification

FROM silver.loan_accounts l
JOIN silver.core_banking_customers c ON l.customer_id = c.customer_id
LEFT JOIN silver.loan_payments p ON l.loan_id = p.loan_id
GROUP BY 
    l.customer_id, c.customer_name, l.loan_id, l.loan_type_standardized,
    l.principal_amount, l.principal_outstanding, l.interest_rate,
    l.interest_rate_band, l.emi_amount, l.disbursement_date,
    l.maturity_date, l.loan_status;


-- =============================================================================
-- 4. FRAUD ALERT SUMMARY
-- =============================================================================

CREATE OR REPLACE VIEW gold.fraud_alert_summary AS
WITH card_txn_risk AS (
    SELECT 
        ct.txn_id,
        ct.card_number,
        cc.customer_id,
        ct.merchant_name,
        ct.merchant_category,
        ct.amount,
        ct.txn_date,
        ct.txn_timestamp,
        ct.high_value_flag,
        ct.unusual_time_flag,
        ct.weekend_flag,
        
        -- Velocity: transactions in last 1 hour
        (SELECT COUNT(*) 
         FROM silver.card_transactions ct2 
         WHERE ct2.card_number = ct.card_number 
           AND ct2.txn_timestamp BETWEEN DATEADD(HOUR, -1, ct.txn_timestamp) AND ct.txn_timestamp
        ) AS txn_count_last_1hr,
        
        -- Velocity: transactions in last 24 hours
        (SELECT COUNT(*) 
         FROM silver.card_transactions ct2 
         WHERE ct2.card_number = ct.card_number 
           AND ct2.txn_timestamp BETWEEN DATEADD(HOUR, -24, ct.txn_timestamp) AND ct.txn_timestamp
        ) AS txn_count_last_24hr,
        
        -- Amount: daily cumulative
        (SELECT SUM(amount) 
         FROM silver.card_transactions ct2 
         WHERE ct2.card_number = ct.card_number 
           AND ct2.txn_date = ct.txn_date
        ) AS daily_cumulative_amount,
        
        -- Risk Score (simple rules-based)
        (CASE WHEN ct.amount > 50000 THEN 30 ELSE 0 END +
         CASE WHEN ct.unusual_time_flag THEN 25 ELSE 0 END +
         CASE WHEN ct.weekend_flag THEN 10 ELSE 0 END +
         CASE WHEN (SELECT COUNT(*) FROM silver.card_transactions ct2 
                    WHERE ct2.card_number = ct.card_number 
                      AND ct2.txn_timestamp BETWEEN DATEADD(HOUR, -1, ct.txn_timestamp) AND ct.txn_timestamp) > 5 
              THEN 35 ELSE 0 END
        ) AS risk_score
        
    FROM silver.card_transactions ct
    JOIN silver.credit_cards cc ON ct.card_number = cc.card_number
    WHERE ct.status = 'SUCCESS'
)
SELECT 
    *,
    CASE 
        WHEN risk_score >= 80 THEN 'CRITICAL'
        WHEN risk_score >= 50 THEN 'HIGH'
        WHEN risk_score >= 30 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS alert_level,
    CASE 
        WHEN risk_score >= 80 THEN 'BLOCK Transaction + Alert Customer + Alert Fraud Team'
        WHEN risk_score >= 50 THEN 'FLAG for Review + Alert Customer'
        WHEN risk_score >= 30 THEN 'LOG for Pattern Analysis'
        ELSE 'No Action'
    END AS recommended_action
FROM card_txn_risk
WHERE risk_score >= 30;  -- Only show risky transactions


-- =============================================================================
-- 5. SBV REGULATORY REPORT - LARGE EXPOSURE
-- =============================================================================

CREATE OR REPLACE VIEW gold.sbv_large_exposure AS
-- Report: Loans > VND 5 billion to single borrower
SELECT 
    c.customer_id,
    c.customer_name,
    c.nationality,
    SUM(l.principal_outstanding) AS total_exposure,
    COUNT(DISTINCT l.loan_id) AS loan_count,
    LISTAGG(DISTINCT l.loan_type_standardized, ', ') AS loan_types,
    CASE 
        WHEN SUM(l.principal_outstanding) > 5000000000 THEN 'EXCEEDS LIMIT'
        WHEN SUM(l.principal_outstanding) > 3000000000 THEN 'NEAR LIMIT'
        ELSE 'WITHIN LIMIT'
    END AS exposure_status,
    CURRENT_DATE AS report_date
FROM silver.loan_accounts l
JOIN silver.core_banking_customers c ON l.customer_id = c.customer_id
WHERE l.loan_status = 'ACTIVE'
GROUP BY c.customer_id, c.customer_name, c.nationality
HAVING SUM(l.principal_outstanding) > 3000000000  -- VND 3 billion threshold
ORDER BY total_exposure DESC;


-- =============================================================================
-- 6. DREMIO REFLECTION CATALOG
-- =============================================================================

-- This view helps Dremio administrators understand which Gold views
-- should have Reflections (materialized accelerations) enabled

CREATE OR REPLACE VIEW gold.reflection_catalog AS
SELECT 
    'customer_360' AS view_name,
    'RAW' AS reflection_type,
    'Customer 360° dashboard' AS purpose,
    'customer_id' AS partition_column,
    '1M+' AS estimated_rows,
    'CRITICAL' AS priority
UNION ALL
SELECT 
    'daily_transaction_summary',
    'AGGREGATE',
    'Daily transaction dashboards',
    'txn_date',
    '500K+',
    'HIGH'
UNION ALL
SELECT 
    'credit_risk_dashboard',
    'RAW',
    'Risk management dashboard',
    'customer_id',
    '100K+',
    'HIGH'
UNION ALL
SELECT 
    'fraud_alert_summary',
    'RAW',
    'Real-time fraud monitoring',
    'txn_date',
    '50K+',
    'CRITICAL'
UNION ALL
SELECT 
    'sbv_large_exposure',
    'AGGREGATE',
    'SBV regulatory report',
    'customer_id',
    '10K+',
    'CRITICAL';
