-- =============================================================================
-- CREDIT RISK ANALYTICS
-- =============================================================================
-- Purpose: Analyze credit risk across loan portfolio
-- Tool:    Dremio SQL
-- Layer:   Gold
-- =============================================================================

-- =============================================================================
-- 1. PORTFOLIO RISK SUMMARY
-- =============================================================================

CREATE OR REPLACE VIEW gold.credit_risk_summary AS
SELECT 
    CURRENT_DATE AS report_date,
    
    -- Portfolio Size
    COUNT(DISTINCT loan_id) AS total_active_loans,
    SUM(principal_amount) AS total_disbursed,
    SUM(principal_outstanding) AS total_outstanding,
    
    -- Risk Distribution
    COUNT(CASE WHEN risk_classification = 'STANDARD' THEN 1 END) AS standard_count,
    COUNT(CASE WHEN risk_classification = 'SPECIAL_MENTION' THEN 1 END) AS special_mention_count,
    COUNT(CASE WHEN risk_classification = 'SUB_STANDARD' THEN 1 END) AS sub_standard_count,
    COUNT(CASE WHEN risk_classification = 'DOUBTFUL' THEN 1 END) AS doubtful_count,
    COUNT(CASE WHEN risk_classification = 'LOSS' THEN 1 END) AS loss_count,
    
    -- Exposure by Risk
    SUM(CASE WHEN risk_classification = 'STANDARD' THEN principal_outstanding ELSE 0 END) AS standard_exposure,
    SUM(CASE WHEN risk_classification = 'SPECIAL_MENTION' THEN principal_outstanding ELSE 0 END) AS special_mention_exposure,
    SUM(CASE WHEN risk_classification = 'SUB_STANDARD' THEN principal_outstanding ELSE 0 END) AS sub_standard_exposure,
    SUM(CASE WHEN risk_classification = 'DOUBTFUL' THEN principal_outstanding ELSE 0 END) AS doubtful_exposure,
    SUM(CASE WHEN risk_classification = 'LOSS' THEN principal_outstanding ELSE 0 END) AS loss_exposure,
    
    -- Key Ratios
    ROUND(
        SUM(CASE WHEN risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') 
            THEN principal_outstanding ELSE 0 END) * 100.0 / 
        NULLIF(SUM(principal_outstanding), 0)
    , 2) AS npl_ratio,
    
    ROUND(AVG(interest_rate), 2) AS avg_interest_rate,
    ROUND(AVG(payment_success_rate), 2) AS avg_payment_success_rate

FROM gold.credit_risk_dashboard
WHERE loan_status = 'ACTIVE';


-- =============================================================================
-- 2. RISK BY LOAN TYPE
-- =============================================================================

CREATE OR REPLACE VIEW gold.risk_by_loan_type AS
SELECT 
    loan_type,
    
    -- Volume
    COUNT(*) AS loan_count,
    SUM(principal_amount) AS total_disbursed,
    SUM(principal_outstanding) AS total_outstanding,
    
    -- Risk Profile
    COUNT(CASE WHEN risk_classification = 'STANDARD' THEN 1 END) AS standard_count,
    COUNT(CASE WHEN risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') THEN 1 END) AS npa_count,
    
    ROUND(
        COUNT(CASE WHEN risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') THEN 1 END) * 100.0 / 
        NULLIF(COUNT(*), 0)
    , 2) AS npa_rate,
    
    -- Interest Rate Profile
    ROUND(AVG(interest_rate), 2) AS avg_interest_rate,
    MIN(interest_rate) AS min_interest_rate,
    MAX(interest_rate) AS max_interest_rate,
    
    -- Payment Performance
    ROUND(AVG(payment_success_rate), 2) AS avg_payment_success_rate,
    SUM(total_payments_made) AS total_payments,
    SUM(successful_payments) AS successful_payments,
    SUM(failed_payments) AS failed_payments

FROM gold.credit_risk_dashboard
WHERE loan_status = 'ACTIVE'
GROUP BY loan_type
ORDER BY total_outstanding DESC;


-- =============================================================================
-- 3. RISK BY CUSTOMER SEGMENT
-- =============================================================================

CREATE OR REPLACE VIEW gold.risk_by_customer_segment AS
SELECT 
    c.customer_id,
    c.customer_name,
    -- Customer Segment (by relationship value)
    CASE 
        WHEN (SELECT net_relationship_value FROM gold.customer_360 
              WHERE customer_id = c.customer_id) >= 10000000000 
            THEN 'PLATINUM'
        WHEN (SELECT net_relationship_value FROM gold.customer_360 
              WHERE customer_id = c.customer_id) >= 5000000000 
            THEN 'GOLD'
        WHEN (SELECT net_relationship_value FROM gold.customer_360 
              WHERE customer_id = c.customer_id) >= 1000000000 
            THEN 'SILVER'
        ELSE 'STANDARD'
    END AS customer_segment,
    
    -- Portfolio Exposure
    COUNT(DISTINCT l.loan_id) AS loan_count,
    SUM(l.principal_outstanding) AS total_exposure,
    SUM(l.emi_amount) AS total_monthly_emi,
    
    -- Risk Metrics
    MIN(l.estimated_dpd) AS best_dpd,
    MAX(l.estimated_dpd) AS worst_dpd,
    ROUND(AVG(l.payment_success_rate), 2) AS avg_payment_success,
    
    -- Risk Classification
    CASE 
        WHEN MAX(l.estimated_dpd) > 90 THEN 'HIGH_RISK'
        WHEN MAX(l.estimated_dpd) > 30 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS customer_risk_level

FROM silver.core_banking_customers c
JOIN silver.loan_accounts l ON c.customer_id = l.customer_id
WHERE l.loan_status = 'ACTIVE'
GROUP BY c.customer_id, c.customer_name
ORDER BY total_exposure DESC;


-- =============================================================================
-- 4. CONCENTRATION RISK
-- =============================================================================

CREATE OR REPLACE VIEW gold.concentration_risk AS
SELECT 
    'BY_LOAN_TYPE' AS concentration_type,
    loan_type AS category,
    SUM(principal_outstanding) AS exposure,
    ROUND(SUM(principal_outstanding) * 100.0 / 
        (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
         WHERE loan_status = 'ACTIVE'), 2) AS pct_of_portfolio,
    CASE 
        WHEN SUM(principal_outstanding) * 100.0 / 
            (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
             WHERE loan_status = 'ACTIVE') > 30 
        THEN 'HIGH_CONCENTRATION'
        WHEN SUM(principal_outstanding) * 100.0 / 
            (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
             WHERE loan_status = 'ACTIVE') > 20 
        THEN 'MEDIUM_CONCENTRATION'
        ELSE 'LOW_CONCENTRATION'
    END AS concentration_level
FROM silver.loan_accounts
WHERE loan_status = 'ACTIVE'
GROUP BY loan_type

UNION ALL

SELECT 
    'BY_CUSTOMER' AS concentration_type,
    c.customer_name AS category,
    SUM(l.principal_outstanding) AS exposure,
    ROUND(SUM(l.principal_outstanding) * 100.0 / 
        (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
         WHERE loan_status = 'ACTIVE'), 2) AS pct_of_portfolio,
    CASE 
        WHEN SUM(l.principal_outstanding) * 100.0 / 
            (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
             WHERE loan_status = 'ACTIVE') > 10 
        THEN 'HIGH_CONCENTRATION'
        WHEN SUM(l.principal_outstanding) * 100.0 / 
            (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
             WHERE loan_status = 'ACTIVE') > 5 
        THEN 'MEDIUM_CONCENTRATION'
        ELSE 'LOW_CONCENTRATION'
    END AS concentration_level
FROM silver.core_banking_customers c
JOIN silver.loan_accounts l ON c.customer_id = l.customer_id
WHERE l.loan_status = 'ACTIVE'
GROUP BY c.customer_id, c.customer_name
HAVING SUM(l.principal_outstanding) > 0
ORDER BY exposure DESC;


-- =============================================================================
-- 5. PROVISION REQUIREMENT CALCULATION
-- =============================================================================

CREATE OR REPLACE VIEW gold.provision_requirement AS
SELECT 
    risk_classification,
    COUNT(*) AS loan_count,
    SUM(principal_outstanding) AS exposure,
    -- Provision rates (SBV Circular 06/2020)
    CASE 
        WHEN risk_classification = 'STANDARD' THEN 0.01  -- 1%
        WHEN risk_classification = 'SPECIAL_MENTION' THEN 0.02  -- 2%
        WHEN risk_classification = 'SUB_STANDARD' THEN 0.25  -- 25%
        WHEN risk_classification = 'DOUBTFUL' THEN 0.50  -- 50%
        WHEN risk_classification = 'LOSS' THEN 1.00  -- 100%
        ELSE 0.01
    END AS provision_rate,
    -- Required Provision
    SUM(principal_outstanding) * 
        CASE 
            WHEN risk_classification = 'STANDARD' THEN 0.01
            WHEN risk_classification = 'SPECIAL_MENTION' THEN 0.02
            WHEN risk_classification = 'SUB_STANDARD' THEN 0.25
            WHEN risk_classification = 'DOUBTFUL' THEN 0.50
            WHEN risk_classification = 'LOSS' THEN 1.00
            ELSE 0.01
        END AS required_provision
FROM gold.credit_risk_dashboard
WHERE loan_status = 'ACTIVE'
GROUP BY risk_classification
ORDER BY 
    CASE risk_classification
        WHEN 'STANDARD' THEN 1
        WHEN 'SPECIAL_MENTION' THEN 2
        WHEN 'SUB_STANDARD' THEN 3
        WHEN 'DOUBTFUL' THEN 4
        WHEN 'LOSS' THEN 5
    END;


-- =============================================================================
-- 6. RISK TREND ANALYSIS
-- =============================================================================

CREATE OR REPLACE VIEW gold.risk_trend AS
SELECT 
    report_date,
    
    -- Portfolio Metrics
    total_active_loans,
    total_outstanding,
    
    -- NPL Metrics
    npa_amount,
    npa_ratio,
    LAG(npa_ratio, 1) OVER (ORDER BY report_date) AS prev_month_npa,
    npa_ratio - LAG(npa_ratio, 1) OVER (ORDER BY report_date) AS npa_change,
    
    -- Provision Metrics
    total_provisions,
    provision_coverage_ratio,
    
    -- Capital Metrics
    car_ratio,
    
    -- Trend Indicators
    CASE 
        WHEN npa_ratio > LAG(npa_ratio, 1) OVER (ORDER BY report_date) 
        THEN 'DETERIORATING'
        WHEN npa_ratio < LAG(npa_ratio, 1) OVER (ORDER BY report_date) 
        THEN 'IMPROVING'
        ELSE 'STABLE'
    END AS npa_trend,
    
    CASE 
        WHEN car_ratio > LAG(car_ratio, 1) OVER (ORDER BY report_date) 
        THEN 'IMPROVING'
        WHEN car_ratio < LAG(car_ratio, 1) OVER (ORDER BY report_date) 
        THEN 'DETERIORATING'
        ELSE 'STABLE'
    END AS car_trend

FROM gold.credit_risk_summary
WHERE report_date >= DATEADD(MONTH, -12, CURRENT_DATE)
ORDER BY report_date;
