-- =============================================================================
-- NPA (Non-Performing Asset) TRACKING
-- =============================================================================
-- Purpose: Track and monitor Non-Performing Assets
-- Tool:    Dremio SQL
-- Layer:   Gold
-- Reference: SBV Circular 06/2020
-- =============================================================================

-- =============================================================================
-- 1. CURRENT NPA STATUS
-- =============================================================================

CREATE OR REPLACE VIEW gold.npa_status AS
SELECT 
    CURRENT_DATE AS report_date,
    
    -- Total Portfolio
    COUNT(DISTINCT loan_id) AS total_active_loans,
    SUM(principal_outstanding) AS total_portfolio,
    
    -- NPA Count and Amount
    COUNT(CASE WHEN risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') 
          THEN 1 END) AS npa_count,
    SUM(CASE WHEN risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') 
         THEN principal_outstanding ELSE 0 END) AS npa_amount,
    
    -- NPA Ratio
    ROUND(
        SUM(CASE WHEN risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') 
             THEN principal_outstanding ELSE 0 END) * 100.0 / 
        NULLIF(SUM(principal_outstanding), 0)
    , 2) AS npa_ratio,
    
    -- Classification Breakdown
    SUM(CASE WHEN risk_classification = 'SUB_STANDARD' 
         THEN principal_outstanding ELSE 0 END) AS sub_standard_amount,
    SUM(CASE WHEN risk_classification = 'DOUBTFUL' 
         THEN principal_outstanding ELSE 0 END) AS doubtful_amount,
    SUM(CASE WHEN risk_classification = 'LOSS' 
         THEN principal_outstanding ELSE 0 END) AS loss_amount,
    
    -- Provision Coverage
    SUM(CASE WHEN risk_classification = 'SUB_STANDARD' 
         THEN principal_outstanding * 0.25 ELSE 0 END) +
    SUM(CASE WHEN risk_classification = 'DOUBTFUL' 
         THEN principal_outstanding * 0.50 ELSE 0 END) +
    SUM(CASE WHEN risk_classification = 'LOSS' 
         THEN principal_outstanding * 1.00 ELSE 0 END) AS total_provisions,
    
    ROUND(
        (SUM(CASE WHEN risk_classification = 'SUB_STANDARD' 
              THEN principal_outstanding * 0.25 ELSE 0 END) +
         SUM(CASE WHEN risk_classification = 'DOUBTFUL' 
              THEN principal_outstanding * 0.50 ELSE 0 END) +
         SUM(CASE WHEN risk_classification = 'LOSS' 
              THEN principal_outstanding * 1.00 ELSE 0 END)) * 100.0 /
        NULLIF(SUM(CASE WHEN risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') 
                   THEN principal_outstanding ELSE 0 END), 0)
    , 2) AS provision_coverage_ratio

FROM gold.credit_risk_dashboard
WHERE loan_status = 'ACTIVE';


-- =============================================================================
-- 2. NPA TREND (Monthly)
-- =============================================================================

CREATE OR REPLACE VIEW gold.npa_trend AS
WITH monthly_npa AS (
    SELECT 
        DATE_TRUNC('MONTH', report_date) AS report_month,
        total_active_loans,
        total_outstanding,
        npa_amount,
        npa_ratio,
        total_provisions,
        provision_coverage_ratio
    FROM gold.credit_risk_summary
    WHERE report_date >= DATEADD(YEAR, -2, CURRENT_DATE)
)
SELECT 
    report_month,
    total_active_loans,
    total_outstanding,
    npa_amount,
    npa_ratio,
    total_provisions,
    provision_coverage_ratio,
    
    -- Month-over-Month Changes
    LAG(npa_ratio, 1) OVER (ORDER BY report_month) AS prev_month_npa_ratio,
    npa_ratio - LAG(npa_ratio, 1) OVER (ORDER BY report_month) AS mom_npa_change,
    
    LAG(npa_amount, 1) OVER (ORDER BY report_month) AS prev_month_npa_amount,
    npa_amount - LAG(npa_amount, 1) OVER (ORDER BY report_month) AS mom_npa_amount_change,
    
    -- Year-over-Year Changes
    LAG(npa_ratio, 12) OVER (ORDER BY report_month) AS yoy_npa_ratio,
    npa_ratio - LAG(npa_ratio, 12) OVER (ORDER BY report_month) AS yoy_npa_change,
    
    -- Trend Indicators
    CASE 
        WHEN npa_ratio > LAG(npa_ratio, 1) OVER (ORDER BY report_month) 
        THEN 'DETERIORATING'
        WHEN npa_ratio < LAG(npa_ratio, 1) OVER (ORDER BY report_month) 
        THEN 'IMPROVING'
        ELSE 'STABLE'
    END AS trend,
    
    -- NPL Classification (SBV)
    CASE 
        WHEN npa_ratio <= 2.0 THEN 'GREEN'
        WHEN npa_ratio <= 3.0 THEN 'YELLOW'
        WHEN npa_ratio <= 5.0 THEN 'ORANGE'
        ELSE 'RED'
    END AS sbv_classification

FROM monthly_npa
ORDER BY report_month DESC;


-- =============================================================================
-- 3. NPA BY LOAN TYPE
-- =============================================================================

CREATE OR REPLACE VIEW gold.npa_by_loan_type AS
SELECT 
    loan_type,
    
    -- Portfolio
    COUNT(*) AS total_loans,
    SUM(principal_outstanding) AS total_outstanding,
    
    -- NPA
    COUNT(CASE WHEN risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') 
          THEN 1 END) AS npa_count,
    SUM(CASE WHEN risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') 
         THEN principal_outstanding ELSE 0 END) AS npa_amount,
    
    -- NPA Ratio
    ROUND(
        SUM(CASE WHEN risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') 
             THEN principal_outstanding ELSE 0 END) * 100.0 / 
        NULLIF(SUM(principal_outstanding), 0)
    , 2) AS npa_ratio,
    
    -- Classification Breakdown
    SUM(CASE WHEN risk_classification = 'SUB_STANDARD' 
         THEN principal_outstanding ELSE 0 END) AS sub_standard,
    SUM(CASE WHEN risk_classification = 'DOUBTFUL' 
         THEN principal_outstanding ELSE 0 END) AS doubtful,
    SUM(CASE WHEN risk_classification = 'LOSS' 
         THEN principal_outstanding ELSE 0 END) AS loss,
    
    -- Risk Indicator
    CASE 
        WHEN SUM(CASE WHEN risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') 
                 THEN principal_outstanding ELSE 0 END) * 100.0 / 
             NULLIF(SUM(principal_outstanding), 0) > 5.0 
        THEN 'HIGH_RISK'
        WHEN SUM(CASE WHEN risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') 
                 THEN principal_outstanding ELSE 0 END) * 100.0 / 
             NULLIF(SUM(principal_outstanding), 0) > 3.0 
        THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS risk_indicator

FROM gold.credit_risk_dashboard
WHERE loan_status = 'ACTIVE'
GROUP BY loan_type
ORDER BY npa_ratio DESC;


-- =============================================================================
-- 4. NPA BY BRANCH
-- =============================================================================

CREATE OR REPLACE VIEW gold.npa_by_branch AS
SELECT 
    a.branch_code,
    
    -- Portfolio
    COUNT(DISTINCT l.loan_id) AS total_loans,
    SUM(l.principal_outstanding) AS total_outstanding,
    
    -- NPA
    COUNT(CASE WHEN l.risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') 
          THEN 1 END) AS npa_count,
    SUM(CASE WHEN l.risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') 
         THEN l.principal_outstanding ELSE 0 END) AS npa_amount,
    
    -- NPA Ratio
    ROUND(
        SUM(CASE WHEN l.risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') 
             THEN l.principal_outstanding ELSE 0 END) * 100.0 / 
        NULLIF(SUM(l.principal_outstanding), 0)
    , 2) AS npa_ratio,
    
    -- Branch Manager Performance
    ROUND(AVG(l.payment_success_rate), 2) AS avg_payment_success,
    ROUND(AVG(l.estimated_dpd), 0) AS avg_dpd,
    
    -- Risk Flag
    CASE 
        WHEN SUM(CASE WHEN l.risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') 
                 THEN l.principal_outstanding ELSE 0 END) * 100.0 / 
             NULLIF(SUM(l.principal_outstanding), 0) > 5.0 
        THEN 'HIGH_RISK_BRANCH'
        WHEN SUM(CASE WHEN l.risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS') 
                 THEN l.principal_outstanding ELSE 0 END) * 100.0 / 
             NULLIF(SUM(l.principal_outstanding), 0) > 3.0 
        THEN 'MEDIUM_RISK_BRANCH'
        ELSE 'LOW_RISK_BRANCH'
    END AS branch_risk_flag

FROM silver.core_banking_accounts a
JOIN silver.loan_accounts l ON a.customer_id = l.customer_id
WHERE l.loan_status = 'ACTIVE'
GROUP BY a.branch_code
ORDER BY npa_ratio DESC;


-- =============================================================================
-- 5. TOP NPA ACCOUNTS (For Recovery Team)
-- =============================================================================

CREATE OR REPLACE VIEW gold.top_npa_accounts AS
SELECT 
    l.loan_id,
    l.customer_id,
    c.customer_name,
    c.phone,
    c.email,
    l.loan_type,
    l.principal_outstanding,
    l.emi_amount,
    l.disbursement_date,
    l.estimated_dpd,
    l.risk_classification,
    l.payment_success_rate,
    
    -- Recovery Priority
    CASE 
        WHEN l.estimated_dpd > 180 THEN 'P1_LEGAL_ACTION'
        WHEN l.estimated_dpd > 90 THEN 'P2_RECOVERY'
        WHEN l.estimated_dpd > 60 THEN 'P3_RESTRUCTURE'
        ELSE 'P4_FOLLOWUP'
    END AS recovery_priority,
    
    -- Potential Loss
    l.principal_outstanding * 
        CASE 
            WHEN l.risk_classification = 'SUB_STANDARD' THEN 0.25
            WHEN l.risk_classification = 'DOUBTFUL' THEN 0.50
            WHEN l.risk_classification = 'LOSS' THEN 1.00
            ELSE 0.01
        END AS potential_provision,
    
    -- Days Since Last Payment
    DATEDIFF(DAY, 
        (SELECT MAX(p.payment_date) FROM silver.loan_payments p 
         WHERE p.loan_id = l.loan_id), 
        CURRENT_DATE
    ) AS days_since_last_payment

FROM silver.loan_accounts l
JOIN silver.core_banking_customers c ON l.customer_id = c.customer_id
WHERE l.loan_status = 'ACTIVE'
  AND l.risk_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS')
ORDER BY l.estimated_dpd DESC, l.principal_outstanding DESC
LIMIT 100;


-- =============================================================================
-- 6. NPA FORECAST (Simple)
-- =============================================================================

CREATE OR REPLACE VIEW gold.npa_forecast AS
WITH recent_trend AS (
    SELECT 
        report_date,
        npa_ratio,
        -- Simple moving average
        AVG(npa_ratio) OVER (ORDER BY report_date 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_3m,
        -- Trend
        npa_ratio - LAG(npa_ratio, 1) OVER (ORDER BY report_date) AS monthly_change
    FROM gold.npa_trend
    WHERE report_date >= DATEADD(MONTH, -6, CURRENT_DATE)
)
SELECT 
    CURRENT_DATE AS forecast_date,
    
    -- Current Status
    (SELECT npa_ratio FROM gold.npa_status) AS current_npa,
    
    -- 3-Month Moving Average
    (SELECT AVG(npa_ratio) FROM recent_trend) AS moving_avg_3m,
    
    -- Trend
    (SELECT AVG(monthly_change) FROM recent_trend 
     WHERE monthly_change IS NOT NULL) AS avg_monthly_change,
    
    -- 3-Month Forecast
    (SELECT npa_ratio FROM recent_trend 
     ORDER BY report_date DESC LIMIT 1) +
    (SELECT AVG(monthly_change) FROM recent_trend 
     WHERE monthly_change IS NOT NULL) * 3 AS forecast_3m,
    
    -- Risk Level
    CASE 
        WHEN (SELECT npa_ratio FROM gold.npa_status) > 5.0 THEN 'HIGH'
        WHEN (SELECT npa_ratio FROM gold.npa_status) > 3.0 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS current_risk_level,
    
    -- Recommendation
    CASE 
        WHEN (SELECT npa_ratio FROM gold.npa_status) > 5.0 
        THEN 'URGENT: Increase provisions, tighten lending criteria'
        WHEN (SELECT npa_ratio FROM gold.npa_status) > 3.0 
        THEN 'CAUTION: Monitor closely, review high-risk accounts'
        ELSE 'NORMAL: Continue standard monitoring'
    END AS recommendation;
