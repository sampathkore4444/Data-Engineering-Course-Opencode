-- =============================================================================
-- MART MODEL: Credit Risk Dashboard
-- =============================================================================
-- Purpose: Credit risk analysis for risk management
-- Layer:   Mart (Gold)
-- =============================================================================

{{
    config(
        materialized='view',
        schema='gold',
        tags=['mart', 'risk', 'gold']
    )
}}

WITH loans AS (
    SELECT * FROM {{ ref('stg_loans') }}
),

risk_summary AS (
    SELECT
        loan_type,
        loan_type_standardized,
        
        -- Portfolio Metrics
        COUNT(*) AS total_loans,
        SUM(principal_amount) AS total_disbursed,
        SUM(principal_outstanding) AS total_outstanding,
        
        -- Risk Distribution
        COUNT(CASE WHEN loan_status = 'ACTIVE' THEN 1 END) AS active_loans,
        COUNT(CASE WHEN loan_status = 'MATURED' THEN 1 END) AS matured_loans,
        
        -- Interest Rate Profile
        ROUND(AVG(interest_rate), 2) AS avg_interest_rate,
        MIN(interest_rate) AS min_interest_rate,
        MAX(interest_rate) AS max_interest_rate,
        
        -- EMI Profile
        SUM(emi_amount) AS total_monthly_emi,
        ROUND(AVG(emi_amount), 2) AS avg_emi,
        
        -- Risk Metrics
        ROUND(
            SUM(CASE WHEN loan_status = 'MATURED' THEN principal_outstanding ELSE 0 END) * 100.0 /
            NULLIF(SUM(principal_outstanding), 0)
        , 2) AS npa_ratio,
        
        CURRENT_TIMESTAMP AS _computed_at
        
    FROM loans
    GROUP BY loan_type, loan_type_standardized
)

SELECT * FROM risk_summary
