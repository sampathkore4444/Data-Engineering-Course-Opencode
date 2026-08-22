-- =============================================================================
-- INTERMEDIATE MODEL: Customer Loans Aggregation
-- =============================================================================
-- Purpose: Aggregate loan data per customer
-- Layer:   Intermediate
-- =============================================================================

{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'loans']
    )
}}

WITH loans AS (
    SELECT * FROM {{ ref('stg_loans') }}
),

aggregated AS (
    SELECT
        customer_id,
        COUNT(*) AS total_loans,
        SUM(principal_amount) AS total_loan_amount,
        SUM(principal_outstanding) AS total_loan_outstanding,
        SUM(emi_amount) AS total_monthly_emi,
        ROUND(AVG(interest_rate), 2) AS avg_interest_rate,
        MAX(last_updated) AS last_loan_update
    FROM loans
    WHERE loan_status = 'ACTIVE'
    GROUP BY customer_id
)

SELECT * FROM aggregated
