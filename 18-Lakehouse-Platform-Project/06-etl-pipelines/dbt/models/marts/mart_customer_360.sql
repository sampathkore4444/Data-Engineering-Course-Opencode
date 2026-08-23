-- =============================================================================
-- MART MODEL: Customer 360 View
-- =============================================================================
-- Purpose: Complete customer view across all banking products
-- Layer:   Mart (Gold)
-- =============================================================================

{{
    config(
        materialized='view',
        schema='gold',
        tags=['mart', 'customers', 'gold']
    )
}}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

accounts AS (
    SELECT * FROM {{ ref('int_customer_accounts') }}
),

cards AS (
    SELECT * FROM {{ ref('int_customer_cards') }}
),

loans AS (
    SELECT * FROM {{ ref('int_customer_loans') }}
),

customer_360 AS (
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
        
        -- Account Metrics
        COALESCE(a.total_accounts, 0) AS total_accounts,
        COALESCE(a.savings_accounts, 0) AS savings_accounts,
        COALESCE(a.current_accounts, 0) AS current_accounts,
        COALESCE(a.total_balance, 0) AS total_balance,
        COALESCE(a.total_available_balance, 0) AS total_available_balance,
        
        -- Card Metrics
        COALESCE(cr.total_cards, 0) AS total_cards,
        COALESCE(cr.total_card_limit, 0) AS total_card_limit,
        COALESCE(cr.total_credit_used, 0) AS total_card_outstanding,
        COALESCE(cr.total_available_credit, 0) AS total_card_available,
        COALESCE(cr.avg_utilization_pct, 0) AS avg_card_utilization_pct,
        
        -- Loan Metrics
        COALESCE(l.total_loans, 0) AS total_loans,
        COALESCE(l.total_loan_amount, 0) AS total_loan_amount,
        COALESCE(l.total_loan_outstanding, 0) AS total_loan_outstanding,
        COALESCE(l.total_monthly_emi, 0) AS total_monthly_emi,
        COALESCE(l.avg_interest_rate, 0) AS avg_loan_interest_rate,
        
        -- Computed Metrics
        (COALESCE(a.total_balance, 0) 
         + COALESCE(cr.total_card_limit, 0) 
         - COALESCE(l.total_loan_outstanding, 0)) AS net_relationship_value,
        
        -- Customer Segment
        CASE 
            WHEN (COALESCE(a.total_balance, 0) 
                  + COALESCE(cr.total_card_limit, 0) 
                  - COALESCE(l.total_loan_outstanding, 0)) >= 10000000000 THEN 'PLATINUM'
            WHEN (COALESCE(a.total_balance, 0) 
                  + COALESCE(cr.total_card_limit, 0) 
                  - COALESCE(l.total_loan_outstanding, 0)) >= 5000000000 THEN 'GOLD'
            WHEN (COALESCE(a.total_balance, 0) 
                  + COALESCE(cr.total_card_limit, 0) 
                  - COALESCE(l.total_loan_outstanding, 0)) >= 1000000000 THEN 'SILVER'
            ELSE 'STANDARD'
        END AS customer_segment,
        
        -- Risk Indicators
        CASE 
            WHEN COALESCE(l.total_loan_outstanding, 0) > 10000000000 THEN 'HIGH'
            WHEN COALESCE(l.total_loan_outstanding, 0) > 5000000000 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS credit_exposure_band,
        
        -- Timestamps
        c.created_date AS customer_since,
        GREATEST(
            COALESCE(a.last_account_update, '1900-01-01'),
            COALESCE(cr.last_card_update, '1900-01-01'),
            COALESCE(l.last_loan_update, '1900-01-01')
        ) AS last_activity_date,
        CURRENT_TIMESTAMP AS _computed_at
        
    FROM customers c
    LEFT JOIN accounts a ON c.customer_id = a.customer_id
    LEFT JOIN cards cr ON c.customer_id = cr.customer_id
    LEFT JOIN loans l ON c.customer_id = l.customer_id
)

SELECT * FROM customer_360
