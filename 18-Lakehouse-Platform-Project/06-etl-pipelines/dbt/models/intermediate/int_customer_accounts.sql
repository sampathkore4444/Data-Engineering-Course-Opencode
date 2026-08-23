-- =============================================================================
-- INTERMEDIATE MODEL: Customer Accounts Aggregation
-- =============================================================================
-- Purpose: Aggregate account data per customer
-- Layer:   Intermediate
-- =============================================================================

{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'customers']
    )
}}

WITH accounts AS (
    SELECT * FROM {{ ref('stg_accounts') }}
),

aggregated AS (
    SELECT
        customer_id,
        COUNT(*) AS total_accounts,
        COUNT(CASE WHEN account_type_standardized = 'SAVINGS' THEN 1 END) AS savings_accounts,
        COUNT(CASE WHEN account_type_standardized = 'CURRENT' THEN 1 END) AS current_accounts,
        COUNT(CASE WHEN account_type_standardized = 'FIXED_DEPOSIT' THEN 1 END) AS fd_accounts,
        SUM(current_balance) AS total_balance,
        SUM(available_balance) AS total_available_balance,
        MAX(last_updated) AS last_account_update
    FROM accounts
    WHERE status_standardized = 'ACTIVE'
    GROUP BY customer_id
)

SELECT * FROM aggregated
