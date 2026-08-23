-- =============================================================================
-- MART MODEL: Daily Transaction Summary
-- =============================================================================
-- Purpose: Daily aggregated transaction metrics
-- Layer:   Mart (Gold)
-- =============================================================================

{{
    config(
        materialized='view',
        schema='gold',
        tags=['mart', 'transactions', 'gold']
    )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

daily_summary AS (
    SELECT
        txn_date,
        channel_standardized AS channel,
        txn_type_standardized AS txn_type,
        
        -- Volume Metrics
        COUNT(*) AS transaction_count,
        COUNT(DISTINCT account_id) AS unique_accounts,
        
        -- Amount Metrics
        SUM(amount) AS total_amount,
        AVG(amount) AS avg_amount,
        MIN(amount) AS min_amount,
        MAX(amount) AS max_amount,
        
        -- Risk Metrics
        SUM(CASE WHEN amount > 100000 THEN 1 ELSE 0 END) AS high_value_count,
        SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) AS weekend_count,
        SUM(CASE WHEN time_bucket = 'NIGHT' THEN 1 ELSE 0 END) AS night_count,
        
        CURRENT_TIMESTAMP AS _computed_at
        
    FROM transactions
    WHERE status = 'SUCCESS'
    GROUP BY txn_date, channel_standardized, txn_type_standardized
)

SELECT * FROM daily_summary
