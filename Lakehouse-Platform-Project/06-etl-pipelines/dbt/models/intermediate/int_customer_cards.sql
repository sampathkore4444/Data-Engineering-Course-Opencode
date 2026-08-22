-- =============================================================================
-- INTERMEDIATE MODEL: Customer Cards Aggregation
-- =============================================================================
-- Purpose: Aggregate credit card data per customer
-- Layer:   Intermediate
-- =============================================================================

{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'cards']
    )
}}

WITH cards AS (
    SELECT * FROM {{ ref('stg_cards') }}
),

aggregated AS (
    SELECT
        customer_id,
        COUNT(*) AS total_cards,
        SUM(card_limit) AS total_card_limit,
        SUM(credit_used) AS total_credit_used,
        SUM(available_credit) AS total_available_credit,
        ROUND(AVG(utilization_pct), 2) AS avg_utilization_pct,
        MAX(last_updated) AS last_card_update
    FROM cards
    WHERE status = 'ACTIVE'
    GROUP BY customer_id
)

SELECT * FROM aggregated
