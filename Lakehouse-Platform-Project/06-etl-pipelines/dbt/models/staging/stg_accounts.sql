-- =============================================================================
-- STAGING MODEL: Accounts
-- =============================================================================
-- Purpose: Stage and clean account data from source
-- Layer:   Staging (Bronze → Silver)
-- =============================================================================

{{
    config(
        materialized='view',
        schema='staging',
        tags=['staging', 'accounts']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'accounts') }}
),

cleaned AS (
    SELECT
        account_id,
        customer_id,
        UPPER(TRIM(account_type)) AS account_type,
        CASE 
            WHEN UPPER(account_type) IN ('SAV', 'SAVINGS') THEN 'SAVINGS'
            WHEN UPPER(account_type) IN ('CUR', 'CURRENT') THEN 'CURRENT'
            WHEN UPPER(account_type) IN ('FD', 'FIXED') THEN 'FIXED_DEPOSIT'
            WHEN UPPER(account_type) IN ('RD', 'RECURRING') THEN 'RECURRING_DEPOSIT'
            ELSE 'OTHER'
        END AS account_type_standardized,
        UPPER(TRIM(currency)) AS currency,
        opening_date,
        GREATEST(current_balance, 0) AS current_balance,
        LEAST(available_balance, GREATEST(current_balance, 0)) AS available_balance,
        UPPER(TRIM(status)) AS status,
        CASE 
            WHEN UPPER(status) IN ('ACTIVE', 'A') THEN 'ACTIVE'
            WHEN UPPER(status) IN ('CLOSED', 'C') THEN 'CLOSED'
            WHEN UPPER(status) IN ('DORMANT', 'D') THEN 'DORMANT'
            WHEN UPPER(status) IN ('FROZEN', 'F') THEN 'FROZEN'
            ELSE 'UNKNOWN'
        END AS status_standardized,
        TRIM(branch_code) AS branch_code,
        last_updated,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM source
    WHERE account_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND current_balance IS NOT NULL
)

SELECT * FROM cleaned
