-- =============================================================================
-- STAGING MODEL: Credit Cards
-- =============================================================================
-- Purpose: Stage and clean credit card data from source
-- Layer:   Staging (Bronze → Silver)
-- =============================================================================

{{
    config(
        materialized='view',
        schema='staging',
        tags=['staging', 'cards']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'cards') }}
),

cleaned AS (
    SELECT
        card_number,
        customer_id,
        UPPER(TRIM(card_type)) AS card_type,
        CASE 
            WHEN UPPER(card_type) IN ('VISA') THEN 'VISA'
            WHEN UPPER(card_type) IN ('MASTERCARD', 'MC') THEN 'MASTERCARD'
            WHEN UPPER(card_type) IN ('AMEX') THEN 'AMEX'
            WHEN UPPER(card_type) IN ('RUPAY') THEN 'RUPAY'
            ELSE 'OTHER'
        END AS card_brand,
        card_limit,
        CONCAT('XXXX-XXXX-XXXX-', RIGHT(card_number, 4)) AS card_number_masked,
        credit_used,
        GREATEST(card_limit - credit_used, 0) AS available_credit,
        CASE 
            WHEN card_limit > 0 THEN ROUND((credit_used / card_limit) * 100, 2)
            ELSE 0
        END AS utilization_pct,
        issuance_date,
        expiry_date,
        UPPER(TRIM(status)) AS status,
        last_updated,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM source
    WHERE card_number IS NOT NULL
      AND customer_id IS NOT NULL
      AND card_limit > 0
)

SELECT * FROM cleaned
