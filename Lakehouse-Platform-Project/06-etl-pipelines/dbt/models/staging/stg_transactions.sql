-- =============================================================================
-- STAGING MODEL: Transactions
-- =============================================================================
-- Purpose: Stage and clean transaction data from source
-- Layer:   Staging (Bronze → Silver)
-- =============================================================================

{{
    config(
        materialized='view',
        schema='staging',
        tags=['staging', 'transactions']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'transactions') }}
),

cleaned AS (
    SELECT
        txn_id,
        account_id,
        UPPER(TRIM(txn_type)) AS txn_type,
        CASE 
            WHEN UPPER(txn_type) IN ('CR', 'CREDIT', 'DEPOSIT') THEN 'CREDIT'
            WHEN UPPER(txn_type) IN ('DR', 'DEBIT', 'WITHDRAWAL') THEN 'DEBIT'
            WHEN UPPER(txn_type) IN ('TRF', 'TRANSFER') THEN 'TRANSFER'
            ELSE 'OTHER'
        END AS txn_type_standardized,
        ABS(amount) AS amount,
        UPPER(TRIM(currency)) AS currency,
        txn_date,
        txn_timestamp,
        TRIM(description) AS description,
        TRIM(reference) AS reference_number,
        UPPER(TRIM(channel)) AS channel,
        CASE 
            WHEN UPPER(channel) IN ('ATM') THEN 'ATM'
            WHEN UPPER(channel) IN ('MOBILE', 'APP') THEN 'MOBILE'
            WHEN UPPER(channel) IN ('WEB', 'ONLINE') THEN 'ONLINE'
            WHEN UPPER(channel) IN ('BRANCH', 'COUNTER') THEN 'BRANCH'
            WHEN UPPER(channel) IN ('UPI') THEN 'UPI'
            WHEN UPPER(channel) IN ('NEFT', 'RTGS', 'IMPS') THEN 'BANK_TRANSFER'
            ELSE 'OTHER'
        END AS channel_standardized,
        UPPER(TRIM(status)) AS status,
        CASE 
            WHEN DAYOFWEEK(txn_date) IN (1, 7) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        CASE 
            WHEN HOUR(txn_timestamp) BETWEEN 6 AND 11 THEN 'MORNING'
            WHEN HOUR(txn_timestamp) BETWEEN 12 AND 17 THEN 'AFTERNOON'
            WHEN HOUR(txn_timestamp) BETWEEN 18 AND 22 THEN 'EVENING'
            ELSE 'NIGHT'
        END AS time_bucket,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM source
    WHERE txn_id IS NOT NULL
      AND account_id IS NOT NULL
      AND amount > 0
      AND txn_date >= '2020-01-01'
)

SELECT * FROM cleaned
