-- =============================================================================
-- STAGING MODEL: Customers
-- =============================================================================
-- Purpose: Stage and clean customer data from source
-- Layer:   Staging (Bronze → Silver)
-- =============================================================================

{{
    config(
        materialized='view',
        schema='staging',
        tags=['staging', 'customers']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),

cleaned AS (
    SELECT
        customer_id,
        TRIM(UPPER(customer_name)) AS customer_name,
        TO_DATE(dob) AS date_of_birth,
        CASE 
            WHEN UPPER(gender) IN ('M', 'MALE') THEN 'MALE'
            WHEN UPPER(gender) IN ('F', 'FEMALE') THEN 'FEMALE'
            ELSE 'OTHER'
        END AS gender,
        TRIM(UPPER(nationality)) AS nationality,
        TRIM(pan_number) AS pan_number,
        CASE 
            WHEN email LIKE '%@%.%' THEN LOWER(TRIM(email))
            ELSE NULL
        END AS email,
        CASE 
            WHEN phone REGEXP '^[0-9]{10,15}$' THEN phone
            ELSE NULL
        END AS phone,
        TRIM(address_line1) AS address_line1,
        TRIM(city) AS city,
        TRIM(state) AS state,
        TRIM(pin_code) AS pin_code,
        created_date,
        last_updated,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM source
    WHERE customer_id IS NOT NULL
      AND customer_id != ''
)

SELECT * FROM cleaned
