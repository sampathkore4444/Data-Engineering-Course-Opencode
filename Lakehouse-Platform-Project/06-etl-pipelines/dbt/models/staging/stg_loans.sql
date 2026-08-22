-- =============================================================================
-- STAGING MODEL: Loans
-- =============================================================================
-- Purpose: Stage and clean loan data from source
-- Layer:   Staging (Bronze → Silver)
-- =============================================================================

{{
    config(
        materialized='view',
        schema='staging',
        tags=['staging', 'loans']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'loans') }}
),

cleaned AS (
    SELECT
        loan_id,
        customer_id,
        UPPER(TRIM(loan_type)) AS loan_type,
        CASE 
            WHEN UPPER(loan_type) IN ('HL', 'HOME') THEN 'HOME_LOAN'
            WHEN UPPER(loan_type) IN ('PL', 'PERSONAL') THEN 'PERSONAL_LOAN'
            WHEN UPPER(loan_type) IN ('CL', 'CAR', 'AUTO') THEN 'CAR_LOAN'
            WHEN UPPER(loan_type) IN ('BL', 'BUSINESS') THEN 'BUSINESS_LOAN'
            WHEN UPPER(loan_type) IN ('ED', 'EDUCATION') THEN 'EDUCATION_LOAN'
            WHEN UPPER(loan_type) IN ('GL', 'GOLD') THEN 'GOLD_LOAN'
            ELSE 'OTHER'
        END AS loan_type_standardized,
        principal_amount,
        GREATEST(principal_outstanding, 0) AS principal_outstanding,
        interest_rate,
        CASE 
            WHEN interest_rate < 8 THEN 'LOW'
            WHEN interest_rate BETWEEN 8 AND 12 THEN 'MEDIUM'
            WHEN interest_rate > 12 THEN 'HIGH'
            ELSE 'UNKNOWN'
        END AS interest_rate_band,
        tenure_months,
        emi_amount,
        disbursement_date,
        maturity_date,
        CASE 
            WHEN maturity_date >= CURRENT_DATE THEN 'ACTIVE'
            WHEN maturity_date < CURRENT_DATE THEN 'MATURED'
            ELSE 'UNKNOWN'
        END AS loan_status,
        last_updated,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM source
    WHERE loan_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND principal_amount > 0
      AND interest_rate > 0
      AND interest_rate < 30
)

SELECT * FROM cleaned
