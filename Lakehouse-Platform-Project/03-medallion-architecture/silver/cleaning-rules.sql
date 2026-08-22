-- =============================================================================
-- SILVER LAYER: Data Cleansing Rules for Banking Data
-- =============================================================================
-- Purpose: Clean, validate, deduplicate, and conform Bronze data
-- Input:   Bronze layer (raw, append-only)
-- Output:  Silver layer (validated, deduplicated, schema-enforced)
-- =============================================================================

-- =============================================================================
-- 1. CUSTOMER DATA CLEANSING
-- =============================================================================

-- Deduplicate customers (keep latest by LAST_UPDATED)
CREATE OR REPLACE VIEW silver.core_banking_customers AS
WITH ranked_customers AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id 
            ORDER BY last_updated DESC
        ) AS rn
    FROM bronze.core_banking_customers
    WHERE customer_id IS NOT NULL  -- Remove nulls
      AND customer_id != ''        -- Remove empty strings
)
SELECT 
    customer_id,
    TRIM(UPPER(customer_name)) AS customer_name,  -- Standardize
    TO_DATE(dob) AS date_of_birth,                 -- Type cast
    CASE 
        WHEN UPPER(gender) IN ('M', 'MALE') THEN 'MALE'
        WHEN UPPER(gender) IN ('F', 'FEMALE') THEN 'FEMALE'
        ELSE 'OTHER'
    END AS gender,                                 -- Standardize
    TRIM(UPPER(nationality)) AS nationality,
    TRIM(pan_number) AS pan_number,                -- Remove spaces
    CASE 
        WHEN email LIKE '%@%.%' THEN LOWER(TRIM(email))
        ELSE NULL
    END AS email,                                  -- Validate email
    CASE 
        WHEN phone REGEXP '^[0-9]{10,15}$' THEN phone
        ELSE NULL
    END AS phone,                                  -- Validate phone
    TRIM(address_line1) AS address_line1,
    TRIM(city) AS city,
    TRIM(state) AS state,
    TRIM(pin_code) AS pin_code,
    created_date,
    last_updated
FROM ranked_customers
WHERE rn = 1;  -- Keep only latest record


-- =============================================================================
-- 2. ACCOUNT DATA CLEANSING
-- =============================================================================

CREATE OR REPLACE VIEW silver.core_banking_accounts AS
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
    -- Validate balances (must be non-negative for assets)
    GREATEST(current_balance, 0) AS current_balance,
    GREATEST(available_balance, 0) AS available_balance,
    -- Ensure available <= current
    LEAST(available_balance, GREATEST(current_balance, 0)) AS available_balance_safe,
    UPPER(TRIM(status)) AS status,
    CASE 
        WHEN UPPER(status) IN ('ACTIVE', 'A') THEN 'ACTIVE'
        WHEN UPPER(status) IN ('CLOSED', 'C') THEN 'CLOSED'
        WHEN UPPER(status) IN ('DORMANT', 'D') THEN 'DORMANT'
        WHEN UPPER(status) IN ('FROZEN', 'F') THEN 'FROZEN'
        ELSE 'UNKNOWN'
    END AS status_standardized,
    branch_code,
    last_updated
FROM bronze.core_banking_accounts
WHERE account_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND current_balance IS NOT NULL
  AND opening_date <= CURRENT_DATE;  -- No future opening dates


-- =============================================================================
-- 3. TRANSACTION DATA CLEANSING
-- =============================================================================

CREATE OR REPLACE VIEW silver.core_banking_transactions AS
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
    -- Validate amount (must be positive)
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
    -- Add business day flag
    CASE 
        WHEN DAYOFWEEK(txn_date) IN (1, 7) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend,
    -- Add time bucket
    CASE 
        WHEN HOUR(txn_timestamp) BETWEEN 6 AND 11 THEN 'MORNING'
        WHEN HOUR(txn_timestamp) BETWEEN 12 AND 17 THEN 'AFTERNOON'
        WHEN HOUR(txn_timestamp) BETWEEN 18 AND 22 THEN 'EVENING'
        ELSE 'NIGHT'
    END AS time_bucket
FROM bronze.core_banking_transactions
WHERE txn_id IS NOT NULL
  AND account_id IS NOT NULL
  AND amount > 0                    -- No zero or negative amounts
  AND txn_date >= '2020-01-01'      -- No ancient transactions
  AND txn_date <= CURRENT_DATE;     -- No future transactions


-- =============================================================================
-- 4. CREDIT CARD DATA CLEANSING
-- =============================================================================

CREATE OR REPLACE VIEW silver.credit_cards AS
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
    -- Mask card number (show only last 4)
    CONCAT('XXXX-XXXX-XXXX-', RIGHT(card_number, 4)) AS card_number_masked,
    credit_used,
    GREATEST(card_limit - credit_used, 0) AS available_credit,
    -- Credit utilization percentage
    CASE 
        WHEN card_limit > 0 THEN ROUND((credit_used / card_limit) * 100, 2)
        ELSE 0
    END AS utilization_pct,
    issuance_date,
    expiry_date,
    UPPER(TRIM(status)) AS status,
    last_updated
FROM bronze.credit_cards
WHERE card_number IS NOT NULL
  AND customer_id IS NOT NULL
  AND card_limit > 0
  AND expiry_date > CURRENT_DATE;  -- Only active (not expired) cards


-- =============================================================================
-- 5. LOAN DATA CLEANSING
-- =============================================================================

CREATE OR REPLACE VIEW silver.loan_accounts AS
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
        WHEN UPPER(loan_type) IN ('BL', 'GOLD') THEN 'GOLD_LOAN'
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
    last_updated
FROM bronze.loan_accounts
WHERE loan_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND principal_amount > 0
  AND interest_rate > 0
  AND interest_rate < 30;  -- Sanity check: no loan > 30%


-- =============================================================================
-- 6. CARD TRANSACTION DATA CLEANSING
-- =============================================================================

CREATE OR REPLACE VIEW silver.card_transactions AS
SELECT 
    txn_id,
    card_number,
    merchant_id,
    TRIM(merchant_name) AS merchant_name,
    UPPER(TRIM(merchant_category)) AS merchant_category,
    ABS(amount) AS amount,
    UPPER(TRIM(currency)) AS currency,
    txn_date,
    txn_timestamp,
    CASE 
        WHEN UPPER(txn_type) IN ('PURCHASE', 'SALE') THEN 'PURCHASE'
        WHEN UPPER(txn_type) IN ('REFUND') THEN 'REFUND'
        WHEN UPPER(txn_type) IN ('CASHBACK') THEN 'CASHBACK'
        ELSE 'OTHER'
    END AS txn_type_standardized,
    -- Risk flags
    CASE 
        WHEN amount > 50000 THEN TRUE
        ELSE FALSE 
    END AS high_value_flag,
    CASE 
        WHEN HOUR(txn_timestamp) BETWEEN 0 AND 5 THEN TRUE
        ELSE FALSE 
    END AS unusual_time_flag,
    CASE 
        WHEN DAYOFWEEK(txn_date) IN (1, 7) THEN TRUE
        ELSE FALSE 
    END AS weekend_flag,
    UPPER(TRIM(status)) AS status
FROM bronze.card_transactions
WHERE txn_id IS NOT NULL
  AND card_number IS NOT NULL
  AND amount > 0
  AND txn_date >= '2020-01-01'
  AND txn_date <= CURRENT_DATE;


-- =============================================================================
-- 7. LOAN PAYMENT DATA CLEANSING
-- =============================================================================

CREATE OR REPLACE VIEW silver.loan_payments AS
SELECT 
    payment_id,
    loan_id,
    payment_date,
    GREATEST(amount, 0) AS amount,
    CASE 
        WHEN UPPER(payment_mode) IN ('AUTO-DEBIT', 'AUTO_DEBIT') THEN 'AUTO_DEBIT'
        WHEN UPPER(payment_mode) IN ('NEFT', 'RTGS', 'IMPS') THEN 'BANK_TRANSFER'
        WHEN UPPER(payment_mode) IN ('CHEQUE', 'CHQ') THEN 'CHEQUE'
        WHEN UPPER(payment_mode) IN ('CASH') THEN 'CASH'
        WHEN UPPER(payment_mode) IN ('UPI') THEN 'UPI'
        ELSE 'OTHER'
    END AS payment_mode_standardized,
    CASE 
        WHEN UPPER(status) IN ('SUCCESS', 'COMPLETED', 'CLEARED') THEN 'SUCCESS'
        WHEN UPPER(status) IN ('PENDING', 'PROCESSING') THEN 'PENDING'
        WHEN UPPER(status) IN ('FAILED', 'BOUNCED', 'REJECTED') THEN 'FAILED'
        ELSE 'UNKNOWN'
    END AS status_standardized,
    reference_number
FROM bronze.loan_payments
WHERE payment_id IS NOT NULL
  AND loan_id IS NOT NULL
  AND amount > 0
  AND payment_date >= '2020-01-01'
  AND payment_date <= CURRENT_DATE;


-- =============================================================================
-- 8. DATA QUALITY METRICS (Silver Layer)
-- =============================================================================

-- Generate data quality score for each table
CREATE OR REPLACE VIEW silver.data_quality_metrics AS
SELECT 
    'core_banking_accounts' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT account_id) AS unique_primary_keys,
    ROUND(COUNT(DISTINCT account_id) * 100.0 / COUNT(*), 2) AS uniqueness_pct,
    ROUND(COUNT(CASE WHEN customer_id IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS completeness_pct,
    ROUND(COUNT(CASE WHEN current_balance >= 0 THEN 1 END) * 100.0 / COUNT(*), 2) AS validity_pct,
    CURRENT_TIMESTAMP AS measured_at
FROM silver.core_banking_accounts

UNION ALL

SELECT 
    'core_banking_transactions' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT txn_id) AS unique_primary_keys,
    ROUND(COUNT(DISTINCT txn_id) * 100.0 / COUNT(*), 2) AS uniqueness_pct,
    ROUND(COUNT(CASE WHEN account_id IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS completeness_pct,
    ROUND(COUNT(CASE WHEN amount > 0 THEN 1 END) * 100.0 / COUNT(*), 2) AS validity_pct,
    CURRENT_TIMESTAMP AS measured_at
FROM silver.core_banking_transactions

UNION ALL

SELECT 
    'credit_cards' AS table_name,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT card_number) AS unique_primary_keys,
    ROUND(COUNT(DISTINCT card_number) * 100.0 / COUNT(*), 2) AS uniqueness_pct,
    ROUND(COUNT(CASE WHEN customer_id IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) AS completeness_pct,
    ROUND(COUNT(CASE WHEN card_limit > 0 THEN 1 END) * 100.0 / COUNT(*), 2) AS validity_pct,
    CURRENT_TIMESTAMP AS measured_at
FROM silver.credit_cards;
