-- =============================================================================
-- AML (Anti-Money Laundering) MONITORING
-- =============================================================================
-- Purpose: Detect suspicious transactions for SBV compliance
-- Tool:    Dremio SQL
-- Layer:   Silver → Gold
-- Reference: Decision 1168/QD-NHNN
-- =============================================================================

-- =============================================================================
-- 1. CASH TRANSACTION REPORTING (CTR)
-- =============================================================================
-- SBV requires reporting cash transactions > VND 500 million

CREATE OR REPLACE VIEW gold.aml_ctr AS
SELECT 
    t.txn_id,
    t.account_id,
    a.customer_id,
    c.customer_name,
    c.pan_number,
    t.amount,
    t.txn_date,
    t.txn_timestamp,
    t.txn_type_standardized,
    t.channel_standardized,
    t.description,
    t.reference_number,
    'CTR' AS report_type,
    'CASH_TRANSACTION_REPORT' AS sbv_form,
    CASE 
        WHEN t.amount >= 500000000 THEN 'MANDATORY'
        WHEN t.amount >= 300000000 THEN 'RECOMMENDED'
        ELSE 'OPTIONAL'
    END AS reporting_requirement
FROM silver.core_banking_transactions t
JOIN silver.core_banking_accounts a ON t.account_id = a.account_id
JOIN silver.core_banking_customers c ON a.customer_id = c.customer_id
WHERE t.txn_type_standardized = 'DEBIT'
  AND t.amount >= 300000000  -- VND 300 million threshold
  AND t.txn_date >= DATEADD(DAY, -1, CURRENT_DATE);  -- Daily report


-- =============================================================================
-- 2. SUSPICIOUS TRANSACTION REPORTING (STR)
-- =============================================================================

CREATE OR REPLACE VIEW gold.aml_str AS
WITH customer_patterns AS (
    SELECT 
        a.customer_id,
        c.customer_name,
        c.pan_number,
        -- Transaction patterns
        COUNT(t.txn_id) AS total_txns_30d,
        SUM(t.amount) AS total_amount_30d,
        AVG(t.amount) AS avg_amount_30d,
        MAX(t.amount) AS max_amount_30d,
        COUNT(DISTINCT t.txn_date) AS active_days_30d,
        -- Cash patterns
        SUM(CASE WHEN t.channel_standardized = 'BRANCH' THEN t.amount ELSE 0 END) AS cash_amount_30d,
        COUNT(CASE WHEN t.channel_standardized = 'BRANCH' THEN 1 END) AS cash_count_30d,
        -- Structuring detection (multiple transactions just below threshold)
        COUNT(CASE WHEN t.amount BETWEEN 400000000 AND 499999999 THEN 1 END) AS near_threshold_count
    FROM silver.core_banking_transactions t
    JOIN silver.core_banking_accounts a ON t.account_id = a.account_id
    JOIN silver.core_banking_customers c ON a.customer_id = c.customer_id
    WHERE t.txn_date >= DATEADD(DAY, -30, CURRENT_DATE)
    GROUP BY a.customer_id, c.customer_name, c.pan_number
)
SELECT 
    cp.customer_id,
    cp.customer_name,
    cp.pan_number,
    cp.total_txns_30d,
    cp.total_amount_30d,
    cp.cash_amount_30d,
    cp.cash_count_30d,
    cp.near_threshold_count,
    
    -- Suspicion Score
    (
        -- High cash volume
        CASE WHEN cp.cash_amount_30d > 5000000000 THEN 30 ELSE 0 END +
        -- Multiple near-threshold transactions (structuring)
        CASE WHEN cp.near_threshold_count > 3 THEN 40 ELSE 0 END +
        -- Unusual transaction frequency
        CASE WHEN cp.total_txns_30d > 100 THEN 20 ELSE 0 END +
        -- High single transaction
        CASE WHEN cp.max_amount_30d > 2000000000 THEN 25 ELSE 0 END +
        -- Round number transactions (potential layering)
        CASE WHEN cp.total_amount_30d % 100000000 = 0 THEN 10 ELSE 0 END
    ) AS suspicion_score,
    
    -- Risk Classification
    CASE 
        WHEN (
            CASE WHEN cp.cash_amount_30d > 5000000000 THEN 30 ELSE 0 END +
            CASE WHEN cp.near_threshold_count > 3 THEN 40 ELSE 0 END +
            CASE WHEN cp.total_txns_30d > 100 THEN 20 ELSE 0 END +
            CASE WHEN cp.max_amount_30d > 2000000000 THEN 25 ELSE 0 END
        ) >= 60 THEN 'HIGH'
        WHEN (
            CASE WHEN cp.cash_amount_30d > 5000000000 THEN 30 ELSE 0 END +
            CASE WHEN cp.near_threshold_count > 3 THEN 40 ELSE 0 END +
            CASE WHEN cp.total_txns_30d > 100 THEN 20 ELSE 0 END +
            CASE WHEN cp.max_amount_30d > 2000000000 THEN 25 ELSE 0 END
        ) >= 30 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_classification,
    
    -- Recommended Action
    CASE 
        WHEN (
            CASE WHEN cp.cash_amount_30d > 5000000000 THEN 30 ELSE 0 END +
            CASE WHEN cp.near_threshold_count > 3 THEN 40 ELSE 0 END +
            CASE WHEN cp.total_txns_30d > 100 THEN 20 ELSE 0 END +
            CASE WHEN cp.max_amount_30d > 2000000000 THEN 25 ELSE 0 END
        ) >= 60 THEN 'FILE_STR_IMMEDIATELY'
        WHEN (
            CASE WHEN cp.cash_amount_30d > 5000000000 THEN 30 ELSE 0 END +
            CASE WHEN cp.near_threshold_count > 3 THEN 40 ELSE 0 END +
            CASE WHEN cp.total_txns_30d > 100 THEN 20 ELSE 0 END +
            CASE WHEN cp.max_amount_30d > 2000000000 THEN 25 ELSE 0 END
        ) >= 30 THEN 'ENHANCED_MONITORING'
        ELSE 'STANDARD_MONITORING'
    END AS recommended_action,
    
    -- Suspicion Reasons
    LISTAGG(
        CASE 
            WHEN cp.cash_amount_30d > 5000000000 THEN 'High cash volume'
            WHEN cp.near_threshold_count > 3 THEN 'Potential structuring'
            WHEN cp.total_txns_30d > 100 THEN 'Unusual frequency'
            WHEN cp.max_amount_30d > 2000000000 THEN 'Large single transaction'
        END, '; '
    ) WITHIN GROUP (ORDER BY 1) AS suspicion_reasons,
    
    CURRENT_DATE AS report_date

FROM customer_patterns cp
WHERE (
    -- High cash volume
    cp.cash_amount_30d > 5000000000 OR
    -- Structuring pattern
    cp.near_threshold_count > 3 OR
    -- Unusual frequency
    cp.total_txns_30d > 100 OR
    -- Large transaction
    cp.max_amount_30d > 2000000000
)
ORDER BY suspicion_score DESC;


-- =============================================================================
-- 3. PEP (Politically Exposed Persons) MONITORING
-- =============================================================================

CREATE OR REPLACE VIEW gold.aml_pep_monitoring AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.pan_number,
    -- PEP Flag (would come from KYC data)
    COALESCE(pep.is_pep, FALSE) AS is_pep,
    pep.pep_category,
    pep.pep_country,
    
    -- Transaction summary
    COUNT(t.txn_id) AS total_txns_30d,
    SUM(t.amount) AS total_amount_30d,
    MAX(t.amount) AS max_single_txn,
    
    -- Enhanced due diligence required
    CASE 
        WHEN COALESCE(pep.is_pep, FALSE) = TRUE THEN 'REQUIRED'
        ELSE 'NOT_REQUIRED'
    END AS edd_requirement,
    
    -- Risk level
    CASE 
        WHEN COALESCE(pep.is_pep, FALSE) = TRUE AND SUM(t.amount) > 1000000000 THEN 'HIGH'
        WHEN COALESCE(pep.is_pep, FALSE) = TRUE THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_level

FROM silver.core_banking_customers c
LEFT JOIN gold.pep_registry pep ON c.customer_id = pep.customer_id
LEFT JOIN silver.core_banking_transactions t ON c.customer_id = t.account_id
    AND t.txn_date >= DATEADD(DAY, -30, CURRENT_DATE)
WHERE COALESCE(pep.is_pep, FALSE) = TRUE
GROUP BY 
    c.customer_id, c.customer_name, c.pan_number,
    pep.is_pep, pep.pep_category, pep.pep_country;


-- =============================================================================
-- 4. DAILY AML SUMMARY REPORT
-- =============================================================================

CREATE OR REPLACE VIEW gold.aml_daily_summary AS
SELECT 
    CURRENT_DATE AS report_date,
    
    -- CTR Summary
    (SELECT COUNT(*) FROM gold.aml_ctr) AS ctr_count,
    (SELECT SUM(amount) FROM gold.aml_ctr) AS ctr_total_amount,
    
    -- STR Summary
    (SELECT COUNT(*) FROM gold.aml_str 
     WHERE risk_classification = 'HIGH') AS high_risk_str_count,
    (SELECT COUNT(*) FROM gold.aml_str 
     WHERE risk_classification = 'MEDIUM') AS medium_risk_str_count,
    
    -- PEP Summary
    (SELECT COUNT(*) FROM gold.aml_pep_monitoring) AS pep_count,
    (SELECT SUM(total_amount_30d) FROM gold.aml_pep_monitoring) AS pep_total_amount,
    
    -- Overall Risk
    CASE 
        WHEN (SELECT COUNT(*) FROM gold.aml_str 
              WHERE risk_classification = 'HIGH') > 5 THEN 'ELEVATED'
        WHEN (SELECT COUNT(*) FROM gold.aml_str 
              WHERE risk_classification = 'HIGH') > 0 THEN 'WATCH'
        ELSE 'NORMAL'
    END AS overall_risk_status,
    
    -- Compliance Status
    CASE 
        WHEN (SELECT COUNT(*) FROM gold.aml_ctr 
              WHERE report_date = CURRENT_DATE) > 0 
        THEN 'CTR_SUBMITTED'
        ELSE 'CTR_PENDING'
    END AS ctr_status,
    
    CURRENT_TIMESTAMP AS generated_at;


-- =============================================================================
-- 5. AML ALERT AGING REPORT
-- =============================================================================

SELECT 
    customer_id,
    customer_name,
    suspicion_score,
    risk_classification,
    recommended_action,
    report_date,
    DATEDIFF(DAY, report_date, CURRENT_DATE()) AS age_days,
    -- SLA Status
    CASE 
        WHEN risk_classification = 'HIGH' AND DATEDIFF(DAY, report_date, CURRENT_DATE()) > 1 
            THEN 'SLA_BREACH'
        WHEN risk_classification = 'MEDIUM' AND DATEDIFF(DAY, report_date, CURRENT_DATE()) > 3 
            THEN 'SLA_WARNING'
        ELSE 'ON_TRACK'
    END AS sla_status
FROM gold.aml_str
WHERE recommended_action IN ('FILE_STR_IMMEDIATELY', 'ENHANCED_MONITORING')
ORDER BY suspicion_score DESC, report_date ASC;
