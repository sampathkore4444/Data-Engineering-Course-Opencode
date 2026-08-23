-- =============================================================================
-- FRAUD DETECTION RULES - Real-Time Banking Fraud Prevention
-- =============================================================================
-- Purpose: Define and execute fraud detection rules
-- Tool:    Dremio SQL
-- Layer:   Silver → Gold
-- =============================================================================

-- =============================================================================
-- 1. HIGH-VALUE TRANSACTION RULE
-- =============================================================================

CREATE OR REPLACE VIEW gold.fraud_rule_high_value AS
SELECT 
    ct.txn_id,
    ct.card_number,
    cc.customer_id,
    ct.merchant_name,
    ct.merchant_category,
    ct.amount,
    ct.txn_date,
    ct.txn_timestamp,
    'HIGH_VALUE' AS rule_name,
    CASE 
        WHEN ct.amount > 100000000 THEN 'CRITICAL'
        WHEN ct.amount > 50000000 THEN 'HIGH'
        WHEN ct.amount > 20000000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    CONCAT('Transaction amount VND ', FORMAT_NUMBER(ct.amount, 'VND'), 
           ' exceeds threshold') AS description
FROM silver.card_transactions ct
JOIN silver.credit_cards cc ON ct.card_number = cc.card_number
WHERE ct.status = 'SUCCESS'
  AND ct.amount > 20000000;  -- VND 20 million threshold


-- =============================================================================
-- 2. VELOCITY CHECK RULE
-- =============================================================================

CREATE OR REPLACE VIEW gold.fraud_rule_velocity AS
WITH txn_velocity_1hr AS (
    SELECT 
        card_number,
        COUNT(*) AS txn_count_1hr,
        SUM(amount) AS total_amount_1hr,
        MAX(amount) AS max_amount_1hr,
        COUNT(DISTINCT merchant_category) AS distinct_categories_1hr
    FROM silver.card_transactions
    WHERE txn_timestamp >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
    GROUP BY card_number
),
txn_velocity_24hr AS (
    SELECT 
        card_number,
        COUNT(*) AS txn_count_24hr,
        SUM(amount) AS total_amount_24hr
    FROM silver.card_transactions
    WHERE txn_timestamp >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
    GROUP BY card_number
)
SELECT 
    v1.card_number,
    cc.customer_id,
    v1.txn_count_1hr,
    v1.total_amount_1hr,
    v1.max_amount_1hr,
    v1.distinct_categories_1hr,
    v2.txn_count_24hr,
    v2.total_amount_24hr,
    'VELOCITY' AS rule_name,
    CASE 
        WHEN v1.txn_count_1hr > 10 THEN 'CRITICAL'
        WHEN v1.txn_count_1hr > 5 THEN 'HIGH'
        WHEN v1.txn_count_1hr > 3 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    CONCAT('Card has ', v1.txn_count_1hr, ' transactions in last hour') AS description
FROM txn_velocity_1hr v1
JOIN txn_velocity_24hr v2 ON v1.card_number = v2.card_number
JOIN silver.credit_cards cc ON v1.card_number = cc.card_number
WHERE v1.txn_count_1hr > 3;  -- More than 3 transactions in 1 hour


-- =============================================================================
-- 3. UNUSUAL TIME RULE
-- =============================================================================

CREATE OR REPLACE VIEW gold.fraud_rule_unusual_time AS
SELECT 
    ct.txn_id,
    ct.card_number,
    cc.customer_id,
    ct.merchant_name,
    ct.amount,
    ct.txn_timestamp,
    'UNUSUAL_TIME' AS rule_name,
    CASE 
        WHEN HOUR(ct.txn_timestamp) BETWEEN 0 AND 3 THEN 'HIGH'
        WHEN HOUR(ct.txn_timestamp) BETWEEN 4 AND 5 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity,
    CONCAT('Transaction at ', HOUR(ct.txn_timestamp), ':', 
           MINUTE(ct.txn_timestamp), ' (unusual hours)') AS description
FROM silver.card_transactions ct
JOIN silver.credit_cards cc ON ct.card_number = cc.card_number
WHERE ct.status = 'SUCCESS'
  AND HOUR(ct.txn_timestamp) BETWEEN 0 AND 5  -- 12 AM to 5 AM
  AND ct.amount > 5000000;  -- Only flag significant amounts


-- =============================================================================
-- 4. CARD-NOT-PRESENT (CNP) PATTERN RULE
-- =============================================================================

CREATE OR REPLACE VIEW gold.fraud_rule_cnp AS
WITH customer_cnp_history AS (
    SELECT 
        card_number,
        COUNT(*) AS cnp_txn_count_30d,
        SUM(amount) AS cnp_total_30d,
        AVG(amount) AS cnp_avg_30d,
        MAX(amount) AS cnp_max_30d
    FROM silver.card_transactions
    WHERE merchant_category = 'ONLINE'
      AND txn_date >= DATEADD(DAY, -30, CURRENT_DATE)
    GROUP BY card_number
),
recent_cnp AS (
    SELECT 
        ct.txn_id,
        ct.card_number,
        cc.customer_id,
        ct.merchant_name,
        ct.amount,
        ct.txn_timestamp
    FROM silver.card_transactions ct
    JOIN silver.credit_cards cc ON ct.card_number = cc.card_number
    WHERE ct.txn_timestamp >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
      AND ct.merchant_category = 'ONLINE'
)
SELECT 
    rc.txn_id,
    rc.card_number,
    rc.customer_id,
    rc.merchant_name,
    rc.amount,
    rc.txn_timestamp,
    'CNP_PATTERN' AS rule_name,
    CASE 
        WHEN ch.cnp_txn_count_30d IS NULL THEN 'HIGH'  -- No CNP history
        WHEN rc.amount > ch.cnp_avg_30d * 3 THEN 'HIGH'  -- 3x average
        WHEN rc.amount > ch.cnp_avg_30d * 2 THEN 'MEDIUM'  -- 2x average
        ELSE 'LOW'
    END AS severity,
    CASE 
        WHEN ch.cnp_txn_count_30d IS NULL THEN 'No online transaction history'
        WHEN rc.amount > ch.cnp_avg_30d * 3 THEN 'Amount 3x above average'
        WHEN rc.amount > ch.cnp_avg_30d * 2 THEN 'Amount 2x above average'
        ELSE 'Within normal range'
    END AS description
FROM recent_cnp rc
LEFT JOIN customer_cnp_history ch ON rc.card_number = ch.card_number
WHERE rc.amount > 10000000;  -- Online txns > VND 10 million


-- =============================================================================
-- 5. GEOGRAPHIC ANOMALY RULE
-- =============================================================================

CREATE OR REPLACE VIEW gold.fraud_rule_geo_anomaly AS
WITH customer_locations AS (
    SELECT 
        card_number,
        merchant_category,
        COUNT(*) AS txn_count
    FROM silver.card_transactions
    WHERE txn_date >= DATEADD(DAY, -30, CURRENT_DATE)
    GROUP BY card_number, merchant_category
),
recent_txn AS (
    SELECT 
        ct.txn_id,
        ct.card_number,
        cc.customer_id,
        ct.merchant_name,
        ct.merchant_category,
        ct.amount,
        ct.txn_timestamp
    FROM silver.card_transactions ct
    JOIN silver.credit_cards cc ON ct.card_number = cc.card_number
    WHERE ct.txn_timestamp >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
)
SELECT 
    rt.txn_id,
    rt.card_number,
    rt.customer_id,
    rt.merchant_name,
    rt.amount,
    rt.txn_timestamp,
    'GEO_ANOMALY' AS rule_name,
    'MEDIUM' AS severity,
    CONCAT('First-time merchant category: ', rt.merchant_category) AS description
FROM recent_txn rt
LEFT JOIN customer_locations cl 
    ON rt.card_number = cl.card_number 
    AND rt.merchant_category = cl.merchant_category
WHERE cl.txn_count IS NULL;  -- No history in this category


-- =============================================================================
-- 6. COMBINED FRAUD SCORE
-- =============================================================================

CREATE OR REPLACE VIEW gold.fraud_score AS
WITH all_alerts AS (
    SELECT * FROM gold.fraud_rule_high_value
    UNION ALL
    SELECT * FROM gold.fraud_rule_velocity
    UNION ALL
    SELECT * FROM gold.fraud_rule_unusual_time
    UNION ALL
    SELECT * FROM gold.fraud_rule_cnp
    UNION ALL
    SELECT * FROM gold.fraud_rule_geo_anomaly
)
SELECT 
    txn_id,
    card_number,
    customer_id,
    merchant_name,
    amount,
    txn_timestamp,
    LISTAGG(rule_name, ', ') WITHIN GROUP (ORDER BY rule_name) AS triggered_rules,
    COUNT(*) AS rule_count,
    MAX(CASE 
        WHEN severity = 'CRITICAL' THEN 4
        WHEN severity = 'HIGH' THEN 3
        WHEN severity = 'MEDIUM' THEN 2
        ELSE 1
    END) AS max_severity_score,
    -- Combined fraud score
    (COUNT(*) * 10 +  -- Rule count weight
     MAX(CASE 
         WHEN severity = 'CRITICAL' THEN 40
         WHEN severity = 'HIGH' THEN 30
         WHEN severity = 'MEDIUM' THEN 20
         ELSE 10
     END)) AS fraud_score,
    CASE 
        WHEN COUNT(*) >= 3 OR MAX(severity) = 'CRITICAL' THEN 'BLOCK'
        WHEN COUNT(*) >= 2 OR MAX(severity) = 'HIGH' THEN 'REVIEW'
        ELSE 'MONITOR'
    END AS recommended_action,
    CURRENT_TIMESTAMP AS evaluated_at
FROM all_alerts
GROUP BY txn_id, card_number, customer_id, merchant_name, amount, txn_timestamp;


-- =============================================================================
-- 7. FRAUD ALERT SUMMARY (For Dashboard)
-- =============================================================================

CREATE OR REPLACE VIEW gold.fraud_alert_summary AS
SELECT 
    DATE(txn_timestamp) AS alert_date,
    HOUR(txn_timestamp) AS alert_hour,
    triggered_rules,
    recommended_action,
    COUNT(*) AS alert_count,
    SUM(amount) AS total_amount_at_risk,
    COUNT(DISTINCT card_number) AS affected_cards,
    COUNT(DISTINCT customer_id) AS affected_customers
FROM gold.fraud_score
WHERE evaluated_at >= DATEADD(DAY, -7, CURRENT_DATE)
GROUP BY 
    DATE(txn_timestamp),
    HOUR(txn_timestamp),
    triggered_rules,
    recommended_action
ORDER BY alert_date DESC, alert_hour DESC;
