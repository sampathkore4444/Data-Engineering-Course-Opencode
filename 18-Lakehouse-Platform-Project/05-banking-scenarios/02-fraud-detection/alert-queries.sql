-- =============================================================================
-- FRAUD ALERT QUERIES - Real-Time Monitoring
-- =============================================================================
-- Purpose: Generate and manage fraud alerts
-- Tool:    Dremio SQL
-- Layer:   Gold
-- =============================================================================

-- =============================================================================
-- 1. REAL-TIME ALERT DASHBOARD QUERY
-- =============================================================================

-- Get current alerts (last 1 hour)
SELECT 
    fs.txn_id,
    fs.card_number,
    CONCAT('XXXX-', RIGHT(fs.card_number, 4)) AS card_masked,
    fs.customer_id,
    c.customer_name,
    fs.merchant_name,
    FORMAT_NUMBER(fs.amount, 'VND') AS amount,
    fs.triggered_rules,
    fs.rule_count,
    fs.fraud_score,
    fs.recommended_action,
    fs.evaluated_at,
    -- Age of alert
    DATEDIFF(MINUTE, fs.evaluated_at, CURRENT_TIMESTAMP()) AS age_minutes,
    -- Customer contact
    c.phone AS customer_phone,
    c.email AS customer_email
FROM gold.fraud_score fs
JOIN silver.core_banking_customers c ON fs.customer_id = c.customer_id
WHERE fs.evaluated_at >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
  AND fs.recommended_action IN ('BLOCK', 'REVIEW')
ORDER BY fs.fraud_score DESC, fs.evaluated_at DESC;


-- =============================================================================
-- 2. CRITICAL ALERTS (Immediate Action Required)
-- =============================================================================

SELECT 
    fs.txn_id,
    fs.card_number,
    fs.customer_id,
    c.customer_name,
    c.phone AS customer_phone,
    fs.merchant_name,
    FORMAT_NUMBER(fs.amount, 'VND') AS amount,
    fs.triggered_rules,
    fs.fraud_score,
    fs.evaluated_at,
    -- Auto-block recommendation
    CASE 
        WHEN fs.fraud_score >= 80 THEN 'AUTO_BLOCK'
        WHEN fs.fraud_score >= 60 THEN 'MANUAL_REVIEW'
        ELSE 'LOG_ONLY'
    END AS action_required,
    -- Contact customer
    CONCAT('Contact ', c.phone, ' or ', c.email) AS customer_contact_info
FROM gold.fraud_score fs
JOIN silver.core_banking_customers c ON fs.customer_id = c.customer_id
WHERE fs.fraud_score >= 60  -- High fraud score
  AND fs.evaluated_at >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
ORDER BY fs.fraud_score DESC;


-- =============================================================================
-- 3. DAILY FRAUD SUMMARY REPORT
-- =============================================================================

SELECT 
    DATE(txn_timestamp) AS report_date,
    
    -- Volume Metrics
    COUNT(*) AS total_alerts,
    COUNT(DISTINCT card_number) AS unique_cards_flagged,
    COUNT(DISTINCT customer_id) AS unique_customers_affected,
    
    -- Amount Metrics
    SUM(amount) AS total_amount_flagged,
    AVG(amount) AS avg_amount_flagged,
    MAX(amount) AS max_amount_flagged,
    
    -- Action Metrics
    SUM(CASE WHEN recommended_action = 'BLOCK' THEN 1 ELSE 0 END) AS blocked_count,
    SUM(CASE WHEN recommended_action = 'REVIEW' THEN 1 ELSE 0 END) AS review_count,
    SUM(CASE WHEN recommended_action = 'MONITOR' THEN 1 ELSE 0 END) AS monitor_count,
    
    -- Rule Metrics
    SUM(CASE WHEN triggered_rules LIKE '%HIGH_VALUE%' THEN 1 ELSE 0 END) AS high_value_count,
    SUM(CASE WHEN triggered_rules LIKE '%VELOCITY%' THEN 1 ELSE 0 END) AS velocity_count,
    SUM(CASE WHEN triggered_rules LIKE '%UNUSUAL_TIME%' THEN 1 ELSE 0 END) AS unusual_time_count,
    SUM(CASE WHEN triggered_rules LIKE '%CNP_PATTERN%' THEN 1 ELSE 0 END) AS cnp_count,
    SUM(CASE WHEN triggered_rules LIKE '%GEO_ANOMALY%' THEN 1 ELSE 0 END) AS geo_count,
    
    -- Effectiveness
    ROUND(
        SUM(CASE WHEN recommended_action = 'BLOCK' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    ) AS block_rate_pct

FROM gold.fraud_score
WHERE txn_timestamp >= DATEADD(DAY, -7, CURRENT_DATE)
GROUP BY DATE(txn_timestamp)
ORDER BY report_date DESC;


-- =============================================================================
-- 4. TOP RISKY CARDS (Last 30 Days)
-- =============================================================================

SELECT 
    card_number,
    CONCAT('XXXX-', RIGHT(card_number, 4)) AS card_masked,
    customer_id,
    COUNT(*) AS total_alerts,
    COUNT(DISTINCT triggered_rules) AS distinct_rules_triggered,
    SUM(amount) AS total_flagged_amount,
    AVG(fraud_score) AS avg_fraud_score,
    MAX(fraud_score) AS max_fraud_score,
    MIN(evaluated_at) AS first_alert,
    MAX(evaluated_at) AS last_alert,
    -- Risk level
    CASE 
        WHEN COUNT(*) >= 10 OR MAX(fraud_score) >= 80 THEN 'HIGH_RISK'
        WHEN COUNT(*) >= 5 OR MAX(fraud_score) >= 60 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS card_risk_level
FROM gold.fraud_score
WHERE evaluated_at >= DATEADD(DAY, -30, CURRENT_DATE)
GROUP BY card_number, customer_id
HAVING COUNT(*) >= 3  -- At least 3 alerts
ORDER BY total_alerts DESC, avg_fraud_score DESC
LIMIT 50;


-- =============================================================================
-- 5. FRAUD PATTERN ANALYSIS (Hourly Distribution)
-- =============================================================================

SELECT 
    HOUR(txn_timestamp) AS hour_of_day,
    DAYOFWEEK(txn_timestamp) AS day_of_week,
    COUNT(*) AS alert_count,
    SUM(amount) AS total_amount,
    AVG(fraud_score) AS avg_fraud_score,
    -- Heatmap data
    CASE 
        WHEN COUNT(*) > 10 THEN 'VERY_HIGH'
        WHEN COUNT(*) > 5 THEN 'HIGH'
        WHEN COUNT(*) > 2 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS frequency_band
FROM gold.fraud_score
WHERE evaluated_at >= DATEADD(DAY, -30, CURRENT_DATE)
GROUP BY HOUR(txn_timestamp), DAYOFWEEK(txn_timestamp)
ORDER BY hour_of_day, day_of_week;


-- =============================================================================
-- 6. MERCHANT RISK ANALYSIS
-- =============================================================================

SELECT 
    merchant_name,
    merchant_category,
    COUNT(*) AS total_alerts,
    COUNT(DISTINCT card_number) AS unique_cards_flagged,
    SUM(amount) AS total_flagged_amount,
    AVG(fraud_score) AS avg_fraud_score,
    -- Merchant risk score
    CASE 
        WHEN COUNT(*) > 20 THEN 'HIGH_RISK_MERCHANT'
        WHEN COUNT(*) > 10 THEN 'MEDIUM_RISK_MERCHANT'
        ELSE 'LOW_RISK_MERCHANT'
    END AS merchant_risk_level,
    -- Recommendation
    CASE 
        WHEN COUNT(*) > 20 THEN 'Consider blocking this merchant'
        WHEN COUNT(*) > 10 THEN 'Monitor closely'
        ELSE 'Normal monitoring'
    END AS recommendation
FROM gold.fraud_score
WHERE evaluated_at >= DATEADD(DAY, -30, CURRENT_DATE)
GROUP BY merchant_name, merchant_category
HAVING COUNT(*) >= 5
ORDER BY total_alerts DESC;


-- =============================================================================
-- 7. ALERT AGING REPORT (SLA Monitoring)
-- =============================================================================

SELECT 
    txn_id,
    card_number,
    customer_id,
    fraud_score,
    recommended_action,
    evaluated_at,
    DATEDIFF(MINUTE, evaluated_at, CURRENT_TIMESTAMP()) AS age_minutes,
    -- SLA Status
    CASE 
        WHEN recommended_action = 'BLOCK' 
             AND DATEDIFF(MINUTE, evaluated_at, CURRENT_TIMESTAMP()) > 5 
             THEN 'SLA_BREACH'
        WHEN recommended_action = 'REVIEW' 
             AND DATEDIFF(MINUTE, evaluated_at, CURRENT_TIMESTAMP()) > 15 
             THEN 'SLA_WARNING'
        ELSE 'ON_TRACK'
    END AS sla_status,
    -- Priority
    CASE 
        WHEN fraud_score >= 80 THEN 'P1'
        WHEN fraud_score >= 60 THEN 'P2'
        WHEN fraud_score >= 40 THEN 'P3'
        ELSE 'P4'
    END AS priority
FROM gold.fraud_score
WHERE recommended_action IN ('BLOCK', 'REVIEW')
  AND evaluated_at >= DATEADD(HOUR, -4, CURRENT_TIMESTAMP())
ORDER BY fraud_score DESC, evaluated_at ASC;
