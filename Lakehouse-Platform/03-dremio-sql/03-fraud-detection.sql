-- Dremio SQL Scripts: Real-Time Fraud Detection
-- ==============================================

-- Query 1: Detect Suspicious Transactions (High Amount + Unusual Pattern)
-- Flags transactions that are unusually large for the customer
SELECT 
    t.transaction_id,
    t.customer_id,
    c.customer_name,
    t.account_id,
    t.transaction_type,
    t.amount,
    t.transaction_date,
    t.description,
    
    -- Customer's average transaction (last 30 days)
    avg_stats.avg_amount AS customer_avg_amount,
    avg_stats.avg_daily_count AS customer_avg_daily_count,
    
    -- How many standard deviations from average
    (t.amount - avg_stats.avg_amount) / avg_stats.stddev_amount AS z_score,
    
    -- Risk Score
    CASE 
        WHEN (t.amount - avg_stats.avg_amount) / avg_stats.stddev_amount > 3 THEN 'CRITICAL'
        WHEN (t.amount - avg_stats.avg_amount) / avg_stats.stddev_amount > 2 THEN 'HIGH'
        WHEN (t.amount - avg_stats.avg_amount) / avg_stats.stddev_amount > 1.5 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS fraud_risk_score
    
FROM "banking-vault"."virtual.transaction_analytics" t
JOIN "banking-postgres".core_banking.customers c ON t.customer_id = c.customer_id
JOIN (
    -- Calculate customer's transaction statistics
    SELECT 
        customer_id,
        AVG(amount) AS avg_amount,
        STDDEV(amount) AS stddev_amount,
        COUNT(*) / 30.0 AS avg_daily_count
    FROM "banking-vault"."virtual.transaction_analytics"
    WHERE transaction_date >= CURRENT_DATE() - INTERVAL '30' DAY
    GROUP BY customer_id
) avg_stats ON t.customer_id = avg_stats.customer_id
WHERE t.transaction_date >= CURRENT_DATE() - INTERVAL '1' DAY  -- Today's transactions
  AND t.amount > 100000  -- Transactions above 1 Lakh
ORDER BY z_score DESC;

-- Query 2: Detect Multiple Failed Transactions (Card Fraud)
-- Cards with multiple declined transactions
SELECT 
    ct.card_id,
    ct.customer_id,
    c.customer_name,
    ct.card_type,
    ct.card_number,
    
    COUNT(*) AS failed_count,
    SUM(ct.transaction_amount) AS total_attempted_amount,
    MAX(ct.transaction_date) AS last_failed_attempt,
    MIN(ct.transaction_date) AS first_failed_attempt,
    
    -- Risk Indicators
    CASE 
        WHEN COUNT(*) >= 5 THEN 'CRITICAL - Potential Card Theft'
        WHEN COUNT(*) >= 3 THEN 'HIGH - Possible Fraud Attempt'
        WHEN COUNT(*) >= 2 THEN 'MEDIUM - Monitor Closely'
        ELSE 'LOW'
    END AS fraud_indicator
    
FROM "banking-mysql".credit_cards.card_transactions ct
JOIN "banking-mysql".credit_cards.credit_cards cc ON ct.card_id = cc.card_id
JOIN "banking-postgres".core_banking.customers c ON cc.customer_id = c.customer_id
WHERE ct.status = 'DECLINED'
  AND ct.transaction_date >= CURRENT_DATE() - INTERVAL '7' DAY
GROUP BY ct.card_id, ct.customer_id, c.customer_name, ct.card_type, ct.card_number
HAVING COUNT(*) >= 2
ORDER BY failed_count DESC;

-- Query 3: Detect Card Transaction Velocity (Unusual Frequency)
-- Cards with unusually high number of transactions in short time
SELECT 
    ct.card_id,
    ct.customer_id,
    c.customer_name,
    ct.card_type,
    
    -- Hourly transaction count
    COUNT(*) AS transactions_in_hour,
    SUM(ct.transaction_amount) AS total_amount_in_hour,
    
    -- Unique merchants
    COUNT(DISTINCT ct.merchant_name) AS unique_merchants,
    COUNT(DISTINCT ct.merchant_category) AS unique_categories,
    
    -- Risk Score
    CASE 
        WHEN COUNT(*) >= 10 THEN 'CRITICAL - Card Cloning Possible'
        WHEN COUNT(*) >= 5 THEN 'HIGH - Unusual Frequency'
        ELSE 'MEDIUM'
    END AS velocity_risk
    
FROM "banking-mysql".credit_cards.card_transactions ct
JOIN "banking-mysql".credit_cards.credit_cards cc ON ct.card_id = cc.card_id
JOIN "banking-postgres".core_banking.customers c ON cc.customer_id = c.customer_id
WHERE ct.transaction_date >= CURRENT_DATE() - INTERVAL '1' HOUR
GROUP BY ct.card_id, ct.customer_id, c.customer_name, ct.card_type
HAVING COUNT(*) >= 3
ORDER BY transactions_in_hour DESC;

-- Query 4: Detect Geographic Anomalies (Impossible Travel)
-- Transactions from different cities within short time
SELECT 
    t1.transaction_id AS txn1_id,
    t1.customer_id,
    c.customer_name,
    t1.description AS txn1_location,
    t1.transaction_date AS txn1_time,
    t1.amount AS txn1_amount,
    
    t2.transaction_id AS txn2_id,
    t2.description AS txn2_location,
    t2.transaction_date AS txn2_time,
    t2.amount AS txn2_amount,
    
    DATEDIFF(minute, t1.transaction_date, t2.transaction_date) AS minutes_between,
    
    'CRITICAL - Possible Card Theft' AS fraud_indicator
    
FROM "banking-vault"."virtual.transaction_analytics" t1
JOIN "banking-vault"."virtual.transaction_analytics" t2 
    ON t1.customer_id = t2.customer_id
    AND t1.transaction_id != t2.transaction_id
    AND t2.transaction_date > t1.transaction_date
    AND DATEDIFF(minute, t1.transaction_date, t2.transaction_date) <= 30
JOIN "banking-postgres".core_banking.customers c ON t1.customer_id = c.customer_id
WHERE t1.transaction_type = 'DEBIT'
  AND t2.transaction_type = 'DEBIT'
  AND t1.description != t2.description  -- Different locations
ORDER BY minutes_between ASC;

-- Query 5: Detect Unusual Spending Patterns
-- Customers with sudden spike in spending
SELECT 
    c.customer_id,
    c.customer_name,
    c.city,
    
    -- Current week spending
    current_week.total_spend AS current_week_spend,
    current_week.transaction_count AS current_week_count,
    
    -- Historical average (last 4 weeks)
    hist_avg.avg_weekly_spend AS avg_weekly_spend,
    hist_avg.avg_weekly_count AS avg_weekly_count,
    
    -- Spike detection
    current_week.total_spend / hist_avg.avg_weekly_spend AS spend_multiplier,
    
    -- Risk Score
    CASE 
        WHEN current_week.total_spend / hist_avg.avg_weekly_spend > 5 THEN 'CRITICAL'
        WHEN current_week.total_spend / hist_avg.avg_weekly_spend > 3 THEN 'HIGH'
        WHEN current_week.total_spend / hist_avg.avg_weekly_spend > 2 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS spike_risk
    
FROM "banking-postgres".core_banking.customers c
JOIN (
    -- Current week spending
    SELECT 
        customer_id,
        SUM(amount) AS total_spend,
        COUNT(*) AS transaction_count
    FROM "banking-vault"."virtual.transaction_analytics"
    WHERE transaction_date >= DATE_TRUNC('week', CURRENT_DATE())
    GROUP BY customer_id
) current_week ON c.customer_id = current_week.customer_id
JOIN (
    -- Historical average
    SELECT 
        customer_id,
        AVG(weekly_spend) AS avg_weekly_spend,
        AVG(weekly_count) AS avg_weekly_count
    FROM (
        SELECT 
            customer_id,
            DATE_TRUNC('week', transaction_date) AS week_start,
            SUM(amount) AS weekly_spend,
            COUNT(*) AS weekly_count
        FROM "banking-vault"."virtual.transaction_analytics"
        WHERE transaction_date >= CURRENT_DATE() - INTERVAL '28' DAY
          AND transaction_date < DATE_TRUNC('week', CURRENT_DATE())
        GROUP BY customer_id, DATE_TRUNC('week', transaction_date)
    ) weekly_data
    GROUP BY customer_id
) hist_avg ON c.customer_id = hist_avg.customer_id
WHERE current_week.total_spend / hist_avg.avg_weekly_spend > 2
ORDER BY spend_multiplier DESC;

-- Query 6: Detect Card Testing (Small Amount Transactions)
-- Fraudsters test stolen cards with small amounts
SELECT 
    ct.card_id,
    ct.customer_id,
    c.customer_name,
    ct.card_number,
    
    COUNT(*) AS small_txn_count,
    SUM(ct.transaction_amount) AS total_small_amount,
    COUNT(DISTINCT ct.merchant_name) AS unique_merchants,
    
    -- Risk Score
    CASE 
        WHEN COUNT(*) >= 10 AND COUNT(DISTINCT ct.merchant_name) >= 5 
            THEN 'CRITICAL - Card Testing Detected'
        WHEN COUNT(*) >= 5 AND COUNT(DISTINCT ct.merchant_name) >= 3 
            THEN 'HIGH - Possible Card Testing'
        ELSE 'MEDIUM'
    END AS testing_risk
    
FROM "banking-mysql".credit_cards.card_transactions ct
JOIN "banking-mysql".credit_cards.credit_cards cc ON ct.card_id = cc.card_id
JOIN "banking-postgres".core_banking.customers c ON cc.customer_id = c.customer_id
WHERE ct.transaction_amount < 100  -- Small amount
  AND ct.transaction_type = 'PURCHASE'
  AND ct.transaction_date >= CURRENT_DATE() - INTERVAL '1' DAY
GROUP BY ct.card_id, ct.customer_id, c.customer_name, ct.card_number
HAVING COUNT(*) >= 3
ORDER BY small_txn_count DESC;

-- Query 7: Detect NPA Customers - Potential Default Risk
-- Customers approaching NPA classification
SELECT 
    l.customer_id,
    c.customer_name,
    c.phone,
    c.email,
    c.city,
    
    l.loan_id,
    l.loan_type,
    l.loan_amount,
    l.principal_outstanding,
    l.days_past_due,
    l.npa_classification,
    
    -- Risk Indicators
    CASE 
        WHEN l.days_past_due >= 90 THEN 'ALREADY_NPA'
        WHEN l.days_past_due >= 60 THEN 'CRITICAL - Approaching NPA'
        WHEN l.days_past_due >= 30 THEN 'HIGH - Early Warning'
        WHEN l.days_past_due >= 15 THEN 'MEDIUM - Watch'
        ELSE 'CURRENT'
    END AS npa_risk,
    
    -- Recommended Action
    CASE 
        WHEN l.days_past_due >= 90 THEN 'Initiate Recovery Process'
        WHEN l.days_past_due >= 60 THEN 'Send Legal Notice'
        WHEN l.days_past_due >= 30 THEN 'Contact Customer - Restructuring'
        WHEN l.days_past_due >= 15 THEN 'Send Reminder SMS/Email'
        ELSE 'Monitor'
    END AS recommended_action
    
FROM "banking-postgres".core_banking.loan_accounts l
JOIN "banking-postgres".core_banking.customers c ON l.customer_id = c.customer_id
WHERE l.loan_status = 'ACTIVE'
  AND l.days_past_due >= 15
ORDER BY l.days_past_due DESC;

-- Query 8: Fraud Summary Dashboard Query
-- Aggregated fraud alerts for monitoring
SELECT 
    'HIGH_AMOUNT_TXNS' AS alert_type,
    COUNT(*) AS alert_count,
    SUM(amount) AS total_amount_at_risk
FROM "banking-vault"."virtual.transaction_analytics"
WHERE transaction_date >= CURRENT_DATE() - INTERVAL '1' DAY
  AND amount > 500000

UNION ALL

SELECT 
    'DECLINED_TRANSACTIONS' AS alert_type,
    COUNT(*) AS alert_count,
    SUM(transaction_amount) AS total_amount_at_risk
FROM "banking-mysql".credit_cards.card_transactions
WHERE status = 'DECLINED'
  AND transaction_date >= CURRENT_DATE() - INTERVAL '1' DAY

UNION ALL

SELECT 
    'NPA_ACCOUNTS' AS alert_type,
    COUNT(*) AS alert_count,
    SUM(principal_outstanding) AS total_amount_at_risk
FROM "banking-postgres".core_banking.loan_accounts
WHERE npa_classification IN ('SUB_STANDARD', 'DOUBTFUL', 'LOSS')
  AND loan_status = 'ACTIVE'

ORDER BY alert_count DESC;