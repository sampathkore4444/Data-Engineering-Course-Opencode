-- =============================================================================
-- CEO EXECUTIVE DASHBOARD
-- =============================================================================
-- Purpose: Provide real-time KPIs for CEO and Board
-- Tool:    Dremio SQL
-- Layer:   Gold
-- Refresh: Every 15 minutes
-- =============================================================================

-- =============================================================================
-- 1. MAIN CEO DASHBOARD VIEW
-- =============================================================================

CREATE OR REPLACE VIEW gold.ceo_dashboard AS
SELECT 
    CURRENT_DATE AS report_date,
    
    -- ═══════════════════════════════════════════════════════════════
    -- PROFITABILITY METRICS
    -- ═══════════════════════════════════════════════════════════════
    
    -- Total Assets
    (SELECT SUM(current_balance) FROM silver.core_banking_accounts 
     WHERE status = 'ACTIVE') +
    (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE') AS total_assets,
    
    -- Total Deposits
    (SELECT SUM(current_balance) FROM silver.core_banking_accounts 
     WHERE status = 'ACTIVE') AS total_deposits,
    
    -- Total Loans
    (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE') AS total_loans,
    
    -- Net Interest Margin (simplified)
    ROUND(
        ((SELECT SUM(principal_outstanding * interest_rate / 100) 
          FROM silver.loan_accounts WHERE loan_status = 'ACTIVE') -
         (SELECT SUM(current_balance * 0.02) 
          FROM silver.core_banking_accounts 
          WHERE account_type_standardized IN ('SAVINGS', 'CURRENT') 
          AND status = 'ACTIVE')) * 100.0 /
        NULLIF((SELECT SUM(current_balance) FROM silver.core_banking_accounts 
                WHERE status = 'ACTIVE'), 0)
    , 2) AS net_interest_margin_pct,
    
    -- Return on Assets (simplified)
    ROUND(
        (SELECT SUM(principal_outstanding * interest_rate / 100) 
         FROM silver.loan_accounts WHERE loan_status = 'ACTIVE') * 0.3 * 100.0 /
        NULLIF((SELECT SUM(current_balance) FROM silver.core_banking_accounts 
                WHERE status = 'ACTIVE') +
               (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
                WHERE loan_status = 'ACTIVE'), 0)
    , 2) AS return_on_assets_pct,
    
    -- ═══════════════════════════════════════════════════════════════
    -- ASSET QUALITY METRICS
    -- ═══════════════════════════════════════════════════════════════
    
    -- NPL Ratio
    ROUND(
        (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
         WHERE loan_status = 'ACTIVE' AND estimated_dpd > 90) * 100.0 /
        NULLIF((SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
                WHERE loan_status = 'ACTIVE'), 0)
    , 2) AS npl_ratio,
    
    -- Provision Coverage Ratio
    ROUND(
        (SELECT SUM(principal_outstanding * 0.5) FROM silver.loan_accounts 
         WHERE loan_status = 'ACTIVE' AND estimated_dpd > 90) * 100.0 /
        NULLIF((SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
                WHERE loan_status = 'ACTIVE' AND estimated_dpd > 90), 0)
    , 2) AS provision_coverage_ratio,
    
    -- ═══════════════════════════════════════════════════════════════
    -- CAPITAL ADEQUACY METRICS
    -- ═══════════════════════════════════════════════════════════════
    
    -- CET1 Ratio
    (SELECT cet1_ratio FROM gold.basel_iii_car) AS cet1_ratio,
    
    -- Tier 1 Ratio
    (SELECT tier1_ratio FROM gold.basel_iii_car) AS tier1_ratio,
    
    -- CAR (Capital Adequacy Ratio)
    (SELECT car_ratio FROM gold.basel_iii_car) AS car_ratio,
    
    -- ═══════════════════════════════════════════════════════════════
    -- LIQUIDITY METRICS
    -- ═══════════════════════════════════════════════════════════════
    
    -- Liquidity Coverage Ratio (simplified)
    ROUND(
        (SELECT SUM(available_balance) FROM silver.core_banking_accounts 
         WHERE status = 'ACTIVE') * 100.0 /
        NULLIF((SELECT SUM(current_balance) FROM silver.core_banking_accounts 
                WHERE account_type_standardized = 'SAVINGS' 
                AND status = 'ACTIVE'), 0)
    , 2) AS lcr_ratio,
    
    -- ═══════════════════════════════════════════════════════════════
    -- CUSTOMER METRICS
    -- ═══════════════════════════════════════════════════════════════
    
    -- Total Customers
    (SELECT COUNT(DISTINCT customer_id) FROM silver.core_banking_customers) AS total_customers,
    
    -- New Customers (Last 30 Days)
    (SELECT COUNT(DISTINCT customer_id) FROM silver.core_banking_customers 
     WHERE created_date >= DATEADD(DAY, -30, CURRENT_DATE)) AS new_customers_30d,
    
    -- Active Customers (Transactions in Last 30 Days)
    (SELECT COUNT(DISTINCT account_id) FROM silver.core_banking_transactions 
     WHERE txn_date >= DATEADD(DAY, -30, CURRENT_DATE)) AS active_customers_30d,
    
    -- Customer Retention Rate (simplified)
    ROUND(
        (SELECT COUNT(DISTINCT account_id) FROM silver.core_banking_transactions 
         WHERE txn_date >= DATEADD(DAY, -30, CURRENT_DATE)) * 100.0 /
        NULLIF((SELECT COUNT(DISTINCT customer_id) FROM silver.core_banking_customers 
                WHERE created_date <= DATEADD(DAY, -30, CURRENT_DATE)), 0)
    , 2) AS customer_retention_pct,
    
    -- ═══════════════════════════════════════════════════════════════
    -- OPERATIONAL METRICS
    -- ═══════════════════════════════════════════════════════════════
    
    -- Digital Transaction Percentage
    ROUND(
        (SELECT COUNT(*) FROM silver.core_banking_transactions 
         WHERE channel_standardized IN ('MOBILE', 'ONLINE') 
         AND txn_date >= DATEADD(DAY, -30, CURRENT_DATE)) * 100.0 /
        NULLIF((SELECT COUNT(*) FROM silver.core_banking_transactions 
                WHERE txn_date >= DATEADD(DAY, -30, CURRENT_DATE)), 0)
    , 2) AS digital_txn_pct,
    
    -- Transaction Volume (Last 30 Days)
    (SELECT COUNT(*) FROM silver.core_banking_transactions 
     WHERE txn_date >= DATEADD(DAY, -30, CURRENT_DATE)) AS txn_volume_30d,
    
    -- Transaction Value (Last 30 Days)
    (SELECT SUM(amount) FROM silver.core_banking_transactions 
     WHERE txn_date >= DATEADD(DAY, -30, CURRENT_DATE)) AS txn_value_30d,
    
    -- ═══════════════════════════════════════════════════════════════
    -- RISK ALERTS
    -- ═══════════════════════════════════════════════════════════════
    
    -- Fraud Alerts (Last 24 Hours)
    (SELECT COUNT(*) FROM gold.fraud_score 
     WHERE evaluated_at >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
     AND recommended_action IN ('BLOCK', 'REVIEW')) AS fraud_alerts_24h,
    
    -- AML Alerts (Last 24 Hours)
    (SELECT COUNT(*) FROM gold.aml_str 
     WHERE risk_classification = 'HIGH') AS aml_alerts,
    
    -- Compliance Status
    (SELECT compliance_status FROM gold.basel_iii_car) AS basel_compliance,
    
    -- ═══════════════════════════════════════════════════════════════
    -- TREND INDICATORS
    -- ═══════════════════════════════════════════════════════════════
    
    -- NPL Trend
    CASE 
        WHEN (SELECT npa_ratio FROM gold.npa_status) > 
             (SELECT prev_month_npa FROM gold.npa_trend 
              ORDER BY report_date DESC LIMIT 1) 
        THEN 'DETERIORATING'
        ELSE 'IMPROVING'
    END AS npl_trend,
    
    -- CAR Trend
    CASE 
        WHEN (SELECT car_ratio FROM gold.basel_iii_car) > 10.0 
        THEN 'STRONG'
        WHEN (SELECT car_ratio FROM gold.basel_iii_car) > 8.0 
        THEN 'ADEQUATE'
        ELSE 'WEAK'
    END AS car_status,
    
    CURRENT_TIMESTAMP AS last_updated;


-- =============================================================================
-- 2. CEO MONTHLY TREND VIEW
-- =============================================================================

CREATE OR REPLACE VIEW gold.ceo_monthly_trend AS
SELECT 
    DATE_TRUNC('MONTH', report_date) AS report_month,
    
    -- Key Metrics
    SUM(txn_value_30d) AS monthly_txn_value,
    AVG(npl_ratio) AS avg_npl_ratio,
    AVG(car_ratio) AS avg_car_ratio,
    AVG(net_interest_margin_pct) AS avg_nim,
    
    -- Month-over-Month Changes
    SUM(txn_value_30d) - LAG(SUM(txn_value_30d)) OVER (ORDER BY DATE_TRUNC('MONTH', report_date)) AS mom_txn_change,
    AVG(npl_ratio) - LAG(AVG(npl_ratio)) OVER (ORDER BY DATE_TRUNC('MONTH', report_date)) AS mom_npl_change,
    AVG(car_ratio) - LAG(AVG(car_ratio)) OVER (ORDER BY DATE_TRUNC('MONTH', report_date)) AS mom_car_change,
    
    -- Year-over-Year Changes
    SUM(txn_value_30d) - LAG(SUM(txn_value_30d), 12) OVER (ORDER BY DATE_TRUNC('MONTH', report_date)) AS yoy_txn_change,
    AVG(npl_ratio) - LAG(AVG(npl_ratio), 12) OVER (ORDER BY DATE_TRUNC('MONTH', report_date)) AS yoy_npl_change

FROM gold.ceo_dashboard
WHERE report_date >= DATEADD(YEAR, -2, CURRENT_DATE)
GROUP BY DATE_TRUNC('MONTH', report_date)
ORDER BY report_month DESC;


-- =============================================================================
-- 3. CEO STRATEGIC ALERTS VIEW
-- =============================================================================

CREATE OR REPLACE VIEW gold.ceo_strategic_alerts AS
SELECT 
    CURRENT_DATE AS alert_date,
    
    -- Alert 1: NPL Improvement
    CASE 
        WHEN (SELECT npl_ratio FROM gold.ceo_dashboard) < 
             (SELECT prev_month_npa FROM gold.npa_trend 
              ORDER BY report_date DESC LIMIT 1)
        THEN CONCAT('✅ NPL ratio improved: ', 
             (SELECT prev_month_npa FROM gold.npa_trend 
              ORDER BY report_date DESC LIMIT 1), '% → ',
             (SELECT npl_ratio FROM gold.ceo_dashboard), '%')
        ELSE NULL
    END AS alert_npl_improvement,
    
    -- Alert 2: Digital Adoption
    CASE 
        WHEN (SELECT digital_txn_pct FROM gold.ceo_dashboard) > 70
        THEN CONCAT('✅ Digital adoption target met: ', 
             (SELECT digital_txn_pct FROM gold.ceo_dashboard), '%')
        ELSE CONCAT('⚠️ Digital adoption below target: ', 
             (SELECT digital_txn_pct FROM gold.ceo_dashboard), '% (target: 70%)')
    END AS alert_digital_adoption,
    
    -- Alert 3: Capital Adequacy
    CASE 
        WHEN (SELECT car_ratio FROM gold.ceo_dashboard) > 10.0
        THEN CONCAT('✅ CAR strong: ', (SELECT car_ratio FROM gold.ceo_dashboard), '%')
        WHEN (SELECT car_ratio FROM gold.ceo_dashboard) > 8.0
        THEN CONCAT('⚠️ CAR adequate: ', (SELECT car_ratio FROM gold.ceo_dashboard), '%')
        ELSE CONCAT('🔴 CAR weak: ', (SELECT car_ratio FROM gold.ceo_dashboard), '% - ACTION REQUIRED')
    END AS alert_capital_adequacy,
    
    -- Alert 4: Fraud Activity
    CASE 
        WHEN (SELECT fraud_alerts_24h FROM gold.ceo_dashboard) > 10
        THEN CONCAT('🔴 High fraud activity: ', 
             (SELECT fraud_alerts_24h FROM gold.ceo_dashboard), ' alerts in 24h')
        WHEN (SELECT fraud_alerts_24h FROM gold.ceo_dashboard) > 5
        THEN CONCAT('⚠️ Elevated fraud activity: ', 
             (SELECT fraud_alerts_24h FROM gold.ceo_dashboard), ' alerts in 24h')
        ELSE '✅ Fraud activity normal'
    END AS alert_fraud_activity,
    
    -- Alert 5: Customer Growth
    CASE 
        WHEN (SELECT new_customers_30d FROM gold.ceo_dashboard) > 1000
        THEN CONCAT('✅ Strong customer growth: ', 
             (SELECT new_customers_30d FROM gold.ceo_dashboard), ' new customers')
        WHEN (SELECT new_customers_30d FROM gold.ceo_dashboard) > 500
        THEN CONCAT('⚠️ Moderate customer growth: ', 
             (SELECT new_customers_30d FROM gold.ceo_dashboard), ' new customers')
        ELSE CONCAT('🔴 Low customer growth: ', 
             (SELECT new_customers_30d FROM gold.ceo_dashboard), ' new customers')
    END AS alert_customer_growth,
    
    -- Alert 6: Compliance
    CASE 
        WHEN (SELECT basel_compliance FROM gold.ceo_dashboard) = 'COMPLIANT'
        THEN '✅ Basel III compliant'
        ELSE '🔴 Basel III NON-COMPLIANT - URGENT ACTION REQUIRED'
    END AS alert_compliance;


-- =============================================================================
-- 4. BOARD PRESENTATION VIEW
-- =============================================================================

CREATE OR REPLACE VIEW gold.board_presentation AS
SELECT 
    'FINANCIAL HIGHLIGHTS' AS section,
    'Total Assets' AS metric,
    FORMAT_NUMBER((SELECT total_assets FROM gold.ceo_dashboard), 'VND') AS value,
    'Target: Grow 15% YoY' AS target,
    CASE 
        WHEN (SELECT total_assets FROM gold.ceo_dashboard) > 0 
        THEN 'ON_TRACK' ELSE 'BEHIND'
    END AS status

UNION ALL

SELECT 
    'FINANCIAL HIGHLIGHTS',
    'Net Interest Margin',
    CONCAT((SELECT net_interest_margin_pct FROM gold.ceo_dashboard), '%'),
    'Target: > 2.5%',
    CASE 
        WHEN (SELECT net_interest_margin_pct FROM gold.ceo_dashboard) > 2.5 
        THEN 'ON_TRACK' ELSE 'BEHIND'
    END

UNION ALL

SELECT 
    'ASSET QUALITY',
    'NPL Ratio',
    CONCAT((SELECT npl_ratio FROM gold.ceo_dashboard), '%'),
    'Target: < 3%',
    CASE 
        WHEN (SELECT npl_ratio FROM gold.ceo_dashboard) < 3.0 
        THEN 'ON_TRACK' ELSE 'BEHIND'
    END

UNION ALL

SELECT 
    'CAPITAL ADEQUACY',
    'CAR',
    CONCAT((SELECT car_ratio FROM gold.ceo_dashboard), '%'),
    'Target: > 10%',
    CASE 
        WHEN (SELECT car_ratio FROM gold.ceo_dashboard) > 10.0 
        THEN 'ON_TRACK' ELSE 'BEHIND'
    END

UNION ALL

SELECT 
    'DIGITAL TRANSFORMATION',
    'Digital Transaction %',
    CONCAT((SELECT digital_txn_pct FROM gold.ceo_dashboard), '%'),
    'Target: > 70%',
    CASE 
        WHEN (SELECT digital_txn_pct FROM gold.ceo_dashboard) > 70.0 
        THEN 'ON_TRACK' ELSE 'BEHIND'
    END

UNION ALL

SELECT 
    'CUSTOMER METRICS',
    'Customer Retention',
    CONCAT((SELECT customer_retention_pct FROM gold.ceo_dashboard), '%'),
    'Target: > 90%',
    CASE 
        WHEN (SELECT customer_retention_pct FROM gold.ceo_dashboard) > 90.0 
        THEN 'ON_TRACK' ELSE 'BEHIND'
    END;
