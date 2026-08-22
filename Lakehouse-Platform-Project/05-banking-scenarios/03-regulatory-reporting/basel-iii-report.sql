-- =============================================================================
-- BASEL III CAPITAL ADEQUACY REPORT
-- =============================================================================
-- Purpose: Generate Basel III CAR report for SBV submission
-- Tool:    Dremio SQL
-- Layer:   Gold
-- Frequency: Monthly (T+10)
-- =============================================================================

-- =============================================================================
-- 1. RISK-WEIGHTED ASSETS (RWA) CALCULATION
-- =============================================================================

CREATE OR REPLACE VIEW gold.basel_iii_rwa AS
WITH credit_risk_rwa AS (
    -- Credit Risk: Risk-weighted assets for loans
    SELECT 
        'CREDIT_RISK' AS risk_category,
        loan_type_standardized AS sub_category,
        SUM(principal_outstanding) AS exposure,
        CASE 
            WHEN loan_type_standardized = 'HOME_LOAN' THEN 0.35  -- 35% risk weight
            WHEN loan_type_standardized = 'PERSONAL_LOAN' THEN 0.75  -- 75% risk weight
            WHEN loan_type_standardized = 'CAR_LOAN' THEN 0.65  -- 65% risk weight
            WHEN loan_type_standardized = 'BUSINESS_LOAN' THEN 1.00  -- 100% risk weight
            WHEN loan_type_standardized = 'EDUCATION_LOAN' THEN 0.75  -- 75% risk weight
            WHEN loan_type_standardized = 'GOLD_LOAN' THEN 0.50  -- 50% risk weight
            ELSE 1.00  -- Default 100%
        END AS risk_weight,
        SUM(principal_outstanding) * 
            CASE 
                WHEN loan_type_standardized = 'HOME_LOAN' THEN 0.35
                WHEN loan_type_standardized = 'PERSONAL_LOAN' THEN 0.75
                WHEN loan_type_standardized = 'CAR_LOAN' THEN 0.65
                WHEN loan_type_standardized = 'BUSINESS_LOAN' THEN 1.00
                WHEN loan_type_standardized = 'EDUCATION_LOAN' THEN 0.75
                WHEN loan_type_standardized = 'GOLD_LOAN' THEN 0.50
                ELSE 1.00
            END AS risk_weighted_exposure
    FROM silver.loan_accounts
    WHERE loan_status = 'ACTIVE'
    GROUP BY loan_type_standardized
),
market_risk_rwa AS (
    -- Market Risk: Trading book positions (simplified)
    SELECT 
        'MARKET_RISK' AS risk_category,
        'TRADING_BOOK' AS sub_category,
        SUM(card_limit - credit_used) AS exposure,
        0.08 AS risk_weight,  -- 8% for trading book
        SUM(card_limit - credit_used) * 0.08 AS risk_weighted_exposure
    FROM silver.credit_cards
    WHERE status = 'ACTIVE'
),
operational_rwa AS (
    -- Operational Risk (simplified: 15% of total income)
    SELECT 
        'OPERATIONAL_RISK' AS risk_category,
        'OPERATIONAL' AS sub_category,
        SUM(amount) AS exposure,
        0.15 AS risk_weight,  -- 15% for operational risk
        SUM(amount) * 0.15 AS risk_weighted_exposure
    FROM silver.core_banking_transactions
    WHERE txn_date >= DATEADD(YEAR, -1, CURRENT_DATE)
      AND txn_type_standardized = 'DEBIT'
)
SELECT * FROM credit_risk_rwa
UNION ALL
SELECT * FROM market_risk_rwa
UNION ALL
SELECT * FROM operational_rwa;


-- =============================================================================
-- 2. CAPITAL COMPONENTS CALCULATION
-- =============================================================================

CREATE OR REPLACE VIEW gold.basel_iii_capital AS
WITH tier1_common AS (
    -- Tier 1 Common Capital (CET1)
    SELECT 
        'TIER1_COMMON' AS capital_type,
        -- Paid-up share capital
        5000000000000 AS paid_up_capital,  -- VND 5 trillion
        -- Share premium
        200000000000 AS share_premium,
        -- Retained earnings (from P&L)
        (SELECT SUM(net_income) 
         FROM gold.monthly_pnl 
         WHERE year = YEAR(CURRENT_DATE)) AS retained_earnings,
        -- Less: Regulatory deductions
        0 AS deductions
),
tier1_additional AS (
    -- Tier 1 Additional Capital (AT1)
    SELECT 
        'TIER1_ADDITIONAL' AS capital_type,
        500000000000 AS additional_tier1  -- VND 500 billion
),
tier2 AS (
    -- Tier 2 Capital
    SELECT 
        'TIER2' AS capital_type,
        1000000000000 AS subordinated_debt,  -- VND 1 trillion
        500000000000 AS general_provisions,  -- VND 500 billion
        0 AS revaluation_reserves
)
SELECT 
    t1c.capital_type,
    (t1c.paid_up_capital + t1c.share_premium + t1c.retained_earnings - t1c.deductions) AS amount,
    'TIER1_COMMON' AS tier
FROM tier1_common t1c

UNION ALL

SELECT 
    t1a.capital_type,
    t1a.additional_tier1 AS amount,
    'TIER1_ADDITIONAL' AS tier
FROM tier1_additional t1a

UNION ALL

SELECT 
    t2.capital_type,
    (t2.subordinated_debt + t2.general_provisions + t2.revaluation_reserves) AS amount,
    'TIER2' AS tier
FROM tier2 t2;


-- =============================================================================
-- 3. CAR CALCULATION
-- =============================================================================

CREATE OR REPLACE VIEW gold.basel_iii_car AS
WITH total_capital AS (
    SELECT 
        SUM(CASE WHEN tier = 'TIER1_COMMON' THEN amount ELSE 0 END) AS cet1_capital,
        SUM(CASE WHEN tier IN ('TIER1_COMMON', 'TIER1_ADDITIONAL') THEN amount ELSE 0 END) AS tier1_capital,
        SUM(amount) AS total_capital
    FROM gold.basel_iii_capital
),
total_rwa AS (
    SELECT 
        SUM(risk_weighted_exposure) AS total_rwa
    FROM gold.basel_iii_rwa
)
SELECT 
    CURRENT_DATE AS report_date,
    
    -- Capital Amounts
    tc.cet1_capital,
    tc.tier1_capital,
    tc.total_capital,
    
    -- Risk-Weighted Assets
    tr.total_rwa,
    
    -- Capital Adequacy Ratios
    ROUND(tc.cet1_capital / tr.total_rwa * 100, 2) AS cet1_ratio,
    ROUND(tc.tier1_capital / tr.total_rwa * 100, 2) AS tier1_ratio,
    ROUND(tc.total_capital / tr.total_rwa * 100, 2) AS car_ratio,
    
    -- SBV Requirements
    4.5 AS sbv_cet1_min,  -- Minimum CET1: 4.5%
    6.0 AS sbv_tier1_min,  -- Minimum Tier 1: 6.0%
    8.0 AS sbv_car_min,    -- Minimum CAR: 8.0%
    8.0 AS sbv_buffer,     -- Capital buffer: 8.0%
    
    -- Compliance Status
    CASE 
        WHEN tc.cet1_capital / tr.total_rwa * 100 >= 4.5 
         AND tc.tier1_capital / tr.total_rwa * 100 >= 6.0
         AND tc.total_capital / tr.total_rwa * 100 >= 8.0
        THEN 'COMPLIANT'
        ELSE 'NON_COMPLIANT'
    END AS compliance_status,
    
    -- Capital Surplus/Deficit
    tc.cet1_capital - (tr.total_rwa * 0.045) AS cet1_surplus,
    tc.tier1_capital - (tr.total_rwa * 0.06) AS tier1_surplus,
    tc.total_capital - (tr.total_rwa * 0.08) AS car_surplus

FROM total_capital tc
CROSS JOIN total_rwa tr;


-- =============================================================================
-- 4. LARGE EXPOSURE REPORT
-- =============================================================================

CREATE OR REPLACE VIEW gold.basel_iii_large_exposure AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.nationality,
    SUM(l.principal_outstanding) AS total_exposure,
    COUNT(DISTINCT l.loan_id) AS loan_count,
    LISTAGG(DISTINCT l.loan_type_standardized, ', ') AS loan_types,
    -- SBV Limit: 15% of Tier 1 Capital
    (SELECT tier1_capital * 0.15 FROM gold.basel_iii_car) AS single_borrower_limit,
    -- Exposure Status
    CASE 
        WHEN SUM(l.principal_outstanding) > (SELECT tier1_capital * 0.15 FROM gold.basel_iii_car) 
            THEN 'EXCEEDS_LIMIT'
        WHEN SUM(l.principal_outstanding) > (SELECT tier1_capital * 0.10 FROM gold.basel_iii_car) 
            THEN 'WARNING'
        ELSE 'WITHIN_LIMIT'
    END AS exposure_status,
    -- Percentage of limit
    ROUND(
        SUM(l.principal_outstanding) / (SELECT tier1_capital * 0.15 FROM gold.basel_iii_car) * 100, 2
    ) AS pct_of_limit,
    CURRENT_DATE AS report_date
FROM silver.loan_accounts l
JOIN silver.core_banking_customers c ON l.customer_id = c.customer_id
WHERE l.loan_status = 'ACTIVE'
GROUP BY c.customer_id, c.customer_name, c.nationality
HAVING SUM(l.principal_outstanding) > (SELECT tier1_capital * 0.10 FROM gold.basel_iii_car)
ORDER BY total_exposure DESC;


-- =============================================================================
-- 5. SBV CALL REPORT (Daily)
-- =============================================================================

CREATE OR REPLACE VIEW gold.sbv_call_report AS
SELECT 
    CURRENT_DATE AS report_date,
    
    -- Balance Sheet Summary
    (SELECT SUM(current_balance) FROM silver.core_banking_accounts 
     WHERE account_type_standardized = 'SAVINGS') AS total_savings_deposits,
    (SELECT SUM(current_balance) FROM silver.core_banking_accounts 
     WHERE account_type_standardized = 'CURRENT') AS total_current_deposits,
    (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE') AS total_loan_outstanding,
    (SELECT SUM(card_limit) FROM silver.credit_cards 
     WHERE status = 'ACTIVE') AS total_card_limits,
    
    -- Capital
    (SELECT cet1_capital FROM gold.basel_iii_car) AS cet1_capital,
    (SELECT tier1_capital FROM gold.basel_iii_car) AS tier1_capital,
    (SELECT total_capital FROM gold.basel_iii_car) AS total_capital,
    (SELECT total_rwa FROM gold.basel_iii_car) AS risk_weighted_assets,
    
    -- Ratios
    (SELECT cet1_ratio FROM gold.basel_iii_car) AS cet1_ratio,
    (SELECT tier1_ratio FROM gold.basel_iii_car) AS tier1_ratio,
    (SELECT car_ratio FROM gold.basel_iii_car) AS car_ratio,
    
    -- Asset Quality
    (SELECT COUNT(*) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE' AND principal_outstanding > 0) AS total_active_loans,
    (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE' AND estimated_dpd > 90) AS npa_amount,
    ROUND(
        (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
         WHERE loan_status = 'ACTIVE' AND estimated_dpd > 90) * 100.0 /
        NULLIF((SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
                WHERE loan_status = 'ACTIVE'), 0)
    , 2) AS npa_ratio,
    
    -- Compliance
    (SELECT compliance_status FROM gold.basel_iii_car) AS basel_compliance,
    CURRENT_TIMESTAMP AS generated_at;
