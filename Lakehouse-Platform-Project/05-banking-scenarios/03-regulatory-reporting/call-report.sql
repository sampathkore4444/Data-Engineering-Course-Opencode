-- =============================================================================
-- SBV CALL REPORT (Daily Regulatory Submission)
-- =============================================================================
-- Purpose: Generate daily Call Report for SBV portal
-- Tool:    Dremio SQL
-- Layer:   Gold
-- Frequency: Daily (T+1)
-- Reference: Circular 23/2014
-- =============================================================================

-- =============================================================================
-- 1. BALANCE SHEET SUMMARY
-- =============================================================================

CREATE OR REPLACE VIEW gold.call_report_balance_sheet AS
SELECT 
    CURRENT_DATE AS report_date,
    
    -- Assets
    (SELECT SUM(current_balance) FROM silver.core_banking_accounts 
     WHERE account_type_standardized = 'SAVINGS' AND status = 'ACTIVE') AS cash_and_balances,
    (SELECT SUM(current_balance) FROM silver.core_banking_accounts 
     WHERE account_type_standardized = 'CURRENT' AND status = 'ACTIVE') AS current_accounts_balances,
    (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE' AND loan_type_standardized = 'HOME_LOAN') AS home_loans,
    (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE' AND loan_type_standardized = 'PERSONAL_LOAN') AS personal_loans,
    (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE' AND loan_type_standardized = 'CAR_LOAN') AS car_loans,
    (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE' AND loan_type_standardized = 'BUSINESS_LOAN') AS business_loans,
    (SELECT SUM(card_limit) FROM silver.credit_cards 
     WHERE status = 'ACTIVE') AS credit_card_limits,
    
    -- Total Assets
    (SELECT SUM(current_balance) FROM silver.core_banking_accounts 
     WHERE status = 'ACTIVE') +
    (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE') AS total_assets,
    
    -- Liabilities
    (SELECT SUM(current_balance) FROM silver.core_banking_accounts 
     WHERE account_type_standardized = 'SAVINGS' AND status = 'ACTIVE') AS customer_deposits_savings,
    (SELECT SUM(current_balance) FROM silver.core_banking_accounts 
     WHERE account_type_standardized = 'CURRENT' AND status = 'ACTIVE') AS customer_deposits_current,
    0 AS interbank_borrowing,  -- Placeholder
    0 AS other_liabilities,  -- Placeholder
    
    -- Total Liabilities
    (SELECT SUM(current_balance) FROM silver.core_banking_accounts 
     WHERE status = 'ACTIVE') AS total_liabilities,
    
    -- Equity
    5000000000000 AS paid_up_capital,  -- VND 5 trillion
    200000000000 AS share_premium,
    1000000000000 AS retained_earnings,  -- Placeholder
    
    -- Total Equity
    6200000000000 AS total_equity;


-- =============================================================================
-- 2. CAPITAL ADEQUACY SECTION
-- =============================================================================

CREATE OR REPLACE VIEW gold.call_report_capital AS
SELECT 
    CURRENT_DATE AS report_date,
    
    -- Capital Components
    (SELECT cet1_capital FROM gold.basel_iii_car) AS cet1_capital,
    (SELECT tier1_capital FROM gold.basel_iii_car) AS tier1_capital,
    (SELECT total_capital FROM gold.basel_iii_car) AS total_capital,
    
    -- Risk-Weighted Assets
    (SELECT total_rwa FROM gold.basel_iii_car) AS risk_weighted_assets,
    
    -- Capital Ratios
    (SELECT cet1_ratio FROM gold.basel_iii_car) AS cet1_ratio,
    (SELECT tier1_ratio FROM gold.basel_iii_car) AS tier1_ratio,
    (SELECT car_ratio FROM gold.basel_iii_car) AS car_ratio,
    
    -- SBV Requirements
    4.5 AS sbv_cet1_minimum,
    6.0 AS sbv_tier1_minimum,
    8.0 AS sbv_car_minimum,
    8.0 AS sbv_buffer_requirement,
    
    -- Compliance Status
    (SELECT compliance_status FROM gold.basel_iii_car) AS compliance_status;


-- =============================================================================
-- 3. ASSET QUALITY SECTION
-- =============================================================================

CREATE OR REPLACE VIEW gold.call_report_asset_quality AS
SELECT 
    CURRENT_DATE AS report_date,
    
    -- Total Loans
    (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE') AS total_loans,
    
    -- Classification (SBV Circular 06/2020)
    (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE' AND estimated_dpd = 0) AS standard_loans,
    (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE' AND estimated_dpd BETWEEN 1 AND 30) AS special_mention,
    (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE' AND estimated_dpd BETWEEN 31 AND 60) AS sub_standard,
    (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE' AND estimated_dpd BETWEEN 61 AND 90) AS doubtful,
    (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
     WHERE loan_status = 'ACTIVE' AND estimated_dpd > 90) AS loss,
    
    -- NPL Ratio
    ROUND(
        (SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
         WHERE loan_status = 'ACTIVE' AND estimated_dpd > 90) * 100.0 /
        NULLIF((SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
                WHERE loan_status = 'ACTIVE'), 0)
    , 2) AS npl_ratio,
    
    -- Provision Coverage
    ROUND(
        (SELECT SUM(principal_outstanding * 0.15) FROM silver.loan_accounts 
         WHERE loan_status = 'ACTIVE' AND estimated_dpd > 90) * 100.0 /
        NULLIF((SELECT SUM(principal_outstanding) FROM silver.loan_accounts 
                WHERE loan_status = 'ACTIVE' AND estimated_dpd > 90), 0)
    , 2) AS provision_coverage_ratio;


-- =============================================================================
-- 4. LIQUIDITY SECTION
-- =============================================================================

CREATE OR REPLACE VIEW gold.call_report_liquidity AS
SELECT 
    CURRENT_DATE AS report_date,
    
    -- Liquidity Metrics
    (SELECT SUM(current_balance) FROM silver.core_banking_accounts 
     WHERE status = 'ACTIVE') AS total_deposits,
    (SELECT SUM(available_balance) FROM silver.core_banking_accounts 
     WHERE status = 'ACTIVE') AS liquid_assets,
    
    -- Liquidity Coverage Ratio (LCR)
    ROUND(
        (SELECT SUM(available_balance) FROM silver.core_banking_accounts 
         WHERE status = 'ACTIVE') * 100.0 /
        NULLIF((SELECT SUM(current_balance) FROM silver.core_banking_accounts 
                WHERE account_type_standardized = 'SAVINGS' 
                AND status = 'ACTIVE'), 0)
    , 2) AS lcr_ratio,
    
    -- Net Stable Funding Ratio (NSFR)
    120 AS nsfr_ratio,  -- Placeholder: would calculate from actual data
    
    -- SBV Requirements
    100 AS sbv_lcr_minimum,
    100 AS sbv_nsfr_minimum,
    
    -- Compliance
    CASE 
        WHEN (SELECT SUM(available_balance) FROM silver.core_banking_accounts 
              WHERE status = 'ACTIVE') * 100.0 /
             NULLIF((SELECT SUM(current_balance) FROM silver.core_banking_accounts 
                     WHERE account_type_standardized = 'SAVINGS' 
                     AND status = 'ACTIVE'), 0) >= 100
        THEN 'COMPLIANT'
        ELSE 'NON_COMPLIANT'
    END AS lcr_compliance;


-- =============================================================================
-- 5. COMPLETE CALL REPORT
-- =============================================================================

CREATE OR REPLACE VIEW gold.call_report AS
SELECT 
    bs.report_date,
    
    -- Balance Sheet
    bs.cash_and_balances,
    bs.total_assets,
    bs.customer_deposits_savings,
    bs.customer_deposits_current,
    bs.total_liabilities,
    bs.total_equity,
    
    -- Capital
    cap.cet1_capital,
    cap.tier1_capital,
    cap.total_capital,
    cap.risk_weighted_assets,
    cap.cet1_ratio,
    cap.tier1_ratio,
    cap.car_ratio,
    cap.compliance_status,
    
    -- Asset Quality
    aq.total_loans,
    aq.standard_loans,
    aq.special_mention,
    aq.sub_standard,
    aq.doubtful,
    aq.loss,
    aq.npl_ratio,
    aq.provision_coverage_ratio,
    
    -- Liquidity
    liq.total_deposits,
    liq.liquid_assets,
    liq.lcr_ratio,
    liq.lcr_compliance,
    
    -- Submission Status
    'PENDING' AS submission_status,
    CURRENT_TIMESTAMP AS generated_at

FROM gold.call_report_balance_sheet bs
CROSS JOIN gold.call_report_capital cap
CROSS JOIN gold.call_report_asset_quality aq
CROSS JOIN gold.call_report_liquidity liq;


-- =============================================================================
-- 6. VALIDATION QUERIES
-- =============================================================================

-- Verify balance sheet balances
SELECT 
    'Balance Sheet Check' AS validation,
    CASE 
        WHEN total_assets = total_liabilities + total_equity 
        THEN 'PASS' 
        ELSE 'FAIL' 
    END AS result,
    total_assets,
    total_liabilities,
    total_equity,
    (total_assets - total_liabilities - total_equity) AS difference
FROM gold.call_report_balance_sheet;

-- Verify capital ratios
SELECT 
    'Capital Ratio Check' AS validation,
    CASE 
        WHEN car_ratio >= 8.0 THEN 'PASS'
        ELSE 'FAIL'
    END AS result,
    car_ratio,
    8.0 AS required_ratio,
    (car_ratio - 8.0) AS surplus
FROM gold.call_report_capital;

-- Verify NPL ratio
SELECT 
    'NPL Ratio Check' AS validation,
    CASE 
        WHEN npl_ratio <= 3.0 THEN 'PASS'
        WHEN npl_ratio <= 5.0 THEN 'WARNING'
        ELSE 'FAIL'
    END AS result,
    npl_ratio,
    3.0 AS target_ratio,
    5.0 AS threshold_ratio
FROM gold.call_report_asset_quality;
