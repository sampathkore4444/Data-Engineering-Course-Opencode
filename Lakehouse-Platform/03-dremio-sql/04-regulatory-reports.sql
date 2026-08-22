-- Dremio SQL Scripts: Regulatory Reporting (Basel III, NPA, etc.)
-- =============================================================

-- Query 1: Basel III Capital Adequacy Report
-- Risk-weighted assets calculation
SELECT 
    'BASEL_III_REPORT' AS report_type,
    CURRENT_DATE() AS report_date,
    
    -- Total Assets
    (SELECT SUM(balance) FROM "banking-postgres".core_banking.accounts 
     WHERE account_type IN ('SAVINGS', 'CURRENT', 'FD')) AS total_deposits,
    
    -- Total Loans
    (SELECT SUM(principal_outstanding) FROM "banking-postgres".core_banking.loan_accounts 
     WHERE loan_status = 'ACTIVE') AS total_advances,
    
    -- Total Card Outstanding
    (SELECT SUM(outstanding) FROM "banking-mysql".credit_cards.credit_cards 
     WHERE card_status = 'ACTIVE') AS total_card_outstanding,
    
    -- Risk Classification
    (SELECT SUM(CASE WHEN npa_classification = 'STANDARD' 
                     THEN principal_outstanding * 0.20 
                     ELSE principal_outstanding * 1.00 END) 
     FROM "banking-postgres".core_banking.loan_accounts 
     WHERE loan_status = 'ACTIVE') AS risk_weighted_assets,
    
    -- Capital Requirements
    (SELECT SUM(principal_outstanding * 0.08) 
     FROM "banking-postgres".core_banking.loan_accounts 
     WHERE loan_status = 'ACTIVE' 
       AND npa_classification = 'STANDARD') AS minimum_capital_requirement;

-- Query 2: NPA (Non-Performing Assets) Report
-- Required by RBI for all banks
SELECT 
    'NPA_REPORT' AS report_type,
    CURRENT_DATE() AS report_date,
    
    -- NPA Summary
    COUNT(DISTINCT CASE WHEN npa_classification != 'STANDARD' 
                        THEN loan_id END) AS total_npa_accounts,
    
    SUM(CASE WHEN npa_classification != 'STANDARD' 
             THEN principal_outstanding ELSE 0 END) AS total_npa_amount,
    
    SUM(CASE WHEN npa_classification = 'SUB_STANDARD' 
             THEN principal_outstanding ELSE 0 END) AS sub_standard_amount,
    
    SUM(CASE WHEN npa_classification = 'DOUBTFUL' 
             THEN principal_outstanding ELSE 0 END) AS doubtful_amount,
    
    SUM(CASE WHEN npa_classification = 'LOSS' 
             THEN principal_outstanding ELSE 0 END) AS loss_amount,
    
    -- NPA Ratio
    (SUM(CASE WHEN npa_classification != 'STANDARD' 
              THEN principal_outstanding ELSE 0 END) /
     SUM(principal_outstanding) * 100) AS npa_ratio_pct,
    
    -- Provision Required
    SUM(CASE WHEN npa_classification = 'SUB_STANDARD' 
             THEN principal_outstanding * 0.15 ELSE 0 END) AS sub_standard_provision,
    SUM(CASE WHEN npa_classification = 'DOUBTFUL' 
             THEN principal_outstanding * 0.40 ELSE 0 END) AS doubtful_provision,
    SUM(CASE WHEN npa_classification = 'LOSS' 
             THEN principal_outstanding * 1.00 ELSE 0 END) AS loss_provision,
    
    -- Total Provision
    (SUM(CASE WHEN npa_classification = 'SUB_STANDARD' 
              THEN principal_outstanding * 0.15 ELSE 0 END) +
     SUM(CASE WHEN npa_classification = 'DOUBTFUL' 
              THEN principal_outstanding * 0.40 ELSE 0 END) +
     SUM(CASE WHEN npa_classification = 'LOSS' 
              THEN principal_outstanding * 1.00 ELSE 0 END)) AS total_provision_required
    
FROM "banking-postgres".core_banking.loan_accounts
WHERE loan_status = 'ACTIVE';

-- Query 3: Loan Classification Report (Detailed NPA Breakdown)
SELECT 
    npa_classification,
    COUNT(*) AS account_count,
    SUM(loan_amount) AS total_loan_amount,
    SUM(principal_outstanding) AS outstanding_amount,
    AVG(days_past_due) AS avg_days_past_due,
    MAX(days_past_due) AS max_days_past_due,
    SUM(emi_amount) AS total_monthly_emi,
    
    -- Age-wise breakdown
    SUM(CASE WHEN days_past_due BETWEEN 90 AND 180 THEN principal_outstanding ELSE 0 END) AS days_90_180,
    SUM(CASE WHEN days_past_due BETWEEN 181 AND 365 THEN principal_outstanding ELSE 0 END) AS days_181_365,
    SUM(CASE WHEN days_past_due > 365 THEN principal_outstanding ELSE 0 END) AS days_over_365
    
FROM "banking-postgres".core_banking.loan_accounts
WHERE loan_status = 'ACTIVE'
GROUP BY npa_classification
ORDER BY 
    CASE npa_classification
        WHEN 'STANDARD' THEN 1
        WHEN 'SUB_STANDARD' THEN 2
        WHEN 'DOUBTFUL' THEN 3
        WHEN 'LOSS' THEN 4
    END;

-- Query 4: Credit Card Risk Report
SELECT 
    'CARD_RISK_REPORT' AS report_type,
    CURRENT_DATE() AS report_date,
    
    -- Overall Card Statistics
    COUNT(DISTINCT card_id) AS total_cards,
    COUNT(DISTINCT CASE WHEN card_status = 'ACTIVE' THEN card_id END) AS active_cards,
    COUNT(DISTINCT CASE WHEN card_status = 'BLOCKED' THEN card_id END) AS blocked_cards,
    COUNT(DISTINCT CASE WHEN card_status = 'CANCELLED' THEN card_id END) AS cancelled_cards,
    
    -- Credit Exposure
    SUM(CASE WHEN card_status = 'ACTIVE' THEN credit_limit ELSE 0 END) AS total_credit_limit,
    SUM(CASE WHEN card_status = 'ACTIVE' THEN outstanding ELSE 0 END) AS total_outstanding,
    
    -- Utilization Analysis
    (SUM(CASE WHEN card_status = 'ACTIVE' THEN outstanding ELSE 0 END) /
     SUM(CASE WHEN card_status = 'ACTIVE' THEN credit_limit ELSE 0 END) * 100) AS overall_utilization_pct,
    
    -- High Utilization Cards (Risk)
    COUNT(CASE WHEN (outstanding / credit_limit) > 0.90 THEN 1 END) AS high_utilization_cards,
    SUM(CASE WHEN (outstanding / credit_limit) > 0.90 THEN outstanding ELSE 0 END) AS high_utilization_amount,
    
    -- Overdue Amount
    (SELECT SUM(closing_balance) 
     FROM "banking-mysql".credit_cards.card_billing 
     WHERE payment_status = 'UNPAID') AS total_overdue_amount,
    
    -- Delinquent Cards
    (SELECT COUNT(DISTINCT card_id) 
     FROM "banking-mysql".credit_cards.card_billing 
     WHERE payment_status = 'UNPAID') AS delinquent_cards
    
FROM "banking-mysql".credit_cards.credit_cards;

-- Query 5: Asset Classification Report (RBI Compliant)
SELECT 
    'ASSET_CLASSIFICATION' AS report_type,
    loan_type,
    npa_classification,
    COUNT(*) AS account_count,
    SUM(loan_amount) AS san_position,
    SUM(principal_outstanding) AS net_book_value,
    SUM(principal_outstanding) AS risky_value,
    
    -- Provision Rates (RBI Guidelines)
    CASE npa_classification
        WHEN 'STANDARD' THEN 0.40
        WHEN 'SUB_STANDARD' THEN 15.00
        WHEN 'DOUBTFUL' THEN 40.00
        WHEN 'LOSS' THEN 100.00
    END AS provision_rate_pct,
    
    -- Provision Amount
    CASE npa_classification
        WHEN 'STANDARD' THEN SUM(principal_outstanding) * 0.0040
        WHEN 'SUB_STANDARD' THEN SUM(principal_outstanding) * 0.15
        WHEN 'DOUBTFUL' THEN SUM(principal_outstanding) * 0.40
        WHEN 'LOSS' THEN SUM(principal_outstanding) * 1.00
    END AS provision_amount
    
FROM "banking-postgres".core_banking.loan_accounts
WHERE loan_status = 'ACTIVE'
GROUP BY loan_type, npa_classification
ORDER BY loan_type, npa_classification;

-- Query 6: Daily Transaction Report (For MIS)
SELECT 
    'DAILY_TXN_REPORT' AS report_type,
    CURRENT_DATE() AS report_date,
    
    -- Transaction Counts
    COUNT(*) AS total_transactions,
    COUNT(CASE WHEN transaction_type = 'CREDIT' THEN 1 END) AS credit_count,
    COUNT(CASE WHEN transaction_type = 'DEBIT' THEN 1 END) AS debit_count,
    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) AS failed_count,
    
    -- Transaction Amounts
    SUM(CASE WHEN transaction_type = 'CREDIT' THEN amount ELSE 0 END) AS total_credit_amount,
    SUM(CASE WHEN transaction_type = 'DEBIT' THEN amount ELSE 0 END) AS total_debit_amount,
    AVG(amount) AS avg_transaction_amount,
    MAX(amount) AS max_transaction_amount,
    
    -- By Channel
    COUNT(CASE WHEN description LIKE '%NEFT%' THEN 1 END) AS neft_count,
    COUNT(CASE WHEN description LIKE '%RTGS%' THEN 1 END) AS rtgs_count,
    COUNT(CASE WHEN description LIKE '%IMPS%' THEN 1 END) AS imps_count,
    COUNT(CASE WHEN description LIKE '%UPI%' THEN 1 END) AS upi_count,
    COUNT(CASE WHEN description LIKE '%ATM%' THEN 1 END) AS atm_count
    
FROM "banking-vault"."virtual.transaction_analytics"
WHERE transaction_date >= CURRENT_DATE() - INTERVAL '1' DAY;

-- Query 7: Customer Concentration Report
-- Top customers by exposure (Required by regulators)
SELECT 
    customer_id,
    customer_name,
    customer_type,
    city,
    
    -- Total Exposure
    (COALESCE(a.balance, 0) + 
     COALESCE(cc.outstanding, 0) + 
     COALESCE(l.principal_outstanding, 0)) AS total_exposure,
    
    -- Breakdown
    a.balance AS deposit_balance,
    cc.outstanding AS card_exposure,
    l.principal_outstanding AS loan_exposure,
    
    -- Percentage of Total Bank Exposure
    (COALESCE(a.balance, 0) + 
     COALESCE(cc.outstanding, 0) + 
     COALESCE(l.principal_outstanding, 0)) / 
    (SELECT SUM(balance + outstanding + principal_outstanding) 
     FROM "banking-vault"."virtual.customer_360") * 100 AS exposure_pct
    
FROM "banking-postgres".core_banking.customers c
LEFT JOIN "banking-vault"."virtual.customer_accounts" a ON c.customer_id = a.customer_id
LEFT JOIN "banking-vault"."virtual.customer_cards" cc ON c.customer_id = cc.customer_id
LEFT JOIN "banking-vault"."virtual.customer_loans" l ON c.customer_id = l.customer_id
ORDER BY total_exposure DESC
LIMIT 20;

-- Query 8: Geographic Risk Report
-- Loan performance by geography (Required by RBI)
SELECT 
    c.state,
    c.city,
    
    -- Loan Statistics
    COUNT(DISTINCT l.loan_id) AS total_loans,
    SUM(l.loan_amount) AS total_disbursed,
    SUM(l.principal_outstanding) AS total_outstanding,
    
    -- NPA Statistics
    COUNT(DISTINCT CASE WHEN l.npa_classification != 'STANDARD' 
                        THEN l.loan_id END) AS npa_accounts,
    SUM(CASE WHEN l.npa_classification != 'STANDARD' 
             THEN l.principal_outstanding ELSE 0 END) AS npa_amount,
    
    -- NPA Ratio
    (SUM(CASE WHEN l.npa_classification != 'STANDARD' 
              THEN l.principal_outstanding ELSE 0 END) /
     SUM(l.principal_outstanding) * 100) AS npa_ratio_pct,
    
    -- Average Days Past Due
    AVG(l.days_past_due) AS avg_dpd
    
FROM "banking-postgres".core_banking.loan_accounts l
JOIN "banking-postgres".core_banking.customers c ON l.customer_id = c.customer_id
WHERE l.loan_status = 'ACTIVE'
GROUP BY c.state, c.city
ORDER BY npa_ratio_pct DESC;

-- Query 9: Provision Coverage Report
-- Shows adequacy of loan loss provisions
SELECT 
    npa_classification,
    COUNT(*) AS account_count,
    SUM(principal_outstanding) AS npa_amount,
    
    -- Provision Required
    CASE npa_classification
        WHEN 'SUB_STANDARD' THEN SUM(principal_outstanding) * 0.15
        WHEN 'DOUBTFUL' THEN SUM(principal_outstanding) * 0.40
        WHEN 'LOSS' THEN SUM(principal_outstanding) * 1.00
    END AS provision_required,
    
    -- Provision Percentage
    CASE npa_classification
        WHEN 'SUB_STANDARD' THEN 15.00
        WHEN 'DOUBTFUL' THEN 40.00
        WHEN 'LOSS' THEN 100.00
    END AS provision_rate_pct
    
FROM "banking-postgres".core_banking.loan_accounts
WHERE loan_status = 'ACTIVE'
  AND npa_classification != 'STANDARD'
GROUP BY npa_classification
ORDER BY provision_rate_pct DESC;