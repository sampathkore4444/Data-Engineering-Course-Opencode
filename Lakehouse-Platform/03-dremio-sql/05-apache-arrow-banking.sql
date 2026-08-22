-- Apache Arrow Banking Scenario
-- ==============================================
-- Understanding how Apache Arrow powers Dremio for banking analytics

-- ============================================================================
-- SECTION 1: WHAT IS APACHE ARROW AND WHY IT MATTERS FOR BANKING
-- ============================================================================

/*
┌─────────────────────────────────────────────────────────────────────────┐
│                    APACHE ARROW IN BANKING                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  Apache Arrow is a columnar memory format that enables:                │
│                                                                         │
│  1. ZERO-COPY READING    → No serialization overhead                    │
│  2. COLUMNAR STORAGE     → Read only needed columns (10-100x faster)   │
│  3. SIMD OPTIMIZATION    → CPU processes multiple values at once        │
│  4. CROSS-SYSTEM SHARING → No conversion between systems                │
│                                                                         │
│  Banking Impact:                                                        │
│  • Query 1 billion transactions in SECONDS instead of HOURS            │
│  • Real-time fraud detection on card transactions                       │
│  • Instant Customer 360° view across all products                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- SECTION 2: APACHE ARROW VS TRADITIONAL FORMAT (BANKING EXAMPLE)
-- ============================================================================

-- Scenario: Bank has 100 million card transactions
-- Question: What is the total spend by merchant category for last 30 days?

-- TRADITIONAL ROW-BASED FORMAT (e.g., CSV, JSON, Row-based Parquet)
-- ─────────────────────────────────────────────────────────────────────
-- The database must read ALL columns for EVERY row, then filter

/*
┌─────────────────────────────────────────────────────────────────────┐
│           ROW-BASED STORAGE (Traditional)                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  To get merchant_category + amount:                                  │
│                                                                     │
│  Row 1: [txn_id, card_id, txn_date, amount, merchant_category,     │
│          merchant_name, location, channel, status, ...]  ← READ ALL │
│  Row 2: [txn_id, card_id, txn_date, amount, merchant_category,     │
│          merchant_name, location, channel, status, ...]  ← READ ALL │
│  ...                                                                 │
│  Row 100,000,000: [txn_id, card_id, txn_date, amount, ...]          │
│                                                                     │
│  Data Read: 100M rows × 50 columns = 5 BILLION values              │
│  Time: ~45 minutes                                                   │
│  Disk I/O: 500 GB                                                    │
└─────────────────────────────────────────────────────────────────────┘
*/

-- APACHE ARROW COLUMNAR FORMAT (Used by Dremio)
-- ─────────────────────────────────────────────────────────────────────
-- The database reads ONLY the columns needed

/*
┌─────────────────────────────────────────────────────────────────────┐
│           COLUMNAR STORAGE (Apache Arrow)                           │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  To get merchant_category + amount:                                  │
│                                                                     │
│  Column: merchant_category → [ELECTRONICS, FOOD, TRAVEL, ...]       │
│  Column: amount           → [15000, 800, 45000, ...]                │
│                                                                     │
│  Data Read: 100M rows × 2 columns = 200 MILLION values             │
│  Time: ~3 seconds                                                    │
│  Disk I/O: 2 GB                                                      │
│                                                                     │
│  SPEEDUP: 900x faster!                                               │
└─────────────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- SECTION 3: BANKING QUERY WITH APACHE ARROW OPTIMIZATION
-- ============================================================================

-- Query 1: Card Transaction Analysis (Arrow-Optimized)
-- This query demonstrates how Arrow's columnar format speeds up banking analytics

SELECT 
    merchant_category,
    card_type,
    -- Aggregate metrics - only reads these 4 columns from disk
    COUNT(*) AS transaction_count,
    SUM(transaction_amount) AS total_amount,
    AVG(transaction_amount) AS avg_amount,
    MAX(transaction_amount) AS max_amount,
    
    -- Time-based aggregation
    EXTRACT(HOUR FROM transaction_date) AS hour_of_day,
    CASE 
        WHEN EXTRACT(HOUR FROM transaction_date) BETWEEN 0 AND 6 THEN 'NIGHT'
        WHEN EXTRACT(HOUR FROM transaction_date) BETWEEN 7 AND 12 THEN 'MORNING'
        WHEN EXTRACT(HOUR FROM transaction_date) BETWEEN 13 AND 18 THEN 'AFTERNOON'
        ELSE 'EVENING'
    END AS time_period
    
FROM "banking-vault"."virtual.card_transaction_analytics"
WHERE transaction_date >= CURRENT_DATE - INTERVAL '30' DAY
  AND status = 'POSTED'
GROUP BY merchant_category, card_type, EXTRACT(HOUR FROM transaction_date)
ORDER BY total_amount DESC;

/*
WHY THIS IS FAST WITH ARROW:
┌─────────────────────────────────────────────────────────────────────┐
│  Columns Read from Disk:                                            │
│  ✓ merchant_category    (1 column)                                  │
│  ✓ card_type            (1 column)                                  │
│  ✓ transaction_amount   (1 column)                                  │
│  ✓ transaction_date     (1 column)                                  │
│  ✓ status               (1 column - for filter)                     │
│                                                                     │
│  Columns NOT Read (skipped by Arrow):                               │
│  ✗ transaction_id                                                │
│  ✗ card_id                                                         │
│  ✗ card_number                                                     │
│  ✗ merchant_name                                                   │
│  ✗ posting_date                                                    │
│  ✗ description                                                     │
│  ... (40+ more columns)                                             │
│                                                                     │
│  Result: 90% less disk I/O = 90% faster query                       │
└─────────────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- SECTION 4: ARROW REFLECTIONS FOR BANKING DASHBOARDS
-- ============================================================================

-- Reflection 1: Daily Transaction Summary (Pre-Aggregated)
-- Dremio creates an Arrow-based reflection for instant dashboard queries

/*
┌─────────────────────────────────────────────────────────────────────┐
│                    ARROW REFLECTION ARCHITECTURE                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Raw Transaction Data (100M rows)                                   │
│       │                                                              │
│       ▼                                                              │
│  ┌─────────────────────────────────────────┐                        │
│  │  DREMIO REFLECTION ENGINE               │                        │
│  │  (Uses Apache Arrow internally)         │                        │
│  │                                         │                        │
│  │  1. Reads raw data in Arrow format      │                        │
│  │  2. Applies aggregations                │                        │
│  │  3. Stores result as Arrow reflection   │                        │
│  └─────────────────────────────────────────┘                        │
│       │                                                              │
│       ▼                                                              │
│  Daily Transaction Summary (30 rows)                                │
│  ┌─────────────────────────────────────────┐                        │
│  │  Arrow Reflection (Pre-aggregated)      │                        │
│  │                                         │                        │
│  │  [date, category, count, total, avg]    │ ← Stored in Arrow     │
│  │  [2025-01-15, ELECTRONICS, 5000, ...]   │   format in memory    │
│  │  [2025-01-15, FOOD, 8000, ...]          │                        │
│  │  ...                                    │                        │
│  └─────────────────────────────────────────┘                        │
│       │                                                              │
│       ▼                                                              │
│  Dashboard Query (Instant - reads 30 rows instead of 100M)         │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
*/

-- SQL to Create Arrow Reflection in Dremio (via UI or API)
-- This is typically done via Dremio UI, but here's the concept:

/*
CREATE REFLECTION banking_daily_txns_reflection
ON "banking-vault"."virtual.card_transaction_analytics"
USING 
    DISPLAY COLUMNS (transaction_date, merchant_category, card_type)
    MEASURES (COUNT(*), SUM(transaction_amount), AVG(transaction_amount))
    GROUP BY (transaction_date, merchant_category, card_type)
    REFRESH EVERY 15 MINUTES;
*/

-- Query that uses the reflection (runs in milliseconds)
SELECT 
    transaction_date,
    merchant_category,
    SUM(transaction_amount) AS daily_total,
    COUNT(*) AS daily_count
FROM "banking-vault"."virtual.card_transaction_analytics"
WHERE transaction_date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY transaction_date, merchant_category
ORDER BY transaction_date, daily_total DESC;

-- ============================================================================
-- SECTION 5: ARROW FOR REAL-TIME FRAUD DETECTION
-- ============================================================================

-- Scenario: Detect fraudulent card transactions in real-time
-- Arrow enables sub-second analysis on millions of transactions

WITH recent_transactions AS (
    SELECT 
        transaction_id,
        card_id,
        customer_id,
        transaction_amount,
        merchant_category,
        merchant_name,
        transaction_date,
        status,
        
        -- Arrow-powered window functions (fast in-memory processing)
        COUNT(*) OVER (
            PARTITION BY card_id 
            ORDER BY transaction_date 
            RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
        ) AS txn_count_last_hour,
        
        SUM(transaction_amount) OVER (
            PARTITION BY card_id 
            ORDER BY transaction_date 
            RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW
        ) AS total_amount_last_hour,
        
        -- Compare to customer's average
        transaction_amount / NULLIF(
            AVG(transaction_amount) OVER (
                PARTITION BY customer_id
            ), 0
        ) AS amount_vs_avg
        
    FROM "banking-vault"."virtual.card_transaction_analytics"
    WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR
      AND status = 'POSTED'
)
SELECT 
    transaction_id,
    card_id,
    customer_id,
    transaction_amount,
    merchant_category,
    transaction_date,
    
    -- Fraud Risk Scoring (Arrow processes this in-memory)
    CASE 
        WHEN txn_count_last_hour > 10 THEN 'HIGH_VELOCITY'
        WHEN total_amount_last_hour > 500000 THEN 'HIGH_VALUE'
        WHEN amount_vs_avg > 10 THEN 'UNUSUAL_AMOUNT'
        WHEN merchant_category IN ('CRYPTOCURRENCY', 'GAMBLING') THEN 'HIGH_RISK_MERCHANT'
        ELSE 'NORMAL'
    END AS fraud_flag,
    
    txn_count_last_hour,
    total_amount_last_hour,
    ROUND(amount_vs_avg, 2) AS amount_ratio
    
FROM recent_transactions
WHERE txn_count_last_hour > 5
   OR total_amount_last_hour > 200000
   OR amount_vs_avg > 5
ORDER BY transaction_amount DESC;

/*
WHY ARROW MATTERS FOR FRAUD DETECTION:
┌─────────────────────────────────────────────────────────────────────┐
│  Without Arrow (Traditional):                                       │
│  • Scan 100M transactions: 45 minutes                               │
│  • Apply fraud rules: 30 minutes                                    │
│  • Total: 75 minutes (TOO SLOW - fraud already happened!)           │
│                                                                     │
│  With Apache Arrow (Dremio):                                        │
│  • Scan 100M transactions: 3 seconds                                │
│  • Apply fraud rules: 1 second (in-memory)                         │
│  • Total: 4 seconds (FAST ENOUGH - stop fraud in real-time!)        │
│                                                                     │
│  Banking Impact:                                                     │
│  • Prevent $X million in fraud losses per year                      │
│  • Meet regulatory requirements for real-time monitoring            │
│  • Reduce false positives (better customer experience)              │
└─────────────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- SECTION 6: ARROW FOR CUSTOMER 360° DASHBOARD
-- ============================================================================

-- Scenario: CEO wants instant Customer 360° view
-- Arrow reflections make this possible in milliseconds

SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_type,
    c.city,
    
    -- Account Summary (from reflection)
    a.total_accounts,
    a.savings_balance,
    a.current_balance,
    
    -- Cards Summary (from reflection)
    cc.total_cards,
    cc.total_card_outstanding,
    cc.total_credit_limit,
    
    -- Loans Summary (from reflection)
    l.total_loans,
    l.total_loan_outstanding,
    l.npa_accounts,
    
    -- Computed Metrics (Arrow processes in-memory)
    (a.savings_balance + a.current_balance + 
     cc.total_card_outstanding + l.total_loan_outstanding) AS total_relationship,
    
    -- Customer Tier (computed)
    CASE 
        WHEN (a.savings_balance + a.current_balance + 
              cc.total_card_outstanding + l.total_loan_outstanding) > 10000000 
            THEN 'PLATINUM'
        WHEN (a.savings_balance + a.current_balance + 
              cc.total_card_outstanding + l.total_loan_outstanding) > 5000000 
            THEN 'GOLD'
        WHEN (a.savings_balance + a.current_balance + 
              cc.total_card_outstanding + l.total_loan_outstanding) > 1000000 
            THEN 'SILVER'
        ELSE 'BRONZE'
    END AS customer_tier,
    
    -- Risk Score (computed)
    CASE 
        WHEN l.npa_accounts > 0 THEN 'HIGH_RISK'
        WHEN cc.total_card_outstanding / NULLIF(cc.total_credit_limit, 0) > 0.90 
            THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END AS risk_score
    
FROM "banking-vault"."virtual.customer_360" c
-- These are Arrow reflections - instant access
LEFT JOIN (
    SELECT customer_id, 
           COUNT(*) AS total_accounts,
           SUM(CASE WHEN account_type = 'SAVINGS' THEN balance ELSE 0 END) AS savings_balance,
           SUM(CASE WHEN account_type = 'CURRENT' THEN balance ELSE 0 END) AS current_balance
    FROM "banking-vault"."virtual.customer_accounts"
    GROUP BY customer_id
) a ON c.customer_id = a.customer_id
LEFT JOIN (
    SELECT customer_id,
           COUNT(*) AS total_cards,
           SUM(outstanding) AS total_card_outstanding,
           SUM(credit_limit) AS total_credit_limit
    FROM "banking-vault"."virtual.customer_cards"
    GROUP BY customer_id
) cc ON c.customer_id = cc.customer_id
LEFT JOIN (
    SELECT customer_id,
           COUNT(*) AS total_loans,
           SUM(principal_outstanding) AS total_loan_outstanding,
           COUNT(CASE WHEN npa_classification != 'STANDARD' THEN 1 END) AS npa_accounts
    FROM "banking-vault"."virtual.customer_loans"
    GROUP BY customer_id
) l ON c.customer_id = l.customer_id
WHERE c.customer_id = 'CUST-001';

/*
ARROW PERFORMANCE FOR DASHBOARDS:
┌─────────────────────────────────────────────────────────────────────┐
│  Dashboard: CEO Customer 360° View                                  │
│                                                                     │
│  WITHOUT Arrow Reflections:                                         │
│  • Query joins 4 tables (10M + 2M + 500K + 1M rows)               │
│  • Scans: 13.5 million rows                                        │
│  • Time: 45 seconds                                                 │
│  • User Experience: "This is too slow!"                             │
│                                                                     │
│  WITH Arrow Reflections:                                            │
│  • Query uses pre-aggregated reflections                            │
│  • Scans: 10,000 rows (already aggregated)                         │
│  • Time: 50 milliseconds                                            │
│  • User Experience: "Wow, instant!"                                 │
│                                                                     │
│  SPEEDUP: 900x faster                                               │
└─────────────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- SECTION 7: ARROW FOR REGULATORY REPORTING
-- ============================================================================

-- Scenario: Generate Basel III report in minutes, not hours
-- Arrow enables fast aggregation on billions of rows

WITH loan_risk_data AS (
    SELECT 
        loan_id,
        customer_id,
        loan_type,
        principal_outstanding,
        npa_classification,
        days_past_due,
        
        -- Risk weight calculation (Arrow processes in-memory)
        CASE 
            WHEN loan_type = 'HOME' THEN 0.35
            WHEN loan_type = 'VEHICLE' THEN 0.75
            WHEN loan_type = 'PERSONAL' THEN 1.00
            WHEN loan_type = 'EDUCATION' THEN 0.50
            ELSE 1.00
        END AS risk_weight,
        
        -- Provision rate (RBI guidelines)
        CASE 
            WHEN npa_classification = 'STANDARD' THEN 0.0040
            WHEN npa_classification = 'SUB_STANDARD' THEN 0.15
            WHEN npa_classification = 'DOUBTFUL' THEN 0.40
            WHEN npa_classification = 'LOSS' THEN 1.00
            ELSE 0.0040
        END AS provision_rate
        
    FROM "banking-vault"."virtual.loan_performance"
    WHERE loan_status = 'ACTIVE'
)
SELECT 
    'BASEL_III_REPORT' AS report_type,
    CURRENT_DATE AS report_date,
    
    -- Total Portfolio
    COUNT(*) AS total_accounts,
    SUM(principal_outstanding) AS total_exposure,
    
    -- Risk-Weighted Assets
    SUM(principal_outstanding * risk_weight) AS risk_weighted_assets,
    
    -- Capital Requirements (8% of RWA)
    SUM(principal_outstanding * risk_weight) * 0.08 AS minimum_capital,
    
    -- Provision Requirements
    SUM(principal_outstanding * provision_rate) AS total_provisions,
    
    -- NPA Summary
    COUNT(CASE WHEN npa_classification != 'STANDARD' THEN 1 END) AS npa_accounts,
    SUM(CASE WHEN npa_classification != 'STANDARD' 
        THEN principal_outstanding ELSE 0 END) AS npa_amount,
    
    -- NPA Ratio
    ROUND(
        SUM(CASE WHEN npa_classification != 'STANDARD' 
            THEN principal_outstanding ELSE 0 END) / 
        SUM(principal_outstanding) * 100, 2
    ) AS npa_ratio_pct,
    
    -- Compliance Status
    CASE 
        WHEN (SUM(principal_outstanding * risk_weight) * 0.08) <= 
             (SUM(principal_outstanding) * 0.10)  -- Assuming 10% capital
        THEN 'COMPLIANT'
        ELSE 'NON-COMPLIANT'
    END AS capital_adequacy_status
    
FROM loan_risk_data;

/*
ARROW FOR REGULATORY REPORTS:
┌─────────────────────────────────────────────────────────────────────┐
│  Report: Basel III Capital Adequacy                                 │
│                                                                     │
│  Data Size: 50 million loan records                                 │
│                                                                     │
│  WITHOUT Arrow:                                                     │
│  • Scan 50M rows: 20 minutes                                        │
│  • Calculate risk weights: 15 minutes                               │
│  • Generate report: 10 minutes                                      │
│  • Total: 45 minutes                                                │
│                                                                     │
│  WITH Arrow (Dremio):                                               │
│  • Scan 50M rows (columnar): 2 seconds                              │
│  • Calculate risk weights (in-memory): 1 second                     │
│  • Generate report: 0.5 seconds                                     │
│  • Total: 3.5 seconds                                               │
│                                                                     │
│  SPEEDUP: 770x faster                                               │
│                                                                     │
│  Business Value:                                                    │
│  • Regulatory reports generated in minutes, not hours               │
│  • Faster decision-making for risk management                       │
│  • Real-time compliance monitoring possible                         │
└─────────────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- SECTION 8: APACHE ARROW PERFORMANCE COMPARISON
-- ============================================================================

-- Query 1: Large Table Scan Performance
-- Compare Arrow vs Row-based performance

-- Traditional Row-Based Query (simulated)
/*
-- Time: ~45 minutes on 100M rows
SELECT merchant_category, COUNT(*), SUM(amount)
FROM card_transactions
WHERE transaction_date >= '2025-01-01'
GROUP BY merchant_category;
*/

-- Arrow-Optimized Query (Dremio)
-- Time: ~3 seconds on 100M rows (same query!)
SELECT 
    merchant_category, 
    COUNT(*) AS transaction_count, 
    SUM(transaction_amount) AS total_amount
FROM "banking-vault"."virtual.card_transaction_analytics"
WHERE transaction_date >= '2025-01-01'
GROUP BY merchant_category;

-- Query 2: Complex Aggregation Performance

-- Traditional: ~2 hours on 500M rows
-- Arrow (Dremio): ~10 seconds on 500M rows

SELECT 
    c.customer_type,
    c.city,
    DATE_TRUNC('month', t.transaction_date) AS month,
    COUNT(DISTINCT t.customer_id) AS unique_customers,
    COUNT(*) AS transaction_count,
    SUM(t.amount) AS total_amount,
    AVG(t.amount) AS avg_amount,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.amount) AS median_amount
FROM "banking-vault"."virtual.transaction_analytics" t
JOIN "banking-vault"."virtual.customer_master" c ON t.customer_id = c.customer_id
WHERE t.transaction_date >= '2024-01-01'
GROUP BY c.customer_type, c.city, DATE_TRUNC('month', t.transaction_date)
ORDER BY total_amount DESC;

/*
PERFORMANCE COMPARISON SUMMARY:
┌─────────────────────────────────────────────────────────────────────┐
│                    APACHE ARROW PERFORMANCE                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Operation              │ Row-Based │ Arrow (Dremio) │ Speedup     │
│  ─────────────────────────────────────────────────────────────────  │
│  Scan 100M rows         │ 45 min    │ 3 sec          │ 900x        │
│  Filter + Aggregate     │ 30 min    │ 2 sec          │ 900x        │
│  Join 4 tables          │ 2 hours   │ 10 sec         │ 720x        │
│  Complex analytics      │ 3 hours   │ 15 sec         │ 720x        │
│  Dashboard query        │ 45 sec    │ 50 ms          │ 900x        │
│                                                                     │
│  Key Arrow Features:                                                │
│  • Columnar: Read only needed columns                              │
│  • In-memory: No disk I/O for processing                           │
│  • SIMD: CPU processes 8+ values at once                           │
│  • Zero-copy: No serialization overhead                            │
│  • Parallel: Multiple cores work simultaneously                    │
│                                                                     │
│  Banking Benefits:                                                  │
│  ✓ Real-time fraud detection (stop fraud in seconds)               │
│  ✓ Instant dashboards (CEO gets answers in milliseconds)           │
│  ✓ Fast regulatory reports (Basel III in minutes)                  │
│  ✓ Better customer experience (call center queries are instant)    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- SECTION 9: APACHE ARROW DATA SHARING (BANKING USE CASE)
-- ============================================================================

-- Scenario: Share data between Core Banking and Cards System
-- Arrow enables zero-copy data sharing between systems

/*
┌─────────────────────────────────────────────────────────────────────┐
│                    ARROW DATA SHARING IN BANKING                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Problem:                                                           │
│  • Core Banking (Oracle) needs to share customer data with          │
│    Cards System (Mainframe) for fraud detection                     │
│  • Traditional: Extract → Transform → Load (slow, data duplication)│
│  • Arrow: Share memory directly (instant, no copy)                 │
│                                                                     │
│  Solution with Arrow:                                               │
│                                                                     │
│  ┌──────────────┐         ┌──────────────┐                         │
│  │ Core Banking │  Arrow  │ Cards System │                         │
│  │ (PostgreSQL) │◄──────►│ (MySQL)      │                         │
│  │              │ Memory │              │                         │
│  │ Customer     │ Share  │ Card         │                         │
│  │ Data         │        │ Transactions │                         │
│  └──────────────┘         └──────────────┘                         │
│         │                        │                                  │
│         └───────────┬────────────┘                                  │
│                     │                                               │
│                     ▼                                               │
│           ┌──────────────┐                                          │
│           │ Dremio       │                                          │
│           │ (Arrow-based)│                                          │
│           │              │                                          │
│           │ Single query │                                          │
│           │ across both  │                                          │
│           │ systems      │                                          │
│           └──────────────┘                                          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
*/

-- Query that joins data across systems using Arrow
-- This query runs in seconds because Arrow shares memory directly

SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_type,
    
    -- From Core Banking (PostgreSQL)
    a.account_id,
    a.balance,
    a.account_type,
    
    -- From Credit Cards (MySQL)
    cc.card_id,
    cc.card_number,
    cc.outstanding,
    cc.credit_limit,
    
    -- Computed: Total exposure
    (a.balance + cc.outstanding) AS total_exposure,
    
    -- Risk Flag (Arrow computes in-memory)
    CASE 
        WHEN (a.balance + cc.outstanding) > 1000000 THEN 'HIGH_EXPOSURE'
        WHEN cc.outstanding / NULLIF(cc.credit_limit, 0) > 0.90 THEN 'HIGH_UTILIZATION'
        ELSE 'NORMAL'
    END AS risk_flag
    
FROM "banking-postgres".core_banking.customers c
-- Arrow-powered cross-system join (no data movement!)
JOIN "banking-postgres".core_banking.accounts a 
    ON c.customer_id = a.customer_id
JOIN "banking-mysql".credit_cards.credit_cards cc 
    ON c.customer_id = cc.customer_id
WHERE c.customer_id = 'CUST-001';

/*
CROSS-SYSTEM DATA SHARING WITH ARROW:
┌─────────────────────────────────────────────────────────────────────┐
│  Traditional Approach:                                              │
│  1. Extract data from PostgreSQL (10 minutes)                      │
│  2. Transform and clean (5 minutes)                                │
│  3. Load into MySQL (10 minutes)                                   │
│  4. Run query (5 seconds)                                          │
│  Total: 25 minutes + data duplication                              │
│                                                                     │
│  Arrow Approach (Dremio):                                           │
│  1. Query runs directly on both systems (0 seconds setup)          │
│  2. Arrow shares memory between systems (instant)                  │
│  3. Query executes (3 seconds)                                     │
│  Total: 3 seconds + no data duplication                            │
│                                                                     │
│  Benefits:                                                          │
│  ✓ No ETL pipeline needed                                          │
│  ✓ No data duplication (saves storage costs)                       │
│  ✓ Always fresh data (real-time access)                            │
│  ✓ Faster development (no data movement code)                      │
└─────────────────────────────────────────────────────────────────────┘
*/

-- ============================================================================
-- SECTION 10: APACHE ARROW BANKING DASHBOARD QUERIES
-- ============================================================================

-- Dashboard 1: Real-Time Transaction Monitoring
-- Arrow enables sub-second updates on live transaction feed

SELECT 
    DATE_TRUNC('minute', transaction_date) AS minute,
    merchant_category,
    card_type,
    COUNT(*) AS transactions_per_minute,
    SUM(transaction_amount) AS volume_per_minute,
    AVG(transaction_amount) AS avg_transaction_size,
    
    -- Real-time fraud indicators
    COUNT(CASE WHEN transaction_amount > 100000 THEN 1 END) AS high_value_count,
    COUNT(CASE WHEN merchant_category IN ('CRYPTOCURRENCY', 'GAMBLING') 
          THEN 1 END) AS high_risk_merchant_count
    
FROM "banking-vault"."virtual.card_transaction_analytics"
WHERE transaction_date >= CURRENT_TIMESTAMP - INTERVAL '1' HOUR
GROUP BY DATE_TRUNC('minute', transaction_date), merchant_category, card_type
ORDER BY minute DESC, volume_per_minute DESC;

-- Dashboard 2: Executive KPI Summary
-- Arrow reflections provide instant access to aggregated data

SELECT 
    CURRENT_DATE AS report_date,
    
    -- Deposit Metrics
    (SELECT SUM(balance) FROM "banking-vault"."virtual.customer_accounts" 
     WHERE account_type = 'SAVINGS') AS total_savings,
    (SELECT SUM(balance) FROM "banking-vault"."virtual.customer_accounts" 
     WHERE account_type = 'CURRENT') AS total_current,
    
    -- Lending Metrics
    (SELECT SUM(principal_outstanding) FROM "banking-vault"."virtual.customer_loans" 
     WHERE loan_status = 'ACTIVE') AS total_advances,
    (SELECT COUNT(CASE WHEN npa_classification != 'STANDARD' THEN 1 END) 
     FROM "banking-vault"."virtual.customer_loans" 
     WHERE loan_status = 'ACTIVE') AS npa_count,
    
    -- Card Metrics
    (SELECT SUM(outstanding) FROM "banking-vault"."virtual.customer_cards" 
     WHERE card_status = 'ACTIVE') AS card_outstanding,
    (SELECT COUNT(*) FROM "banking-vault"."virtual.customer_cards" 
     WHERE card_status = 'ACTIVE') AS active_cards,
    
    -- Customer Metrics
    (SELECT COUNT(*) FROM "banking-vault"."virtual.customer_master") AS total_customers,
    (SELECT COUNT(DISTINCT customer_id) FROM "banking-vault"."virtual.card_transaction_analytics"
     WHERE transaction_date >= CURRENT_DATE) AS daily_active_customers;

-- Dashboard 3: NPA Tracking (Real-Time)
-- Arrow enables instant NPA monitoring

SELECT 
    npa_classification,
    COUNT(*) AS account_count,
    SUM(principal_outstanding) AS outstanding_amount,
    AVG(days_past_due) AS avg_dpd,
    MAX(days_past_due) AS max_dpd,
    
    -- Provision required
    SUM(CASE 
        WHEN npa_classification = 'SUB_STANDARD' THEN principal_outstanding * 0.15
        WHEN npa_classification = 'DOUBTFUL' THEN principal_outstanding * 0.40
        WHEN npa_classification = 'LOSS' THEN principal_outstanding * 1.00
        ELSE 0
    END) AS provision_required
    
FROM "banking-vault"."virtual.loan_performance"
WHERE loan_status = 'ACTIVE'
GROUP BY npa_classification
ORDER BY 
    CASE npa_classification
        WHEN 'STANDARD' THEN 1
        WHEN 'SUB_STANDARD' THEN 2
        WHEN 'DOUBTFUL' THEN 3
        WHEN 'LOSS' THEN 4
    END;

-- ============================================================================
-- SECTION 11: APACHE ARROW KEY TAKEAWAYS FOR BANKING
-- ============================================================================

/*
┌─────────────────────────────────────────────────────────────────────┐
│            APACHE ARROW BANKING SUMMARY                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  WHAT IS ARROW?                                                     │
│  • Columnar memory format for fast analytics                        │
│  • Powers Dremio's 10-100x performance advantage                   │
│  • Enables zero-copy data sharing between systems                   │
│                                                                     │
│  BANKING USE CASES:                                                 │
│                                                                     │
│  1. REAL-TIME FRAUD DETECTION                                       │
│     • Detect fraud in milliseconds, not hours                       │
│     • Prevent millions in losses annually                           │
│                                                                     │
│  2. INSTANT DASHBOARDS                                              │
│     • CEO gets answers in milliseconds                              │
│     • No more "waiting for reports"                                 │
│                                                                     │
│  3. FAST REGULATORY REPORTS                                         │
│     • Basel III, AML, NPA reports in minutes                        │
│     • Meet compliance deadlines easily                              │
│                                                                     │
│  4. CROSS-SYSTEM INTEGRATION                                        │
│     • Join Core Banking + Cards + Loans instantly                   │
│     • No ETL pipelines needed for ad-hoc queries                   │
│                                                                     │
│  5. BETTER CUSTOMER EXPERIENCE                                      │
│     • Call center agents get instant Customer 360°                  │
│     • Faster loan approval decisions                                │
│                                                                     │
│  PERFORMANCE GAINS:                                                 │
│  • 900x faster queries on large datasets                           │
│  • Real-time analytics on 100M+ rows                               │
│  • Sub-second dashboard updates                                     │
│                                                                     │
│  HOW TO USE IN DREMIO:                                              │
│  1. Store data in columnar formats (Parquet, Iceberg, Delta)       │
│  2. Create reflections for frequently accessed queries              │
│  3. Use Dremio's Arrow-based query engine                           │
│  4. Enable zero-copy cloning for test environments                  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
*/