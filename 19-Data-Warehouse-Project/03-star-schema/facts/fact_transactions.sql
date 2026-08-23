-- =============================================================================
-- FACT TABLE: fact_transactions
-- =============================================================================
-- Type: Transaction Fact (one row per transaction)
-- Purpose: Store individual banking transactions with measures
-- Grain: One row per transaction
-- =============================================================================

CREATE TABLE dw.fact_transactions (
    -- Surrogate Key
    transaction_sk      SERIAL PRIMARY KEY,

    -- Foreign Keys (to dimensions)
    date_key            INT NOT NULL REFERENCES dw.dim_date(date_key),
    customer_sk         INT NOT NULL REFERENCES dw.dim_customer(customer_sk),
    account_sk          INT NOT NULL REFERENCES dw.dim_account(account_sk),
    branch_sk           INT NOT NULL REFERENCES dw.dim_branch(branch_sk),

    -- Degenerate Dimension (from source)
    txn_id              BIGINT,
    txn_type            VARCHAR(20),  -- CREDIT, DEBIT, TRANSFER
    channel             VARCHAR(20),  -- ATM, MOBILE, ONLINE, BRANCH

    -- Measures (additive)
    transaction_amount  DECIMAL(18,2),
    fee_amount          DECIMAL(18,2) DEFAULT 0,
    tax_amount          DECIMAL(18,2) DEFAULT 0,

    -- Derived Measures
    net_amount          DECIMAL(18,2),  -- transaction_amount + fee_amount + tax_amount

    -- Semi-Additive Measures
    running_balance     DECIMAL(18,2),

    -- Non-Additive Measures
    is_high_value       BOOLEAN,  -- amount > 100,000,000 VND
    is_weekend          BOOLEAN,

    -- Audit Columns
    source_system       VARCHAR(50),
    etl_batch_id        VARCHAR(50),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- INDEXES (for query performance)
-- =============================================================================
CREATE INDEX idx_fact_txn_date ON dw.fact_transactions(date_key);
CREATE INDEX idx_fact_txn_customer ON dw.fact_transactions(customer_sk);
CREATE INDEX idx_fact_txn_account ON dw.fact_transactions(account_sk);
CREATE INDEX idx_fact_txn_branch ON dw.fact_transactions(branch_sk);
CREATE INDEX idx_fact_txn_type ON dw.fact_transactions(txn_type);
CREATE INDEX idx_fact_txn_high_value ON dw.fact_transactions(is_high_value);

-- =============================================================================
-- PARTITIONING (by date for performance)
-- =============================================================================
-- Note: PostgreSQL 16 supports native partitioning
-- This is an example of range partitioning by year

-- =============================================================================
-- SAMPLE DATA
-- =============================================================================
INSERT INTO dw.fact_transactions (
    date_key, customer_sk, account_sk, branch_sk,
    txn_id, txn_type, channel,
    transaction_amount, fee_amount, tax_amount, net_amount,
    is_high_value, is_weekend, source_system
)
SELECT
    TO_CHAR(t.txn_date, 'YYYYMMDD')::INT AS date_key,
    c.customer_sk,
    a.account_sk,
    b.branch_sk,
    t.txn_id,
    t.txn_type,
    t.channel,
    t.amount AS transaction_amount,
    0 AS fee_amount,
    0 AS tax_amount,
    t.amount AS net_amount,
    CASE WHEN t.amount > 100000000 THEN TRUE ELSE FALSE END AS is_high_value,
    CASE WHEN EXTRACT(DOW FROM t.txn_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    'CORE_BANKING' AS source_system
FROM cbs.transactions t
JOIN dw.dim_account a ON t.account_id = a.account_id
JOIN dw.dim_customer c ON a.customer_id = c.customer_id AND c.is_current = TRUE
JOIN dw.dim_branch b ON a.branch_code = b.branch_code;

-- =============================================================================
-- AGGREGATION VIEW (for BI tools)
-- =============================================================================
CREATE OR REPLACE VIEW vw_daily_transaction_summary AS
SELECT
    d.full_date,
    d.day_name,
    d.month_name,
    d.quarter_name,
    d.year,
    c.customer_segment,
    a.account_type,
    b.region,
    t.txn_type,
    t.channel,
    COUNT(*) AS transaction_count,
    SUM(t.transaction_amount) AS total_amount,
    AVG(t.transaction_amount) AS avg_amount,
    MAX(t.transaction_amount) AS max_amount,
    SUM(CASE WHEN t.is_high_value THEN 1 ELSE 0 END) AS high_value_count
FROM dw.fact_transactions t
JOIN dw.dim_date d ON t.date_key = d.date_key
JOIN dw.dim_customer c ON t.customer_sk = c.customer_sk
JOIN dw.dim_account a ON t.account_sk = a.account_sk
JOIN dw.dim_branch b ON t.branch_sk = b.branch_sk
GROUP BY d.full_date, d.day_name, d.month_name, d.quarter_name, d.year,
         c.customer_segment, a.account_type, b.region, t.txn_type, t.channel;

COMMENT ON TABLE dw.fact_transactions IS 'Transaction fact table - one row per transaction';
COMMENT ON VIEW vw_daily_transaction_summary IS 'Pre-aggregated daily transaction summary for BI';
