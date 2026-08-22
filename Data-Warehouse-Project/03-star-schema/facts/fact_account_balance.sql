-- =============================================================================
-- FACT TABLE: fact_account_balance (Daily Snapshot)
-- =============================================================================
-- Type: Periodic Snapshot Fact (one row per account per day)
-- Purpose: Track daily account balances for trend analysis
-- Grain: One row per account per day
-- =============================================================================

CREATE TABLE dw.fact_account_balance (
    -- Surrogate Key
    balance_sk          SERIAL PRIMARY KEY,

    -- Foreign Keys
    date_key            INT NOT NULL REFERENCES dw.dim_date(date_key),
    customer_sk         INT NOT NULL REFERENCES dw.dim_customer(customer_sk),
    account_sk          INT NOT NULL REFERENCES dw.dim_account(account_sk),
    branch_sk           INT NOT NULL REFERENCES dw.dim_branch(branch_sk),

    -- Measures (semi-additive - can sum across accounts, NOT across dates)
    opening_balance     DECIMAL(18,2),
    closing_balance     DECIMAL(18,2),
    min_balance         DECIMAL(18,2),
    max_balance         DECIMAL(18,2),
    avg_balance         DECIMAL(18,2),

    -- Transaction Counts
    credit_count        INT DEFAULT 0,
    debit_count         INT DEFAULT 0,
    total_credits       DECIMAL(18,2) DEFAULT 0,
    total_debits        DECIMAL(18,2) DEFAULT 0,

    -- Derived Measures
    net_flow            DECIMAL(18,2),  -- total_credits - total_debits
    balance_change      DECIMAL(18,2),  -- closing_balance - opening_balance

    -- Audit
    source_system       VARCHAR(50),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_fact_balance_date ON dw.fact_account_balance(date_key);
CREATE INDEX idx_fact_balance_account ON dw.fact_account_balance(account_sk);
CREATE INDEX idx_fact_balance_customer ON dw.fact_account_balance(customer_sk);

-- =============================================================================
-- SAMPLE DATA
-- =============================================================================
INSERT INTO dw.fact_account_balance (
    date_key, customer_sk, account_sk, branch_sk,
    opening_balance, closing_balance, min_balance, max_balance, avg_balance,
    credit_count, debit_count, total_credits, total_debits,
    net_flow, balance_change, source_system
)
SELECT
    TO_CHAR(t.txn_date, 'YYYYMMDD')::INT AS date_key,
    c.customer_sk,
    a.account_sk,
    b.branch_sk,
    a.current_balance - CASE WHEN t.txn_type = 'CREDIT' THEN t.amount ELSE -t.amount END AS opening_balance,
    a.current_balance AS closing_balance,
    a.current_balance - CASE WHEN t.txn_type = 'CREDIT' THEN t.amount ELSE 0 END AS min_balance,
    a.current_balance AS max_balance,
    a.current_balance AS avg_balance,
    CASE WHEN t.txn_type = 'CREDIT' THEN 1 ELSE 0 END AS credit_count,
    CASE WHEN t.txn_type = 'DEBIT' THEN 1 ELSE 0 END AS debit_count,
    CASE WHEN t.txn_type = 'CREDIT' THEN t.amount ELSE 0 END AS total_credits,
    CASE WHEN t.txn_type = 'DEBIT' THEN t.amount ELSE 0 END AS total_debits,
    CASE WHEN t.txn_type = 'CREDIT' THEN t.amount ELSE -t.amount END AS net_flow,
    CASE WHEN t.txn_type = 'CREDIT' THEN t.amount ELSE -t.amount END AS balance_change,
    'CORE_BANKING' AS source_system
FROM cbs.transactions t
JOIN dw.dim_account a ON t.account_id = a.account_id
JOIN dw.dim_customer c ON a.customer_id = c.customer_id AND c.is_current = TRUE
JOIN dw.dim_branch b ON a.branch_code = b.branch_code;

COMMENT ON TABLE dw.fact_account_balance IS 'Daily account balance snapshot - semi-additive fact';
