-- =============================================================================
-- DIMENSION TABLE: dim_account
-- =============================================================================
-- Type: Slowly Changing Dimension (SCD) Type 1 (overwrite)
-- Purpose: Store account information
-- =============================================================================

CREATE TABLE dw.dim_account (
    account_sk          SERIAL PRIMARY KEY,
    account_id          VARCHAR(20) NOT NULL,
    customer_id         VARCHAR(20),
    account_type        VARCHAR(20),
    account_type_group  VARCHAR(20),  -- DEPOSIT, CURRENT, LOAN
    currency            VARCHAR(3),
    opening_date        DATE,
    status              VARCHAR(20),
    branch_code         VARCHAR(10),
    interest_rate       DECIMAL(5,2),
    is_active           BOOLEAN DEFAULT TRUE,
    source_system       VARCHAR(50),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_dim_account_id ON dw.dim_account(account_id);
CREATE INDEX idx_dim_account_type ON dw.dim_account(account_type);
CREATE INDEX idx_dim_account_status ON dw.dim_account(status);

-- Sample Data
INSERT INTO dw.dim_account (
    account_id, customer_id, account_type, account_type_group, currency,
    opening_date, status, branch_code, interest_rate, is_active, source_system
)
SELECT
    a.account_id,
    a.customer_id,
    a.account_type,
    CASE
        WHEN a.account_type IN ('SAVINGS', 'FIXED_DEPOSIT', 'RECURRING_DEPOSIT') THEN 'DEPOSIT'
        WHEN a.account_type = 'CURRENT' THEN 'CURRENT'
        ELSE 'OTHER'
    END AS account_type_group,
    a.currency,
    a.opening_date,
    a.status,
    a.branch_code,
    a.interest_rate,
    CASE WHEN a.status = 'ACTIVE' THEN TRUE ELSE FALSE END AS is_active,
    'CORE_BANKING'
FROM cbs.accounts a;

COMMENT ON TABLE dw.dim_account IS 'Account dimension with SCD Type 1 (overwrite)';
