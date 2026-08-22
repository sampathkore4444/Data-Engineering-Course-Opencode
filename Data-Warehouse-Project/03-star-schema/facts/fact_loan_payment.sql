-- =============================================================================
-- FACT TABLE: fact_loan_payment
-- =============================================================================
-- Type: Transaction Fact (one row per payment)
-- Purpose: Track loan payments for NPA analysis and risk reporting
-- Grain: One row per loan payment
-- =============================================================================

CREATE TABLE dw.fact_loan_payment (
    -- Surrogate Key
    payment_sk          SERIAL PRIMARY KEY,

    -- Foreign Keys
    date_key            INT NOT NULL REFERENCES dw.dim_date(date_key),
    customer_sk         INT NOT NULL REFERENCES dw.dim_customer(customer_sk),
    branch_sk           INT NOT NULL REFERENCES dw.dim_branch(branch_sk),

    -- Degenerate Dimensions
    loan_id             VARCHAR(20),
    loan_type           VARCHAR(30),
    payment_id          BIGINT,

    -- Measures
    payment_amount      DECIMAL(18,2),
    principal_amount    DECIMAL(18,2),
    interest_amount     DECIMAL(18,2),

    -- Loan Details
    principal_outstanding   DECIMAL(18,2),
    interest_rate           DECIMAL(5,2),
    emi_amount              DECIMAL(18,2),
    tenure_months           INT,
    months_completed        INT,
    months_remaining        INT,

    -- Derived Measures
    payment_success     BOOLEAN,
    days_past_due       INT DEFAULT 0,
    is_npa              BOOLEAN DEFAULT FALSE,  -- Non-Performing Asset

    -- Audit
    source_system       VARCHAR(50),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_fact_loan_date ON dw.fact_loan_payment(date_key);
CREATE INDEX idx_fact_loan_customer ON dw.fact_loan_payment(customer_sk);
CREATE INDEX idx_fact_loan_type ON dw.fact_loan_payment(loan_type);
CREATE INDEX idx_fact_loan_npa ON dw.fact_loan_payment(is_npa);

-- =============================================================================
-- SAMPLE DATA
-- =============================================================================
INSERT INTO dw.fact_loan_payment (
    date_key, customer_sk, branch_sk,
    loan_id, loan_type, payment_id,
    payment_amount, principal_amount, interest_amount,
    principal_outstanding, interest_rate, emi_amount, tenure_months,
    months_completed, months_remaining,
    payment_success, days_past_due, is_npa, source_system
)
SELECT
    TO_CHAR(p.payment_date, 'YYYYMMDD')::INT AS date_key,
    c.customer_sk,
    b.branch_sk,
    l.loan_id,
    l.loan_type,
    p.payment_id,
    p.amount AS payment_amount,
    l.principal_outstanding / l.tenure_months AS principal_amount,
    p.amount - (l.principal_outstanding / l.tenure_months) AS interest_amount,
    l.principal_outstanding,
    l.interest_rate,
    l.emi_amount,
    l.tenure_months,
    1 AS months_completed,
    l.tenure_months - 1 AS months_remaining,
    CASE WHEN p.status = 'SUCCESS' THEN TRUE ELSE FALSE END AS payment_success,
    0 AS days_past_due,
    FALSE AS is_npa,
    'LOANS' AS source_system
FROM loans.loan_payments p
JOIN loans.loan_accounts l ON p.loan_id = l.loan_id
JOIN dw.dim_customer c ON l.customer_id = c.customer_id AND c.is_current = TRUE
JOIN dw.dim_branch b ON 'BR001' = b.branch_code;  -- Simplified branch mapping

COMMENT ON TABLE dw.fact_loan_payment IS 'Loan payment fact table for NPA analysis';
