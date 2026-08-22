--##############################################################################
-- Core Banking System - Database Schema (Oracle-like PostgreSQL)
-- Production Banking Source System
--##############################################################################

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

--##############################################################################
-- CUSTOMERS TABLE
--##############################################################################
CREATE TABLE IF NOT EXISTS customers (
    customer_id         VARCHAR(20) PRIMARY KEY,
    customer_number     VARCHAR(15) UNIQUE NOT NULL,
    customer_type       VARCHAR(20) NOT NULL CHECK (customer_type IN ('INDIVIDUAL', 'CORPORATE', 'SME')),
    first_name          VARCHAR(100) NOT NULL,
    last_name           VARCHAR(100),
    company_name        VARCHAR(200),
    date_of_birth       DATE,
    national_id         VARCHAR(20),
    phone_number        VARCHAR(20),
    email               VARCHAR(100),
    address_line1       VARCHAR(200),
    address_line2       VARCHAR(200),
    city                VARCHAR(100),
    state               VARCHAR(100),
    country             VARCHAR(50) DEFAULT 'Vietnam',
    postal_code         VARCHAR(10),
    risk_rating         VARCHAR(10) CHECK (risk_rating IN ('LOW', 'MEDIUM', 'HIGH', 'VERY_HIGH')),
    kyc_status          VARCHAR(20) DEFAULT 'PENDING' CHECK (kyc_status IN ('PENDING', 'VERIFIED', 'EXPIRED', 'REJECTED')),
    kyc_expiry_date     DATE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active           BOOLEAN DEFAULT TRUE
);

-- Audit trigger for customers
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_customers_timestamp
    BEFORE UPDATE ON customers
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp();

--##############################################################################
-- ACCOUNTS TABLE
--##############################################################################
CREATE TABLE IF NOT EXISTS accounts (
    account_id          VARCHAR(20) PRIMARY KEY,
    account_number      VARCHAR(15) UNIQUE NOT NULL,
    customer_id         VARCHAR(20) NOT NULL REFERENCES customers(customer_id),
    account_type        VARCHAR(20) NOT NULL CHECK (account_type IN ('SAVINGS', 'CURRENT', 'FIXED_DEPOSIT', 'RECURRING')),
    account_status      VARCHAR(20) DEFAULT 'ACTIVE' CHECK (account_status IN ('ACTIVE', 'DORMANT', 'FROZEN', 'CLOSED')),
    currency            VARCHAR(3) DEFAULT 'VND',
    balance             DECIMAL(18,2) DEFAULT 0.00,
    available_balance   DECIMAL(18,2) DEFAULT 0.00,
    interest_rate       DECIMAL(5,4),
    opened_date         DATE NOT NULL,
    closed_date         DATE,
    branch_code         VARCHAR(10),
    last_transaction_at TIMESTAMP,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER update_accounts_timestamp
    BEFORE UPDATE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION update_timestamp();

--##############################################################################
-- TRANSACTIONS TABLE
--##############################################################################
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id      VARCHAR(30) PRIMARY KEY,
    account_id          VARCHAR(20) NOT NULL REFERENCES accounts(account_id),
    transaction_type    VARCHAR(20) NOT NULL CHECK (transaction_type IN ('CREDIT', 'DEBIT', 'TRANSFER', 'INTEREST', 'FEE')),
    amount              DECIMAL(18,2) NOT NULL,
    currency            VARCHAR(3) DEFAULT 'VND',
    balance_after       DECIMAL(18,2),
    description         VARCHAR(500),
    reference_number    VARCHAR(30),
    channel             VARCHAR(20) CHECK (channel IN ('BRANCH', 'ATM', 'MOBILE', 'INTERNET', 'WIRE', 'ACH')),
    merchant_id         VARCHAR(20),
    merchant_category   VARCHAR(50),
    location            VARCHAR(100),
    status              VARCHAR(20) DEFAULT 'COMPLETED' CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED', 'REVERSED')),
    fraud_flag          BOOLEAN DEFAULT FALSE,
    fraud_reason        VARCHAR(200),
    transaction_time    TIMESTAMP NOT NULL,
    value_date          DATE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Partition transactions by month for performance
CREATE TABLE IF NOT EXISTS transactions_history (
    LIKE transactions INCLUDING ALL
) PARTITION BY RANGE (transaction_time);

-- Create partitions for each month
CREATE TABLE transactions_2024_01 PARTITION OF transactions_history
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE transactions_2024_02 PARTITION OF transactions_history
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE transactions_2024_03 PARTITION OF transactions_history
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');

--##############################################################################
-- CARDS TABLE
--##############################################################################
CREATE TABLE IF NOT EXISTS cards (
    card_id             VARCHAR(20) PRIMARY KEY,
    card_number         VARCHAR(20) NOT NULL, -- Masked in production
    card_type           VARCHAR(20) NOT NULL CHECK (card_type IN ('DEBIT', 'CREDIT', 'PREPAID')),
    card_brand          VARCHAR(10) CHECK (card_brand IN ('VISA', 'MASTERCARD', 'JCB', 'UNIONPAY')),
    customer_id         VARCHAR(20) NOT NULL REFERENCES customers(customer_id),
    account_id          VARCHAR(20) REFERENCES accounts(account_id),
    card_status         VARCHAR(20) DEFAULT 'ACTIVE' CHECK (card_status IN ('ACTIVE', 'BLOCKED', 'EXPIRED', 'CANCELLED')),
    credit_limit        DECIMAL(18,2),
    outstanding_amount  DECIMAL(18,2) DEFAULT 0.00,
    available_credit    DECIMAL(18,2),
    billing_cycle_day   INT,
    expiry_date         DATE,
    issued_date         DATE NOT NULL,
    last_used_at        TIMESTAMP,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--##############################################################################
-- LOANS TABLE
--##############################################################################
CREATE TABLE IF NOT EXISTS loans (
    loan_id             VARCHAR(20) PRIMARY KEY,
    loan_number         VARCHAR(15) UNIQUE NOT NULL,
    customer_id         VARCHAR(20) NOT NULL REFERENCES customers(customer_id),
    loan_type           VARCHAR(30) NOT NULL CHECK (loan_type IN ('PERSONAL', 'HOME', 'AUTO', 'EDUCATION', 'BUSINESS', 'OVERDRAFT')),
    loan_status         VARCHAR(20) DEFAULT 'ACTIVE' CHECK (loan_status IN ('APPROVED', 'ACTIVE', 'CLOSED', 'DEFAULTED', 'WRITTEN_OFF')),
    principal_amount    DECIMAL(18,2) NOT NULL,
    principal_outstanding DECIMAL(18,2) NOT NULL,
    interest_rate       DECIMAL(5,4) NOT NULL,
    emi_amount          DECIMAL(18,2),
    tenure_months       INT NOT NULL,
    emis_paid           INT DEFAULT 0,
    disbursement_date   DATE NOT NULL,
    maturity_date       DATE,
    last_emi_date       DATE,
    collateral_type     VARCHAR(50),
    collateral_value    DECIMAL(18,2),
    npa_classification  VARCHAR(20) CHECK (npa_classification IN ('STD', 'SUB', 'DOUBTFUL', 'LOSS')),
    npa_date            DATE,
    branch_code         VARCHAR(10),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--##############################################################################
-- FIXED DEPOSITS TABLE
--##############################################################################
CREATE TABLE IF NOT EXISTS fixed_deposits (
    fd_id               VARCHAR(20) PRIMARY KEY,
    fd_number           VARCHAR(15) UNIQUE NOT NULL,
    customer_id         VARCHAR(20) NOT NULL REFERENCES customers(customer_id),
    account_id          VARCHAR(20) NOT NULL REFERENCES accounts(account_id),
    principal_amount    DECIMAL(18,2) NOT NULL,
    interest_rate       DECIMAL(5,4) NOT NULL,
    tenure_months       INT NOT NULL,
    maturity_amount     DECIMAL(18,2),
    deposit_date        DATE NOT NULL,
    maturity_date       DATE NOT NULL,
    status              VARCHAR(20) DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'MATURED', 'PREMATURELY_CLOSED')),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--##############################################################################
-- BRANCHES TABLE
--##############################################################################
CREATE TABLE IF NOT EXISTS branches (
    branch_code         VARCHAR(10) PRIMARY KEY,
    branch_name         VARCHAR(100) NOT NULL,
    branch_type         VARCHAR(20) CHECK (branch_type IN ('MAIN', 'SUB', 'EXTENSION', 'DIGITAL')),
    address             VARCHAR(200),
    city                VARCHAR(100),
    state               VARCHAR(100),
    region              VARCHAR(50),
    manager_name        VARCHAR(100),
    phone               VARCHAR(20),
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--##############################################################################
-- MERCHANTS TABLE
--##############################################################################
CREATE TABLE IF NOT EXISTS merchants (
    merchant_id         VARCHAR(20) PRIMARY KEY,
    merchant_name       VARCHAR(200) NOT NULL,
    merchant_category   VARCHAR(50),
    mcc_code            VARCHAR(5),
    business_type       VARCHAR(50),
    registered_address  VARCHAR(200),
    contact_phone       VARCHAR(20),
    contact_email       VARCHAR(100),
    bank_account_number VARCHAR(20),
    settlement_cycle    VARCHAR(20) DEFAULT 'T+1',
    risk_rating         VARCHAR(10) CHECK (risk_rating IN ('LOW', 'MEDIUM', 'HIGH')),
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--##############################################################################
-- INDEXES for Performance
--##############################################################################

-- Customers
CREATE INDEX idx_customers_type ON customers(customer_type);
CREATE INDEX idx_customers_risk ON customers(risk_rating);
CREATE INDEX idx_customers_branch ON customers(customer_id);

-- Accounts
CREATE INDEX idx_accounts_customer ON accounts(customer_id);
CREATE INDEX idx_accounts_type ON accounts(account_type);
CREATE INDEX idx_accounts_status ON accounts(account_status);
CREATE INDEX idx_accounts_branch ON accounts(branch_code);

-- Transactions (critical for performance)
CREATE INDEX idx_transactions_account ON transactions(account_id);
CREATE INDEX idx_transactions_time ON transactions(transaction_time);
CREATE INDEX idx_transactions_type ON transactions(transaction_type);
CREATE INDEX idx_transactions_fraud ON transactions(fraud_flag);
CREATE INDEX idx_transactions_merchant ON transactions(merchant_id);
CREATE INDEX idx_transactions_channel ON transactions(channel);

-- Cards
CREATE INDEX idx_cards_customer ON cards(customer_id);
CREATE INDEX idx_cards_status ON cards(card_status);

-- Loans
CREATE INDEX idx_loans_customer ON loans(customer_id);
CREATE INDEX idx_loans_type ON loans(loan_type);
CREATE INDEX idx_loans_status ON loans(loan_status);
CREATE INDEX idx_loans_npa ON loans(npa_classification);

--##############################################################################
-- VIEWS for Common Queries
--##############################################################################

-- Customer Summary View
CREATE OR REPLACE VIEW v_customer_summary AS
SELECT 
    c.customer_id,
    c.customer_number,
    c.customer_type,
    c.first_name || ' ' || COALESCE(c.last_name, '') AS customer_name,
    c.risk_rating,
    c.kyc_status,
    COUNT(DISTINCT a.account_id) AS total_accounts,
    SUM(COALESCE(a.balance, 0)) AS total_balance,
    COUNT(DISTINCT cd.card_id) AS total_cards,
    SUM(COALESCE(cd.outstanding_amount, 0)) AS total_card_outstanding,
    COUNT(DISTINCT l.loan_id) AS total_loans,
    SUM(COALESCE(l.principal_outstanding, 0)) AS total_loan_outstanding
FROM customers c
LEFT JOIN accounts a ON c.customer_id = a.customer_id
LEFT JOIN cards cd ON c.customer_id = cd.customer_id
LEFT JOIN loans l ON c.customer_id = l.customer_id
WHERE c.is_active = TRUE
GROUP BY c.customer_id, c.customer_number, c.customer_type, 
         c.first_name, c.last_name, c.risk_rating, c.kyc_status;

-- Daily Transaction Summary View
CREATE OR REPLACE VIEW v_daily_transaction_summary AS
SELECT 
    DATE(transaction_time) AS transaction_date,
    account_id,
    transaction_type,
    channel,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount,
    MAX(amount) AS max_amount,
    SUM(CASE WHEN fraud_flag = TRUE THEN 1 ELSE 0 END) AS fraud_count
FROM transactions
WHERE status = 'COMPLETED'
GROUP BY DATE(transaction_time), account_id, transaction_type, channel;

-- Branch Performance View
CREATE OR REPLACE VIEW v_branch_performance AS
SELECT 
    b.branch_code,
    b.branch_name,
    b.region,
    COUNT(DISTINCT a.account_id) AS total_accounts,
    SUM(a.balance) AS total_deposits,
    COUNT(DISTINCT l.loan_id) AS total_loans,
    SUM(l.principal_outstanding) AS total_loan_book,
    COUNT(DISTINCT CASE WHEN a.opened_date >= CURRENT_DATE - INTERVAL '30' DAY THEN a.account_id END) AS new_accounts_30d
FROM branches b
LEFT JOIN accounts a ON b.branch_code = a.branch_code
LEFT JOIN customers c ON a.customer_id = c.customer_id
LEFT JOIN loans l ON c.customer_id = l.customer_id
WHERE b.is_active = TRUE
GROUP BY b.branch_code, b.branch_name, b.region;

--##############################################################################
-- COMMENTS for Documentation
--##############################################################################

COMMENT ON TABLE customers IS 'Master table for all bank customers - Individual, Corporate, SME';
COMMENT ON TABLE accounts IS 'All deposit accounts (Savings, Current, Fixed Deposit)';
COMMENT ON TABLE transactions IS 'All financial transactions across all channels';
COMMENT ON TABLE cards IS 'Debit, Credit and Prepaid cards issued to customers';
COMMENT ON TABLE loans IS 'All loan products (Personal, Home, Auto, Business)';
COMMENT ON TABLE fixed_deposits IS 'Fixed deposit investments';
COMMENT ON TABLE branches IS 'Bank branch master data';
COMMENT ON TABLE merchants IS 'Merchant master data for card transactions';

COMMENT ON COLUMN transactions.fraud_flag IS 'TRUE if transaction flagged for potential fraud';
COMMENT ON COLUMN loans.npa_classification IS 'Non-Performing Asset classification: STD(Standard), SUB(Sub-standard), DOUBTFUL, LOSS';
COMMENT ON COLUMN customers.kyc_status IS 'Know Your Customer verification status';
