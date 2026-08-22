-- =============================================================================
-- CORE BANKING SOURCE SYSTEM (OLTP)
-- =============================================================================
-- Purpose: Transactional database for core banking operations
-- Type:    OLTP (Online Transaction Processing)
-- =============================================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS cbs;

-- =============================================================================
-- CUSTOMERS TABLE
-- =============================================================================
CREATE TABLE cbs.customers (
    customer_id         VARCHAR(20) PRIMARY KEY,
    customer_name       VARCHAR(100) NOT NULL,
    date_of_birth       DATE,
    gender              VARCHAR(10),
    nationality         VARCHAR(50),
    pan_number          VARCHAR(20),
    email               VARCHAR(100),
    phone               VARCHAR(20),
    address_line1       VARCHAR(200),
    address_line2       VARCHAR(200),
    city                VARCHAR(50),
    state               VARCHAR(50),
    pin_code            VARCHAR(10),
    customer_type       VARCHAR(20) DEFAULT 'INDIVIDUAL',  -- INDIVIDUAL, CORPORATE
    kyc_status          VARCHAR(20) DEFAULT 'VERIFIED',    -- VERIFIED, PENDING, REJECTED
    account_open_date   DATE DEFAULT CURRENT_DATE,
    last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- ACCOUNTS TABLE
-- =============================================================================
CREATE TABLE cbs.accounts (
    account_id          VARCHAR(20) PRIMARY KEY,
    customer_id         VARCHAR(20) NOT NULL REFERENCES cbs.customers(customer_id),
    account_type        VARCHAR(20) NOT NULL,  -- SAVINGS, CURRENT, FIXED_DEPOSIT
    currency            VARCHAR(3) DEFAULT 'VND',
    opening_date        DATE NOT NULL,
    current_balance     DECIMAL(18,2) DEFAULT 0,
    available_balance   DECIMAL(18,2) DEFAULT 0,
    status              VARCHAR(20) DEFAULT 'ACTIVE',  -- ACTIVE, CLOSED, DORMANT, FROZEN
    branch_code         VARCHAR(10) NOT NULL,
    interest_rate       DECIMAL(5,2) DEFAULT 0,
    last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- TRANSACTIONS TABLE
-- =============================================================================
CREATE TABLE cbs.transactions (
    txn_id              BIGSERIAL PRIMARY KEY,
    account_id          VARCHAR(20) NOT NULL REFERENCES cbs.accounts(account_id),
    txn_type            VARCHAR(20) NOT NULL,  -- CREDIT, DEBIT, TRANSFER
    amount              DECIMAL(18,2) NOT NULL,
    currency            VARCHAR(3) DEFAULT 'VND',
    txn_date            DATE NOT NULL,
    txn_timestamp       TIMESTAMP NOT NULL,
    description         VARCHAR(200),
    reference_number    VARCHAR(50),
    channel             VARCHAR(20),  -- ATM, MOBILE, ONLINE, BRANCH
    status              VARCHAR(20) DEFAULT 'SUCCESS',  -- SUCCESS, FAILED, PENDING
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- INDEXES (for OLTP performance)
-- =============================================================================
CREATE INDEX idx_customers_city ON cbs.customers(city);
CREATE INDEX idx_customers_type ON cbs.customers(customer_type);
CREATE INDEX idx_accounts_customer ON cbs.accounts(customer_id);
CREATE INDEX idx_accounts_branch ON cbs.accounts(branch_code);
CREATE INDEX idx_accounts_type ON cbs.accounts(account_type);
CREATE INDEX idx_transactions_account ON cbs.transactions(account_id);
CREATE INDEX idx_transactions_date ON cbs.transactions(txn_date);
CREATE INDEX idx_transactions_type ON cbs.transactions(txn_type);

-- =============================================================================
-- SAMPLE DATA
-- =============================================================================

-- Insert customers
INSERT INTO cbs.customers (customer_id, customer_name, date_of_birth, gender, nationality, pan_number, email, phone, city, state, pin_code, customer_type)
VALUES
('CUST-001', 'Nguyen Van A', '1985-01-15', 'MALE', 'VIETNAMESE', 'PAN001', 'nguyena@email.com', '0901234567', 'Ho Chi Minh', 'HCM', '700000', 'INDIVIDUAL'),
('CUST-002', 'Tran Thi B', '1990-05-20', 'FEMALE', 'VIETNAMESE', 'PAN002', 'tranb@email.com', '0912345678', 'Ha Noi', 'HN', '100000', 'INDIVIDUAL'),
('CUST-003', 'Le Minh C', '1988-03-10', 'MALE', 'VIETNAMESE', 'PAN003', 'lec@email.com', '0923456789', 'Da Nang', 'DN', '550000', 'INDIVIDUAL'),
('CUST-004', 'Pham Hong D', '1992-07-25', 'FEMALE', 'VIETNAMESE', 'PAN004', 'phamd@email.com', '0934567890', 'Can Tho', 'CT', '900000', 'INDIVIDUAL'),
('CUST-005', 'Hoang Van E', '1987-11-30', 'MALE', 'VIETNAMESE', 'PAN005', 'hoangne@email.com', '0945678901', 'Hai Phong', 'HP', '610000', 'INDIVIDUAL'),
('CUST-006', 'Vietcombank Corp', '2010-01-01', NULL, 'VIETNAMESE', 'PAN006', 'corp@vietcombank.vn', '02812345678', 'Ho Chi Minh', 'HCM', '700000', 'CORPORATE'),
('CUST-007', 'Samsung VN', '2015-06-15', NULL, 'KOREAN', 'PAN007', 'info@samsung.vn', '02898765432', 'Ho Chi Minh', 'HCM', '700000', 'CORPORATE'),
('CUST-008', 'Doan Thi F', '1995-09-12', 'FEMALE', 'VIETNAMESE', 'PAN008', 'doanf@email.com', '0956789012', 'Hue', 'TH', '530000', 'INDIVIDUAL'),
('CUST-009', 'Bui Van G', '1980-02-28', 'MALE', 'VIETNAMESE', 'PAN009', 'buievg@email.com', '0967890123', 'Nha Trang', 'KH', '650000', 'INDIVIDUAL'),
('CUST-010', 'Nguyen Thi H', '1998-12-05', 'FEMALE', 'VIETNAMESE', 'PAN010', 'nguyenh@email.com', '0978901234', 'Vung Tau', 'BR', '790000', 'INDIVIDUAL');

-- Insert accounts
INSERT INTO cbs.accounts (account_id, customer_id, account_type, opening_date, current_balance, available_balance, status, branch_code, interest_rate)
VALUES
('ACC-001', 'CUST-001', 'SAVINGS', '2020-01-01', 150000000, 145000000, 'ACTIVE', 'BR001', 3.5),
('ACC-002', 'CUST-001', 'CURRENT', '2020-01-01', 25000000, 25000000, 'ACTIVE', 'BR001', 0.5),
('ACC-003', 'CUST-002', 'SAVINGS', '2020-02-15', 250000000, 245000000, 'ACTIVE', 'BR002', 3.5),
('ACC-004', 'CUST-003', 'SAVINGS', '2020-03-20', 75000000, 70000000, 'ACTIVE', 'BR003', 3.5),
('ACC-005', 'CUST-004', 'CURRENT', '2020-04-10', 50000000, 50000000, 'ACTIVE', 'BR004', 0.5),
('ACC-006', 'CUST-005', 'SAVINGS', '2020-05-05', 100000000, 95000000, 'ACTIVE', 'BR005', 3.5),
('ACC-007', 'CUST-006', 'CURRENT', '2010-01-01', 5000000000, 5000000000, 'ACTIVE', 'BR001', 0.5),
('ACC-008', 'CUST-007', 'CURRENT', '2015-06-15', 2000000000, 2000000000, 'ACTIVE', 'BR001', 0.5),
('ACC-009', 'CUST-008', 'SAVINGS', '2023-01-01', 30000000, 28000000, 'ACTIVE', 'BR006', 3.5),
('ACC-010', 'CUST-009', 'FIXED_DEPOSIT', '2023-06-01', 500000000, 500000000, 'ACTIVE', 'BR007', 6.5),
('ACC-011', 'CUST-010', 'SAVINGS', '2024-01-01', 10000000, 9500000, 'ACTIVE', 'BR008', 3.5),
('ACC-012', 'CUST-002', 'FIXED_DEPOSIT', '2023-01-01', 300000000, 300000000, 'ACTIVE', 'BR002', 6.5);

-- Insert transactions
INSERT INTO cbs.transactions (account_id, txn_type, amount, txn_date, txn_timestamp, description, reference_number, channel, status)
VALUES
('ACC-001', 'CREDIT', 50000000, '2024-01-15', '2024-01-15 10:30:00', 'Salary', 'REF001', 'BANK_TRANSFER', 'SUCCESS'),
('ACC-001', 'DEBIT', 2500000, '2024-01-15', '2024-01-15 14:20:00', 'Bill Payment', 'REF002', 'MOBILE', 'SUCCESS'),
('ACC-002', 'CREDIT', 10000000, '2024-01-15', '2024-01-15 09:15:00', 'Transfer', 'REF003', 'ONLINE', 'SUCCESS'),
('ACC-003', 'DEBIT', 5000000, '2024-01-15', '2024-01-15 11:45:00', 'ATM Withdrawal', 'REF004', 'ATM', 'SUCCESS'),
('ACC-004', 'CREDIT', 15000000, '2024-01-15', '2024-01-15 16:30:00', 'Deposit', 'REF005', 'BRANCH', 'SUCCESS'),
('ACC-005', 'DEBIT', 3000000, '2024-01-15', '2024-01-15 13:10:00', 'Payment', 'REF006', 'MOBILE', 'SUCCESS'),
('ACC-006', 'CREDIT', 20000000, '2024-01-15', '2024-01-15 15:25:00', 'Transfer', 'REF007', 'ONLINE', 'SUCCESS'),
('ACC-001', 'CREDIT', 50000000, '2024-02-15', '2024-02-15 10:30:00', 'Salary', 'REF008', 'BANK_TRANSFER', 'SUCCESS'),
('ACC-001', 'DEBIT', 2500000, '2024-02-15', '2024-02-15 14:20:00', 'Bill Payment', 'REF009', 'MOBILE', 'SUCCESS'),
('ACC-003', 'CREDIT', 10000000, '2024-02-15', '2024-02-15 09:15:00', 'Transfer', 'REF010', 'ONLINE', 'SUCCESS'),
('ACC-007', 'CREDIT', 500000000, '2024-01-20', '2024-01-20 09:00:00', 'Corporate Deposit', 'REF011', 'BANK_TRANSFER', 'SUCCESS'),
('ACC-007', 'DEBIT', 100000000, '2024-01-25', '2024-01-25 14:00:00', 'Vendor Payment', 'REF012', 'ONLINE', 'SUCCESS'),
('ACC-008', 'CREDIT', 200000000, '2024-02-01', '2024-02-01 10:00:00', 'Sales Receipt', 'REF013', 'BANK_TRANSFER', 'SUCCESS'),
('ACC-008', 'DEBIT', 50000000, '2024-02-05', '2024-02-05 11:00:00', 'Payroll', 'REF014', 'ONLINE', 'SUCCESS'),
('ACC-010', 'CREDIT', 500000000, '2023-06-01', '2023-06-01 10:00:00', 'FD Booking', 'REF015', 'BRANCH', 'SUCCESS');
