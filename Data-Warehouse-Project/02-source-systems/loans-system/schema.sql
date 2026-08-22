-- =============================================================================
-- LOANS SOURCE SYSTEM (OLTP)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS loans;

-- Loan Accounts
CREATE TABLE loans.loan_accounts (
    loan_id                 VARCHAR(20) PRIMARY KEY,
    customer_id             VARCHAR(20) NOT NULL,
    loan_type               VARCHAR(30) NOT NULL,  -- HOME_LOAN, PERSONAL_LOAN, CAR_LOAN, BUSINESS_LOAN
    principal_amount        DECIMAL(18,2) NOT NULL,
    principal_outstanding   DECIMAL(18,2) NOT NULL,
    interest_rate           DECIMAL(5,2) NOT NULL,
    tenure_months           INT NOT NULL,
    emi_amount              DECIMAL(18,2) NOT NULL,
    disbursement_date       DATE NOT NULL,
    maturity_date           DATE NOT NULL,
    status                  VARCHAR(20) DEFAULT 'ACTIVE',
    last_updated            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Loan Payments
CREATE TABLE loans.loan_payments (
    payment_id          BIGSERIAL PRIMARY KEY,
    loan_id             VARCHAR(20) NOT NULL REFERENCES loans.loan_accounts(loan_id),
    payment_date        DATE NOT NULL,
    amount              DECIMAL(18,2) NOT NULL,
    payment_mode        VARCHAR(20),  -- AUTO_DEBIT, BANK_TRANSFER, CASH
    status              VARCHAR(20) DEFAULT 'SUCCESS',
    reference_number    VARCHAR(50),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_loans_customer ON loans.loan_accounts(customer_id);
CREATE INDEX idx_loans_type ON loans.loan_accounts(loan_type);
CREATE INDEX idx_payments_loan ON loans.loan_payments(loan_id);
CREATE INDEX idx_payments_date ON loans.loan_payments(payment_date);

-- Sample Data
INSERT INTO loans.loan_accounts (loan_id, customer_id, loan_type, principal_amount, principal_outstanding, interest_rate, tenure_months, emi_amount, disbursement_date, maturity_date, status)
VALUES
('HL-001', 'CUST-001', 'HOME_LOAN', 2000000000, 1500000000, 9.5, 240, 16800000, '2020-01-01', '2040-01-01', 'ACTIVE'),
('PL-001', 'CUST-002', 'PERSONAL_LOAN', 100000000, 50000000, 12.0, 60, 2224000, '2022-06-01', '2027-06-01', 'ACTIVE'),
('CL-001', 'CUST-003', 'CAR_LOAN', 500000000, 300000000, 8.5, 84, 6500000, '2021-03-01', '2028-03-01', 'ACTIVE'),
('BL-001', 'CUST-004', 'BUSINESS_LOAN', 1000000000, 800000000, 10.0, 120, 13200000, '2020-09-01', '2030-09-01', 'ACTIVE'),
('PL-002', 'CUST-005', 'PERSONAL_LOAN', 200000000, 150000000, 11.5, 48, 5000000, '2023-01-01', '2027-01-01', 'ACTIVE'),
('HL-002', 'CUST-006', 'HOME_LOAN', 5000000000, 4000000000, 8.5, 360, 38500000, '2018-01-01', '2048-01-01', 'ACTIVE');

INSERT INTO loans.loan_payments (loan_id, payment_date, amount, payment_mode, status, reference_number)
VALUES
('HL-001', '2024-01-05', 16800000, 'AUTO_DEBIT', 'SUCCESS', 'PAY-001'),
('HL-001', '2024-02-05', 16800000, 'AUTO_DEBIT', 'SUCCESS', 'PAY-002'),
('PL-001', '2024-01-10', 2224000, 'BANK_TRANSFER', 'SUCCESS', 'PAY-003'),
('CL-001', '2024-01-15', 6500000, 'AUTO_DEBIT', 'SUCCESS', 'PAY-004'),
('BL-001', '2024-01-20', 13200000, 'BANK_TRANSFER', 'SUCCESS', 'PAY-005'),
('PL-002', '2024-01-25', 5000000, 'AUTO_DEBIT', 'SUCCESS', 'PAY-006'),
('HL-002', '2024-01-05', 38500000, 'AUTO_DEBIT', 'SUCCESS', 'PAY-007');
