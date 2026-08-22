-- Loans System Schema (PostgreSQL - Loan Management)

CREATE TABLE loan_accounts (
    loan_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    loan_type VARCHAR(30) NOT NULL,      -- HOME, PERSONAL, VEHICLE, EDUCATION
    loan_amount DECIMAL(15,2) NOT NULL,
    principal_outstanding DECIMAL(15,2) NOT NULL,
    interest_rate DECIMAL(5,2) NOT NULL,  -- Annual percentage
    tenure_months INT NOT NULL,
    emi_amount DECIMAL(15,2) NOT NULL,
    disbursement_date DATE,
    first_emi_date DATE,
    maturity_date DATE,
    loan_status VARCHAR(20) DEFAULT 'ACTIVE',  -- ACTIVE, CLOSED, NPA, WRITTEN_OFF
    npa_classification VARCHAR(20),           -- STANDARD, SUB_STANDARD, DOUBTFUL, LOSS
    days_past_due INT DEFAULT 0,
    last_payment_date DATE,
    property_address TEXT,                     -- For home/vehicle loans
    collateral_value DECIMAL(15,2),
    insurance_expiry DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE loan_payments (
    payment_id VARCHAR(30) PRIMARY KEY,
    loan_id VARCHAR(20) NOT NULL,
    payment_date DATE NOT NULL,
    emi_number INT NOT NULL,
    principal_component DECIMAL(15,2),
    interest_component DECIMAL(15,2),
    total_payment DECIMAL(15,2),
    payment_mode VARCHAR(20),          -- EMI_AUTO, NEFT, UPI, CHEQUE
    status VARCHAR(20) DEFAULT 'SUCCESS',
    late_fee DECIMAL(10,2) DEFAULT 0.00,
    FOREIGN KEY (loan_id) REFERENCES loan_accounts(loan_id)
);

CREATE TABLE loan_overdue (
    overdue_id VARCHAR(20) PRIMARY KEY,
    loan_id VARCHAR(20) NOT NULL,
    overdue_date DATE,
    overdue_amount DECIMAL(15,2),
    overdue_days INT,
    collection_agent VARCHAR(50),
    collection_status VARCHAR(20),     -- PENDING, CONTACTED, RECOVERY_PLAN, LEGAL
    remarks TEXT,
    FOREIGN KEY (loan_id) REFERENCES loan_accounts(loan_id)
);

-- Indexes
CREATE INDEX idx_loans_customer ON loan_accounts(customer_id);
CREATE INDEX idx_loans_status ON loan_accounts(loan_status);
CREATE INDEX idx_loans_npa ON loan_accounts(npa_classification);
CREATE INDEX idx_payments_loan ON loan_payments(loan_id);
CREATE INDEX idx_payments_date ON loan_payments(payment_date);
CREATE INDEX idx_overdue_loan ON loan_overdue(loan_id);