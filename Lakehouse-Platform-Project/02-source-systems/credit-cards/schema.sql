-- Credit Cards System Schema (MySQL - Mainframe Emulation)

CREATE TABLE credit_cards (
    card_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    card_number VARCHAR(16) NOT NULL,  -- Masked: ****-****-****-1234
    card_type VARCHAR(20) NOT NULL,    -- VISA, MASTERCARD, AMEX
    credit_limit DECIMAL(15,2) NOT NULL,
    outstanding DECIMAL(15,2) DEFAULT 0.00,
    available_credit DECIMAL(15,2) GENERATED ALWAYS AS (credit_limit - outstanding),
    card_status VARCHAR(20) DEFAULT 'ACTIVE',
    issue_date DATE,
    expiry_date DATE,
    billing_cycle INT DEFAULT 1,       -- 1st, 15th of month
    reward_points INT DEFAULT 0,
    annual_fee DECIMAL(10,2) DEFAULT 0.00,
    last_transaction_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE card_transactions (
    transaction_id VARCHAR(30) PRIMARY KEY,
    card_id VARCHAR(20) NOT NULL,
    transaction_amount DECIMAL(15,2) NOT NULL,
    transaction_type VARCHAR(20),     -- PURCHASE, PAYMENT, FEE, INTEREST
    merchant_name VARCHAR(100),
    merchant_category VARCHAR(50),
    transaction_date TIMESTAMP,
    posting_date DATE,
    description VARCHAR(200),
    status VARCHAR(20) DEFAULT 'POSTED',  -- PENDING, POSTED, DECLINED
    FOREIGN KEY (card_id) REFERENCES credit_cards(card_id)
);

CREATE TABLE card_billing (
    billing_id VARCHAR(20) PRIMARY KEY,
    card_id VARCHAR(20) NOT NULL,
    billing_month DATE NOT NULL,      -- First day of billing month
    opening_balance DECIMAL(15,2),
    total_purchases DECIMAL(15,2),
    total_payments DECIMAL(15,2),
    total_fees DECIMAL(15,2),
    total_interest DECIMAL(15,2),
    closing_balance DECIMAL(15,2),
    minimum_payment DECIMAL(15,2),
    due_date DATE,
    payment_status VARCHAR(20) DEFAULT 'UNPAID',  -- UNPAID, PAID, PARTIAL
    FOREIGN KEY (card_id) REFERENCES credit_cards(card_id)
);

-- Indexes
CREATE INDEX idx_cards_customer ON credit_cards(customer_id);
CREATE INDEX idx_transactions_card ON card_transactions(card_id);
CREATE INDEX idx_transactions_date ON card_transactions(transaction_date);
CREATE INDEX idx_billing_card ON card_billing(card_id);