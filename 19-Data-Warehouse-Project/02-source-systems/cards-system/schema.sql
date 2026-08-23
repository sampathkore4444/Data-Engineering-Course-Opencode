-- =============================================================================
-- CREDIT CARDS SOURCE SYSTEM (OLTP)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS cards;

-- Credit Cards
CREATE TABLE cards.credit_cards (
    card_number         VARCHAR(20) PRIMARY KEY,
    customer_id         VARCHAR(20) NOT NULL,
    card_type           VARCHAR(20) NOT NULL,  -- VISA, MASTERCARD, AMEX, RUPAY
    card_limit          DECIMAL(18,2) NOT NULL,
    credit_used         DECIMAL(18,2) DEFAULT 0,
    available_credit    DECIMAL(18,2) GENERATED ALWAYS AS (card_limit - credit_used) STORED,
    issuance_date       DATE NOT NULL,
    expiry_date         DATE NOT NULL,
    status              VARCHAR(20) DEFAULT 'ACTIVE',
    last_updated        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Card Transactions
CREATE TABLE cards.card_transactions (
    txn_id              BIGSERIAL PRIMARY KEY,
    card_number         VARCHAR(20) NOT NULL REFERENCES cards.credit_cards(card_number),
    merchant_id         VARCHAR(20),
    merchant_name       VARCHAR(100),
    merchant_category   VARCHAR(50),
    amount              DECIMAL(18,2) NOT NULL,
    currency            VARCHAR(3) DEFAULT 'VND',
    txn_date            DATE NOT NULL,
    txn_timestamp       TIMESTAMP NOT NULL,
    txn_type            VARCHAR(20) DEFAULT 'PURCHASE',
    status              VARCHAR(20) DEFAULT 'SUCCESS',
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_cards_customer ON cards.credit_cards(customer_id);
CREATE INDEX idx_card_txns_card ON cards.card_transactions(card_number);
CREATE INDEX idx_card_txns_date ON cards.card_transactions(txn_date);

-- Sample Data
INSERT INTO cards.credit_cards (card_number, customer_id, card_type, card_limit, credit_used, issuance_date, expiry_date, status)
VALUES
('4532015112830366', 'CUST-001', 'VISA', 50000000, 30000000, '2022-01-01', '2027-01-01', 'ACTIVE'),
('5425233430109903', 'CUST-001', 'MASTERCARD', 30000000, 7500000, '2023-06-15', '2028-06-15', 'ACTIVE'),
('4916338506082832', 'CUST-002', 'VISA', 80000000, 45000000, '2021-03-10', '2026-03-10', 'ACTIVE'),
('5123456789012346', 'CUST-003', 'MASTERCARD', 40000000, 20000000, '2022-09-20', '2027-09-20', 'ACTIVE'),
('4024007103939509', 'CUST-004', 'VISA', 60000000, 35000000, '2023-01-05', '2028-01-05', 'ACTIVE'),
('5555666677778884', 'CUST-005', 'MASTERCARD', 25000000, 12500000, '2022-05-15', '2027-05-15', 'ACTIVE');

INSERT INTO cards.card_transactions (card_number, merchant_id, merchant_name, merchant_category, amount, txn_date, txn_timestamp, txn_type, status)
VALUES
('4532015112830366', 'M001', 'Coffee Shop', 'FOOD_AND_DRINK', 150000, '2024-01-15', '2024-01-15 08:30:00', 'PURCHASE', 'SUCCESS'),
('4532015112830366', 'M002', 'Electronics Store', 'ELECTRONICS', 5000000, '2024-01-15', '2024-01-15 14:15:00', 'PURCHASE', 'SUCCESS'),
('5425233430109903', 'M003', 'Restaurant', 'FOOD_AND_DRINK', 500000, '2024-01-15', '2024-01-15 19:45:00', 'PURCHASE', 'SUCCESS'),
('4916338506082832', 'M004', 'Online Shopping', 'ONLINE', 2500000, '2024-01-15', '2024-01-15 11:20:00', 'PURCHASE', 'SUCCESS'),
('5123456789012346', 'M005', 'Gas Station', 'TRANSPORTATION', 800000, '2024-01-15', '2024-01-15 16:30:00', 'PURCHASE', 'SUCCESS'),
('4024007103939509', 'M006', 'Supermarket', 'GROCERY', 1200000, '2024-01-15', '2024-01-15 10:00:00', 'PURCHASE', 'SUCCESS'),
('5555666677778884', 'M007', 'Clothing Store', 'SHOPPING', 3000000, '2024-01-15', '2024-01-15 15:45:00', 'PURCHASE', 'SUCCESS');
