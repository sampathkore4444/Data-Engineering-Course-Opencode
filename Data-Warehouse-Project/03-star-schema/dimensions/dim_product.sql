-- =============================================================================
-- DIMENSION TABLE: dim_product
-- =============================================================================
-- Type: Static Dimension
-- Purpose: Store banking product information
-- =============================================================================

CREATE TABLE dw.dim_product (
    product_sk          SERIAL PRIMARY KEY,
    product_code        VARCHAR(20) NOT NULL,
    product_name        VARCHAR(100),
    product_category    VARCHAR(50),  -- DEPOSIT, LOAN, CARD, INVESTMENT
    product_subcategory VARCHAR(50),
    interest_rate_min   DECIMAL(5,2),
    interest_rate_max   DECIMAL(5,2),
    min_balance         DECIMAL(18,2),
    is_active           BOOLEAN DEFAULT TRUE,
    source_system       VARCHAR(50),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_dim_product_code ON dw.dim_product(product_code);
CREATE INDEX idx_dim_product_category ON dw.dim_product(product_category);

-- Sample Data
INSERT INTO dw.dim_product (product_code, product_name, product_category, product_subcategory, interest_rate_min, interest_rate_max, min_balance, source_system)
VALUES
('SAV-001', 'Regular Savings', 'DEPOSIT', 'SAVINGS', 3.0, 4.0, 100000, 'CORE_BANKING'),
('SAV-002', 'High Yield Savings', 'DEPOSIT', 'SAVINGS', 5.0, 7.0, 10000000, 'CORE_BANKING'),
('CUR-001', 'Current Account', 'DEPOSIT', 'CURRENT', 0.0, 1.0, 0, 'CORE_BANKING'),
('FD-001', 'Fixed Deposit 6M', 'DEPOSIT', 'FIXED_DEPOSIT', 6.0, 7.0, 1000000, 'CORE_BANKING'),
('FD-002', 'Fixed Deposit 12M', 'DEPOSIT', 'FIXED_DEPOSIT', 7.0, 8.0, 1000000, 'CORE_BANKING'),
('HL-001', 'Home Loan', 'LOAN', 'HOME_LOAN', 8.0, 10.0, 0, 'LOANS'),
('PL-001', 'Personal Loan', 'LOAN', 'PERSONAL_LOAN', 10.0, 15.0, 0, 'LOANS'),
('CL-001', 'Car Loan', 'LOAN', 'CAR_LOAN', 7.0, 9.0, 0, 'LOANS'),
('BL-001', 'Business Loan', 'LOAN', 'BUSINESS_LOAN', 9.0, 12.0, 0, 'LOANS'),
('CC-001', 'Visa Credit Card', 'CARD', 'CREDIT_CARD', 0, 0, 0, 'CARDS'),
('CC-002', 'Mastercard Credit Card', 'CARD', 'CREDIT_CARD', 0, 0, 0, 'CARDS');

COMMENT ON TABLE dw.dim_product IS 'Banking product dimension';
