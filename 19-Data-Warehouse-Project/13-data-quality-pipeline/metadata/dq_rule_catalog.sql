-- Data Quality Rule Catalog
-- Banking Data Warehouse

-- Create DQ schema
CREATE SCHEMA IF NOT EXISTS dq;

-- =====================================================
-- RULE CATALOG TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS dq.rule_catalog (
    rule_id SERIAL PRIMARY KEY,
    rule_name VARCHAR(200) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,  -- uniqueness, completeness, range, freshness, etc.
    table_name VARCHAR(200) NOT NULL,
    column_name VARCHAR(200),
    description TEXT,
    owner VARCHAR(100),
    priority VARCHAR(20),  -- CRITICAL, HIGH, MEDIUM, LOW
    severity VARCHAR(20),  -- CRITICAL, HIGH, MEDIUM, LOW
    action VARCHAR(20),    -- BLOCK, ALERT, LOG
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT current_user,
    
    -- Rule configuration (JSON)
    rule_config JSONB,
    
    -- Constraints
    UNIQUE(rule_name, table_name)
);

-- =====================================================
-- INSERT SAMPLE RULES
-- =====================================================
INSERT INTO dq.rule_catalog (rule_name, rule_type, table_name, column_name, description, owner, priority, severity, action, rule_config)
VALUES
-- Staging Rules
('customer_id_uniqueness', 'uniqueness', 'staging.stg_customers', 'customer_id', 'Customer ID must be unique', 'data-engineering', 'CRITICAL', 'CRITICAL', 'BLOCK', '{"threshold": 100}'),
('customer_id_not_null', 'completeness', 'staging.stg_customers', 'customer_id', 'Customer ID cannot be null', 'data-engineering', 'CRITICAL', 'CRITICAL', 'BLOCK', '{"threshold": 100}'),
('email_not_null', 'completeness', 'staging.stg_customers', 'email', 'Email should not be null', 'data-engineering', 'HIGH', 'HIGH', 'ALERT', '{"threshold": 95}'),
('email_format', 'pattern', 'staging.stg_customers', 'email', 'Email must be valid format', 'data-engineering', 'MEDIUM', 'MEDIUM', 'ALERT', '{"pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", "threshold": 98}'),
('customer_type_valid', 'accepted_values', 'staging.stg_customers', 'customer_type', 'Customer type must be valid', 'data-engineering', 'HIGH', 'HIGH', 'BLOCK', '{"values": ["INDIVIDUAL", "CORPORATE"], "threshold": 100}'),
('customers_freshness', 'freshness', 'staging.stg_customers', 'updated_at', 'Customer data must be fresh', 'data-engineering', 'HIGH', 'HIGH', 'ALERT', '{"max_age_hours": 24}'),

-- Account Rules
('account_id_uniqueness', 'uniqueness', 'staging.stg_accounts', 'account_id', 'Account ID must be unique', 'data-engineering', 'CRITICAL', 'CRITICAL', 'BLOCK', '{"threshold": 100}'),
('account_id_not_null', 'completeness', 'staging.stg_accounts', 'account_id', 'Account ID cannot be null', 'data-engineering', 'CRITICAL', 'CRITICAL', 'BLOCK', '{"threshold": 100}'),
('balance_not_null', 'completeness', 'staging.stg_accounts', 'balance', 'Balance cannot be null', 'data-engineering', 'CRITICAL', 'CRITICAL', 'BLOCK', '{"threshold": 100}'),
('balance_positive', 'range', 'staging.stg_accounts', 'balance', 'Balance must be positive', 'data-engineering', 'CRITICAL', 'CRITICAL', 'BLOCK', '{"min_value": 0, "max_value": 999999999999, "threshold": 100}'),
('accounts_freshness', 'freshness', 'staging.stg_accounts', 'updated_at', 'Account data must be fresh', 'data-engineering', 'HIGH', 'HIGH', 'ALERT', '{"max_age_hours": 24}'),

-- Transaction Rules
('transaction_id_uniqueness', 'uniqueness', 'staging.stg_transactions', 'transaction_id', 'Transaction ID must be unique', 'data-engineering', 'CRITICAL', 'CRITICAL', 'BLOCK', '{"threshold": 100}'),
('amount_not_null', 'completeness', 'staging.stg_transactions', 'amount', 'Amount cannot be null', 'data-engineering', 'CRITICAL', 'CRITICAL', 'BLOCK', '{"threshold": 100}'),
('amount_positive', 'range', 'staging.stg_transactions', 'amount', 'Amount must be positive', 'data-engineering', 'CRITICAL', 'CRITICAL', 'BLOCK', '{"min_value": 0.01, "max_value": 999999999999, "threshold": 100}'),
('transaction_type_valid', 'accepted_values', 'staging.stg_transactions', 'transaction_type', 'Transaction type must be valid', 'data-engineering', 'CRITICAL', 'CRITICAL', 'BLOCK', '{"values": ["DEBIT", "CREDIT", "TRANSFER"], "threshold": 100}'),
('transactions_freshness', 'freshness', 'staging.stg_transactions', 'created_at', 'Transaction data must be fresh', 'data-engineering', 'CRITICAL', 'CRITICAL', 'ALERT', '{"max_age_hours": 1}'),

-- Gold Table Rules
('dim_customer_sk_uniqueness', 'uniqueness', 'gold.dim_customer', 'customer_sk', 'Customer SK must be unique', 'data-analytics', 'CRITICAL', 'CRITICAL', 'BLOCK', '{"threshold": 100}'),
('dim_customer_segment_valid', 'accepted_values', 'gold.dim_customer', 'customer_segment', 'Customer segment must be valid', 'data-analytics', 'HIGH', 'HIGH', 'BLOCK', '{"values": ["PLATINUM", "GOLD", "SILVER", "STANDARD"], "threshold": 100}'),
('fact_txn_amount_anomaly', 'statistical', 'gold.fact_transactions', 'amount', 'Amount anomaly detection', 'data-analytics', 'MEDIUM', 'MEDIUM', 'ALERT', '{"method": "zscore", "threshold": 3}');

-- =====================================================
-- CREATE INDEXES
-- =====================================================
CREATE INDEX idx_rule_catalog_table ON dq.rule_catalog(table_name);
CREATE INDEX idx_rule_catalog_type ON dq.rule_catalog(rule_type);
CREATE INDEX idx_rule_catalog_severity ON dq.rule_catalog(severity);
CREATE INDEX idx_rule_catalog_enabled ON dq.rule_catalog(enabled);

-- =====================================================
-- CREATE VIEW FOR ACTIVE RULES
-- =====================================================
CREATE OR REPLACE VIEW dq.vw_active_rules AS
SELECT 
    rule_id,
    rule_name,
    rule_type,
    table_name,
    column_name,
    description,
    owner,
    priority,
    severity,
    action,
    rule_config,
    created_at
FROM dq.rule_catalog
WHERE enabled = TRUE
ORDER BY 
    CASE priority 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        WHEN 'LOW' THEN 4 
    END;

COMMENT ON TABLE dq.rule_catalog IS 'Catalog of all data quality rules';
COMMENT ON COLUMN dq.rule_catalog.rule_config IS 'JSON configuration for the rule';
