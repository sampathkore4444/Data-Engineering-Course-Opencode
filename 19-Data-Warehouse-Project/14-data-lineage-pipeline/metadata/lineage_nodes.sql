-- Data Lineage Nodes
-- Banking Data Warehouse

-- Create lineage schema
CREATE SCHEMA IF NOT EXISTS lineage;

-- =====================================================
-- LINEAGE NODES TABLE (Sources and Targets)
-- =====================================================
CREATE TABLE IF NOT EXISTS lineage.nodes (
    node_id SERIAL PRIMARY KEY,
    node_name VARCHAR(200) NOT NULL,
    node_type VARCHAR(50) NOT NULL,  -- SOURCE, TARGET, TRANSFORMATION
    node_category VARCHAR(50),  -- DATABASE, TABLE, COLUMN, VIEW, FILE, API
    
    -- Location
    database_name VARCHAR(100),
    schema_name VARCHAR(100),
    table_name VARCHAR(200),
    column_name VARCHAR(200),
    
    -- Metadata
    description TEXT,
    owner VARCHAR(100),
    data_type VARCHAR(50),
    is_pii BOOLEAN DEFAULT FALSE,  -- Personally Identifiable Information
    is_sensitive BOOLEAN DEFAULT FALSE,
    
    -- Source system info
    source_system VARCHAR(100),  -- CORE_BANKING, CARDS, LOANS, etc.
    source_type VARCHAR(50),  -- ORACLE, POSTGRESQL, MAINFRAME, API, FILE
    
    -- Tags
    tags TEXT[],  -- Array of tags
    
    -- Status
    status VARCHAR(20) DEFAULT 'ACTIVE',  -- ACTIVE, DEPRECATED, ARCHIVED
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT current_user,
    
    -- Unique constraint
    UNIQUE(node_name, node_type, database_name, schema_name, table_name, column_name)
);

-- =====================================================
-- INDEXES
-- =====================================================
CREATE INDEX idx_nodes_type ON lineage.nodes(node_type);
CREATE INDEX idx_nodes_category ON lineage.nodes(node_category);
CREATE INDEX idx_nodes_table ON lineage.nodes(table_name);
CREATE INDEX idx_nodes_column ON lineage.nodes(column_name);
CREATE INDEX idx_nodes_source ON lineage.nodes(source_system);
CREATE INDEX idx_nodes_pii ON lineage.nodes(is_pii);
CREATE INDEX idx_nodes_status ON lineage.nodes(status);

-- =====================================================
-- INSERT SAMPLE NODES - SOURCE SYSTEMS
-- =====================================================
INSERT INTO lineage.nodes (node_name, node_type, node_category, database_name, schema_name, table_name, column_name, description, source_system, source_type, data_type, is_pii, tags)
VALUES
-- Core Banking Source
('core_banking.customers', 'SOURCE', 'TABLE', 'core_banking', 'public', 'customers', NULL, 'Customer master data from core banking', 'CORE_BANKING', 'POSTGRESQL', NULL, FALSE, ARRAY['source', 'master-data']),
('core_banking.customers.customer_id', 'SOURCE', 'COLUMN', 'core_banking', 'public', 'customers', 'customer_id', 'Unique customer identifier', 'CORE_BANKING', 'POSTGRESQL', 'VARCHAR', FALSE, ARRAY['primary-key']),
('core_banking.customers.email', 'SOURCE', 'COLUMN', 'core_banking', 'public', 'customers', 'email', 'Customer email address', 'CORE_BANKING', 'POSTGRESQL', 'VARCHAR', TRUE, ARRAY['pii', 'contact']),
('core_banking.customers.full_name', 'SOURCE', 'COLUMN', 'core_banking', 'public', 'customers', 'full_name', 'Customer full name', 'CORE_BANKING', 'POSTGRESQL', 'VARCHAR', TRUE, ARRAY['pii', 'identity']),

('core_banking.accounts', 'SOURCE', 'TABLE', 'core_banking', 'public', 'accounts', NULL, 'Bank account data', 'CORE_BANKING', 'POSTGRESQL', NULL, FALSE, ARRAY['source', 'financial']),
('core_banking.accounts.account_id', 'SOURCE', 'COLUMN', 'core_banking', 'public', 'accounts', 'account_id', 'Unique account identifier', 'CORE_BANKING', 'POSTGRESQL', 'VARCHAR', FALSE, ARRAY['primary-key']),
('core_banking.accounts.balance', 'SOURCE', 'COLUMN', 'core_banking', 'public', 'accounts', 'balance', 'Current account balance', 'CORE_BANKING', 'POSTGRESQL', 'DECIMAL', FALSE, ARRAY['financial', 'sensitive']),

('core_banking.transactions', 'SOURCE', 'TABLE', 'core_banking', 'public', 'transactions', NULL, 'Transaction history', 'CORE_BANKING', 'POSTGRESQL', NULL, FALSE, ARRAY['source', 'transactional']),
('core_banking.transactions.transaction_id', 'SOURCE', 'COLUMN', 'core_banking', 'public', 'transactions', 'transaction_id', 'Unique transaction identifier', 'CORE_BANKING', 'POSTGRESQL', 'VARCHAR', FALSE, ARRAY['primary-key']),
('core_banking.transactions.amount', 'SOURCE', 'COLUMN', 'core_banking', 'public', 'transactions', 'amount', 'Transaction amount', 'CORE_BANKING', 'POSTGRESQL', 'DECIMAL', FALSE, ARRAY['financial']),

-- Cards System Source
('cards_system.credit_cards', 'SOURCE', 'TABLE', 'cards_system', 'public', 'credit_cards', NULL, 'Credit card accounts', 'CARDS', 'POSTGRESQL', NULL, FALSE, ARRAY['source', 'cards']),
('cards_system.credit_cards.card_id', 'SOURCE', 'COLUMN', 'cards_system', 'public', 'credit_cards', 'card_id', 'Unique card identifier', 'CARDS', 'POSTGRESQL', 'VARCHAR', FALSE, ARRAY['primary-key']),

-- Loans System Source
('loans_system.loans', 'SOURCE', 'TABLE', 'loans_system', 'public', 'loans', NULL, 'Loan accounts', 'LOANS', 'POSTGRESQL', NULL, FALSE, ARRAY['source', 'loans']),
('loans_system.loans.loan_id', 'SOURCE', 'COLUMN', 'loans_system', 'public', 'loans', 'loan_id', 'Unique loan identifier', 'LOANS', 'POSTGRESQL', 'VARCHAR', FALSE, ARRAY['primary-key']);

-- =====================================================
-- INSERT SAMPLE NODES - STAGING
-- =====================================================
INSERT INTO lineage.nodes (node_name, node_type, node_category, database_name, schema_name, table_name, column_name, description, data_type, is_pii, tags)
VALUES
('banking_dw.staging.stg_customers', 'TARGET', 'TABLE', 'banking_dw', 'staging', 'stg_customers', NULL, 'Cleaned customer data', NULL, FALSE, ARRAY['staging', 'cleaned']),
('banking_dw.staging.stg_customers.customer_id', 'TARGET', 'COLUMN', 'banking_dw', 'staging', 'stg_customers', 'customer_id', 'Customer ID (cleaned)', 'VARCHAR', FALSE, ARRAY['primary-key']),
('banking_dw.staging.stg_customers.email', 'TARGET', 'COLUMN', 'banking_dw', 'staging', 'stg_customers', 'email', 'Customer email (cleaned)', 'VARCHAR', TRUE, ARRAY['pii']),

('banking_dw.staging.stg_accounts', 'TARGET', 'TABLE', 'banking_dw', 'staging', 'stg_accounts', NULL, 'Cleaned account data', NULL, FALSE, ARRAY['staging', 'cleaned']),
('banking_dw.staging.stg_transactions', 'TARGET', 'TABLE', 'banking_dw', 'staging', 'stg_transactions', NULL, 'Cleaned transaction data', NULL, FALSE, ARRAY['staging', 'cleaned']);

-- =====================================================
-- INSERT SAMPLE NODES - GOLD
-- =====================================================
INSERT INTO lineage.nodes (node_name, node_type, node_category, database_name, schema_name, table_name, column_name, description, data_type, is_pii, tags)
VALUES
('banking_dw.gold.dim_customer', 'TARGET', 'TABLE', 'banking_dw', 'gold', 'dim_customer', NULL, 'Customer dimension (SCD Type 2)', NULL, FALSE, ARRAY['gold', 'dimension']),
('banking_dw.gold.dim_customer.customer_sk', 'TARGET', 'COLUMN', 'banking_dw', 'gold', 'dim_customer', 'customer_sk', 'Customer surrogate key', 'INT', FALSE, ARRAY['surrogate-key']),
('banking_dw.gold.dim_customer.customer_id', 'TARGET', 'COLUMN', 'banking_dw', 'gold', 'dim_customer', 'customer_id', 'Customer natural key', 'VARCHAR', FALSE, ARRAY['natural-key']),
('banking_dw.gold.dim_customer.email', 'TARGET', 'COLUMN', 'banking_dw', 'gold', 'dim_customer', 'email', 'Customer email', 'VARCHAR', TRUE, ARRAY['pii']),
('banking_dw.gold.dim_customer.customer_segment', 'TARGET', 'COLUMN', 'banking_dw', 'gold', 'dim_customer', 'customer_segment', 'Customer segment (PLATINUM/GOLD/SILVER/STANDARD)', 'VARCHAR', FALSE, ARRAY['business-rule']),

('banking_dw.gold.fact_transactions', 'TARGET', 'TABLE', 'banking_dw', 'gold', 'fact_transactions', NULL, 'Transaction facts', NULL, FALSE, ARRAY['gold', 'fact']),
('banking_dw.gold.fact_transactions.transaction_id', 'TARGET', 'COLUMN', 'banking_dw', 'gold', 'fact_transactions', 'transaction_id', 'Transaction identifier', 'VARCHAR', FALSE, ARRAY['primary-key']),
('banking_dw.gold.fact_transactions.amount', 'TARGET', 'COLUMN', 'banking_dw', 'gold', 'fact_transactions', 'amount', 'Transaction amount', 'DECIMAL', FALSE, ARRAY['financial']);

-- =====================================================
-- CREATE VIEW FOR NODES
-- =====================================================
CREATE OR REPLACE VIEW lineage.vw_nodes AS
SELECT 
    node_id,
    node_name,
    node_type,
    node_category,
    database_name,
    schema_name,
    table_name,
    column_name,
    description,
    owner,
    data_type,
    is_pii,
    is_sensitive,
    source_system,
    source_type,
    tags,
    status,
    created_at
FROM lineage.nodes
WHERE status = 'ACTIVE'
ORDER BY node_type, node_category, table_name, column_name;

COMMENT ON TABLE lineage.nodes IS 'Data lineage nodes (sources and targets)';
COMMENT ON COLUMN lineage.nodes.is_pii IS 'Indicates if column contains Personally Identifiable Information';
