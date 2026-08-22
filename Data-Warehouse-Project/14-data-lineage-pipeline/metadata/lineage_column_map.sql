-- Data Lineage Column Mapping
-- Banking Data Warehouse

-- =====================================================
-- COLUMN MAPPING TABLE (Detailed column relationships)
-- =====================================================
CREATE TABLE IF NOT EXISTS lineage.column_mapping (
    mapping_id SERIAL PRIMARY KEY,
    
    -- Source column
    source_database VARCHAR(100) NOT NULL,
    source_schema VARCHAR(100) NOT NULL,
    source_table VARCHAR(200) NOT NULL,
    source_column VARCHAR(200) NOT NULL,
    source_data_type VARCHAR(50),
    
    -- Target column
    target_database VARCHAR(100) NOT NULL,
    target_schema VARCHAR(100) NOT NULL,
    target_table VARCHAR(200) NOT NULL,
    target_column VARCHAR(200) NOT NULL,
    target_data_type VARCHAR(50),
    
    -- Mapping details
    mapping_type VARCHAR(50) NOT NULL,  -- DIRECT, TRANSFORMED, DERIVED, AGGREGATED
    transformation_logic TEXT,
    transformation_sql TEXT,
    
    -- Data quality
    is_required BOOLEAN DEFAULT TRUE,
    is_pii BOOLEAN DEFAULT FALSE,
    is_sensitive BOOLEAN DEFAULT FALSE,
    validation_rules JSONB,
    
    -- Business context
    business_name VARCHAR(200),
    business_description TEXT,
    business_owner VARCHAR(100),
    
    -- Status
    status VARCHAR(20) DEFAULT 'ACTIVE',
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT current_user,
    
    -- Constraints
    UNIQUE(source_database, source_schema, source_table, source_column,
           target_database, target_schema, target_table, target_column)
);

-- =====================================================
-- INDEXES
-- =====================================================
CREATE INDEX idx_colmap_source ON lineage.column_mapping(source_table, source_column);
CREATE INDEX idx_colmap_target ON lineage.column_mapping(target_table, target_column);
CREATE INDEX idx_colmap_type ON lineage.column_mapping(mapping_type);
CREATE INDEX idx_colmap_pii ON lineage.column_mapping(is_pii);
CREATE INDEX idx_colmap_status ON lineage.column_mapping(status);

-- =====================================================
-- INSERT SAMPLE COLUMN MAPPINGS
-- =====================================================
INSERT INTO lineage.column_mapping (
    source_database, source_schema, source_table, source_column, source_data_type,
    target_database, target_schema, target_table, target_column, target_data_type,
    mapping_type, transformation_logic, transformation_sql,
    is_required, is_pii, business_name, business_description
)
VALUES
-- Customer ID mappings
('core_banking', 'public', 'customers', 'customer_id', 'VARCHAR',
 'banking_dw', 'staging', 'stg_customers', 'customer_id', 'VARCHAR',
 'DIRECT', 'Direct mapping, no transformation', NULL,
 TRUE, FALSE, 'Customer ID', 'Unique identifier for customer'),

('banking_dw', 'staging', 'stg_customers', 'customer_id', 'VARCHAR',
 'banking_dw', 'gold', 'dim_customer', 'customer_id', 'VARCHAR',
 'DIRECT', 'Direct mapping', NULL,
 TRUE, FALSE, 'Customer ID', 'Natural key for customer dimension'),

-- Email mappings (with transformation)
('core_banking', 'public', 'customers', 'email', 'VARCHAR',
 'banking_dw', 'staging', 'stg_customers', 'email', 'VARCHAR',
 'TRANSFORMED', 'TRIM whitespace, convert to lowercase', 'UPPER(TRIM(email))',
 TRUE, TRUE, 'Email', 'Customer email address (cleaned)'),

('banking_dw', 'staging', 'stg_customers', 'email', 'VARCHAR',
 'banking_dw', 'gold', 'dim_customer', 'email', 'VARCHAR',
 'DIRECT', 'Direct mapping', NULL,
 FALSE, TRUE, 'Email', 'Customer email address'),

-- Balance mappings
('core_banking', 'public', 'accounts', 'balance', 'DECIMAL',
 'banking_dw', 'staging', 'stg_accounts', 'balance', 'DECIMAL(15,2)',
 'TRANSFORMED', 'Cast to DECIMAL(15,2)', 'balance::DECIMAL(15,2)',
 TRUE, FALSE, 'Account Balance', 'Current balance in account'),

-- Amount mappings
('core_banking', 'public', 'transactions', 'amount', 'DECIMAL',
 'banking_dw', 'staging', 'stg_transactions', 'amount', 'DECIMAL(15,2)',
 'TRANSFORMED', 'Cast to DECIMAL(15,2)', 'amount::DECIMAL(15,2)',
 TRUE, FALSE, 'Transaction Amount', 'Amount of transaction'),

('banking_dw', 'staging', 'stg_transactions', 'amount', 'DECIMAL(15,2)',
 'banking_dw', 'gold', 'fact_transactions', 'amount', 'DECIMAL(15,2)',
 'DIRECT', 'Direct mapping', NULL,
 TRUE, FALSE, 'Transaction Amount', 'Amount in fact table');

-- =====================================================
-- CREATE VIEW FOR COLUMN MAPPING
-- =====================================================
CREATE OR REPLACE VIEW lineage.vw_column_mapping AS
SELECT 
    m.mapping_id,
    m.source_database || '.' || m.source_schema || '.' || m.source_table || '.' || m.source_column as source_full_name,
    m.target_database || '.' || m.target_schema || '.' || m.target_table || '.' || m.target_column as target_full_name,
    m.mapping_type,
    m.transformation_logic,
    m.transformation_sql,
    m.source_data_type,
    m.target_data_type,
    m.is_required,
    m.is_pii,
    m.is_sensitive,
    m.business_name,
    m.business_description,
    m.business_owner,
    m.status,
    m.created_at
FROM lineage.column_mapping m
WHERE m.status = 'ACTIVE'
ORDER BY m.source_table, m.source_column;

-- =====================================================
-- CREATE VIEW FOR PII DATA FLOW
-- =====================================================
CREATE OR REPLACE VIEW lineage.vw_pii_data_flow AS
SELECT 
    m.source_table,
    m.source_column,
    m.target_table,
    m.target_column,
    m.mapping_type,
    m.transformation_logic,
    m.business_name,
    m.business_description
FROM lineage.column_mapping m
WHERE m.is_pii = TRUE
  AND m.status = 'ACTIVE'
ORDER BY m.source_table, m.source_column;

COMMENT ON TABLE lineage.column_mapping IS 'Detailed column-to-column mapping with transformation logic';
COMMENT ON COLUMN lineage.column_mapping.is_pii IS 'Indicates if column contains Personally Identifiable Information';
COMMENT ON COLUMN lineage.column_mapping.validation_rules IS 'JSON validation rules for the column';
