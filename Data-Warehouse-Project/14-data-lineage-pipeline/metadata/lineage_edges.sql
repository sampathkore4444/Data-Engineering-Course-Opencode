-- Data Lineage Edges
-- Banking Data Warehouse

-- =====================================================
-- LINEAGE EDGES TABLE (Connections between nodes)
-- =====================================================
CREATE TABLE IF NOT EXISTS lineage.edges (
    edge_id SERIAL PRIMARY KEY,
    
    -- Source node (where data comes FROM)
    source_node_id INT NOT NULL REFERENCES lineage.nodes(node_id),
    
    -- Target node (where data goes TO)
    target_node_id INT NOT NULL REFERENCES lineage.nodes(node_id),
    
    -- Edge metadata
    edge_type VARCHAR(50) NOT NULL,  -- DIRECT, TRANSFORMED, AGGREGATED, FILTERED, JOINED
    transformation_type VARCHAR(50),  -- TRIM, UPPER, LOWER, CAST, AGGREGATE, FILTER, etc.
    
    -- Transformation details
    transformation_logic TEXT,  -- SQL or description of transformation
    transformation_sql TEXT,  -- Actual SQL used
    
    -- Lineage level
    lineage_level VARCHAR(50) NOT NULL,  -- TABLE, COLUMN, ROW
    
    -- Process info
    process_name VARCHAR(200),  -- ETL job, dbt model, Airflow DAG
    process_type VARCHAR(50),  -- ETL, DBT, AIRFLOW, MANUAL
    
    -- Metadata
    description TEXT,
    owner VARCHAR(100),
    
    -- Quality
    is_validated BOOLEAN DEFAULT FALSE,
    validated_at TIMESTAMP,
    validated_by VARCHAR(100),
    
    -- Status
    status VARCHAR(20) DEFAULT 'ACTIVE',  -- ACTIVE, DEPRECATED, BROKEN
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT current_user,
    
    -- Constraints
    UNIQUE(source_node_id, target_node_id, lineage_level),
    CHECK (source_node_id != target_node_id)  -- No self-loops
);

-- =====================================================
-- INDEXES
-- =====================================================
CREATE INDEX idx_edges_source ON lineage.edges(source_node_id);
CREATE INDEX idx_edges_target ON lineage.edges(target_node_id);
CREATE INDEX idx_edges_type ON lineage.edges(edge_type);
CREATE INDEX idx_edges_level ON lineage.edges(lineage_level);
CREATE INDEX idx_edges_process ON lineage.edges(process_name);
CREATE INDEX idx_edges_status ON lineage.edges(status);

-- =====================================================
-- INSERT SAMPLE EDGES - TABLE LEVEL LINEAGE
-- =====================================================

-- Source to Staging
INSERT INTO lineage.edges (source_node_id, target_node_id, edge_type, transformation_type, transformation_logic, lineage_level, process_name, process_type, description)
VALUES
-- Core Banking -> Staging
((SELECT node_id FROM lineage.nodes WHERE node_name = 'core_banking.customers' AND node_type = 'SOURCE'),
 (SELECT node_id FROM lineage.nodes WHERE node_name = 'banking_dw.staging.stg_customers' AND node_type = 'TARGET'),
 'TRANSFORMED', 'CLEAN', 'TRIM whitespace, standardize case, validate email format', 'TABLE', 'bronze_ingestion', 'AIRFLOW', 'Extract and clean customer data from core banking'),

((SELECT node_id FROM lineage.nodes WHERE node_name = 'core_banking.accounts' AND node_type = 'SOURCE'),
 (SELECT node_id FROM lineage.nodes WHERE node_name = 'banking_dw.staging.stg_accounts' AND node_type = 'TARGET'),
 'TRANSFORMED', 'CLEAN', 'Standardize account types, validate balances', 'TABLE', 'bronze_ingestion', 'AIRFLOW', 'Extract and clean account data from core banking'),

((SELECT node_id FROM lineage.nodes WHERE node_name = 'core_banking.transactions' AND node_type = 'SOURCE'),
 (SELECT node_id FROM lineage.nodes WHERE node_name = 'banking_dw.staging.stg_transactions' AND node_type = 'TARGET'),
 'TRANSFORMED', 'CLEAN', 'Standardize transaction types, validate amounts', 'TABLE', 'bronze_ingestion', 'AIRFLOW', 'Extract and clean transaction data from core banking');

-- =====================================================
-- INSERT SAMPLE EDGES - COLUMN LEVEL LINEAGE
-- =====================================================
INSERT INTO lineage.edges (source_node_id, target_node_id, edge_type, transformation_type, transformation_logic, transformation_sql, lineage_level, process_name, process_type, description)
VALUES
-- Customer ID lineage
((SELECT node_id FROM lineage.nodes WHERE node_name = 'core_banking.customers.customer_id' AND node_type = 'SOURCE'),
 (SELECT node_id FROM lineage.nodes WHERE node_name = 'banking_dw.staging.stg_customers.customer_id' AND node_type = 'TARGET'),
 'DIRECT', 'NONE', 'Direct mapping, no transformation', NULL, 'COLUMN', 'stg_customers', 'DBT', 'Customer ID passed through without changes'),

((SELECT node_id FROM lineage.nodes WHERE node_name = 'banking_dw.staging.stg_customers.customer_id' AND node_type = 'TARGET'),
 (SELECT node_id FROM lineage.nodes WHERE node_name = 'banking_dw.gold.dim_customer.customer_id' AND node_type = 'TARGET'),
 'DIRECT', 'NONE', 'Direct mapping', NULL, 'COLUMN', 'dim_customer', 'DBT', 'Customer ID mapped to dimension'),

-- Email lineage (with transformation)
((SELECT node_id FROM lineage.nodes WHERE node_name = 'core_banking.customers.email' AND node_type = 'SOURCE'),
 (SELECT node_id FROM lineage.nodes WHERE node_name = 'banking_dw.staging.stg_customers.email' AND node_type = 'TARGET'),
 'TRANSFORMED', 'CLEAN', 'TRIM whitespace, convert to lowercase', 'UPPER(TRIM(email))', 'COLUMN', 'stg_customers', 'DBT', 'Email cleaned and standardized'),

((SELECT node_id FROM lineage.nodes WHERE node_name = 'banking_dw.staging.stg_customers.email' AND node_type = 'TARGET'),
 (SELECT node_id FROM lineage.nodes WHERE node_name = 'banking_dw.gold.dim_customer.email' AND node_type = 'TARGET'),
 'DIRECT', 'NONE', 'Direct mapping', NULL, 'COLUMN', 'dim_customer', 'DBT', 'Email mapped to dimension'),

-- Balance lineage
((SELECT node_id FROM lineage.nodes WHERE node_name = 'core_banking.accounts.balance' AND node_type = 'SOURCE'),
 (SELECT node_id FROM lineage.nodes WHERE node_name = 'banking_dw.staging.stg_accounts.balance' AND node_type = 'TARGET'),
 'DIRECT', 'CAST', 'Cast to DECIMAL(15,2)', 'balance::DECIMAL(15,2)', 'COLUMN', 'stg_accounts', 'DBT', 'Balance cast to proper decimal type'),

-- Amount lineage
((SELECT node_id FROM lineage.nodes WHERE node_name = 'core_banking.transactions.amount' AND node_type = 'SOURCE'),
 (SELECT node_id FROM lineage.nodes WHERE node_name = 'banking_dw.staging.stg_transactions.amount' AND node_type = 'TARGET'),
 'DIRECT', 'CAST', 'Cast to DECIMAL(15,2)', 'amount::DECIMAL(15,2)', 'COLUMN', 'stg_transactions', 'DBT', 'Transaction amount cast to proper type'),

((SELECT node_id FROM lineage.nodes WHERE node_name = 'banking_dw.staging.stg_transactions.amount' AND node_type = 'TARGET'),
 (SELECT node_id FROM lineage.nodes WHERE node_name = 'banking_dw.gold.fact_transactions.amount' AND node_type = 'TARGET'),
 'DIRECT', 'NONE', 'Direct mapping', NULL, 'COLUMN', 'fact_transactions', 'DBT', 'Amount mapped to fact table');

-- =====================================================
-- CREATE VIEW FOR EDGES
-- =====================================================
CREATE OR REPLACE VIEW lineage.vw_edges AS
SELECT 
    e.edge_id,
    sn.node_name as source_name,
    sn.node_type as source_type,
    sn.table_name as source_table,
    sn.column_name as source_column,
    tn.node_name as target_name,
    tn.node_type as target_type,
    tn.table_name as target_table,
    tn.column_name as target_column,
    e.edge_type,
    e.transformation_type,
    e.transformation_logic,
    e.transformation_sql,
    e.lineage_level,
    e.process_name,
    e.process_type,
    e.description,
    e.owner,
    e.status,
    e.created_at
FROM lineage.edges e
JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
WHERE e.status = 'ACTIVE'
ORDER BY e.lineage_level, sn.table_name, tn.table_name;

-- =====================================================
-- CREATE VIEW FOR FULL LINEAGE GRAPH
-- =====================================================
CREATE OR REPLACE VIEW lineage.vw_full_lineage AS
SELECT 
    -- Source
    sn.database_name as source_database,
    sn.schema_name as source_schema,
    sn.table_name as source_table,
    sn.column_name as source_column,
    sn.source_system,
    
    -- Transformation
    e.edge_type,
    e.transformation_type,
    e.transformation_logic,
    e.process_name as transformation_process,
    
    -- Target
    tn.database_name as target_database,
    tn.schema_name as target_schema,
    tn.table_name as target_table,
    tn.column_name as target_column,
    
    -- Metadata
    e.lineage_level,
    e.description,
    e.owner
FROM lineage.edges e
JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
WHERE e.status = 'ACTIVE'
ORDER BY e.lineage_level, sn.table_name, tn.table_name;

COMMENT ON TABLE lineage.edges IS 'Data lineage edges (connections between nodes)';
COMMENT ON COLUMN lineage.edges.edge_type IS 'Type of edge: DIRECT, TRANSFORMED, AGGREGATED, FILTERED, JOINED';
COMMENT ON COLUMN lineage.edges.lineage_level IS 'Level of lineage: TABLE, COLUMN, ROW';
