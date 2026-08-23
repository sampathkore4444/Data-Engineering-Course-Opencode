-- Data Lineage Transforms
-- Banking Data Warehouse

-- =====================================================
-- TRANSFORMS TABLE (Transformation logic tracking)
-- =====================================================
CREATE TABLE IF NOT EXISTS lineage.transforms (
    transform_id SERIAL PRIMARY KEY,
    
    -- Transform identity
    transform_name VARCHAR(200) NOT NULL,
    transform_type VARCHAR(50) NOT NULL,  -- SQL, PYTHON, DBT, AIRFLOW, MANUAL
    
    -- Source and target
    source_table VARCHAR(200),
    target_table VARCHAR(200),
    
    -- Transformation details
    transform_logic TEXT NOT NULL,
    transform_sql TEXT,
    transform_python TEXT,
    
    -- Business context
    business_rule VARCHAR(200),
    business_description TEXT,
    
    -- Process info
    process_name VARCHAR(200),
    process_type VARCHAR(50),  -- ETL, ELT, STREAMING, BATCH
    schedule VARCHAR(100),  -- Daily, Hourly, Real-time
    
    -- Quality
    is_idempotent BOOLEAN DEFAULT TRUE,
    handles_duplicates BOOLEAN DEFAULT FALSE,
    error_handling TEXT,
    
    -- Lineage
    upstream_tables TEXT[],  -- Array of source tables
    downstream_tables TEXT[],  -- Array of target tables
    
    -- Status
    status VARCHAR(20) DEFAULT 'ACTIVE',
    
    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT current_user,
    
    -- Constraints
    UNIQUE(transform_name, source_table, target_table)
);

-- =====================================================
-- INDEXES
-- =====================================================
CREATE INDEX idx_transforms_type ON lineage.transforms(transform_type);
CREATE INDEX idx_transforms_source ON lineage.transforms(source_table);
CREATE INDEX idx_transforms_target ON lineage.transforms(target_table);
CREATE INDEX idx_transforms_process ON lineage.transforms(process_name);
CREATE INDEX idx_transforms_status ON lineage.transforms(status);

-- =====================================================
-- INSERT SAMPLE TRANSFORMS
-- =====================================================
INSERT INTO lineage.transforms (
    transform_name, transform_type, source_table, target_table,
    transform_logic, transform_sql, business_rule, business_description,
    process_name, process_type, schedule, upstream_tables, downstream_tables
)
VALUES
-- Customer transformations
('clean_customer_email', 'SQL', 'core_banking.customers', 'banking_dw.staging.stg_customers',
 'TRIM whitespace and convert email to lowercase',
 'UPPER(TRIM(email)) as email',
 'Email Standardization',
 'All emails must be lowercase and trimmed for consistency',
 'stg_customers', 'DBT', 'Daily',
 ARRAY['core_banking.customers'],
 ARRAY['banking_dw.staging.stg_customers', 'banking_dw.gold.dim_customer']),

('segment_customer', 'SQL', 'banking_dw.staging.stg_customers', 'banking_dw.gold.dim_customer',
 'Assign customer segment based on total balance',
 'CASE
   WHEN total_balance >= 1000000000 THEN ''PLATINUM''
   WHEN total_balance >= 500000000 THEN ''GOLD''
   WHEN total_balance >= 100000000 THEN ''SILVER''
   ELSE ''STANDARD''
 END as customer_segment',
 'Customer Segmentation',
 'Customers are segmented by their total relationship value for targeted services',
 'dim_customer', 'DBT', 'Daily',
 ARRAY['banking_dw.staging.stg_customers', 'banking_dw.staging.stg_accounts'],
 ARRAY['banking_dw.gold.dim_customer']),

-- Account transformations
('standardize_account_type', 'SQL', 'core_banking.accounts', 'banking_dw.staging.stg_accounts',
 'Standardize account type codes',
 'UPPER(account_type) as account_type',
 'Account Type Standardization',
 'All account types must be uppercase for consistency',
 'stg_accounts', 'DBT', 'Daily',
 ARRAY['core_banking.accounts'],
 ARRAY['banking_dw.staging.stg_accounts', 'banking_dw.gold.dim_account']),

-- Transaction transformations
('validate_transaction_amount', 'SQL', 'core_banking.transactions', 'banking_dw.staging.stg_transactions',
 'Ensure transaction amount is positive',
 'CASE WHEN amount > 0 THEN amount ELSE NULL END as amount',
 'Amount Validation',
 'Transaction amounts must be positive; negative amounts are rejected',
 'stg_transactions', 'DBT', 'Daily',
 ARRAY['core_banking.transactions'],
 ARRAY['banking_dw.staging.stg_transactions', 'banking_dw.gold.fact_transactions']),

-- Gold layer transformations
('aggregate_daily_transactions', 'SQL', 'banking_dw.staging.stg_transactions', 'banking_dw.gold.fact_transactions',
 'Aggregate transactions by account and date',
 'SELECT
   account_sk,
   transaction_date_sk,
   transaction_type,
   SUM(amount) as total_amount,
   COUNT(*) as transaction_count
 FROM stg_transactions
 GROUP BY account_sk, transaction_date_sk, transaction_type',
 'Daily Transaction Aggregation',
 'Aggregate daily transaction summaries for reporting',
 'fact_transactions', 'DBT', 'Daily',
 ARRAY['banking_dw.staging.stg_transactions'],
 ARRAY['banking_dw.gold.fact_transactions']),

-- Customer 360 transformation
('build_customer_360', 'SQL', 'banking_dw.gold.dim_customer', 'banking_dw.gold.dim_customer',
 'Combine customer, account, and transaction data into 360 view',
 'SELECT
   c.customer_id,
   c.customer_name,
   c.email,
   c.customer_segment,
   COUNT(DISTINCT a.account_id) as total_accounts,
   SUM(a.balance) as total_balance,
   COUNT(DISTINCT t.transaction_id) as total_transactions
 FROM dim_customer c
 LEFT JOIN dim_account a ON c.customer_id = a.customer_id
 LEFT JOIN fact_transactions t ON a.account_sk = t.account_sk
 GROUP BY c.customer_id, c.customer_name, c.email, c.customer_segment',
 'Customer 360 View',
 'Comprehensive view of customer relationship across all products',
 'customer_360_view', 'DBT', 'Daily',
 ARRAY['banking_dw.gold.dim_customer', 'banking_dw.gold.dim_account', 'banking_dw.gold.fact_transactions'],
 ARRAY['banking_dw.gold.customer_360_report']);

-- =====================================================
-- CREATE VIEW FOR TRANSFORMS
-- =====================================================
CREATE OR REPLACE VIEW lineage.vw_transforms AS
SELECT 
    t.transform_id,
    t.transform_name,
    t.transform_type,
    t.source_table,
    t.target_table,
    t.transform_logic,
    t.transform_sql,
    t.business_rule,
    t.business_description,
    t.process_name,
    t.process_type,
    t.schedule,
    t.upstream_tables,
    t.downstream_tables,
    t.is_idempotent,
    t.handles_duplicates,
    t.status,
    t.created_at
FROM lineage.transforms t
WHERE t.status = 'ACTIVE'
ORDER BY t.source_table, t.target_table;

-- =====================================================
-- CREATE VIEW FOR TRANSFORMATION IMPACT
-- =====================================================
CREATE OR REPLACE VIEW lineage.vw_transformation_impact AS
SELECT 
    t.transform_name,
    t.source_table,
    t.target_table,
    t.business_rule,
    t.business_description,
    t.upstream_tables,
    t.downstream_tables,
    array_length(t.upstream_tables, 1) as upstream_count,
    array_length(t.downstream_tables, 1) as downstream_count,
    CASE 
        WHEN array_length(t.downstream_tables, 1) > 5 THEN 'HIGH IMPACT'
        WHEN array_length(t.downstream_tables, 1) > 2 THEN 'MEDIUM IMPACT'
        ELSE 'LOW IMPACT'
    END as impact_level
FROM lineage.transforms t
WHERE t.status = 'ACTIVE'
ORDER BY array_length(t.downstream_tables, 1) DESC;

COMMENT ON TABLE lineage.transforms IS 'Transformation logic tracking for data lineage';
COMMENT ON COLUMN lineage.transforms.upstream_tables IS 'Array of source tables feeding this transformation';
COMMENT ON COLUMN lineage.transforms.downstream_tables IS 'Array of target tables populated by this transformation';
