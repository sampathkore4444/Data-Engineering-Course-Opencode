-- =============================================================================
-- ROW-LEVEL SECURITY (RLS) FOR BANKING DATA
-- =============================================================================
-- Purpose: Implement row-level access control based on user roles
-- Tool:    Dremio SQL
-- =============================================================================

-- =============================================================================
-- 1. BRANCH-BASED ROW ACCESS
-- =============================================================================

-- Create branch mapping table
CREATE TABLE IF NOT EXISTS banking_security.user_branch_mapping (
    user_id VARCHAR(50) PRIMARY KEY,
    branch_code VARCHAR(10) NOT NULL,
    branch_name VARCHAR(100),
    region VARCHAR(50),
    access_level VARCHAR(20) DEFAULT 'BRANCH',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample mappings
INSERT INTO banking_security.user_branch_mapping VALUES
('rm_1', 'BR001', 'Ho Chi Minh Main', 'SOUTH', 'BRANCH'),
('rm_2', 'BR002', 'Hanoi Branch', 'NORTH', 'BRANCH'),
('bm_1', 'BR001', 'Ho Chi Minh Main', 'SOUTH', 'BRANCH'),
('risk_manager_1', 'ALL', 'All Branches', 'ALL', 'ENTERPRISE'),
('compliance_officer_1', 'ALL', 'All Branches', 'ALL', 'ENTERPRISE');

-- Create row-level security policy for accounts
CREATE OR REPLACE VIEW banking_security.accounts_rls AS
SELECT 
    a.*,
    CASE 
        WHEN ubm.access_level = 'ENTERPRISE' THEN TRUE
        WHEN ubm.access_level = 'REGION' AND a.branch_code IN (
            SELECT branch_code FROM banking_security.user_branch_mapping 
            WHERE region = ubm.region
        ) THEN TRUE
        WHEN ubm.access_level = 'BRANCH' AND a.branch_code = ubm.branch_code THEN TRUE
        ELSE FALSE
    END AS has_access
FROM banking_cleansed.core_banking_accounts a
CROSS JOIN banking_security.user_branch_mapping ubm
WHERE ubm.user_id = CURRENT_USER();

-- =============================================================================
-- 2. CUSTOMER-BASED ROW ACCESS
-- =============================================================================

-- Create customer assignment table
CREATE TABLE IF NOT EXISTS banking_security.customer_assignment (
    customer_id VARCHAR(20) PRIMARY KEY,
    assigned_to VARCHAR(50) NOT NULL,
    assignment_type VARCHAR(20) DEFAULT 'RELATIONSHIP_MANAGER',
    start_date DATE DEFAULT CURRENT_DATE,
    end_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample assignments
INSERT INTO banking_security.customer_assignment VALUES
('CUST-12345', 'rm_1', 'RELATIONSHIP_MANAGER', '2024-01-01', NULL),
('CUST-12346', 'rm_1', 'RELATIONSHIP_MANAGER', '2024-01-01', NULL),
('CUST-12347', 'rm_2', 'RELATIONSHIP_MANAGER', '2024-01-01', NULL);

-- Create row-level security policy for customer data
CREATE OR REPLACE VIEW banking_security.customer_360_rls AS
SELECT 
    c.*,
    CASE 
        WHEN IS_MEMBER_OF_ROLE('data_engineers') THEN TRUE
        WHEN IS_MEMBER_OF_ROLE('risk_team') THEN TRUE
        WHEN IS_MEMBER_OF_ROLE('fraud_team') THEN TRUE
        WHEN IS_MEMBER_OF_ROLE('compliance_team') THEN TRUE
        WHEN IS_MEMBER_OF_ROLE('executive_team') THEN TRUE
        WHEN ca.assigned_to = CURRENT_USER() THEN TRUE
        ELSE FALSE
    END AS has_access
FROM banking_gold.customer_360 c
LEFT JOIN banking_security.customer_assignment ca 
    ON c.customer_id = ca.customer_id
WHERE ca.end_date IS NULL OR ca.end_date >= CURRENT_DATE;

-- =============================================================================
-- 3. TRANSACTION-BASED ROW ACCESS
-- =============================================================================

-- Create transaction access policy
CREATE OR REPLACE VIEW banking_security.transactions_rls AS
SELECT 
    t.*,
    CASE 
        WHEN IS_MEMBER_OF_ROLE('data_engineers') THEN TRUE
        WHEN IS_MEMBER_OF_ROLE('risk_team') THEN TRUE
        WHEN IS_MEMBER_OF_ROLE('fraud_team') THEN TRUE
        WHEN IS_MEMBER_OF_ROLE('compliance_team') THEN TRUE
        WHEN a.branch_code = (
            SELECT branch_code FROM banking_security.user_branch_mapping 
            WHERE user_id = CURRENT_USER() AND access_level = 'BRANCH'
        ) THEN TRUE
        WHEN ca.assigned_to = CURRENT_USER() THEN TRUE
        ELSE FALSE
    END AS has_access
FROM banking_cleansed.core_banking_transactions t
JOIN banking_cleansed.core_banking_accounts a ON t.account_id = a.account_id
LEFT JOIN banking_security.customer_assignment ca 
    ON a.customer_id = ca.customer_id
WHERE ca.end_date IS NULL OR ca.end_date >= CURRENT_DATE;

-- =============================================================================
-- 4. APPLY ROW-LEVEL SECURITY
-- =============================================================================

-- Apply RLS to accounts table
ALTER TABLE banking_cleansed.core_banking_accounts
SET ROW ACCESS POLICY (
    POLICY accounts_branch_policy
    AS (
        SELECT * FROM banking_security.accounts_rls WHERE has_access = TRUE
    )
);

-- Apply RLS to customer data
ALTER TABLE banking_gold.customer_360
SET ROW ACCESS POLICY (
    POLICY customer_assignment_policy
    AS (
        SELECT * FROM banking_security.customer_360_rls WHERE has_access = TRUE
    )
);

-- Apply RLS to transactions
ALTER TABLE banking_cleansed.core_banking_transactions
SET ROW ACCESS POLICY (
    POLICY transactions_branch_policy
    AS (
        SELECT * FROM banking_security.transactions_rls WHERE has_access = TRUE
    )
);

-- =============================================================================
-- 5. VERIFICATION QUERIES
-- =============================================================================

-- Test as Relationship Manager
SET ROLE rm_1;
SELECT customer_id, customer_name, total_balance 
FROM banking_gold.customer_360
WHERE has_access = TRUE
LIMIT 5;
-- Expected: Only customers assigned to rm_1

-- Test as Risk Manager
SET ROLE risk_manager_1;
SELECT customer_id, customer_name, total_balance 
FROM banking_gold.customer_360
WHERE has_access = TRUE
LIMIT 5;
-- Expected: All customers (enterprise access)

-- Reset role
RESET ROLE;

-- Check RLS policies
SELECT 
    schemaname,
    tablename,
    policyname,
    permissive,
    roles,
    cmd,
    qual,
    with_check
FROM pg_policies
WHERE schemaname IN ('banking_cleansed', 'banking_gold');
