-- =============================================================================
-- ROLE HIERARCHY AND RBAC CONFIGURATION
-- =============================================================================
-- Purpose: Define role hierarchy and access control for banking data
-- Tool:    Dremio SQL
-- =============================================================================

-- =============================================================================
-- 1. CREATE ROLES
-- =============================================================================

-- Data Engineering Roles
CREATE ROLE data_engineers;
CREATE ROLE data_architects;
CREATE ROLE platform_admins;

-- Data Analysis Roles
CREATE ROLE data_analysts;
CREATE ROLE senior_analysts;
CREATE ROLE business_analysts;

-- Business Domain Roles
CREATE ROLE risk_team;
CREATE ROLE fraud_team;
CREATE ROLE compliance_team;
CREATE ROLE finance_team;
CREATE ROLE executive_team;

-- Customer Service Roles
CREATE ROLE relationship_managers;
CREATE ROLE branch_managers;
CREATE ROLE call_center;

-- =============================================================================
-- 2. ROLE HIERARCHY (Inheritance)
-- =============================================================================

-- Senior roles inherit from junior roles
GRANT data_engineers TO data_architects;
GRANT data_architects TO platform_admins;

GRANT data_analysts TO senior_analysts;
GRANT senior_analysts TO business_analysts;

-- Domain roles inherit from analysts
GRANT data_analysts TO risk_team;
GRANT data_analysts TO fraud_team;
GRANT data_analysts TO compliance_team;
GRANT data_analysts TO finance_team;
GRANT data_analysts TO executive_team;

-- Customer service roles
GRANT data_analysts TO relationship_managers;
GRANT relationship_managers TO branch_managers;

-- =============================================================================
-- 3. PERMISSION ASSIGNMENTS
-- =============================================================================

-- Data Engineers: Full access to all layers
GRANT ALL ON SPACE banking_raw TO data_engineers;
GRANT ALL ON SPACE banking_cleansed TO data_engineers;
GRANT ALL ON SPACE banking_gold TO data_engineers;
GRANT ALL ON SOURCE minio_s3 TO data_engineers;
GRANT ALL ON FUNCTION * TO data_engineers;

-- Data Analysts: Read access to cleansed and gold
GRANT READ ON SPACE banking_raw TO data_analysts;
GRANT READ ON SPACE banking_cleansed TO data_analysts;
GRANT READ ON SPACE banking_gold TO data_analysts;
GRANT READ ON SOURCE minio_s3 TO data_analysts;

-- Risk Team: Read access to risk-related data
GRANT READ ON SPACE banking_cleansed TO risk_team;
GRANT READ ON SPACE banking_gold TO risk_team;
GRANT READ ON VIEW gold.credit_risk_dashboard TO risk_team;
GRANT READ ON VIEW gold.npa_status TO risk_team;
GRANT READ ON VIEW gold.npa_trend TO risk_team;

-- Fraud Team: Read access to fraud data
GRANT READ ON SPACE banking_cleansed TO fraud_team;
GRANT READ ON SPACE banking_gold TO fraud_team;
GRANT READ ON VIEW gold.fraud_score TO fraud_team;
GRANT READ ON VIEW gold.fraud_alert_summary TO fraud_team;
GRANT WRITE ON VIEW gold.fraud_alert_summary TO fraud_team;

-- Compliance Team: Read access to regulatory reports
GRANT READ ON SPACE banking_gold TO compliance_team;
GRANT READ ON VIEW gold.call_report TO compliance_team;
GRANT READ ON VIEW gold.basel_iii_car TO compliance_team;
GRANT READ ON VIEW gold.aml_daily_summary TO compliance_team;
GRANT READ ON VIEW gold.aml_ctr TO compliance_team;
GRANT READ ON VIEW gold.aml_str TO compliance_team;

-- Finance Team: Read access to financial data
GRANT READ ON SPACE banking_gold TO finance_team;
GRANT READ ON VIEW gold.ceo_dashboard TO finance_team;
GRANT READ ON VIEW gold.call_report TO finance_team;
GRANT READ ON VIEW gold.basel_iii_car TO finance_team;

-- Executive Team: Read access to dashboards
GRANT READ ON SPACE banking_gold TO executive_team;
GRANT READ ON VIEW gold.ceo_dashboard TO executive_team;
GRANT READ ON VIEW gold.board_presentation TO executive_team;

-- Relationship Managers: Limited customer data
GRANT READ ON SPACE banking_cleansed TO relationship_managers;
GRANT READ ON VIEW gold.customer_360 TO relationship_managers;
GRANT READ ON VIEW gold.daily_transaction_summary TO relationship_managers;

-- Branch Managers: Branch-level data
GRANT READ ON SPACE banking_cleansed TO branch_managers;
GRANT READ ON VIEW gold.customer_360 TO branch_managers;
GRANT READ ON VIEW gold.daily_transaction_summary TO branch_managers;
GRANT READ ON VIEW gold.npa_by_branch TO branch_managers;

-- Call Center: Basic customer data
GRANT READ ON VIEW gold.customer_360 TO call_center;

-- =============================================================================
-- 4. USER ASSIGNMENTS
-- =============================================================================

-- Data Engineering Team
GRANT data_engineers TO john_doe;
GRANT data_engineers TO jane_smith;
GRANT data_architects TO mike_wilson;

-- Data Analysis Team
GRANT data_analysts TO sarah_jones;
GRANT senior_analysts TO tom_brown;
GRANT business_analysts TO lisa_white;

-- Risk Team
GRANT risk_team TO risk_manager_1;
GRANT risk_team TO risk_analyst_1;

-- Fraud Team
GRANT fraud_team TO fraud_analyst_1;
GRANT fraud_team TO fraud_investigator_1;

-- Compliance Team
GRANT compliance_team TO compliance_officer_1;
GRANT compliance_team TO regulatory_analyst_1;

-- Finance Team
GRANT finance_team TO cfo_1;
GRANT finance_team TO finance_analyst_1;

-- Executive Team
GRANT executive_team TO ceo_1;
GRANT executive_team TO cto_1;
GRANT executive_team TO board_member_1;

-- Customer Service
GRANT relationship_managers TO rm_1;
GRANT relationship_managers TO rm_2;
GRANT branch_managers TO bm_1;
GRANT call_center TO cs_agent_1;

-- =============================================================================
-- 5. VERIFICATION QUERIES
-- =============================================================================

-- Check role assignments
SELECT 
    groname AS role_name,
    ARRAY_AGG(memid) AS members
FROM pg_roles
WHERE groname IN (
    'data_engineers', 'data_analysts', 'risk_team', 
    'fraud_team', 'compliance_team'
)
GROUP BY groname;

-- Check permissions on views
SELECT 
    schemaname,
    tablename,
    tableowner,
    hasinsertperms,
    hasselectperms,
    hasupdateperms,
    hasdeleteperms
FROM pg_tables
WHERE schemaname = 'gold'
ORDER BY tablename;
