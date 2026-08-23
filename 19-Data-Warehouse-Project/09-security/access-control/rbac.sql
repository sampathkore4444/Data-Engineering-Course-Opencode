-- Role-Based Access Control (RBAC) Setup
-- Banking Data Warehouse

-- Create roles
CREATE ROLE dw_admin WITH LOGIN PASSWORD 'secure_password' CREATEDB;
CREATE ROLE dw_etl WITH LOGIN PASSWORD 'secure_password';
CREATE ROLE dw_analyst WITH LOGIN PASSWORD 'secure_password';
CREATE ROLE dw_viewer WITH LOGIN PASSWORD 'secure_password';
CREATE ROLE dw_auditor WITH LOGIN PASSWORD 'secure_password';

-- Grant schema usage
GRANT USAGE ON SCHEMA staging TO dw_etl;
GRANT USAGE ON SCHEMA gold TO dw_analyst;
GRANT USAGE ON SCHEMA gold TO dw_viewer;
GRANT USAGE ON SCHEMA audit TO dw_auditor;

-- ETL permissions (write to staging, read/write to gold)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO dw_etl;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA gold TO dw_etl;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA staging TO dw_etl;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA gold TO dw_etl;

-- Analyst permissions (read-only on gold)
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO dw_analyst;

-- Viewer permissions (read-only on specific tables)
GRANT SELECT ON gold.dim_customer TO dw_viewer;
GRANT SELECT ON gold.dim_account TO dw_viewer;
GRANT SELECT ON gold.fact_transactions TO dw_viewer;

-- Auditor permissions (read on gold + audit)
GRANT SELECT ON ALL TABLES IN SCHEMA gold TO dw_auditor;
GRANT SELECT ON ALL TABLES IN SCHEMA audit TO dw_auditor;

-- Admin: Full access
GRANT ALL PRIVILEGES ON DATABASE banking_dw TO dw_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO dw_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA gold TO dw_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA audit TO dw_admin;
