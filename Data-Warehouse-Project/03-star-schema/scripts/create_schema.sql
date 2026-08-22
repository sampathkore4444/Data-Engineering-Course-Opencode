-- Create schema for Star Schema
-- Banking Data Warehouse

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

-- Grant permissions
GRANT USAGE ON SCHEMA staging TO dw_etl;
GRANT USAGE ON SCHEMA gold TO dw_analyst;
GRANT USAGE ON SCHEMA audit TO dw_auditor;

-- Set default search path
ALTER DATABASE banking_dw SET search_path TO gold, staging, public;
