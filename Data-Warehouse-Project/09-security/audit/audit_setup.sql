-- Audit Logging Setup
-- Banking Data Warehouse

-- Create audit schema
CREATE SCHEMA IF NOT EXISTS audit;

-- Create audit log table
CREATE TABLE audit.access_log (
    log_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    operation VARCHAR(10) NOT NULL,
    user_name VARCHAR(100) NOT NULL,
    client_ip INET,
    query_text TEXT,
    row_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster queries
CREATE INDEX idx_access_log_table ON audit.access_log(table_name);
CREATE INDEX idx_access_log_user ON audit.access_log(user_name);
CREATE INDEX idx_access_log_created ON audit.access_log(created_at);

-- Create audit trigger function
CREATE OR REPLACE FUNCTION audit.log_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO audit.access_log (table_name, operation, user_name, query_text)
    VALUES (TG_TABLE_NAME, TG_OP, current_user, current_query());
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach triggers to important tables
CREATE TRIGGER audit_dim_customer
AFTER INSERT OR UPDATE OR DELETE ON gold.dim_customer
FOR EACH ROW EXECUTE FUNCTION audit.log_trigger_function();

CREATE TRIGGER audit_fact_transactions
AFTER INSERT OR UPDATE OR DELETE ON gold.fact_transactions
FOR EACH ROW EXECUTE FUNCTION audit.log_trigger_function();

-- Create audit report view
CREATE OR REPLACE VIEW audit.daily_access_report AS
SELECT 
    table_name,
    operation,
    user_name,
    COUNT(*) as access_count,
    MIN(created_at) as first_access,
    MAX(created_at) as last_access
FROM audit.access_log
WHERE created_at >= CURRENT_DATE
GROUP BY table_name, operation, user_name
ORDER BY access_count DESC;
