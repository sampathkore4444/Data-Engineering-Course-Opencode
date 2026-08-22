"""
CDC Metadata Tables
Purpose: Track CDC processing state and errors
"""

-- ============================================
-- 1. CDC Metadata Schema
-- ============================================
CREATE SCHEMA IF NOT EXISTS cdc_metadata;

-- ============================================
-- 2. Processed LSN Table
-- ============================================
CREATE TABLE cdc_metadata.processed_lsn (
    table_name VARCHAR(100) PRIMARY KEY,
    last_lsn VARCHAR(50) NOT NULL,
    records_processed BIGINT DEFAULT 0,
    last_sync_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE cdc_metadata.processed_lsn IS 'Tracks the last processed Log Sequence Number for each table';

-- ============================================
-- 3. CDC Sync Log
-- ============================================
CREATE TABLE cdc_metadata.sync_log (
    sync_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    sync_type VARCHAR(20) NOT NULL,  -- 'initial', 'incremental', 'full'
    records_inserted INT DEFAULT 0,
    records_updated INT DEFAULT 0,
    records_deleted INT DEFAULT 0,
    sync_duration_ms INT,
    status VARCHAR(20) DEFAULT 'running',  -- 'running', 'completed', 'failed'
    error_message TEXT,
    started_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP
);

CREATE INDEX idx_sync_log_table_date 
ON cdc_metadata.sync_log(table_name, started_at DESC);

COMMENT ON TABLE cdc_metadata.sync_log IS 'Logs all CDC synchronization operations';

-- ============================================
-- 4. CDC Sync Errors
-- ============================================
CREATE TABLE cdc_metadata.sync_errors (
    error_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    record_id VARCHAR(100),
    operation VARCHAR(10),  -- INSERT, UPDATE, DELETE
    error_type VARCHAR(50),  -- 'constraint_violation', 'data_type', 'null_value', etc.
    error_message TEXT,
    source_data JSONB,
    error_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_sync_errors_date 
ON cdc_metadata.sync_errors(error_date DESC);

COMMENT ON TABLE cdc_metadata.sync_errors IS 'Tracks all CDC synchronization errors';

-- ============================================
-- 5. CDC Configuration
-- ============================================
CREATE TABLE cdc_metadata.cdc_config (
    config_id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    source_schema VARCHAR(50) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    target_schema VARCHAR(50) NOT NULL,
    target_table VARCHAR(100) NOT NULL,
    enabled BOOLEAN DEFAULT true,
    capture_inserts BOOLEAN DEFAULT true,
    capture_updates BOOLEAN DEFAULT true,
    capture_deletes BOOLEAN DEFAULT true,
    batch_size INT DEFAULT 1000,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE cdc_metadata.cdc_config IS 'Configuration for CDC synchronization per table';

-- ============================================
-- 6. Insert Default Configurations
-- ============================================
INSERT INTO cdc_metadata.cdc_config 
(source_schema, source_table, target_schema, target_table, enabled)
VALUES 
    ('public', 'customers', 'staging', 'stg_customers', true),
    ('public', 'accounts', 'staging', 'stg_accounts', true),
    ('public', 'transactions', 'staging', 'stg_transactions', true),
    ('cards', 'cards', 'staging', 'stg_cards', true),
    ('cards', 'card_transactions', 'staging', 'stg_card_transactions', true),
    ('loans', 'loans', 'staging', 'stg_loans', true),
    ('loans', 'loan_payments', 'staging', 'stg_loan_payments', true);

-- ============================================
-- 7. CDC Monitoring View
-- ============================================
CREATE OR REPLACE VIEW cdc_metadata.vw_cdc_monitoring AS
SELECT 
    c.table_name,
    c.source_schema || '.' || c.source_table AS source_table,
    c.target_schema || '.' || c.target_table AS target_table,
    l.last_lsn,
    l.records_processed,
    l.last_sync_at,
    (SELECT COUNT(*) 
     FROM cdc_metadata.sync_errors e 
     WHERE e.table_name = c.table_name 
       AND e.error_date = CURRENT_DATE) AS errors_today,
    CASE 
        WHEN l.last_sync_at > NOW() - INTERVAL '5 minutes' THEN '✅ Healthy'
        WHEN l.last_sync_at > NOW() - INTERVAL '1 hour' THEN '⚠️ Stale'
        ELSE '❌ Critical'
    END AS status
FROM cdc_metadata.cdc_config c
LEFT JOIN cdc_metadata.processed_lsn l ON c.table_name = l.table_name
WHERE c.enabled = true
ORDER BY l.last_sync_at DESC;

COMMENT ON VIEW cdc_metadata.vw_cdc_monitoring IS 'Real-time CDC monitoring dashboard';

-- ============================================
-- 8. CDC Performance View
-- ============================================
CREATE OR REPLACE VIEW cdc_metadata.vw_cdc_performance AS
SELECT 
    table_name,
    DATE(started_at) AS sync_date,
    COUNT(*) AS sync_count,
    SUM(records_inserted) AS total_inserts,
    SUM(records_updated) AS total_updates,
    SUM(records_deleted) AS total_deletes,
    AVG(sync_duration_ms) AS avg_duration_ms,
    MAX(sync_duration_ms) AS max_duration_ms,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_count
FROM cdc_metadata.sync_log
WHERE started_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY table_name, DATE(started_at)
ORDER BY sync_date DESC, table_name;

COMMENT ON VIEW cdc_metadata.vw_cdc_performance IS 'CDC performance metrics over time';

-- ============================================
-- 9. Function: Get Table CDC Status
-- ============================================
CREATE OR REPLACE FUNCTION cdc_metadata.get_table_cdc_status(
    p_table_name VARCHAR
)
RETURNS TABLE (
    table_name VARCHAR,
    last_lsn VARCHAR,
    minutes_since_sync BIGINT,
    status VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        l.table_name,
        l.last_lsn,
        EXTRACT(EPOCH FROM (NOW() - l.last_sync_at))/60 AS minutes_since_sync,
        CASE 
            WHEN l.last_sync_at > NOW() - INTERVAL '5 minutes' THEN 'Healthy'
            WHEN l.last_sync_at > NOW() - INTERVAL '1 hour' THEN 'Stale'
            ELSE 'Critical'
        END AS status
    FROM cdc_metadata.processed_lsn l
    WHERE l.table_name = p_table_name;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- 10. Function: Log CDC Sync
-- ============================================
CREATE OR REPLACE FUNCTION cdc_metadata.log_cdc_sync(
    p_table_name VARCHAR,
    p_sync_type VARCHAR,
    p_inserts INT,
    p_updates INT,
    p_deletes INT,
    p_duration_ms INT,
    p_status VARCHAR,
    p_error_message TEXT DEFAULT NULL
)
RETURNS INT AS $$
DECLARE
    v_sync_id INT;
BEGIN
    INSERT INTO cdc_metadata.sync_log 
    (table_name, sync_type, records_inserted, records_updated, records_deleted, 
     sync_duration_ms, status, error_message, completed_at)
    VALUES 
    (p_table_name, p_sync_type, p_inserts, p_updates, p_deletes, 
     p_duration_ms, p_status, p_error_message, NOW())
    RETURNING sync_id INTO v_sync_id;
    
    RETURN v_sync_id;
END;
$$ LANGUAGE plpgsql;
