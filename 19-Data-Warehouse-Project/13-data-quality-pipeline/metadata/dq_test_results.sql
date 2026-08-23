-- Data Quality Test Results
-- Banking Data Warehouse

-- =====================================================
-- TEST RESULTS TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS dq.test_results (
    result_id SERIAL PRIMARY KEY,
    rule_id INT NOT NULL REFERENCES dq.rule_catalog(rule_id),
    execution_id UUID NOT NULL,
    table_name VARCHAR(200) NOT NULL,
    column_name VARCHAR(200),
    rule_type VARCHAR(50) NOT NULL,
    test_sql TEXT,
    
    -- Results
    status VARCHAR(20) NOT NULL,  -- PASS, FAIL, WARN, ERROR
    total_rows BIGINT,
    passed_rows BIGINT,
    failed_rows BIGINT,
    pass_rate DECIMAL(5,2),
    
    -- Threshold comparison
    threshold_value DECIMAL(5,2),
    actual_value DECIMAL(5,2),
    threshold_met BOOLEAN,
    
    -- Details
    error_message TEXT,
    sample_failures JSONB,  -- Store sample of failed records
    
    -- Timing
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    duration_ms INT,
    
    -- Metadata
    executed_by VARCHAR(100) DEFAULT current_user,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- INDEXES
-- =====================================================
CREATE INDEX idx_test_results_rule ON dq.test_results(rule_id);
CREATE INDEX idx_test_results_table ON dq.test_results(table_name);
CREATE INDEX idx_test_results_status ON dq.test_results(status);
CREATE INDEX idx_test_results_execution ON dq.test_results(execution_id);
CREATE INDEX idx_test_results_created ON dq.test_results(created_at);

-- =====================================================
-- EXECUTION SUMMARY TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS dq.execution_summary (
    execution_id UUID PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    status VARCHAR(20) NOT NULL,  -- RUNNING, COMPLETED, FAILED
    
    -- Summary counts
    total_rules INT,
    passed_rules INT,
    failed_rules INT,
    warned_rules INT,
    error_rules INT,
    
    -- Overall score
    overall_score DECIMAL(5,2),
    
    -- Metadata
    triggered_by VARCHAR(100),
    trigger_type VARCHAR(50),  -- SCHEDULED, MANUAL, API
    environment VARCHAR(50),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- CREATE VIEW FOR RECENT RESULTS
-- =====================================================
CREATE OR REPLACE VIEW dq.vw_recent_results AS
SELECT 
    r.result_id,
    e.execution_id,
    c.rule_name,
    c.rule_type,
    c.table_name,
    c.column_name,
    c.severity,
    c.action,
    r.status,
    r.total_rows,
    r.passed_rows,
    r.failed_rows,
    r.pass_rate,
    r.threshold_value,
    r.actual_value,
    r.threshold_met,
    r.error_message,
    r.sample_failures,
    r.started_at,
    r.completed_at,
    r.duration_ms
FROM dq.test_results r
JOIN dq.rule_catalog c ON r.rule_id = c.rule_id
JOIN dq.execution_summary e ON r.execution_id = e.execution_id
WHERE r.created_at >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY r.created_at DESC;

-- =====================================================
-- CREATE VIEW FOR FAILED TESTS
-- =====================================================
CREATE OR REPLACE VIEW dq.vw_failed_tests AS
SELECT 
    c.rule_name,
    c.rule_type,
    c.table_name,
    c.column_name,
    c.severity,
    c.action,
    c.owner,
    r.status,
    r.total_rows,
    r.failed_rows,
    r.pass_rate,
    r.error_message,
    r.sample_failures,
    r.completed_at
FROM dq.test_results r
JOIN dq.rule_catalog c ON r.rule_id = c.rule_id
WHERE r.status = 'FAIL'
  AND r.created_at >= CURRENT_DATE - INTERVAL '1 day'
ORDER BY 
    CASE c.severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        WHEN 'LOW' THEN 4 
    END;

COMMENT ON TABLE dq.test_results IS 'Results of data quality test executions';
COMMENT ON TABLE dq.execution_summary IS 'Summary of each DQ pipeline execution';
