-- Data Quality Anomalies
-- Banking Data Warehouse

-- =====================================================
-- ANOMALIES TABLE
-- =====================================================
CREATE TABLE IF NOT EXISTS dq.anomalies (
    anomaly_id SERIAL PRIMARY KEY,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Anomaly details
    table_name VARCHAR(200) NOT NULL,
    column_name VARCHAR(200),
    anomaly_type VARCHAR(50) NOT NULL,  -- STATISTICAL, VOLUME, PATTERN, BUSINESS_RULE
    
    -- Severity
    severity VARCHAR(20) NOT NULL,  -- CRITICAL, HIGH, MEDIUM, LOW
    status VARCHAR(20) DEFAULT 'OPEN',  -- OPEN, INVESTIGATING, RESOLVED, IGNORED
    
    -- Description
    description TEXT NOT NULL,
    root_cause TEXT,
    
    -- Metrics
    expected_value TEXT,
    actual_value TEXT,
    deviation DECIMAL(10,4),
    zscore DECIMAL(10,4),
    
    -- Affected records
    affected_rows INT,
    sample_records JSONB,
    
    -- Resolution
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(100),
    resolution_notes TEXT,
    
    -- Metadata
    detected_by VARCHAR(100) DEFAULT 'DQ_PIPELINE',
    rule_id INT REFERENCES dq.rule_catalog(rule_id),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- ANOMALY HISTORY TABLE (for tracking changes)
-- =====================================================
CREATE TABLE IF NOT EXISTS dq.anomaly_history (
    history_id SERIAL PRIMARY KEY,
    anomaly_id INT NOT NULL REFERENCES dq.anomalies(anomaly_id),
    action VARCHAR(50) NOT NULL,  -- CREATED, UPDATED, RESOLVED, IGNORED
    old_status VARCHAR(20),
    new_status VARCHAR(20),
    changed_by VARCHAR(100),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT
);

-- =====================================================
-- ANOMALY PATTERNS TABLE (for recurring issues)
-- =====================================================
CREATE TABLE IF NOT EXISTS dq.anomaly_patterns (
    pattern_id SERIAL PRIMARY KEY,
    pattern_name VARCHAR(200) NOT NULL,
    description TEXT,
    table_name VARCHAR(200),
    anomaly_type VARCHAR(50),
    
    -- Pattern detection
    recurrence_count INT DEFAULT 1,
    first_detected TIMESTAMP,
    last_detected TIMESTAMP,
    avg_recurrence_days DECIMAL(5,2),
    
    -- Status
    status VARCHAR(20) DEFAULT 'ACTIVE',  -- ACTIVE, RESOLVED, IGNORED
    assigned_to VARCHAR(100),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- INDEXES
-- =====================================================
CREATE INDEX idx_anomalies_table ON dq.anomalies(table_name);
CREATE INDEX idx_anomalies_type ON dq.anomalies(anomaly_type);
CREATE INDEX idx_anomalies_severity ON dq.anomalies(severity);
CREATE INDEX idx_anomalies_status ON dq.anomalies(status);
CREATE INDEX idx_anomalies_detected ON dq.anomalies(detected_at);
CREATE INDEX idx_anomaly_history_anomaly ON dq.anomaly_history(anomaly_id);
CREATE INDEX idx_anomaly_patterns_table ON dq.anomaly_patterns(table_name);

-- =====================================================
-- CREATE VIEW FOR OPEN ANOMALIES
-- =====================================================
CREATE OR REPLACE VIEW dq.vw_open_anomalies AS
SELECT 
    a.anomaly_id,
    a.detected_at,
    a.table_name,
    a.column_name,
    a.anomaly_type,
    a.severity,
    a.status,
    a.description,
    a.expected_value,
    a.actual_value,
    a.deviation,
    a.affected_rows,
    a.sample_records,
    c.rule_name,
    c.owner,
    a.detected_by
FROM dq.anomalies a
LEFT JOIN dq.rule_catalog c ON a.rule_id = c.rule_id
WHERE a.status IN ('OPEN', 'INVESTIGATING')
ORDER BY 
    CASE a.severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        WHEN 'LOW' THEN 4 
    END,
    a.detected_at DESC;

-- =====================================================
-- CREATE VIEW FOR ANOMALY SUMMARY
-- =====================================================
CREATE OR REPLACE VIEW dq.vw_anomaly_summary AS
SELECT 
    DATE(detected_at) as detection_date,
    table_name,
    anomaly_type,
    severity,
    COUNT(*) as anomaly_count,
    SUM(affected_rows) as total_affected_rows,
    MIN(detected_at) as first_detected,
    MAX(detected_at) as last_detected
FROM dq.anomalies
WHERE detected_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(detected_at), table_name, anomaly_type, severity
ORDER BY detection_date DESC, anomaly_count DESC;

-- =====================================================
-- CREATE VIEW FOR ANOMALY TRENDS
-- =====================================================
CREATE OR REPLACE VIEW dq.vw_anomaly_trends AS
SELECT 
    DATE(detected_at) as detection_date,
    COUNT(*) as total_anomalies,
    SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical_count,
    SUM(CASE WHEN severity = 'HIGH' THEN 1 ELSE 0 END) as high_count,
    SUM(CASE WHEN severity = 'MEDIUM' THEN 1 ELSE 0 END) as medium_count,
    SUM(CASE WHEN severity = 'LOW' THEN 1 ELSE 0 END) as low_count,
    SUM(CASE WHEN status = 'RESOLVED' THEN 1 ELSE 0 END) as resolved_count,
    SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) as open_count
FROM dq.anomalies
WHERE detected_at >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(detected_at)
ORDER BY detection_date DESC;

-- =====================================================
-- TRIGGER TO UPDATE UPDATED_AT
-- =====================================================
CREATE OR REPLACE FUNCTION dq.update_anomaly_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_anomaly_timestamp
BEFORE UPDATE ON dq.anomalies
FOR EACH ROW
EXECUTE FUNCTION dq.update_anomaly_timestamp();

-- =====================================================
-- TRIGGER TO LOG STATUS CHANGES
-- =====================================================
CREATE OR REPLACE FUNCTION dq.log_anomaly_status_change()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.status != NEW.status THEN
        INSERT INTO dq.anomaly_history (anomaly_id, action, old_status, new_status, changed_by)
        VALUES (NEW.anomaly_id, 'UPDATED', OLD.status, NEW.status, current_user);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_log_anomaly_status_change
AFTER UPDATE ON dq.anomalies
FOR EACH ROW
EXECUTE FUNCTION dq.log_anomaly_status_change();

COMMENT ON TABLE dq.anomalies IS 'Detected data quality anomalies';
COMMENT ON TABLE dq.anomaly_history IS 'History of anomaly status changes';
COMMENT ON TABLE dq.anomaly_patterns IS 'Recurring anomaly patterns';
