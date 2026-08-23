-- =============================================================================
-- AUDIT LOGGING CONFIGURATION
-- =============================================================================
-- Purpose: Track and log all data access for compliance and security
-- Tool:    Dremio SQL
-- Reference: SBV Circular 39/2014
-- =============================================================================

-- =============================================================================
-- 1. CREATE AUDIT TABLES
-- =============================================================================

-- Main audit log table
CREATE TABLE IF NOT EXISTS banking_audit.access_log (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_id VARCHAR(50) NOT NULL,
    user_role VARCHAR(50),
    action VARCHAR(20) NOT NULL,  -- SELECT, INSERT, UPDATE, DELETE
    object_type VARCHAR(50),  -- TABLE, VIEW, FUNCTION
    object_schema VARCHAR(50),
    object_name VARCHAR(100),
    query_text CLOB,
    rows_affected BIGINT DEFAULT 0,
    execution_time_ms BIGINT DEFAULT 0,
    ip_address VARCHAR(50),
    user_agent VARCHAR(200),
    session_id VARCHAR(100),
    status VARCHAR(20) DEFAULT 'SUCCESS',  -- SUCCESS, FAILURE, DENIED
    error_message CLOB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data access summary (hourly aggregation)
CREATE TABLE IF NOT EXISTS banking_audit.access_summary_hourly (
    summary_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    summary_hour TIMESTAMP NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    action VARCHAR(20) NOT NULL,
    object_name VARCHAR(100),
    access_count BIGINT DEFAULT 0,
    total_rows_accessed BIGINT DEFAULT 0,
    total_execution_time_ms BIGINT DEFAULT 0,
    unique_tables_accessed BIGINT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- PII access tracking (for compliance)
CREATE TABLE IF NOT EXISTS banking_audit.pii_access_log (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_id VARCHAR(50) NOT NULL,
    user_role VARCHAR(50),
    table_name VARCHAR(100) NOT NULL,
    column_name VARCHAR(100) NOT NULL,
    action VARCHAR(20) NOT NULL,
    customer_id VARCHAR(20),  -- If applicable
    access_purpose VARCHAR(100),  -- Business reason
    data_classification VARCHAR(50),  -- PII, PHI, FINANCIAL
    ip_address VARCHAR(50),
    session_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Failed access attempts (security monitoring)
CREATE TABLE IF NOT EXISTS banking_audit.failed_access_log (
    log_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    user_id VARCHAR(50),
    attempted_action VARCHAR(20),
    attempted_object VARCHAR(100),
    failure_reason VARCHAR(100),
    ip_address VARCHAR(50),
    user_agent VARCHAR(200),
    session_id VARCHAR(100),
    retry_count INT DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- 2. CREATE AUDIT TRIGGERS
-- =============================================================================

-- Trigger for SELECT access on sensitive tables
CREATE OR REPLACE TRIGGER trg_audit_customer_access
AFTER SELECT ON banking_cleansed.core_banking_customers
FOR EACH ROW
BEGIN
    INSERT INTO banking_audit.pii_access_log (
        user_id, user_role, table_name, column_name, 
        action, customer_id, access_purpose, data_classification
    ) VALUES (
        CURRENT_USER(),
        CURRENT_ROLE(),
        'core_banking_customers',
        'ALL',
        'SELECT',
        NEW.customer_id,
        'Data access',
        'PII'
    );
END;

-- Trigger for SELECT access on card data
CREATE OR REPLACE TRIGGER trg_audit_card_access
AFTER SELECT ON banking_cleansed.credit_cards
FOR EACH ROW
BEGIN
    INSERT INTO banking_audit.pii_access_log (
        user_id, user_role, table_name, column_name,
        action, customer_id, access_purpose, data_classification
    ) VALUES (
        CURRENT_USER(),
        CURRENT_ROLE(),
        'credit_cards',
        'card_number',
        'SELECT',
        NEW.customer_id,
        'Card data access',
        'PII'
    );
END;

-- =============================================================================
-- 3. AUDIT REPORTING VIEWS
-- =============================================================================

-- Daily access summary
CREATE OR REPLACE VIEW banking_audit.daily_access_summary AS
SELECT 
    DATE(event_timestamp) AS access_date,
    user_id,
    user_role,
    action,
    COUNT(*) AS access_count,
    COUNT(DISTINCT object_name) AS unique_objects,
    SUM(rows_affected) AS total_rows,
    AVG(execution_time_ms) AS avg_execution_time,
    MAX(event_timestamp) AS last_access
FROM banking_audit.access_log
WHERE event_timestamp >= DATEADD(DAY, -7, CURRENT_DATE)
GROUP BY DATE(event_timestamp), user_id, user_role, action
ORDER BY access_date DESC, access_count DESC;

-- PII access report (for compliance)
CREATE OR REPLACE VIEW banking_audit.pii_access_report AS
SELECT 
    DATE(event_timestamp) AS access_date,
    user_id,
    user_role,
    table_name,
    column_name,
    COUNT(*) AS access_count,
    COUNT(DISTINCT customer_id) AS unique_customers_accessed,
    LISTAGG(DISTINCT access_purpose, '; ') WITHIN GROUP (ORDER BY access_purpose) AS purposes,
    MAX(event_timestamp) AS last_access
FROM banking_audit.pii_access_log
WHERE event_timestamp >= DATEADD(DAY, -30, CURRENT_DATE)
GROUP BY DATE(event_timestamp), user_id, user_role, table_name, column_name
ORDER BY access_count DESC;

-- Failed access attempts (security monitoring)
CREATE OR REPLACE VIEW banking_audit.failed_access_report AS
SELECT 
    DATE(event_timestamp) AS attempt_date,
    user_id,
    attempted_action,
    attempted_object,
    failure_reason,
    COUNT(*) AS attempt_count,
    MAX(ip_address) AS source_ip,
    MAX(user_agent) AS source_agent
FROM banking_audit.failed_access_log
WHERE event_timestamp >= DATEADD(DAY, -7, CURRENT_DATE)
GROUP BY DATE(event_timestamp), user_id, attempted_action, 
         attempted_object, failure_reason
ORDER BY attempt_count DESC;

-- High-volume access detection (potential abuse)
CREATE OR REPLACE VIEW banking_audit.high_volume_access AS
SELECT 
    user_id,
    user_role,
    DATE(event_timestamp) AS access_date,
    COUNT(*) AS daily_access_count,
    COUNT(DISTINCT object_name) AS unique_tables,
    CASE 
        WHEN COUNT(*) > 10000 THEN 'CRITICAL'
        WHEN COUNT(*) > 5000 THEN 'HIGH'
        WHEN COUNT(*) > 1000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS volume_level,
    CASE 
        WHEN COUNT(*) > 10000 THEN 'BLOCK AND INVESTIGATE'
        WHEN COUNT(*) > 5000 THEN 'REVIEW AND MONITOR'
        WHEN COUNT(*) > 1000 THEN 'MONITOR'
        ELSE 'NORMAL'
    END AS recommended_action
FROM banking_audit.access_log
WHERE event_timestamp >= DATEADD(DAY, -1, CURRENT_DATE)
GROUP BY user_id, user_role, DATE(event_timestamp)
HAVING COUNT(*) > 1000
ORDER BY daily_access_count DESC;

-- =============================================================================
-- 4. COMPLIANCE REPORTS
-- =============================================================================

-- SBV Audit Trail Report
CREATE OR REPLACE VIEW banking_audit.sbv_audit_trail AS
SELECT 
    event_timestamp,
    user_id,
    user_role,
    action,
    object_type,
    object_schema,
    object_name,
    rows_affected,
    execution_time_ms,
    ip_address,
    status,
    error_message
FROM banking_audit.access_log
WHERE event_timestamp >= DATEADD(DAY, -90, CURRENT_DATE)
  AND object_schema IN ('banking_cleansed', 'banking_gold')
ORDER BY event_timestamp DESC;

-- Data Access Compliance Report
CREATE OR REPLACE VIEW banking_audit.compliance_report AS
SELECT 
    user_id,
    user_role,
    COUNT(*) AS total_access_count,
    COUNT(CASE WHEN action = 'SELECT' THEN 1 END) AS select_count,
    COUNT(CASE WHEN action = 'INSERT' THEN 1 END) AS insert_count,
    COUNT(CASE WHEN action = 'UPDATE' THEN 1 END) AS update_count,
    COUNT(CASE WHEN action = 'DELETE' THEN 1 END) AS delete_count,
    COUNT(CASE WHEN status = 'FAILURE' THEN 1 END) AS failure_count,
    COUNT(CASE WHEN status = 'DENIED' THEN 1 END) AS denied_count,
    MIN(event_timestamp) AS first_access,
    MAX(event_timestamp) AS last_access,
    COUNT(DISTINCT object_name) AS unique_objects_accessed
FROM banking_audit.access_log
WHERE event_timestamp >= DATEADD(DAY, -30, CURRENT_DATE)
GROUP BY user_id, user_role
ORDER BY total_access_count DESC;

-- =============================================================================
-- 5. CLEANUP PROCEDURES
-- =============================================================================

-- Archive old audit logs (keep 7 years for SBV compliance)
CREATE OR REPLACE PROCEDURE banking_audit.archive_old_logs()
AS
BEGIN
    -- Move logs older than 1 year to archive
    INSERT INTO banking_audit.access_log_archive
    SELECT * FROM banking_audit.access_log
    WHERE event_timestamp < DATEADD(YEAR, -1, CURRENT_DATE);
    
    -- Delete archived logs from main table
    DELETE FROM banking_audit.access_log
    WHERE event_timestamp < DATEADD(YEAR, -1, CURRENT_DATE);
    
    -- Log archival completion
    INSERT INTO banking_audit.archival_log (
        archival_date, records_archived, status
    ) VALUES (
        CURRENT_DATE, @@ROWCOUNT, 'SUCCESS'
    );
END;

-- Monthly cleanup procedure
CREATE OR REPLACE PROCEDURE banking_audit.monthly_cleanup()
AS
BEGIN
    -- Archive logs older than 1 year
    CALL banking_audit.archive_old_logs();
    
    -- Clean up summary tables older than 90 days
    DELETE FROM banking_audit.access_summary_hourly
    WHERE summary_hour < DATEADD(DAY, -90, CURRENT_DATE);
    
    -- Log cleanup completion
    INSERT INTO banking_audit.cleanup_log (
        cleanup_date, action, status
    ) VALUES (
        CURRENT_DATE, 'monthly_cleanup', 'SUCCESS'
    );
END;
