-- Data Quality Dashboard Views
-- Banking Data Warehouse

-- =====================================================
-- MAIN DASHBOARD VIEW
-- =====================================================
CREATE OR REPLACE VIEW dq.vw_quality_main_dashboard AS
WITH latest_scores AS (
    SELECT 
        overall_score,
        grade,
        completeness_score,
        uniqueness_score,
        accuracy_score,
        consistency_score,
        timeliness_score,
        total_checks,
        passed_checks,
        failed_checks
    FROM dq.quality_scores
    WHERE table_name IS NULL
      AND score_date = CURRENT_DATE
),
recent_anomalies AS (
    SELECT 
        COUNT(*) as total_anomalies,
        SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical_anomalies,
        SUM(CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END) as open_anomalies
    FROM dq.anomalies
    WHERE detected_at >= CURRENT_DATE
),
recent_tests AS (
    SELECT 
        COUNT(DISTINCT execution_id) as total_executions,
        SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed_tests,
        SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed_tests,
        AVG(duration_ms) as avg_duration_ms
    FROM dq.test_results
    WHERE created_at >= CURRENT_DATE
)
SELECT 
    -- Current Status
    COALESCE(s.overall_score, 0) as current_quality_score,
    COALESCE(s.grade, 'UNKNOWN') as quality_grade,
    s.completeness_score,
    s.uniqueness_score,
    s.accuracy_score,
    s.consistency_score,
    s.timeliness_score,
    
    -- Test Summary
    COALESCE(t.total_executions, 0) as tests_executed,
    COALESCE(t.passed_tests, 0) as tests_passed,
    COALESCE(t.failed_tests, 0) as tests_failed,
    ROUND(COALESCE(t.avg_duration_ms, 0) / 1000.0, 2) as avg_test_duration_sec,
    
    -- Anomaly Summary
    COALESCE(a.total_anomalies, 0) as anomalies_detected,
    COALESCE(a.critical_anomalies, 0) as critical_anomalies,
    COALESCE(a.open_anomalies, 0) as open_anomalies,
    
    -- Health Status
    CASE 
        WHEN COALESCE(s.overall_score, 0) >= 99 THEN '✅ HEALTHY'
        WHEN COALESCE(s.overall_score, 0) >= 95 THEN '⚠️ WARNING'
        WHEN COALESCE(s.overall_score, 0) >= 90 THEN '⚠️ DEGRADED'
        ELSE '❌ CRITICAL'
    END as system_health,
    
    CURRENT_TIMESTAMP as last_updated
FROM latest_scores s
CROSS JOIN recent_anomalies a
CROSS JOIN recent_tests t;

-- =====================================================
-- TABLE QUALITY DASHBOARD
-- =====================================================
CREATE OR REPLACE VIEW dq.vw_table_quality_dashboard AS
SELECT 
    t.table_name,
    t.overall_score,
    t.grade,
    t.completeness_score,
    t.uniqueness_score,
    t.freshness_score,
    t.total_checks,
    t.passed_checks,
    t.failed_checks,
    t.critical_failures,
    t.trend,
    t.score_change,
    
    -- Status indicator
    CASE 
        WHEN t.critical_failures > 0 THEN '🔴 CRITICAL'
        WHEN t.failed_checks > 0 THEN '🟡 WARNING'
        ELSE '🟢 HEALTHY'
    END as status,
    
    -- Last test time
    (SELECT MAX(completed_at) 
     FROM dq.test_results r 
     JOIN dq.rule_catalog c ON r.rule_id = c.rule_id
     WHERE c.table_name = t.table_name) as last_tested,
    
    t.calculated_at
FROM dq.table_quality_scores t
WHERE t.score_date = CURRENT_DATE
ORDER BY t.overall_score ASC;

-- =====================================================
-- QUALITY TREND DASHBOARD
-- =====================================================
CREATE OR REPLACE VIEW dq.vw_quality_trend_dashboard AS
SELECT 
    score_date,
    overall_score,
    grade,
    completeness_score,
    uniqueness_score,
    accuracy_score,
    consistency_score,
    timeliness_score,
    total_checks,
    failed_checks,
    
    -- Rolling averages
    AVG(overall_score) OVER (
        ORDER BY score_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as rolling_7day_avg,
    
    -- Week over week change
    overall_score - LAG(overall_score, 7) OVER (ORDER BY score_date) as wow_change,
    
    -- Month over month change
    overall_score - LAG(overall_score, 30) OVER (ORDER BY score_date) as mom_change
    
FROM dq.quality_scores
WHERE table_name IS NULL
  AND score_date >= CURRENT_DATE - INTERVAL '90 days'
ORDER BY score_date DESC;

-- =====================================================
-- CRITICAL FAILURES DASHBOARD
-- =====================================================
CREATE OR REPLACE VIEW dq.vw_critical_failures_dashboard AS
SELECT 
    c.table_name,
    c.rule_name,
    c.rule_type,
    c.severity,
    c.owner,
    r.status,
    r.total_rows,
    r.failed_rows,
    r.pass_rate,
    r.error_message,
    r.sample_failures,
    r.completed_at,
    
    -- Time since failure
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - r.completed_at)) / 3600 as hours_since_failure,
    
    -- Escalation needed?
    CASE 
        WHEN c.severity = 'CRITICAL' AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - r.completed_at)) / 3600 > 1 
        THEN 'YES - ESCALATE'
        WHEN c.severity = 'HIGH' AND EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - r.completed_at)) / 3600 > 4 
        THEN 'YES - ESCALATE'
        ELSE 'NO'
    END as escalation_needed
    
FROM dq.test_results r
JOIN dq.rule_catalog c ON r.rule_id = c.rule_id
WHERE r.status = 'FAIL'
  AND c.severity IN ('CRITICAL', 'HIGH')
  AND r.created_at >= CURRENT_DATE - INTERVAL '1 day'
ORDER BY 
    CASE c.severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
    END,
    r.completed_at DESC;

-- =====================================================
-- DAILY QUALITY REPORT VIEW
-- =====================================================
CREATE OR REPLACE VIEW dq.vw_daily_quality_report AS
SELECT 
    -- Header
    CURRENT_DATE as report_date,
    'Banking Data Warehouse' as system_name,
    
    -- Overall Score
    (SELECT overall_score FROM dq.quality_scores 
     WHERE table_name IS NULL AND score_date = CURRENT_DATE) as overall_quality_score,
    
    -- Test Summary
    (SELECT COUNT(DISTINCT execution_id) FROM dq.test_results 
     WHERE created_at >= CURRENT_DATE) as tests_executed,
    (SELECT SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) FROM dq.test_results 
     WHERE created_at >= CURRENT_DATE) as tests_passed,
    (SELECT SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) FROM dq.test_results 
     WHERE created_at >= CURRENT_DATE) as tests_failed,
    
    -- Anomaly Summary
    (SELECT COUNT(*) FROM dq.anomalies 
     WHERE detected_at >= CURRENT_DATE) as anomalies_detected,
    (SELECT SUM(CASE WHEN severity = 'CRITICAL' THEN 1 ELSE 0 END) FROM dq.anomalies 
     WHERE detected_at >= CURRENT_DATE) as critical_anomalies,
    
    -- Top Issues
    (SELECT STRING_AGG(table_name || ': ' || rule_name, ', ')
     FROM dq.test_results r
     JOIN dq.rule_catalog c ON r.rule_id = c.rule_id
     WHERE r.status = 'FAIL'
       AND r.created_at >= CURRENT_DATE
       AND c.severity = 'CRITICAL'
     LIMIT 5) as top_critical_issues,
    
    CURRENT_TIMESTAMP as generated_at;

COMMENT ON VIEW dq.vw_quality_main_dashboard IS 'Main quality dashboard with current status';
COMMENT ON VIEW dq.vw_table_quality_dashboard IS 'Quality scores by table';
COMMENT ON VIEW dq.vw_quality_trend_dashboard IS 'Quality trends over time';
COMMENT ON VIEW dq.vw_critical_failures_dashboard IS 'Critical failures requiring attention';
COMMENT ON VIEW dq.vw_daily_quality_report IS 'Daily quality summary report';
