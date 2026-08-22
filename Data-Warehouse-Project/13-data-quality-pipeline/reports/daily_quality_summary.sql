-- Daily Quality Summary Report
-- Banking Data Warehouse

-- =====================================================
-- EXECUTION SUMMARY
-- =====================================================
SELECT 
    '=== EXECUTION SUMMARY ===' as section,
    e.execution_id,
    e.started_at,
    e.completed_at,
    EXTRACT(EPOCH FROM (e.completed_at - e.started_at)) / 60 as duration_minutes,
    e.status,
    e.total_rules,
    e.passed_rules,
    e.failed_rules,
    e.overall_score,
    CASE 
        WHEN e.overall_score >= 99 THEN '✅ EXCELLENT'
        WHEN e.overall_score >= 95 THEN '⚠️ GOOD'
        WHEN e.overall_score >= 90 THEN '⚠️ POOR'
        ELSE '❌ CRITICAL'
    END as quality_status
FROM dq.execution_summary e
WHERE e.started_at >= CURRENT_DATE
ORDER BY e.started_at DESC
LIMIT 1;

-- =====================================================
-- QUALITY SCORES BY TABLE
-- =====================================================
SELECT 
    '=== QUALITY SCORES BY TABLE ===' as section,
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
    t.trend
FROM dq.table_quality_scores t
WHERE t.score_date = CURRENT_DATE
ORDER BY t.overall_score ASC;

-- =====================================================
-- FAILED TESTS DETAIL
-- =====================================================
SELECT 
    '=== FAILED TESTS ===' as section,
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
  AND r.created_at >= CURRENT_DATE
ORDER BY 
    CASE c.severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        WHEN 'LOW' THEN 4 
    END;

-- =====================================================
-- CRITICAL ANOMALIES
-- =====================================================
SELECT 
    '=== CRITICAL ANOMALIES ===' as section,
    a.anomaly_id,
    a.detected_at,
    a.table_name,
    a.column_name,
    a.anomaly_type,
    a.severity,
    a.status,
    a.description,
    a.affected_rows,
    a.sample_records
FROM dq.anomalies a
WHERE a.severity = 'CRITICAL'
  AND a.detected_at >= CURRENT_DATE
ORDER BY a.detected_at DESC;

-- =====================================================
-- QUALITY SCORE TREND (Last 7 days)
-- =====================================================
SELECT 
    '=== QUALITY TREND (7 Days) ===' as section,
    score_date,
    overall_score,
    grade,
    completeness_score,
    uniqueness_score,
    timeliness_score,
    total_checks,
    failed_checks,
    overall_score - LAG(overall_score) OVER (ORDER BY score_date) as daily_change
FROM dq.quality_scores
WHERE table_name IS NULL
  AND score_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY score_date DESC;

-- =====================================================
-- TOP ISSUES BY OWNER
-- =====================================================
SELECT 
    '=== TOP ISSUES BY OWNER ===' as section,
    c.owner,
    COUNT(*) as total_failures,
    SUM(CASE WHEN c.severity = 'CRITICAL' THEN 1 ELSE 0 END) as critical_count,
    SUM(CASE WHEN c.severity = 'HIGH' THEN 1 ELSE 0 END) as high_count,
    STRING_AGG(DISTINCT c.table_name, ', ') as affected_tables
FROM dq.test_results r
JOIN dq.rule_catalog c ON r.rule_id = c.rule_id
WHERE r.status = 'FAIL'
  AND r.created_at >= CURRENT_DATE
GROUP BY c.owner
ORDER BY total_failures DESC;

-- =====================================================
-- RECOMMENDATIONS
-- =====================================================
SELECT 
    '=== RECOMMENDATIONS ===' as section,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM dq.test_results r
            JOIN dq.rule_catalog c ON r.rule_id = c.rule_id
            WHERE r.status = 'FAIL' AND c.severity = 'CRITICAL'
              AND r.created_at >= CURRENT_DATE
        ) THEN '🔴 CRITICAL: Immediate action required for critical failures'
        ELSE '✅ No critical failures'
    END as recommendation_1,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM dq.anomalies
            WHERE severity = 'CRITICAL' AND status = 'OPEN'
              AND detected_at >= CURRENT_DATE
        ) THEN '🔴 CRITICAL: Investigate open critical anomalies'
        ELSE '✅ No open critical anomalies'
    END as recommendation_2,
    CASE 
        WHEN (
            SELECT overall_score FROM dq.quality_scores
            WHERE table_name IS NULL AND score_date = CURRENT_DATE
        ) < 95 THEN '⚠️ WARNING: Quality score below 95%, investigate root causes'
        ELSE '✅ Quality score is healthy'
    END as recommendation_3;
