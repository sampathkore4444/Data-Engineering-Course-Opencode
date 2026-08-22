-- Data Quality Scores
-- Banking Data Warehouse

-- =====================================================
-- QUALITY SCORES TABLE (Time Series)
-- =====================================================
CREATE TABLE IF NOT EXISTS dq.quality_scores (
    score_id SERIAL PRIMARY KEY,
    score_date DATE NOT NULL,
    score_hour INT DEFAULT 0,
    
    -- Dimension scores (0-100)
    completeness_score DECIMAL(5,2),
    uniqueness_score DECIMAL(5,2),
    accuracy_score DECIMAL(5,2),
    consistency_score DECIMAL(5,2),
    timeliness_score DECIMAL(5,2),
    validity_score DECIMAL(5,2),
    integrity_score DECIMAL(5,2),
    
    -- Overall score
    overall_score DECIMAL(5,2),
    
    -- Grade
    grade VARCHAR(20),  -- EXCELLENT, GOOD, POOR, CRITICAL
    
    -- Scope
    table_name VARCHAR(200),  -- NULL for overall score
    schema_name VARCHAR(100),
    
    -- Details
    total_checks INT,
    passed_checks INT,
    failed_checks INT,
    
    -- Metadata
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Unique constraint
    UNIQUE(score_date, score_hour, table_name)
);

-- =====================================================
-- TABLE-LEVEL SCORES
-- =====================================================
CREATE TABLE IF NOT EXISTS dq.table_quality_scores (
    score_id SERIAL PRIMARY KEY,
    score_date DATE NOT NULL,
    table_name VARCHAR(200) NOT NULL,
    
    -- Scores by dimension
    completeness_score DECIMAL(5,2),
    uniqueness_score DECIMAL(5,2),
    freshness_score DECIMAL(5,2),
    accuracy_score DECIMAL(5,2),
    
    -- Overall table score
    overall_score DECIMAL(5,2),
    grade VARCHAR(20),
    
    -- Details
    total_checks INT,
    passed_checks INT,
    failed_checks INT,
    critical_failures INT,
    
    -- Trend
    previous_score DECIMAL(5,2),
    score_change DECIMAL(5,2),
    trend VARCHAR(20),  -- IMPROVING, STABLE, DEGRADING
    
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(score_date, table_name)
);

-- =====================================================
-- INDEXES
-- =====================================================
CREATE INDEX idx_quality_scores_date ON dq.quality_scores(score_date);
CREATE INDEX idx_quality_scores_table ON dq.quality_scores(table_name);
CREATE INDEX idx_quality_scores_grade ON dq.quality_scores(grade);
CREATE INDEX idx_table_scores_date ON dq.table_quality_scores(score_date);
CREATE INDEX idx_table_scores_table ON dq.table_quality_scores(table_name);

-- =====================================================
-- CREATE VIEW FOR QUALITY DASHBOARD
-- =====================================================
CREATE OR REPLACE VIEW dq.vw_quality_dashboard AS
SELECT 
    s.score_date,
    s.overall_score,
    s.grade,
    s.completeness_score,
    s.uniqueness_score,
    s.accuracy_score,
    s.consistency_score,
    s.timeliness_score,
    s.total_checks,
    s.passed_checks,
    s.failed_checks,
    -- Trend
    LAG(s.overall_score) OVER (ORDER BY s.score_date) as previous_day_score,
    s.overall_score - LAG(s.overall_score) OVER (ORDER BY s.score_date) as score_change
FROM dq.quality_scores s
WHERE s.table_name IS NULL  -- Overall scores only
ORDER BY s.score_date DESC
LIMIT 30;

-- =====================================================
-- CREATE VIEW FOR TABLE QUALITY RANKING
-- =====================================================
CREATE OR REPLACE VIEW dq.vw_table_quality_ranking AS
SELECT 
    t.table_name,
    t.overall_score,
    t.grade,
    t.completeness_score,
    t.uniqueness_score,
    t.freshness_score,
    t.total_checks,
    t.failed_checks,
    t.critical_failures,
    t.trend,
    t.score_change,
    t.calculated_at
FROM dq.table_quality_scores t
WHERE t.score_date = CURRENT_DATE
ORDER BY t.overall_score ASC;

-- =====================================================
-- CREATE VIEW FOR QUALITY TRENDS
-- =====================================================
CREATE OR REPLACE VIEW dq.vw_quality_trends AS
SELECT 
    score_date,
    overall_score,
    grade,
    completeness_score,
    uniqueness_score,
    accuracy_score,
    consistency_score,
    timeliness_score
FROM dq.quality_scores
WHERE table_name IS NULL
  AND score_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY score_date;

COMMENT ON TABLE dq.quality_scores IS 'Time series of data quality scores';
COMMENT ON TABLE dq.table_quality_scores IS 'Quality scores by table';
