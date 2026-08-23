-- Impact Analysis Queries
-- Banking Data Warehouse

-- =====================================================
-- IMPACT ANALYSIS 1: What breaks if we change this table?
-- =====================================================
-- Usage: Replace 'gold.dim_customer' with the table you want to change

WITH RECURSIVE impact_chain AS (
    -- Find all direct dependents
    SELECT 
        tn.node_id,
        tn.node_name,
        tn.table_name,
        tn.column_name,
        e.edge_type,
        e.lineage_level,
        1 as impact_depth,
        ARRAY[sn.node_name] as affected_by
    FROM lineage.edges e
    JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
    JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
    WHERE sn.table_name = 'gold.dim_customer'  -- CHANGE THIS
      AND e.status = 'ACTIVE'
      AND sn.status = 'ACTIVE'
      AND tn.status = 'ACTIVE'
    
    UNION ALL
    
    -- Recursively find downstream dependents
    SELECT 
        tn.node_id,
        tn.node_name,
        tn.table_name,
        tn.column_name,
        e.edge_type,
        e.lineage_level,
        ic.impact_depth + 1,
        ic.affected_by || sn.node_name
    FROM lineage.edges e
    JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
    JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
    JOIN impact_chain ic ON sn.node_id = ic.node_id
    WHERE e.status = 'ACTIVE'
      AND sn.status = 'ACTIVE'
      AND tn.status = 'ACTIVE'
      AND ic.impact_depth < 10
)
SELECT 
    impact_depth,
    node_name,
    table_name,
    column_name,
    edge_type,
    lineage_level,
    affected_by,
    CASE 
        WHEN impact_depth = 1 THEN '🔴 DIRECT IMPACT'
        WHEN impact_depth = 2 THEN '🟡 INDIRECT IMPACT'
        ELSE '🟢 CASCADING IMPACT'
    END as impact_level
FROM impact_chain
ORDER BY impact_depth, table_name;

-- =====================================================
-- IMPACT ANALYSIS 2: Impact on Reports/Dashboards
-- =====================================================
SELECT 
    tn.table_name as affected_report,
    tn.column_name as affected_column,
    e.edge_type,
    e.transformation_logic,
    sn.source_system,
    sn.is_pii,
    CASE 
        WHEN sn.is_pii = TRUE THEN '⚠️ PII DATA AFFECTED'
        ELSE 'Standard column'
    END as pii_warning
FROM lineage.edges e
JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
WHERE sn.table_name = 'gold.dim_customer'  -- CHANGE THIS
  AND e.status = 'ACTIVE'
  AND tn.node_category IN ('VIEW', 'REPORT', 'DASHBOARD')
ORDER BY tn.table_name;

-- =====================================================
-- IMPACT ANALYSIS 3: Column Change Impact
-- =====================================================
-- What happens if we change this specific column?

SELECT 
    sn.table_name as source_table,
    sn.column_name as source_column,
    e.edge_type,
    e.transformation_type,
    e.transformation_logic,
    e.process_name,
    tn.table_name as target_table,
    tn.column_name as target_column,
    tn.is_pii as target_is_pii,
    CASE 
        WHEN e.lineage_level = 'COLUMN' THEN 'COLUMN-LEVEL CHANGE REQUIRED'
        ELSE 'TABLE-LEVEL CHANGE REQUIRED'
    END as change_type
FROM lineage.edges e
JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
WHERE sn.table_name = 'gold.dim_customer'  -- CHANGE TABLE
  AND sn.column_name = 'email'  -- CHANGE COLUMN
  AND e.status = 'ACTIVE'
ORDER BY tn.table_name;

-- =====================================================
-- IMPACT ANALYSIS 4: Process Impact
-- =====================================================
-- Which ETL jobs/processes are affected?

SELECT 
    e.process_name,
    e.process_type,
    COUNT(DISTINCT sn.table_name) as affected_source_tables,
    COUNT(DISTINCT tn.table_name) as affected_target_tables,
    STRING_AGG(DISTINCT sn.table_name, ', ') as source_tables,
    STRING_AGG(DISTINCT tn.table_name, ', ') as target_tables,
    BOOL_OR(sn.is_pii) as involves_pii
FROM lineage.edges e
JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
WHERE sn.table_name = 'gold.dim_customer'  -- CHANGE THIS
  AND e.status = 'ACTIVE'
GROUP BY e.process_name, e.process_type
ORDER BY affected_target_tables DESC;

-- =====================================================
-- IMPACT ANALYSIS 5: Risk Assessment
-- =====================================================
SELECT 
    sn.table_name as changed_table,
    sn.column_name as changed_column,
    COUNT(DISTINCT tn.table_name) as downstream_tables_count,
    COUNT(DISTINCT e.process_name) as affected_processes,
    SUM(CASE WHEN tn.is_pii = TRUE THEN 1 ELSE 0 END) as pii_affected_count,
    MAX(e.lineage_level) as max_lineage_level,
    CASE 
        WHEN COUNT(DISTINCT tn.table_name) > 10 THEN '🔴 HIGH RISK'
        WHEN COUNT(DISTINCT tn.table_name) > 5 THEN '🟡 MEDIUM RISK'
        ELSE '🟢 LOW RISK'
    END as risk_level,
    CASE 
        WHEN SUM(CASE WHEN tn.is_pii = TRUE THEN 1 ELSE 0 END) > 0 THEN '⚠️ PII COMPLIANCE REVIEW REQUIRED'
        ELSE '✅ No PII impact'
    END as compliance_requirement
FROM lineage.edges e
JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
WHERE sn.table_name = 'gold.dim_customer'  -- CHANGE THIS
  AND e.status = 'ACTIVE'
GROUP BY sn.table_name, sn.column_name;
