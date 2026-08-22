-- Data Lineage Visualization Views
-- Banking Data Warehouse

-- =====================================================
-- VIEW 1: COMPLETE LINEAGE (All connections)
-- =====================================================
CREATE OR REPLACE VIEW lineage.vw_complete_lineage AS
SELECT 
    -- Source
    sn.node_name as source_node,
    sn.node_type as source_type,
    sn.node_category as source_category,
    sn.table_name as source_table,
    sn.column_name as source_column,
    sn.source_system,
    sn.is_pii as source_is_pii,
    
    -- Edge
    e.edge_type,
    e.transformation_type,
    e.transformation_logic,
    e.process_name,
    e.process_type,
    e.lineage_level,
    
    -- Target
    tn.node_name as target_node,
    tn.node_type as target_type,
    tn.node_category as target_category,
    tn.table_name as target_table,
    tn.column_name as target_column,
    tn.is_pii as target_is_pii
    
FROM lineage.edges e
JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
WHERE e.status = 'ACTIVE'
  AND sn.status = 'ACTIVE'
  AND tn.status = 'ACTIVE'
ORDER BY e.lineage_level, sn.table_name, tn.table_name;

-- =====================================================
-- VIEW 2: UPSTREAM LINEAGE (What feeds this table?)
-- =====================================================
CREATE OR REPLACE VIEW lineage.vw_upstream_lineage AS
WITH RECURSIVE upstream AS (
    -- Start with target table
    SELECT 
        n.node_id,
        n.node_name,
        n.table_name,
        n.column_name,
        0 as depth,
        ARRAY[n.node_name] as path
    FROM lineage.nodes n
    WHERE n.status = 'ACTIVE'
    
    UNION ALL
    
    -- Recursively find upstream nodes
    SELECT 
        sn.node_id,
        sn.node_name,
        sn.table_name,
        sn.column_name,
        u.depth + 1,
        u.path || sn.node_name
    FROM lineage.edges e
    JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
    JOIN upstream u ON e.target_node_id = u.node_id
    WHERE e.status = 'ACTIVE'
      AND sn.status = 'ACTIVE'
      AND u.depth < 10  -- Prevent infinite loops
      AND NOT sn.node_name = ANY(u.path)  -- Prevent cycles
)
SELECT 
    u.node_name,
    u.table_name,
    u.column_name,
    u.depth,
    u.path,
    CASE 
        WHEN u.depth = 0 THEN 'TARGET'
        WHEN u.depth = 1 THEN 'DIRECT SOURCE'
        ELSE 'INDIRECT SOURCE (depth ' || u.depth || ')'
    END as relationship_type
FROM upstream u
ORDER BY u.depth, u.table_name;

-- =====================================================
-- VIEW 3: DOWNSTREAM LINEAGE (What does this table feed?)
-- =====================================================
CREATE OR REPLACE VIEW lineage.vw_downstream_lineage AS
WITH RECURSIVE downstream AS (
    -- Start with source table
    SELECT 
        n.node_id,
        n.node_name,
        n.table_name,
        n.column_name,
        0 as depth,
        ARRAY[n.node_name] as path
    FROM lineage.nodes n
    WHERE n.status = 'ACTIVE'
    
    UNION ALL
    
    -- Recursively find downstream nodes
    SELECT 
        tn.node_id,
        tn.node_name,
        tn.table_name,
        tn.column_name,
        d.depth + 1,
        d.path || tn.node_name
    FROM lineage.edges e
    JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
    JOIN downstream d ON e.source_node_id = d.node_id
    WHERE e.status = 'ACTIVE'
      AND tn.status = 'ACTIVE'
      AND d.depth < 10
      AND NOT tn.node_name = ANY(d.path)
)
SELECT 
    d.node_name,
    d.table_name,
    d.column_name,
    d.depth,
    d.path,
    CASE 
        WHEN d.depth = 0 THEN 'SOURCE'
        WHEN d.depth = 1 THEN 'DIRECT TARGET'
        ELSE 'INDIRECT TARGET (depth ' || d.depth || ')'
    END as relationship_type
FROM downstream d
ORDER BY d.depth, d.table_name;

-- =====================================================
-- VIEW 4: IMPACT ANALYSIS (What breaks if we change this?)
-- =====================================================
CREATE OR REPLACE VIEW lineage.vw_impact_analysis AS
SELECT 
    sn.table_name as source_table,
    sn.column_name as source_column,
    e.transformation_type,
    e.process_name,
    tn.table_name as target_table,
    tn.column_name as target_column,
    e.edge_type,
    e.lineage_level,
    CASE 
        WHEN e.lineage_level = 'COLUMN' THEN 'COLUMN-LEVEL CHANGE REQUIRED'
        WHEN e.lineage_level = 'TABLE' THEN 'TABLE-LEVEL CHANGE REQUIRED'
        ELSE 'MANUAL REVIEW REQUIRED'
    END as change_requirement,
    CASE 
        WHEN tn.is_pii = TRUE THEN '⚠️ PII DATA - SPECIAL HANDLING REQUIRED'
        ELSE 'Standard change'
    END as pii_warning
FROM lineage.edges e
JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
WHERE e.status = 'ACTIVE'
  AND sn.status = 'ACTIVE'
  AND tn.status = 'ACTIVE'
ORDER BY 
    CASE WHEN e.lineage_level = 'COLUMN' THEN 1 ELSE 2 END,
    sn.table_name,
    tn.table_name;

-- =====================================================
-- VIEW 5: PII DATA FLOW (Where does PII data go?)
-- =====================================================
CREATE OR REPLACE VIEW lineage.vw_pii_data_flow AS
SELECT 
    sn.table_name as source_table,
    sn.column_name as source_column,
    sn.source_system,
    e.edge_type,
    e.transformation_type,
    e.process_name,
    tn.table_name as target_table,
    tn.column_name as target_column,
    e.lineage_level,
    CASE 
        WHEN tn.is_pii = TRUE THEN '⚠️ PII PROPAGATED'
        ELSE '✅ PII NOT PROPAGATED'
    END as pii_status
FROM lineage.edges e
JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
WHERE e.status = 'ACTIVE'
  AND sn.is_pii = TRUE
ORDER BY sn.table_name, tn.table_name;

-- =====================================================
-- VIEW 6: LINEAGE SUMMARY BY TABLE
-- =====================================================
CREATE OR REPLACE VIEW lineage.vw_lineage_summary AS
SELECT 
    n.table_name,
    n.schema_name,
    n.database_name,
    n.node_type,
    COUNT(DISTINCT e_in.edge_id) as inbound_edges,
    COUNT(DISTINCT e_out.edge_id) as outbound_edges,
    COUNT(DISTINCT CASE WHEN e_in.lineage_level = 'COLUMN' THEN e_in.edge_id END) as column_level_in,
    COUNT(DISTINCT CASE WHEN e_out.lineage_level = 'COLUMN' THEN e_out.edge_id END) as column_level_out,
    COUNT(DISTINCT sn.source_system) as source_systems,
    BOOL_OR(n.is_pii) as contains_pii
FROM lineage.nodes n
LEFT JOIN lineage.edges e_in ON n.node_id = e_in.target_node_id AND e_in.status = 'ACTIVE'
LEFT JOIN lineage.edges e_out ON n.node_id = e_out.source_node_id AND e_out.status = 'ACTIVE'
LEFT JOIN lineage.nodes sn ON e_in.source_node_id = sn.node_id
WHERE n.status = 'ACTIVE'
GROUP BY n.table_name, n.schema_name, n.database_name, n.node_type
ORDER BY n.table_name;

-- =====================================================
-- VIEW 7: LINEAGE GRAPH (For visualization tools)
-- =====================================================
CREATE OR REPLACE VIEW lineage.vw_lineage_graph AS
-- Nodes
SELECT 
    'node' as element_type,
    n.node_id as id,
    n.node_name as label,
    n.node_type as type,
    n.node_category as category,
    n.source_system,
    n.is_pii,
    NULL as source,
    NULL as target,
    NULL as edge_type
FROM lineage.nodes n
WHERE n.status = 'ACTIVE'

UNION ALL

-- Edges
SELECT 
    'edge' as element_type,
    e.edge_id as id,
    e.edge_type as label,
    e.transformation_type as type,
    e.lineage_level as category,
    NULL as source_system,
    NULL as is_pii,
    sn.node_name as source,
    tn.node_name as target,
    e.edge_type as edge_type
FROM lineage.edges e
JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
WHERE e.status = 'ACTIVE'
  AND sn.status = 'ACTIVE'
  AND tn.status = 'ACTIVE';

COMMENT ON VIEW lineage.vw_complete_lineage IS 'Complete lineage showing all source-to-target connections';
COMMENT ON VIEW lineage.vw_upstream_lineage IS 'Recursive upstream lineage (what feeds a table)';
COMMENT ON VIEW lineage.vw_downstream_lineage IS 'Recursive downstream lineage (what a table feeds)';
COMMENT ON VIEW lineage.vw_impact_analysis IS 'Impact analysis for schema changes';
COMMENT ON VIEW lineage.vw_pii_data_flow IS 'Tracks flow of Personally Identifiable Information';
COMMENT ON VIEW lineage.vw_lineage_summary IS 'Summary of lineage by table';
COMMENT ON VIEW lineage.vw_lineage_graph IS 'Graph format for visualization tools';
