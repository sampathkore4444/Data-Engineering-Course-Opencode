-- Root Cause Analysis Queries
-- Banking Data Warehouse

-- =====================================================
-- ROOT CAUSE ANALYSIS 1: Where did this data come from?
-- =====================================================
-- Usage: Trace back the source of specific data

WITH RECURSIVE source_trace AS (
    -- Start with the problematic data
    SELECT 
        n.node_id,
        n.node_name,
        n.table_name,
        n.column_name,
        n.source_system,
        0 as trace_depth,
        ARRAY[n.node_name] as source_path
    FROM lineage.nodes n
    WHERE n.table_name = 'gold.dim_customer'  -- CHANGE THIS
      AND n.column_name = 'email'  -- CHANGE COLUMN
      AND n.status = 'ACTIVE'
    
    UNION ALL
    
    -- Trace back to source
    SELECT 
        sn.node_id,
        sn.node_name,
        sn.table_name,
        sn.column_name,
        sn.source_system,
        st.trace_depth + 1,
        st.source_path || sn.node_name
    FROM lineage.edges e
    JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
    JOIN source_trace st ON e.target_node_id = st.node_id
    WHERE e.status = 'ACTIVE'
      AND sn.status = 'ACTIVE'
      AND st.trace_depth < 10
)
SELECT 
    trace_depth,
    node_name,
    table_name,
    column_name,
    source_system,
    source_path,
    CASE 
        WHEN trace_depth = 0 THEN '🎯 PROBLEMATIC DATA'
        WHEN trace_depth = 1 THEN '📍 DIRECT SOURCE'
        WHEN trace_depth = 2 THEN '📍 ROOT SOURCE'
        ELSE '📍 ORIGIN'
    END as source_type
FROM source_trace
ORDER BY trace_depth DESC;

-- =====================================================
-- ROOT CAUSE ANALYSIS 2: Transformation Chain
-- =====================================================
-- What transformations were applied to this data?

SELECT 
    e.process_name,
    e.process_type,
    e.edge_type,
    e.transformation_type,
    e.transformation_logic,
    e.transformation_sql,
    sn.table_name as from_table,
    sn.column_name as from_column,
    tn.table_name as to_table,
    tn.column_name as to_column,
    ROW_NUMBER() OVER (ORDER BY e.edge_id) as transformation_step
FROM lineage.edges e
JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
WHERE tn.table_name = 'gold.dim_customer'  -- CHANGE THIS
  AND tn.column_name = 'email'  -- CHANGE COLUMN
  AND e.status = 'ACTIVE'
  AND e.lineage_level = 'COLUMN'
ORDER BY e.edge_id;

-- =====================================================
-- ROOT CAUSE ANALYSIS 3: Data Quality Issue Trace
-- =====================================================
-- When we find bad data, trace it back to the source

-- Example: Find where NULL emails came from
SELECT 
    'STAGING' as layer,
    'stg_customers' as table_name,
    'email' as column_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as null_count,
    ROUND(SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
FROM staging.stg_customers

UNION ALL

SELECT 
    'SOURCE' as layer,
    'customers' as table_name,
    'email' as column_name,
    COUNT(*) as total_rows,
    SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) as null_count,
    ROUND(SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
FROM core_banking.customers;

-- =====================================================
-- ROOT CAUSE ANALYSIS 4: Schema Change Impact
-- =====================================================
-- What changed in the source that affected downstream?

SELECT 
    sn.table_name as source_table,
    sn.column_name as source_column,
    sn.source_system,
    e.edge_type,
    e.process_name,
    e.process_type,
    tn.table_name as target_table,
    tn.column_name as target_column,
    e.transformation_logic,
    CASE 
        WHEN e.edge_type = 'TRANSFORMED' THEN '⚠️ TRANSFORMATION APPLIED'
        WHEN e.edge_type = 'DIRECT' THEN '✅ DIRECT MAPPING'
        ELSE '❓ UNKNOWN'
    END as mapping_type
FROM lineage.edges e
JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
WHERE sn.source_system = 'CORE_BANKING'  -- CHANGE SOURCE SYSTEM
  AND e.status = 'ACTIVE'
ORDER BY sn.table_name, tn.table_name;

-- =====================================================
-- ROOT CAUSE ANALYSIS 5: Data Lineage for Specific Record
-- =====================================================
-- Trace a specific record through the pipeline

-- Example: Trace customer CUST-001
WITH record_trace AS (
    SELECT 
        'SOURCE' as layer,
        'core_banking.customers' as table_name,
        'customer_id' as key_column,
        'CUST-001' as key_value,
        email as source_email,
        NULL as transformed_email,
        NULL as final_email
    FROM core_banking.customers
    WHERE customer_id = 'CUST-001'
    
    UNION ALL
    
    SELECT 
        'STAGING' as layer,
        'staging.stg_customers' as table_name,
        'customer_id' as key_column,
        'CUST-001' as key_value,
        NULL as source_email,
        email as transformed_email,
        NULL as final_email
    FROM staging.stg_customers
    WHERE customer_id = 'CUST-001'
    
    UNION ALL
    
    SELECT 
        'GOLD' as layer,
        'gold.dim_customer' as table_name,
        'customer_id' as key_column,
        'CUST-001' as key_value,
        NULL as source_email,
        NULL as transformed_email,
        email as final_email
    FROM gold.dim_customer
    WHERE customer_id = 'CUST-001'
)
SELECT 
    layer,
    table_name,
    key_column,
    key_value,
    source_email,
    transformed_email,
    final_email,
    CASE 
        WHEN layer = 'SOURCE' THEN source_email
        WHEN layer = 'STAGING' THEN transformed_email
        WHEN layer = 'GOLD' THEN final_email
    END as email_value
FROM record_trace
ORDER BY 
    CASE layer 
        WHEN 'SOURCE' THEN 1 
        WHEN 'STAGING' THEN 2 
        WHEN 'GOLD' THEN 3 
    END;

-- =====================================================
-- ROOT CAUSE ANALYSIS 6: Column-Level Lineage Report
-- =====================================================
-- Complete lineage for a specific column

SELECT 
    -- Source
    sn.database_name || '.' || sn.schema_name || '.' || sn.table_name as source_location,
    sn.column_name as source_column,
    sn.source_system,
    sn.data_type as source_type,
    sn.is_pii as source_pii,
    
    -- Transformation
    e.edge_type,
    e.transformation_type,
    e.transformation_logic,
    e.transformation_sql,
    e.process_name,
    
    -- Target
    tn.database_name || '.' || tn.schema_name || '.' || tn.table_name as target_location,
    tn.column_name as target_column,
    tn.data_type as target_type,
    tn.is_pii as target_pii
    
FROM lineage.edges e
JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
WHERE tn.table_name = 'gold.dim_customer'  -- CHANGE THIS
  AND tn.column_name = 'email'  -- CHANGE COLUMN
  AND e.status = 'ACTIVE'
  AND e.lineage_level = 'COLUMN'
ORDER BY e.edge_id;
