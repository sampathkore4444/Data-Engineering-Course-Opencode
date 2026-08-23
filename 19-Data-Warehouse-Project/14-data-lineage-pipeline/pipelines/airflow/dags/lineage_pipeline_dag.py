"""
Data Lineage Pipeline - Airflow DAG
Banking Data Warehouse

This DAG extracts and maintains data lineage metadata.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import psycopg2
import json

# =====================================================
# DEFAULT ARGS
# =====================================================
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-team@bank.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# =====================================================
# DAG DEFINITION
# =====================================================
dag = DAG(
    'data_lineage_pipeline',
    default_args=default_args,
    description='Data Lineage Pipeline for Banking DW',
    schedule_interval='0 7 * * *',  # Daily at 7 AM (after DQ pipeline)
    start_date=days_ago(1),
    catchup=False,
    tags=['lineage', 'banking', 'metadata'],
    max_active_runs=1,
)

# =====================================================
# HELPER FUNCTIONS
# =====================================================
def extract_table_lineage(**context):
    """Extract table-level lineage from SQL queries."""
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='banking_dw',
        user='dw_admin',
        password='secure_password'
    )
    cursor = conn.cursor()
    
    # Get all active nodes
    cursor.execute("""
        SELECT node_id, node_name, table_name, column_name, node_type
        FROM lineage.nodes
        WHERE status = 'ACTIVE'
    """)
    nodes = {row[1]: row for row in cursor.fetchall()}
    
    # Get all active edges
    cursor.execute("""
        SELECT edge_id, source_node_id, target_node_id, edge_type, lineage_level
        FROM lineage.edges
        WHERE status = 'ACTIVE'
    """)
    edges = cursor.fetchall()
    
    # Log extraction results
    print(f"Extracted {len(nodes)} nodes and {len(edges)} edges")
    
    # Push metrics to XCom
    context['ti'].xcom_push(key='nodes_count', value=len(nodes))
    context['ti'].xcom_push(key='edges_count', value=len(edges))
    
    conn.close()
    return {'nodes': len(nodes), 'edges': len(edges)}


def extract_column_lineage(**context):
    """Extract column-level lineage from column mappings."""
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='banking_dw',
        user='dw_admin',
        password='secure_password'
    )
    cursor = conn.cursor()
    
    # Get column mappings
    cursor.execute("""
        SELECT COUNT(*) as mapping_count
        FROM lineage.column_mapping
        WHERE status = 'ACTIVE'
    """)
    mapping_count = cursor.fetchone()[0]
    
    # Get PII columns
    cursor.execute("""
        SELECT COUNT(*) as pii_count
        FROM lineage.column_mapping
        WHERE is_pii = TRUE AND status = 'ACTIVE'
    """)
    pii_count = cursor.fetchone()[0]
    
    print(f"Extracted {mapping_count} column mappings, {pii_count} PII columns")
    
    context['ti'].xcom_push(key='column_mappings', value=mapping_count)
    context['ti'].xcom_push(key='pii_columns', value=pii_count)
    
    conn.close()
    return {'mappings': mapping_count, 'pii': pii_count}


def validate_lineage(**context):
    """Validate lineage integrity."""
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='banking_dw',
        user='dw_admin',
        password='secure_password'
    )
    cursor = conn.cursor()
    
    # Check for orphan nodes (no edges)
    cursor.execute("""
        SELECT COUNT(*) as orphan_count
        FROM lineage.nodes n
        WHERE n.status = 'ACTIVE'
          AND NOT EXISTS (
              SELECT 1 FROM lineage.edges e 
              WHERE (e.source_node_id = n.node_id OR e.target_node_id = n.node_id)
                AND e.status = 'ACTIVE'
          )
    """)
    orphan_count = cursor.fetchone()[0]
    
    # Check for broken edges (referencing inactive nodes)
    cursor.execute("""
        SELECT COUNT(*) as broken_count
        FROM lineage.edges e
        JOIN lineage.nodes sn ON e.source_node_id = sn.node_id
        JOIN lineage.nodes tn ON e.target_node_id = tn.node_id
        WHERE e.status = 'ACTIVE'
          AND (sn.status != 'ACTIVE' OR tn.status != 'ACTIVE')
    """)
    broken_count = cursor.fetchone()[0]
    
    # Check for circular references
    cursor.execute("""
        WITH RECURSIVE cycle_check AS (
            SELECT 
                source_node_id,
                target_node_id,
                ARRAY[source_node_id, target_node_id] as path,
                1 as depth
            FROM lineage.edges
            WHERE status = 'ACTIVE'
            
            UNION ALL
            
            SELECT 
                e.source_node_id,
                e.target_node_id,
                cc.path || e.target_node_id,
                cc.depth + 1
            FROM lineage.edges e
            JOIN cycle_check cc ON e.source_node_id = cc.target_node_id
            WHERE e.status = 'ACTIVE'
              AND cc.depth < 10
              AND NOT e.target_node_id = ANY(cc.path)
        )
        SELECT COUNT(*) as cycle_count
        FROM cycle_check
        WHERE source_node_id = target_node_id
    """)
    cycle_count = cursor.fetchone()[0]
    
    print(f"Validation: {orphan_count} orphan nodes, {broken_count} broken edges, {cycle_count} cycles")
    
    context['ti'].xcom_push(key='orphan_nodes', value=orphan_count)
    context['ti'].xcom_push(key='broken_edges', value=broken_count)
    context['ti'].xcom_push(key='cycles', value=cycle_count)
    
    # Fail if critical issues found
    if broken_count > 0 or cycle_count > 0:
        raise ValueError(f"Lineage validation failed: {broken_count} broken edges, {cycle_count} cycles")
    
    conn.close()
    return {'orphans': orphan_count, 'broken': broken_count, 'cycles': cycle_count}


def generate_lineage_report(**context):
    """Generate lineage summary report."""
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='banking_dw',
        user='dw_admin',
        password='secure_password'
    )
    cursor = conn.cursor()
    
    # Get lineage summary
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT n.node_id) as total_nodes,
            COUNT(DISTINCT e.edge_id) as total_edges,
            COUNT(DISTINCT CASE WHEN n.node_type = 'SOURCE' THEN n.node_id END) as source_nodes,
            COUNT(DISTINCT CASE WHEN n.node_type = 'TARGET' THEN n.node_id END) as target_nodes,
            COUNT(DISTINCT CASE WHEN n.is_pii = TRUE THEN n.node_id END) as pii_nodes,
            COUNT(DISTINCT e.process_name) as processes
        FROM lineage.nodes n
        LEFT JOIN lineage.edges e ON n.node_id = e.source_node_id OR n.node_id = e.target_node_id
        WHERE n.status = 'ACTIVE'
    """)
    summary = cursor.fetchone()
    
    # Get top source systems
    cursor.execute("""
        SELECT 
            source_system,
            COUNT(*) as table_count
        FROM lineage.nodes
        WHERE source_system IS NOT NULL
          AND status = 'ACTIVE'
        GROUP BY source_system
        ORDER BY table_count DESC
    """)
    source_systems = cursor.fetchall()
    
    report = {
        'total_nodes': summary[0],
        'total_edges': summary[1],
        'source_nodes': summary[2],
        'target_nodes': summary[3],
        'pii_nodes': summary[4],
        'processes': summary[5],
        'source_systems': {row[0]: row[1] for row in source_systems}
    }
    
    print(f"Lineage Report: {json.dumps(report, indent=2)}")
    
    context['ti'].xcom_push(key='lineage_report', value=report)
    
    conn.close()
    return report


# =====================================================
# TASK DEFINITIONS
# =====================================================
task_extract_table_lineage = PythonOperator(
    task_id='extract_table_lineage',
    python_callable=extract_table_lineage,
    dag=dag,
)

task_extract_column_lineage = PythonOperator(
    task_id='extract_column_lineage',
    python_callable=extract_column_lineage,
    dag=dag,
)

task_validate_lineage = PythonOperator(
    task_id='validate_lineage',
    python_callable=validate_lineage,
    dag=dag,
)

task_generate_report = PythonOperator(
    task_id='generate_lineage_report',
    python_callable=generate_lineage_report,
    dag=dag,
)

# =====================================================
# TASK DEPENDENCIES
# =====================================================
[task_extract_table_lineage, task_extract_column_lineage] >> task_validate_lineage
task_validate_lineage >> task_generate_report
