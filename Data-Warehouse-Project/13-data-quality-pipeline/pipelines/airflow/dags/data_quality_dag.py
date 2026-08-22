"""
Data Quality Pipeline - Airflow DAG
Banking Data Warehouse

This DAG orchestrates the data quality pipeline.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
import psycopg2
import uuid
from datetime import datetime, timedelta

# =====================================================
# DEFAULT ARGS
# =====================================================
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-team@bank.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# =====================================================
# DAG DEFINITION
# =====================================================
dag = DAG(
    'data_quality_pipeline',
    default_args=default_args,
    description='Data Quality Pipeline for Banking DW',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['data-quality', 'banking', 'critical'],
    max_active_runs=1,
)

# =====================================================
# HELPER FUNCTIONS
# =====================================================
def create_execution_summary(**context):
    """Create execution summary record."""
    execution_id = str(uuid.uuid4())
    context['ti'].xcom_push(key='execution_id', value=execution_id)
    
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='banking_dw',
        user='dw_admin',
        password='secure_password'
    )
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO dq.execution_summary 
        (execution_id, started_at, status, triggered_by, trigger_type)
        VALUES (%s, %s, 'RUNNING', 'airflow', 'SCHEDULED')
    """, (execution_id, datetime.now()))
    
    conn.commit()
    conn.close()
    
    return execution_id


def run_staging_checks(**context):
    """Run checks on staging tables."""
    execution_id = context['ti'].xcom_pull(key='execution_id')
    
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='banking_dw',
        user='dw_admin',
        password='secure_password'
    )
    cursor = conn.cursor()
    
    checks = [
        # (table, column, rule_type, check_sql, threshold)
        ('staging.stg_customers', 'customer_id', 'uniqueness', 
         "SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT customer_id) FROM staging.stg_customers", 100),
        ('staging.stg_customers', 'customer_id', 'completeness',
         "SELECT COUNT(*), SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) FROM staging.stg_customers", 100),
        ('staging.stg_accounts', 'account_id', 'uniqueness',
         "SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT account_id) FROM staging.stg_accounts", 100),
        ('staging.stg_transactions', 'transaction_id', 'uniqueness',
         "SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT transaction_id) FROM staging.stg_transactions", 100),
    ]
    
    results = []
    for table, column, rule_type, sql, threshold in checks:
        try:
            cursor.execute(sql)
            result = cursor.fetchone()
            total = result[0]
            issues = result[1]
            pass_rate = ((total - issues) / total * 100) if total > 0 else 100
            status = 'PASS' if pass_rate >= threshold else 'FAIL'
            
            # Get rule_id
            cursor.execute("""
                SELECT rule_id FROM dq.rule_catalog 
                WHERE table_name = %s AND column_name = %s AND rule_type = %s
            """, (table, column, rule_type))
            rule_result = cursor.fetchone()
            rule_id = rule_result[0] if rule_result else None
            
            # Store result
            cursor.execute("""
                INSERT INTO dq.test_results 
                (rule_id, execution_id, table_name, column_name, rule_type, 
                 test_sql, status, total_rows, passed_rows, failed_rows, 
                 pass_rate, threshold_value, actual_value, threshold_met,
                 started_at, completed_at, duration_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (rule_id, execution_id, table, column, rule_type, sql, 
                  status, total, total - issues, issues, pass_rate, threshold,
                  pass_rate, pass_rate >= threshold, datetime.now(), datetime.now(), 0))
            
            results.append({'table': table, 'status': status, 'pass_rate': pass_rate})
            
        except Exception as e:
            print(f"Error checking {table}.{column}: {str(e)}")
            results.append({'table': table, 'status': 'ERROR', 'error': str(e)})
    
    conn.commit()
    conn.close()
    
    return results


def run_gold_checks(**context):
    """Run checks on gold tables."""
    execution_id = context['ti'].xcom_pull(key='execution_id')
    
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='banking_dw',
        user='dw_admin',
        password='secure_password'
    )
    cursor = conn.cursor()
    
    checks = [
        ('gold.dim_customer', 'customer_sk', 'uniqueness',
         "SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT customer_sk) FROM gold.dim_customer", 100),
        ('gold.dim_customer', 'customer_id', 'uniqueness',
         "SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT customer_id) FROM gold.dim_customer", 100),
        ('gold.fact_transactions', 'transaction_id', 'uniqueness',
         "SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT transaction_id) FROM gold.fact_transactions", 100),
    ]
    
    results = []
    for table, column, rule_type, sql, threshold in checks:
        try:
            cursor.execute(sql)
            result = cursor.fetchone()
            total = result[0]
            issues = result[1]
            pass_rate = ((total - issues) / total * 100) if total > 0 else 100
            status = 'PASS' if pass_rate >= threshold else 'FAIL'
            
            cursor.execute("""
                SELECT rule_id FROM dq.rule_catalog 
                WHERE table_name = %s AND column_name = %s AND rule_type = %s
            """, (table, column, rule_type))
            rule_result = cursor.fetchone()
            rule_id = rule_result[0] if rule_result else None
            
            cursor.execute("""
                INSERT INTO dq.test_results 
                (rule_id, execution_id, table_name, column_name, rule_type,
                 test_sql, status, total_rows, passed_rows, failed_rows,
                 pass_rate, threshold_value, actual_value, threshold_met,
                 started_at, completed_at, duration_ms)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (rule_id, execution_id, table, column, rule_type, sql,
                  status, total, total - issues, issues, pass_rate, threshold,
                  pass_rate, pass_rate >= threshold, datetime.now(), datetime.now(), 0))
            
            results.append({'table': table, 'status': status, 'pass_rate': pass_rate})
            
        except Exception as e:
            print(f"Error checking {table}.{column}: {str(e)}")
            results.append({'table': table, 'status': 'ERROR', 'error': str(e)})
    
    conn.commit()
    conn.close()
    
    return results


def calculate_scores(**context):
    """Calculate and store quality scores."""
    execution_id = context['ti'].xcom_pull(key='execution_id')
    
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='banking_dw',
        user='dw_admin',
        password='secure_password'
    )
    cursor = conn.cursor()
    
    # Get test results
    cursor.execute("""
        SELECT rule_type, status, pass_rate
        FROM dq.test_results
        WHERE execution_id = %s
    """, (execution_id,))
    
    results = cursor.fetchall()
    
    # Calculate scores
    completeness = [r[2] for r in results if r[0] == 'completeness']
    uniqueness = [r[2] for r in results if r[0] == 'uniqueness']
    freshness = [100 if r[1] == 'PASS' else 0 for r in results if r[0] == 'freshness']
    
    avg_completeness = sum(completeness) / len(completeness) if completeness else 100
    avg_uniqueness = sum(uniqueness) / len(uniqueness) if uniqueness else 100
    avg_freshness = sum(freshness) / len(freshness) if freshness else 100
    
    overall_score = (avg_completeness + avg_uniqueness + avg_freshness) / 3
    
    grade = 'EXCELLENT' if overall_score >= 99 else \
            'GOOD' if overall_score >= 95 else \
            'POOR' if overall_score >= 90 else 'CRITICAL'
    
    # Store scores
    cursor.execute("""
        INSERT INTO dq.quality_scores 
        (score_date, score_hour, completeness_score, uniqueness_score,
         timeliness_score, overall_score, grade, total_checks, passed_checks, failed_checks)
        VALUES (CURRENT_DATE, EXTRACT(HOUR FROM CURRENT_TIMESTAMP), %s, %s, %s, %s, %s, %s, %s, %s)
    """, (avg_completeness, avg_uniqueness, avg_freshness, overall_score, grade,
          len(results), sum(1 for r in results if r[1] == 'PASS'),
          sum(1 for r in results if r[1] != 'PASS')))
    
    conn.commit()
    conn.close()
    
    return {
        'overall_score': overall_score,
        'grade': grade,
        'completeness': avg_completeness,
        'uniqueness': avg_uniqueness,
        'freshness': avg_freshness
    }


def update_execution_summary(**context):
    """Update execution summary with final status."""
    execution_id = context['ti'].xcom_pull(key='execution_id')
    
    conn = psycopg2.connect(
        host='localhost',
        port=5432,
        database='banking_dw',
        user='dw_admin',
        password='secure_password'
    )
    cursor = conn.cursor()
    
    # Get summary counts
    cursor.execute("""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed,
            SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed
        FROM dq.test_results
        WHERE execution_id = %s
    """, (execution_id,))
    
    counts = cursor.fetchone()
    
    cursor.execute("""
        SELECT overall_score FROM dq.quality_scores
        WHERE score_date = CURRENT_DATE
        ORDER BY calculated_at DESC LIMIT 1
    """)
    score_result = cursor.fetchone()
    overall_score = score_result[0] if score_result else 0
    
    cursor.execute("""
        UPDATE dq.execution_summary 
        SET completed_at = %s, status = 'COMPLETED',
            total_rules = %s, passed_rules = %s, failed_rules = %s,
            overall_score = %s
        WHERE execution_id = %s
    """, (datetime.now(), counts[0], counts[1], counts[2], overall_score, execution_id))
    
    conn.commit()
    conn.close()


# =====================================================
# TASK DEFINITIONS
# =====================================================
task_create_summary = PythonOperator(
    task_id='create_execution_summary',
    python_callable=create_execution_summary,
    dag=dag,
)

task_staging_checks = PythonOperator(
    task_id='run_staging_checks',
    python_callable=run_staging_checks,
    dag=dag,
)

task_gold_checks = PythonOperator(
    task_id='run_gold_checks',
    python_callable=run_gold_checks,
    dag=dag,
)

task_calculate_scores = PythonOperator(
    task_id='calculate_scores',
    python_callable=calculate_scores,
    dag=dag,
)

task_update_summary = PythonOperator(
    task_id='update_execution_summary',
    python_callable=update_execution_summary,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE,
)

# =====================================================
# TASK DEPENDENCIES
# =====================================================
task_create_summary >> [task_staging_checks, task_gold_checks]
[task_staging_checks, task_gold_checks] >> task_calculate_scores
task_calculate_scores >> task_update_summary
