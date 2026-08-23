"""
Extract Source Data Pipeline - With Error Handling & Logging
Purpose: Extract from OLTP databases to Staging with robust error handling
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
import logging
import traceback

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

class ETLError(Exception):
    """Custom exception for ETL pipeline errors"""
    def __init__(self, message, table_name=None, error_type=None):
        self.message = message
        self.table_name = table_name
        self.error_type = error_type
        super().__init__(self.message)

def extract_table_with_retry(source_conn, target_conn, table_name, query, insert_query):
    """
    Extract data from source with retry logic and error handling.
    
    Args:
        source_conn: Source database connection
        target_conn: Target database connection
        table_name: Name of the table being extracted
        query: SQL query to extract data
        insert_query: SQL query to insert data
    
    Returns:
        int: Number of records extracted
    """
    try:
        logger.info(f"Starting extraction for table: {table_name}")
        
        # Extract from source
        records = source_conn.get_records(query)
        record_count = len(records)
        logger.info(f"Extracted {record_count} records from {table_name}")
        
        if record_count == 0:
            logger.warning(f"No records found for {table_name}")
            return 0
        
        # Load to target with batch processing
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, record_count, batch_size):
            batch = records[i:i + batch_size]
            
            try:
                target_conn.run(insert_query, parameters=batch)
                total_inserted += len(batch)
                logger.debug(f"Inserted batch {i//batch_size + 1} for {table_name}")
            except Exception as batch_error:
                logger.error(f"Batch insert failed for {table_name}: {batch_error}")
                raise ETLError(
                    f"Batch insert failed: {batch_error}",
                    table_name=table_name,
                    error_type="BATCH_INSERT_ERROR"
                )
        
        logger.info(f"Successfully extracted {total_inserted} records for {table_name}")
        return total_inserted
        
    except ETLError:
        raise
    except Exception as e:
        error_msg = f"Extraction failed for {table_name}: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        raise ETLError(
            error_msg,
            table_name=table_name,
            error_type="EXTRACTION_ERROR"
        )

def extract_customers():
    """Extract customers from core_banking database"""
    source_hook = PostgresHook(postgres_conn_id='source_core_banking')
    dw_hook = PostgresHook(postgres_conn_id='data_warehouse')
    
    source_conn = source_hook.get_conn()
    target_conn = dw_hook.get_conn()
    
    try:
        query = "SELECT customer_id, customer_name, customer_type, phone, email, created_at, updated_at FROM customers"
        insert_query = """
            INSERT INTO staging.stg_customers 
            (customer_id, customer_name, customer_type, phone, email, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                customer_type = EXCLUDED.customer_type,
                phone = EXCLUDED.phone,
                email = EXCLUDED.email,
                updated_at = EXCLUDED.updated_at
        """
        
        count = extract_table_with_retry(source_conn, target_conn.cursor(), 'customers', query, insert_query)
        return count
        
    finally:
        source_conn.close()
        target_conn.close()

def extract_accounts():
    """Extract accounts from core_banking database"""
    source_hook = PostgresHook(postgres_conn_id='source_core_banking')
    dw_hook = PostgresHook(postgres_conn_id='data_warehouse')
    
    source_conn = source_hook.get_conn()
    target_conn = dw_hook.get_conn()
    
    try:
        query = "SELECT account_id, customer_id, account_type, balance, status, branch_id, created_at, updated_at FROM accounts"
        insert_query = """
            INSERT INTO staging.stg_accounts 
            (account_id, customer_id, account_type, balance, status, branch_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (account_id) DO UPDATE SET
                balance = EXCLUDED.balance,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at
        """
        
        count = extract_table_with_retry(source_conn, target_conn.cursor(), 'accounts', query, insert_query)
        return count
        
    finally:
        source_conn.close()
        target_conn.close()

def extract_transactions():
    """Extract transactions from core_banking database"""
    source_hook = PostgresHook(postgres_conn_id='source_core_banking')
    dw_hook = PostgresHook(postgres_conn_id='data_warehouse')
    
    source_conn = source_hook.get_conn()
    target_conn = dw_hook.get_conn()
    
    try:
        query = "SELECT transaction_id, account_id, transaction_type, amount, balance_after, description, transaction_date, created_at FROM transactions"
        insert_query = """
            INSERT INTO staging.stg_transactions 
            (transaction_id, account_id, transaction_type, amount, balance_after, description, transaction_date, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
        """
        
        count = extract_table_with_retry(source_conn, target_conn.cursor(), 'transactions', query, insert_query)
        return count
        
    finally:
        source_conn.close()
        target_conn.close()

def validate_extraction():
    """Validate that extraction completed successfully"""
    hook = PostgresHook(postgres_conn_id='data_warehouse')
    
    # Check record counts
    checks = [
        ("staging.stg_customers", "customer_id"),
        ("staging.stg_accounts", "account_id"),
        ("staging.stg_transactions", "transaction_id"),
    ]
    
    for table, key_column in checks:
        query = f"SELECT COUNT(*) FROM {table}"
        count = hook.get_first(query)[0]
        
        if count == 0:
            raise AirflowException(f"Validation failed: {table} is empty")
        
        logger.info(f"Validation passed: {table} has {count} records")
    
    return True

# Create DAG
with DAG(
    'extract_source_data_with_error_handling',
    default_args=default_args,
    description='Extract data from OLTP with error handling and logging',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'extract', 'banking', 'error-handling'],
) as dag:
    
    # Task 1: Extract customers
    extract_customers_task = PythonOperator(
        task_id='extract_customers',
        python_callable=extract_customers,
        doc='Extract customers from core_banking to staging',
    )
    
    # Task 2: Extract accounts
    extract_accounts_task = PythonOperator(
        task_id='extract_accounts',
        python_callable=extract_accounts,
        doc='Extract accounts from core_banking to staging',
    )
    
    # Task 3: Extract transactions
    extract_transactions_task = PythonOperator(
        task_id='extract_transactions',
        python_callable=extract_transactions,
        doc='Extract transactions from core_banking to staging',
    )
    
    # Task 4: Validate extraction
    validate_task = PythonOperator(
        task_id='validate_extraction',
        python_callable=validate_extraction,
        doc='Validate that all tables have data',
    )
    
    # Task dependencies
    [extract_customers_task, extract_accounts_task, extract_transactions_task] >> validate_task
