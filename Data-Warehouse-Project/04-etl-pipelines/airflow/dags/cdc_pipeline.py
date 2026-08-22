"""
CDC (Change Data Capture) Pipeline
Purpose: Capture real-time changes from source databases and sync to Data Warehouse
Tool:    Apache Airflow + Debezium + Kafka
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.task_group import TaskGroup
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

def check_debezium_connector():
    """Check if Debezium connectors are running"""
    import requests
    
    debezium_url = "http://debezium:8083/connectors"
    
    try:
        response = requests.get(debezium_url, timeout=10)
        connectors = response.json()
        logger.info(f"Active connectors: {connectors}")
        return len(connectors) > 0
    except Exception as e:
        logger.error(f"Debezium check failed: {e}")
        return False

def capture_customer_changes():
    """Capture changes from core_banking.customers table"""
    source_hook = PostgresHook(postgres_conn_id='source_core_banking')
    dw_hook = PostgresHook(postgres_conn_id='data_warehouse')
    
    # Get last processed LSN (Log Sequence Number)
    last_lsn = get_last_lsn(dw_hook, 'customers')
    
    # Query CDC log (simulated - in real scenario, use Debezium)
    query = """
        SELECT 
            operation,
            customer_id,
            customer_name,
            customer_type,
            phone,
            email,
            address,
            created_at,
            updated_at,
            transaction_id,
            commit_timestamp
        FROM cdc.customers_log
        WHERE commit_timestamp > %s
        ORDER BY commit_timestamp;
    """
    
    changes = source_hook.get_records(query, parameters=(last_lsn,))
    
    logger.info(f"Captured {len(changes)} customer changes")
    
    for change in changes:
        operation = change[0]  # INSERT, UPDATE, DELETE
        
        if operation == 'INSERT':
            insert_customer(dw_hook, change[1:])
        elif operation == 'UPDATE':
            update_customer(dw_hook, change[1:])
        elif operation == 'DELETE':
            delete_customer(dw_hook, change[1:])
    
    # Update last LSN
    if changes:
        update_last_lsn(dw_hook, 'customers', changes[-1][-1])

def capture_account_changes():
    """Capture changes from core_banking.accounts table"""
    source_hook = PostgresHook(postgres_conn_id='source_core_banking')
    dw_hook = PostgresHook(postgres_conn_id='data_warehouse')
    
    last_lsn = get_last_lsn(dw_hook, 'accounts')
    
    query = """
        SELECT 
            operation,
            account_id,
            customer_id,
            account_type,
            balance,
            status,
            branch_id,
            created_at,
            updated_at,
            transaction_id,
            commit_timestamp
        FROM cdc.accounts_log
        WHERE commit_timestamp > %s
        ORDER BY commit_timestamp;
    """
    
    changes = source_hook.get_records(query, parameters=(last_lsn,))
    
    logger.info(f"Captured {len(changes)} account changes")
    
    for change in changes:
        operation = change[0]
        
        if operation == 'INSERT':
            insert_account(dw_hook, change[1:])
        elif operation == 'UPDATE':
            update_account(dw_hook, change[1:])
        elif operation == 'DELETE':
            delete_account(dw_hook, change[1:])
    
    if changes:
        update_last_lsn(dw_hook, 'accounts', changes[-1][-1])

def capture_transaction_changes():
    """Capture changes from core_banking.transactions table"""
    source_hook = PostgresHook(postgres_conn_id='source_core_banking')
    dw_hook = PostgresHook(postgres_conn_id='data_warehouse')
    
    last_lsn = get_last_lsn(dw_hook, 'transactions')
    
    query = """
        SELECT 
            operation,
            transaction_id,
            account_id,
            transaction_type,
            amount,
            balance_after,
            description,
            transaction_date,
            transaction_id,
            commit_timestamp
        FROM cdc.transactions_log
        WHERE commit_timestamp > %s
        ORDER BY commit_timestamp;
    """
    
    changes = source_hook.get_records(query, parameters=(last_lsn,))
    
    logger.info(f"Captured {len(changes)} transaction changes")
    
    for change in changes:
        operation = change[0]
        
        if operation == 'INSERT':
            insert_transaction(dw_hook, change[1:])
    
    if changes:
        update_last_lsn(dw_hook, 'transactions', changes[-1][-1])

def get_last_lsn(hook, table_name):
    """Get last processed LSN for a table"""
    query = """
        SELECT last_lsn 
        FROM cdc_metadata.processed_lsn 
        WHERE table_name = %s;
    """
    result = hook.get_first(query, parameters=(table_name,))
    return result[0] if result else '0/0'

def update_last_lsn(hook, table_name, lsn):
    """Update last processed LSN"""
    query = """
        INSERT INTO cdc_metadata.processed_lsn (table_name, last_lsn, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (table_name) 
        DO UPDATE SET last_lsn = %s, updated_at = NOW();
    """
    hook.run(query, parameters=(table_name, lsn, lsn))

def insert_customer(hook, data):
    """Insert new customer into DW"""
    query = """
        INSERT INTO staging.stg_customers 
        (customer_id, customer_name, customer_type, phone, email, address, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING;
    """
    hook.run(query, parameters=data)

def update_customer(hook, data):
    """Update existing customer in DW"""
    query = """
        UPDATE staging.stg_customers 
        SET customer_name = %s, customer_type = %s, phone = %s, email = %s, 
            address = %s, updated_at = %s
        WHERE customer_id = %s;
    """
    # Reorder data for UPDATE (name, type, phone, email, address, updated_at, id)
    hook.run(query, parameters=(data[1], data[2], data[3], data[4], data[5], data[7], data[0]))

def delete_customer(hook, data):
    """Soft delete customer in DW"""
    query = """
        UPDATE staging.stg_customers 
        SET is_deleted = true, deleted_at = NOW()
        WHERE customer_id = %s;
    """
    hook.run(query, parameters=(data[0],))

def insert_account(hook, data):
    """Insert new account into DW"""
    query = """
        INSERT INTO staging.stg_accounts 
        (account_id, customer_id, account_type, balance, status, branch_id, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (account_id) DO NOTHING;
    """
    hook.run(query, parameters=data)

def update_account(hook, data):
    """Update existing account in DW"""
    query = """
        UPDATE staging.stg_accounts 
        SET balance = %s, status = %s, updated_at = %s
        WHERE account_id = %s;
    """
    # Reorder data (balance, status, updated_at, id)
    hook.run(query, parameters=(data[3], data[4], data[7], data[0]))

def delete_account(hook, data):
    """Soft delete account in DW"""
    query = """
        UPDATE staging.stg_accounts 
        SET is_deleted = true, deleted_at = NOW()
        WHERE account_id = %s;
    """
    hook.run(query, parameters=(data[0],))

def insert_transaction(hook, data):
    """Insert new transaction into DW"""
    query = """
        INSERT INTO staging.stg_transactions 
        (transaction_id, account_id, transaction_type, amount, balance_after, 
         description, transaction_date, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO NOTHING;
    """
    hook.run(query, parameters=data)

def sync_dimensions_after_cdc():
    """Re-sync dimensions after CDC changes"""
    # This would trigger the dimension loading DAG
    logger.info("Triggering dimension sync after CDC changes")

def validate_cdc_sync():
    """Validate that CDC sync completed successfully"""
    hook = PostgresHook(postgres_conn_id='data_warehouse')
    
    # Check for any failed records
    query = """
        SELECT COUNT(*) 
        FROM cdc_metadata.sync_errors 
        WHERE error_date = CURRENT_DATE;
    """
    error_count = hook.get_first(query)[0]
    
    if error_count > 0:
        raise ValueError(f"CDC sync had {error_count} errors")
    
    logger.info("CDC sync validation passed")

# Create DAG
with DAG(
    'cdc_realtime_sync',
    default_args=default_args,
    description='Real-time CDC synchronization from source systems',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['cdc', 'realtime', 'banking'],
) as dag:
    
    # Task 1: Check Debezium is running
    check_debezium = PythonOperator(
        task_id='check_debezium_connector',
        python_callable=check_debezium_connector,
    )
    
    # Task Group: Capture changes from all source tables
    with TaskGroup("capture_changes") as capture_changes_group:
        
        capture_customers = PythonOperator(
            task_id='capture_customer_changes',
            python_callable=capture_customer_changes,
        )
        
        capture_accounts = PythonOperator(
            task_id='capture_account_changes',
            python_callable=capture_account_changes,
        )
        
        capture_transactions = PythonOperator(
            task_id='capture_transaction_changes',
            python_callable=capture_transaction_changes,
        )
    
    # Task 3: Sync dimensions after CDC
    sync_dimensions = PythonOperator(
        task_id='sync_dimensions_after_cdc',
        python_callable=sync_dimensions_after_cdc,
    )
    
    # Task 4: Validate CDC sync
    validate_sync = PythonOperator(
        task_id='validate_cdc_sync',
        python_callable=validate_cdc_sync,
    )
    
    # Task flow
    check_debezium >> capture_changes_group >> sync_dimensions >> validate_sync
