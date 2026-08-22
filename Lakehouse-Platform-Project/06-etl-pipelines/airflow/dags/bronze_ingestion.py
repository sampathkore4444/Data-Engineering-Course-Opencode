"""
Bronze Layer Ingestion DAG
Purpose: Ingest raw data from source systems to Bronze layer
Schedule: Hourly
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.task_group import TaskGroup
import json

# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

# DAG Definition
with DAG(
    dag_id='bronze_ingestion',
    default_args=default_args,
    description='Ingest data from source systems to Bronze layer',
    schedule_interval='0 * * * *',  # Every hour
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['bronze', 'ingestion', 'banking'],
) as dag:

    def extract_core_banking(**context):
        """Extract data from Core Banking (Oracle)"""
        import pandas as pd
        import sqlalchemy
        
        # Connection to Oracle
        engine = sqlalchemy.create_engine(
            'oracle+cx_oracle://user:pass@oracle-host:1521/COREBANK'
        )
        
        # Extract accounts
        accounts_df = pd.read_sql(
            'SELECT * FROM CBS.ACCOUNTS WHERE LAST_UPDATED > :cutoff',
            engine,
            params={'cutoff': context['prev_execution_date_success']}
        )
        
        # Extract transactions
        transactions_df = pd.read_sql(
            'SELECT * FROM CBS.TRANSACTIONS WHERE TXN_TIMESTAMP > :cutoff',
            engine,
            params={'cutoff': context['prev_execution_date_success']}
        )
        
        # Save to Bronze (MinIO/S3)
        accounts_df.to_parquet(
            f's3://banking-lake/bronze/core-banking/accounts/ingestion_date={context["ds"]}/',
            index=False
        )
        transactions_df.to_parquet(
            f's3://banking-lake/bronze/core-banking/transactions/ingestion_date={context["ds"]}/',
            index=False
        )
        
        # Log ingestion metadata
        return {
            'source': 'core-banking',
            'accounts_count': len(accounts_df),
            'transactions_count': len(transactions_df),
            'execution_date': context['ds']
        }

    def extract_credit_cards(**context):
        """Extract data from Credit Cards System (Mainframe)"""
        import pandas as pd
        
        # Mainframe extraction via API
        # In production, this would call a mainframe API
        cards_df = pd.DataFrame()  # Placeholder
        card_txns_df = pd.DataFrame()  # Placeholder
        
        # Save to Bronze
        cards_df.to_parquet(
            f's3://banking-lake/bronze/credit-cards/cards/ingestion_date={context["ds"]}/',
            index=False
        )
        card_txns_df.to_parquet(
            f's3://banking-lake/bronze/credit-cards/card_transactions/ingestion_date={context["ds"]}/',
            index=False
        )
        
        return {
            'source': 'credit-cards',
            'cards_count': len(cards_df),
            'transactions_count': len(card_txns_df),
            'execution_date': context['ds']
        }

    def extract_loans(**context):
        """Extract data from Loans System (SQL Server)"""
        import pandas as pd
        import sqlalchemy
        
        # Connection to SQL Server
        engine = sqlalchemy.create_engine(
            'mssql+pyodbc://user:pass@sqlserver-host/LoansDB?driver=ODBC+Driver+17'
        )
        
        # Extract loan accounts
        loans_df = pd.read_sql(
            'SELECT * FROM dbo.LOAN_ACCOUNTS WHERE LAST_UPDATED > :cutoff',
            engine,
            params={'cutoff': context['prev_execution_date_success']}
        )
        
        # Extract loan payments
        payments_df = pd.read_sql(
            'SELECT * FROM dbo.LOAN_PAYMENTS WHERE PAYMENT_DATE > :cutoff',
            engine,
            params={'cutoff': context['prev_execution_date_success']}
        )
        
        # Save to Bronze
        loans_df.to_parquet(
            f's3://banking-lake/bronze/loans/loan_accounts/ingestion_date={context["ds"]}/',
            index=False
        )
        payments_df.to_parquet(
            f's3://banking-lake/bronze/loans/loan_payments/ingestion_date={context["ds"]}/',
            index=False
        )
        
        return {
            'source': 'loans',
            'loans_count': len(loans_df),
            'payments_count': len(payments_df),
            'execution_date': context['ds']
        }

    def validate_bronze_data(**context):
        """Validate data quality in Bronze layer"""
        import pandas as pd
        
        results = []
        
        # Check each source
        for source in ['core-banking', 'credit-cards', 'loans']:
            try:
                df = pd.read_parquet(
                    f's3://banking-lake/bronze/{source}/ingestion_date={context["ds"]}/'
                )
                results.append({
                    'source': source,
                    'row_count': len(df),
                    'status': 'SUCCESS' if len(df) > 0 else 'EMPTY',
                    'columns': list(df.columns)
                })
            except Exception as e:
                results.append({
                    'source': source,
                    'row_count': 0,
                    'status': 'FAILED',
                    'error': str(e)
                })
        
        # Check for failures
        failures = [r for r in results if r['status'] == 'FAILED']
        if failures:
            raise ValueError(f"Bronze validation failed: {failures}")
        
        return results

    # Task Groups
    with TaskGroup('extract_sources') as extract_group:
        extract_core = PythonOperator(
            task_id='extract_core_banking',
            python_callable=extract_core_banking,
            doc='Extract data from Core Banking Oracle database',
        )
        
        extract_cards = PythonOperator(
            task_id='extract_credit_cards',
            python_callable=extract_credit_cards,
            doc='Extract data from Credit Cards Mainframe system',
        )
        
        extract_loans_task = PythonOperator(
            task_id='extract_loans',
            python_callable=extract_loans,
            doc='Extract data from Loans SQL Server database',
        )
        
        [extract_core, extract_cards, extract_loans_task]

    # Validate task
    validate = PythonOperator(
        task_id='validate_bronze_data',
        python_callable=validate_bronze_data,
        doc='Validate data quality in Bronze layer',
    )

    # Task Dependencies
    extract_group >> validate

    # Notification on failure
    from airflow.operators.email import EmailOperator
    
    notify_failure = EmailOperator(
        task_id='notify_failure',
        to=['data-engineering@bank.com'],
        subject='Bronze Ingestion Failed - {{ ds }}',
        html_content='''
        <h3>Bronze Ingestion DAG Failed</h3>
        <p>Execution Date: {{ ds }}</p>
        <p>Please check the logs for details.</p>
        <p><a href="{{ ti.log_url }}">View Logs</a></p>
        ''',
        trigger='one_failed',
    )
    
    validate >> notify_failure
