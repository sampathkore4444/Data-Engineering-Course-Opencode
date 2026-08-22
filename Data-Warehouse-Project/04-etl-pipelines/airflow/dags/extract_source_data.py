"""
Extract Source Data DAG
Purpose: Extract data from OLTP source systems to DW staging area
Schedule: Daily at 1 AM
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='extract_source_data',
    default_args=default_args,
    description='Extract data from source systems to DW staging',
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['extract', 'staging', 'banking'],
) as dag:

    # Task 1: Extract customers
    extract_customers = PostgresOperator(
        task_id='extract_customers',
        postgres_conn_id='source_db',
        sql="""
            INSERT INTO dw.stg_customers (
                customer_id, customer_name, date_of_birth, gender, nationality,
                pan_number, email, phone, city, state, pin_code, customer_type,
                kyc_status, last_updated
            )
            SELECT
                customer_id, customer_name, date_of_birth, gender, nationality,
                pan_number, email, phone, city, state, pin_code, customer_type,
                kyc_status, last_updated
            FROM cbs.customers
            WHERE last_updated > COALESCE(
                (SELECT MAX(last_updated) FROM dw.stg_customers),
                '1900-01-01'::DATE
            )
            ON CONFLICT (customer_id) DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                city = EXCLUDED.city,
                last_updated = EXCLUDED.last_updated;
        """,
    )

    # Task 2: Extract accounts
    extract_accounts = PostgresOperator(
        task_id='extract_accounts',
        postgres_conn_id='source_db',
        sql="""
            INSERT INTO dw.stg_accounts (
                account_id, customer_id, account_type, currency, opening_date,
                current_balance, available_balance, status, branch_code,
                interest_rate, last_updated
            )
            SELECT
                account_id, customer_id, account_type, currency, opening_date,
                current_balance, available_balance, status, branch_code,
                interest_rate, last_updated
            FROM cbs.accounts
            WHERE last_updated > COALESCE(
                (SELECT MAX(last_updated) FROM dw.stg_accounts),
                '1900-01-01'::DATE
            )
            ON CONFLICT (account_id) DO UPDATE SET
                current_balance = EXCLUDED.current_balance,
                available_balance = EXCLUDED.available_balance,
                status = EXCLUDED.status,
                last_updated = EXCLUDED.last_updated;
        """,
    )

    # Task 3: Extract transactions
    extract_transactions = PostgresOperator(
        task_id='extract_transactions',
        postgres_conn_id='source_db',
        sql="""
            INSERT INTO dw.stg_transactions (
                txn_id, account_id, txn_type, amount, currency,
                txn_date, txn_timestamp, description, reference_number,
                channel, status
            )
            SELECT
                txn_id, account_id, txn_type, amount, currency,
                txn_date, txn_timestamp, description, reference_number,
                channel, status
            FROM cbs.transactions
            WHERE txn_date >= CURRENT_DATE - INTERVAL '7 days'
            ON CONFLICT (txn_id) DO NOTHING;
        """,
    )

    # Task 4: Extract cards
    extract_cards = PostgresOperator(
        task_id='extract_cards',
        postgres_conn_id='source_db',
        sql="""
            INSERT INTO dw.stg_cards (
                card_number, customer_id, card_type, card_limit,
                credit_used, issuance_date, expiry_date, status
            )
            SELECT
                card_number, customer_id, card_type, card_limit,
                credit_used, issuance_date, expiry_date, status
            FROM cards.credit_cards
            ON CONFLICT (card_number) DO UPDATE SET
                credit_used = EXCLUDED.credit_used,
                status = EXCLUDED.status;
        """,
    )

    # Task 5: Extract loans
    extract_loans = PostgresOperator(
        task_id='extract_loans',
        postgres_conn_id='source_db',
        sql="""
            INSERT INTO dw.stg_loans (
                loan_id, customer_id, loan_type, principal_amount,
                principal_outstanding, interest_rate, tenure_months,
                emi_amount, disbursement_date, maturity_date, status
            )
            SELECT
                loan_id, customer_id, loan_type, principal_amount,
                principal_outstanding, interest_rate, tenure_months,
                emi_amount, disbursement_date, maturity_date, status
            FROM loans.loan_accounts
            ON CONFLICT (loan_id) DO UPDATE SET
                principal_outstanding = EXCLUDED.principal_outstanding,
                status = EXCLUDED.status;
        """,
    )

    # Task 6: Extract loan payments
    extract_loan_payments = PostgresOperator(
        task_id='extract_loan_payments',
        postgres_conn_id='source_db',
        sql="""
            INSERT INTO dw.stg_loan_payments (
                payment_id, loan_id, payment_date, amount,
                payment_mode, status, reference_number
            )
            SELECT
                payment_id, loan_id, payment_date, amount,
                payment_mode, status, reference_number
            FROM loans.loan_payments
            WHERE payment_date >= CURRENT_DATE - INTERVAL '7 days'
            ON CONFLICT (payment_id) DO NOTHING;
        """,
    )

    # Task dependencies
    [extract_customers, extract_accounts, extract_transactions,
     extract_cards, extract_loans, extract_loan_payments]
