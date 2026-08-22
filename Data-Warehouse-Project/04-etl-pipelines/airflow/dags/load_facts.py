"""
Load Facts DAG
Purpose: Load fact tables from staging and dimensions
Schedule: After dimensions (3 AM daily)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres import PostgresOperator
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': True,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
}

with DAG(
    dag_id='load_facts',
    default_args=default_args,
    description='Load fact tables from staging and dimensions',
    schedule_interval='0 3 * * *',  # Daily at 3 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['load', 'facts', 'banking'],
) as dag:

    # Load fact_transactions
    load_fact_transactions = PostgresOperator(
        task_id='load_fact_transactions',
        postgres_conn_id='dw_db',
        sql="""
            INSERT INTO dw.fact_transactions (
                date_key, customer_sk, account_sk, branch_sk,
                txn_id, txn_type, channel,
                transaction_amount, fee_amount, tax_amount, net_amount,
                is_high_value, is_weekend, source_system
            )
            SELECT
                TO_CHAR(t.txn_date, 'YYYYMMDD')::INT AS date_key,
                c.customer_sk,
                a.account_sk,
                b.branch_sk,
                t.txn_id,
                t.txn_type,
                t.channel,
                t.amount AS transaction_amount,
                0 AS fee_amount,
                0 AS tax_amount,
                t.amount AS net_amount,
                CASE WHEN t.amount > 100000000 THEN TRUE ELSE FALSE END,
                CASE WHEN EXTRACT(DOW FROM t.txn_date) IN (0, 6) THEN TRUE ELSE FALSE END,
                'CORE_BANKING'
            FROM dw.stg_transactions t
            JOIN dw.dim_account a ON t.account_id = a.account_id
            JOIN dw.dim_customer c ON a.customer_id = c.customer_id AND c.is_current = TRUE
            JOIN dw.dim_branch b ON a.branch_code = b.branch_code
            ON CONFLICT (txn_id) DO NOTHING;
        """,
    )

    # Load fact_account_balance
    load_fact_balance = PostgresOperator(
        task_id='load_fact_account_balance',
        postgres_conn_id='dw_db',
        sql="""
            INSERT INTO dw.fact_account_balance (
                date_key, customer_sk, account_sk, branch_sk,
                opening_balance, closing_balance, min_balance, max_balance, avg_balance,
                credit_count, debit_count, total_credits, total_debits,
                net_flow, balance_change, source_system
            )
            SELECT
                TO_CHAR(t.txn_date, 'YYYYMMDD')::INT,
                c.customer_sk,
                a.account_sk,
                b.branch_sk,
                a.current_balance AS opening_balance,
                a.current_balance AS closing_balance,
                a.current_balance AS min_balance,
                a.current_balance AS max_balance,
                a.current_balance AS avg_balance,
                CASE WHEN t.txn_type = 'CREDIT' THEN 1 ELSE 0 END,
                CASE WHEN t.txn_type = 'DEBIT' THEN 1 ELSE 0 END,
                CASE WHEN t.txn_type = 'CREDIT' THEN t.amount ELSE 0 END,
                CASE WHEN t.txn_type = 'DEBIT' THEN t.amount ELSE 0 END,
                CASE WHEN t.txn_type = 'CREDIT' THEN t.amount ELSE -t.amount END,
                CASE WHEN t.txn_type = 'CREDIT' THEN t.amount ELSE -t.amount END,
                'CORE_BANKING'
            FROM dw.stg_transactions t
            JOIN dw.dim_account a ON t.account_id = a.account_id
            JOIN dw.dim_customer c ON a.customer_id = c.customer_id AND c.is_current = TRUE
            JOIN dw.dim_branch b ON a.branch_code = b.branch_code;
        """,
    )

    # Load fact_loan_payment
    load_fact_loan = PostgresOperator(
        task_id='load_fact_loan_payment',
        postgres_conn_id='dw_db',
        sql="""
            INSERT INTO dw.fact_loan_payment (
                date_key, customer_sk, branch_sk,
                loan_id, loan_type, payment_id,
                payment_amount, principal_amount, interest_amount,
                principal_outstanding, interest_rate, emi_amount, tenure_months,
                payment_success, source_system
            )
            SELECT
                TO_CHAR(p.payment_date, 'YYYYMMDD')::INT,
                c.customer_sk,
                b.branch_sk,
                l.loan_id,
                l.loan_type,
                p.payment_id,
                p.amount,
                l.principal_outstanding / l.tenure_months,
                p.amount - (l.principal_outstanding / l.tenure_months),
                l.principal_outstanding,
                l.interest_rate,
                l.emi_amount,
                l.tenure_months,
                CASE WHEN p.status = 'SUCCESS' THEN TRUE ELSE FALSE END,
                'LOANS'
            FROM dw.stg_loan_payments p
            JOIN dw.stg_loans l ON p.loan_id = l.loan_id
            JOIN dw.dim_customer c ON l.customer_id = c.customer_id AND c.is_current = TRUE
            JOIN dw.dim_branch b ON 'BR001' = b.branch_code
            ON CONFLICT (payment_id) DO NOTHING;
        """,
    )

    # Task dependencies
    [load_fact_transactions, load_fact_balance, load_fact_loan]
