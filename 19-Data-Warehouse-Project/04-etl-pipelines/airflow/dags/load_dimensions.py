"""
Load Dimensions DAG
Purpose: Load dimension tables from staging with SCD Type 2
Schedule: After extract (2 AM daily)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres import PostgresOperator
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': True,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='load_dimensions',
    default_args=default_args,
    description='Load dimension tables with SCD Type 2',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['load', 'dimensions', 'scd2', 'banking'],
) as dag:

    # SCD Type 2 for dim_customer
    load_dim_customer = PostgresOperator(
        task_id='load_dim_customer',
        postgres_conn_id='dw_db',
        sql="""
            -- Expire old records
            UPDATE dw.dim_customer
            SET is_current = FALSE,
                expiry_date = CURRENT_DATE - INTERVAL '1 day',
                updated_at = CURRENT_TIMESTAMP
            WHERE is_current = TRUE
              AND customer_id IN (
                  SELECT customer_id FROM dw.stg_customers s
                  WHERE s.customer_name != dw.dim_customer.customer_name
                     OR s.email != dw.dim_customer.email
                     OR s.phone != dw.dim_customer.phone
                     OR s.city != dw.dim_customer.city
              );

            -- Insert new records
            INSERT INTO dw.dim_customer (
                customer_id, customer_name, date_of_birth, gender, nationality,
                pan_number, email, phone, city, state, pin_code, customer_type,
                age, age_group, customer_segment, effective_date, is_current, source_system
            )
            SELECT
                s.customer_id, s.customer_name, s.date_of_birth, s.gender, s.nationality,
                s.pan_number, s.email, s.phone, s.city, s.state, s.pin_code, s.customer_type,
                EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.date_of_birth))::INT,
                CASE
                    WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.date_of_birth)) < 30 THEN 'YOUNG'
                    WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.date_of_birth)) < 45 THEN 'ADULT'
                    WHEN EXTRACT(YEAR FROM AGE(CURRENT_DATE, s.date_of_birth)) < 60 THEN 'MIDDLE_AGE'
                    ELSE 'SENIOR'
                END,
                CASE WHEN s.customer_type = 'CORPORATE' THEN 'CORPORATE' ELSE 'RETAIL' END,
                CURRENT_DATE,
                TRUE,
                'CORE_BANKING'
            FROM dw.stg_customers s
            WHERE s.customer_id NOT IN (
                SELECT customer_id FROM dw.dim_customer WHERE is_current = TRUE
            );
        """,
    )

    # SCD Type 1 for dim_account (simple overwrite)
    load_dim_account = PostgresOperator(
        task_id='load_dim_account',
        postgres_conn_id='dw_db',
        sql="""
            INSERT INTO dw.dim_account (
                account_id, customer_id, account_type, account_type_group,
                currency, opening_date, status, branch_code, interest_rate,
                is_active, source_system
            )
            SELECT
                s.account_id, s.customer_id, s.account_type,
                CASE
                    WHEN s.account_type IN ('SAVINGS', 'FIXED_DEPOSIT') THEN 'DEPOSIT'
                    WHEN s.account_type = 'CURRENT' THEN 'CURRENT'
                    ELSE 'OTHER'
                END,
                s.currency, s.opening_date, s.status, s.branch_code, s.interest_rate,
                CASE WHEN s.status = 'ACTIVE' THEN TRUE ELSE FALSE END,
                'CORE_BANKING'
            FROM dw.stg_accounts s
            ON CONFLICT (account_id) DO UPDATE SET
                current_balance = EXCLUDED.current_balance,
                status = EXCLUDED.status,
                updated_at = CURRENT_TIMESTAMP;
        """,
    )

    # Load dim_branch (static - no changes expected)
    load_dim_branch = PostgresOperator(
        task_id='load_dim_branch',
        postgres_conn_id='dw_db',
        sql="""
            INSERT INTO dw.dim_branch (branch_code, branch_name, branch_type, city, state, region, source_system)
            VALUES
                ('BR001', 'Ho Chi Minh Main', 'MAIN', 'Ho Chi Minh', 'HCM', 'SOUTH', 'CORE_BANKING'),
                ('BR002', 'Hanoi Branch', 'REGIONAL', 'Ha Noi', 'HN', 'NORTH', 'CORE_BANKING'),
                ('BR003', 'Da Nang Branch', 'REGIONAL', 'Da Nang', 'DN', 'CENTRAL', 'CORE_BANKING'),
                ('BR004', 'Can Tho Branch', 'SUB_BRANCH', 'Can Tho', 'CT', 'SOUTH', 'CORE_BANKING'),
                ('BR005', 'Hai Phong Branch', 'REGIONAL', 'Hai Phong', 'HP', 'NORTH', 'CORE_BANKING')
            ON CONFLICT (branch_code) DO NOTHING;
        """,
    )

    # Task dependencies
    [load_dim_customer, load_dim_account, load_dim_branch]
