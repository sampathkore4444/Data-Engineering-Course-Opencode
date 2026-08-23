"""
Regulatory Reports DAG
Purpose: Generate and submit regulatory reports to SBV
Schedule: Daily at 6 AM
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.task_group import TaskGroup
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'compliance-team',
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    dag_id='regulatory_reports',
    default_args=default_args,
    description='Generate and submit regulatory reports to SBV',
    schedule_interval='0 6 * * *',  # 6 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['regulatory', 'sbv', 'banking'],
) as dag:

    def generate_call_report(**context):
        """Generate daily Call Report for SBV"""
        import pandas as pd
        import sqlalchemy
        
        # Connect to Dremio
        engine = sqlalchemy.create_engine(
            'dremio://user:pass@dremio-host:9047/banking-gold'
        )
        
        # Query Call Report data
        call_report = pd.read_sql("""
            SELECT 
                report_date,
                total_assets,
                total_deposits_savings,
                total_deposits_current,
                total_loan_outstanding,
                cet1_capital,
                tier1_capital,
                total_capital,
                risk_weighted_assets,
                cet1_ratio,
                tier1_ratio,
                car_ratio,
                npl_ratio,
                provision_coverage_ratio,
                lcr_ratio
            FROM gold.call_report
            WHERE report_date = CURRENT_DATE
        """, engine)
        
        # Save to file
        report_path = f"/data/reports/sbv/call_report_{context['ds']}.csv"
        call_report.to_csv(report_path, index=False)
        
        logger.info(f"Call Report generated: {report_path}")
        return {'report_path': report_path, 'row_count': len(call_report)}

    def generate_basel_iii_report(**context):
        """Generate monthly Basel III CAR report"""
        import pandas as pd
        import sqlalchemy
        
        engine = sqlalchemy.create_engine(
            'dremio://user:pass@dremio-host:9047/banking-gold'
        )
        
        # Query Basel III data
        basel_report = pd.read_sql("""
            SELECT 
                report_date,
                cet1_capital,
                tier1_capital,
                total_capital,
                total_rwa,
                cet1_ratio,
                tier1_ratio,
                car_ratio,
                sbv_cet1_min,
                sbv_tier1_min,
                sbv_car_min,
                compliance_status,
                cet1_surplus,
                tier1_surplus,
                car_surplus
            FROM gold.basel_iii_car
            WHERE report_date = CURRENT_DATE
        """, engine)
        
        # Save to file
        report_path = f"/data/reports/sbv/basel_iii_{context['ds']}.csv"
        basel_report.to_csv(report_path, index=False)
        
        logger.info(f"Basel III Report generated: {report_path}")
        return {'report_path': report_path}

    def generate_aml_report(**context):
        """Generate daily AML monitoring report"""
        import pandas as pd
        import sqlalchemy
        
        engine = sqlalchemy.create_engine(
            'dremio://user:pass@dremio-host:9047/banking-gold'
        )
        
        # Query AML data
        aml_report = pd.read_sql("""
            SELECT 
                report_date,
                ctr_count,
                ctr_total_amount,
                high_risk_str_count,
                medium_risk_str_count,
                pep_count,
                overall_risk_status
            FROM gold.aml_daily_summary
            WHERE report_date = CURRENT_DATE
        """, engine)
        
        # Save to file
        report_path = f"/data/reports/sbv/aml_{context['ds']}.csv"
        aml_report.to_csv(report_path, index=False)
        
        logger.info(f"AML Report generated: {report_path}")
        return {'report_path': report_path}

    def submit_to_sbv(**context):
        """Submit reports to SBV portal"""
        import requests
        import os
        
        sbv_api_url = os.getenv('SBV_API_URL', 'https://sbv-portal.gov.vn/api')
        sbv_api_key = os.getenv('SBV_API_KEY')
        
        headers = {
            'Authorization': f'Bearer {sbv_api_key}',
            'Content-Type': 'multipart/form-data'
        }
        
        reports = [
            ('call_report', f"/data/reports/sbv/call_report_{context['ds']}.csv"),
            ('aml_report', f"/data/reports/sbv/aml_{context['ds']}.csv"),
        ]
        
        submitted = []
        for report_name, file_path in reports:
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'rb') as f:
                        files = {'file': (os.path.basename(file_path), f)}
                        response = requests.post(
                            f"{sbv_api_url}/reports/submit",
                            headers=headers,
                            files=files,
                            data={'report_type': report_name, 'report_date': context['ds']}
                        )
                        
                        if response.status_code == 200:
                            submitted.append(report_name)
                            logger.info(f"Successfully submitted {report_name}")
                        else:
                            logger.error(f"Failed to submit {report_name}: {response.text}")
                except Exception as e:
                    logger.error(f"Error submitting {report_name}: {e}")
        
        return {'submitted_reports': submitted}

    def generate_executive_summary(**context):
        """Generate executive summary for management"""
        import pandas as pd
        import sqlalchemy
        
        engine = sqlalchemy.create_engine(
            'dremio://user:pass@dremio-host:9047/banking-gold'
        )
        
        # Query executive dashboard
        exec_summary = pd.read_sql("""
            SELECT 
                report_date,
                total_assets,
                net_interest_margin_pct,
                npl_ratio,
                car_ratio,
                lcr_ratio,
                total_customers,
                digital_txn_pct,
                fraud_alerts_24h,
                basel_compliance
            FROM gold.ceo_dashboard
            WHERE report_date = CURRENT_DATE
        """, engine)
        
        # Generate summary
        summary = f"""
        BANKING EXECUTIVE SUMMARY - {context['ds']}
        ========================================
        
        Financial Highlights:
        - Total Assets: {exec_summary['total_assets'].iloc[0]:,.0f} VND
        - Net Interest Margin: {exec_summary['net_interest_margin_pct'].iloc[0]}%
        
        Asset Quality:
        - NPL Ratio: {exec_summary['npl_ratio'].iloc[0]}%
        
        Capital & Liquidity:
        - CAR: {exec_summary['car_ratio'].iloc[0]}%
        - LCR: {exec_summary['lcr_ratio'].iloc[0]}%
        
        Customer Metrics:
        - Total Customers: {exec_summary['total_customers'].iloc[0]:,}
        - Digital Transaction %: {exec_summary['digital_txn_pct'].iloc[0]}%
        
        Risk Alerts:
        - Fraud Alerts (24h): {exec_summary['fraud_alerts_24h'].iloc[0]}
        - Basel Compliance: {exec_summary['basel_compliance'].iloc[0]}
        """
        
        # Save summary
        summary_path = f"/data/reports/executive/summary_{context['ds']}.txt"
        with open(summary_path, 'w') as f:
            f.write(summary)
        
        logger.info(f"Executive summary generated: {summary_path}")
        return {'summary_path': summary_path}

    # Task definitions
    with TaskGroup('generate_reports') as report_group:
        call_report = PythonOperator(
            task_id='generate_call_report',
            python_callable=generate_call_report,
        )
        
        basel_report = PythonOperator(
            task_id='generate_basel_iii_report',
            python_callable=generate_basel_iii_report,
        )
        
        aml_report = PythonOperator(
            task_id='generate_aml_report',
            python_callable=generate_aml_report,
        )
        
        [call_report, basel_report, aml_report]

    submit = PythonOperator(
        task_id='submit_to_sbv',
        python_callable=submit_to_sbv,
    )

    exec_summary = PythonOperator(
        task_id='generate_executive_summary',
        python_callable=generate_executive_summary,
    )

    report_group >> submit >> exec_summary
