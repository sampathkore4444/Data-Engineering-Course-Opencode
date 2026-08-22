"""
Gold Layer Aggregation DAG
Purpose: Create business-ready aggregations from Silver layer
Schedule: After Silver transformation (4 AM daily)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'execution_timeout': timedelta(hours=3),
}

with DAG(
    dag_id='gold_aggregation',
    default_args=default_args,
    description='Create Gold layer business aggregations',
    schedule_interval='0 4 * * *',  # 4 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'aggregation', 'banking'],
) as dag:

    def create_customer_360(**context):
        """Create Customer 360° aggregated view"""
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import *
        
        spark = SparkSession.builder \
            .appName("Gold_Customer_360") \
            .getOrCreate()
        
        # Read Silver data
        customers = spark.read.parquet("s3a://banking-lake/silver/core-banking/customers/")
        accounts = spark.read.parquet("s3a://banking-lake/silver/core-banking/accounts/")
        transactions = spark.read.parquet("s3a://banking-lake/silver/core-banking/transactions/")
        cards = spark.read.parquet("s3a://banking-lake/silver/credit-cards/cards/")
        loans = spark.read.parquet("s3a://banking-lake/silver/loans/loan_accounts/")
        
        # Create Customer 360
        customer_360 = customers.alias("c") \
            .join(accounts.alias("a"), col("c.customer_id") == col("a.customer_id"), "left") \
            .join(transactions.alias("t"), col("a.account_id") == col("t.account_id"), "left") \
            .join(cards.alias("cc"), col("c.customer_id") == col("cc.customer_id"), "left") \
            .join(loans.alias("l"), col("c.customer_id") == col("l.customer_id"), "left") \
            .groupBy(
                col("c.customer_id"),
                col("c.customer_name"),
                col("c.email"),
                col("c.phone"),
                col("c.city")
            ) \
            .agg(
                countDistinct(col("a.account_id")).alias("total_accounts"),
                sum(col("a.current_balance")).alias("total_balance"),
                countDistinct(col("cc.card_number")).alias("total_cards"),
                sum(col("cc.card_limit")).alias("total_card_limit"),
                sum(col("cc.credit_used")).alias("total_card_outstanding"),
                countDistinct(col("l.loan_id")).alias("total_loans"),
                sum(col("l.principal_outstanding")).alias("total_loan_outstanding"),
                sum(col("l.emi_amount")).alias("total_monthly_emi"),
                count(col("t.txn_id")).alias("total_transactions")
            ) \
            .withColumn("net_relationship_value",
                coalesce(col("total_balance"), lit(0)) +
                coalesce(col("total_card_limit"), lit(0)) -
                coalesce(col("total_loan_outstanding"), lit(0))) \
            .withColumn("customer_segment",
                when(col("net_relationship_value") >= 10000000000, "PLATINUM")
                .when(col("net_relationship_value") >= 5000000000, "GOLD")
                .when(col("net_relationship_value") >= 1000000000, "SILVER")
                .otherwise("STANDARD")) \
            .withColumn("updated_at", current_timestamp())
        
        # Write to Gold
        customer_360.write \
            .mode("overwrite") \
            .parquet("s3a://banking-lake/gold/customer-360/")
        
        logger.info(f"Customer 360 created: {customer_360.count()} customers")
        return {'customer_count': customer_360.count()}

    def create_daily_transaction_summary(**context):
        """Create daily transaction summary"""
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import *
        
        spark = SparkSession.builder \
            .appName("Gold_Daily_Transactions") \
            .getOrCreate()
        
        transactions = spark.read.parquet("s3a://banking-lake/silver/core-banking/transactions/")
        
        daily_summary = transactions \
            .groupBy(
                col("txn_date"),
                col("channel_standardized"),
                col("txn_type_standardized")
            ) \
            .agg(
                count("*").alias("transaction_count"),
                sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount"),
                min("amount").alias("min_amount"),
                max("amount").alias("max_amount"),
                countDistinct("account_id").alias("unique_accounts"),
                sum(when(col("amount") > 100000, 1).otherwise(0)).alias("high_value_count")
            ) \
            .withColumn("updated_at", current_timestamp())
        
        daily_summary.write \
            .mode("overwrite") \
            .partitionBy("txn_date") \
            .parquet("s3a://banking-lake/gold/daily-transaction-summary/")
        
        logger.info(f"Daily summary created: {daily_summary.count()} records")
        return {'record_count': daily_summary.count()}

    def create_credit_risk_dashboard(**context):
        """Create credit risk dashboard"""
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import *
        
        spark = SparkSession.builder \
            .appName("Gold_Credit_Risk") \
            .getOrCreate()
        
        loans = spark.read.parquet("s3a://banking-lake/silver/loans/loan_accounts/")
        payments = spark.read.parquet("s3a://banking-lake/silver/loans/loan_payments/")
        customers = spark.read.parquet("s3a://banking-lake/silver/core-banking/customers/")
        
        # Join and aggregate
        risk_dashboard = loans.alias("l") \
            .join(customers.alias("c"), col("l.customer_id") == col("c.customer_id"), "left") \
            .join(payments.alias("p"), col("l.loan_id") == col("p.loan_id"), "left") \
            .groupBy(
                col("l.customer_id"),
                col("c.customer_name"),
                col("l.loan_id"),
                col("l.loan_type"),
                col("l.principal_amount"),
                col("l.principal_outstanding"),
                col("l.interest_rate"),
                col("l.emi_amount"),
                col("l.disbursement_date"),
                col("l.maturity_date")
            ) \
            .agg(
                count(col("p.payment_id")).alias("total_payments_made"),
                sum(when(col("p.status") == "SUCCESS", 1).otherwise(0)).alias("successful_payments"),
                sum(when(col("p.status") == "FAILED", 1).otherwise(0)).alias("failed_payments")
            ) \
            .withColumn("payment_success_rate",
                when(col("total_payments_made") > 0,
                     round(col("successful_payments") * 100.0 / col("total_payments_made"), 2))
                .otherwise(0)) \
            .withColumn("estimated_dpd",
                when(col("maturity_date") >= current_date(), 0)
                .otherwise(datediff(current_date(), col("maturity_date")))) \
            .withColumn("risk_classification",
                when(col("estimated_dpd") > 90, "NPA")
                .when(col("estimated_dpd") > 60, "DOUBTFUL")
                .when(col("estimated_dpd") > 30, "SUB_STANDARD")
                .when(col("estimated_dpd") > 0, "SPECIAL_MENTION")
                .otherwise("STANDARD")) \
            .withColumn("updated_at", current_timestamp())
        
        risk_dashboard.write \
            .mode("overwrite") \
            .parquet("s3a://banking-lake/gold/credit-risk-dashboard/")
        
        logger.info(f"Credit risk dashboard created: {risk_dashboard.count()} loans")
        return {'loan_count': risk_dashboard.count()}

    def update_dremio_reflections(**context):
        """Refresh Dremio reflections after Gold layer update"""
        import requests
        
        dremio_url = "http://dremio:9047"
        headers = {
            "Authorization": f"Bearer {context['params']['dremio_token']}",
            "Content-Type": "application/json"
        }
        
        # Get reflection IDs
        reflections = [
            "customer-360-raw-reflection",
            "daily-transactions-agg-reflection",
            "risk-dashboard-raw-reflection"
        ]
        
        for reflection_name in reflections:
            try:
                # Trigger refresh
                response = requests.post(
                    f"{dremio_url}/api/v3/reflection/{reflection_name}/refresh",
                    headers=headers
                )
                logger.info(f"Refreshed reflection: {reflection_name}")
            except Exception as e:
                logger.error(f"Failed to refresh {reflection_name}: {e}")
        
        return {'reflections_refreshed': len(reflections)}

    # Task definitions
    with TaskGroup('create_gold_views') as gold_group:
        cust_360 = PythonOperator(
            task_id='create_customer_360',
            python_callable=create_customer_360,
        )
        
        daily_txn = PythonOperator(
            task_id='create_daily_transaction_summary',
            python_callable=create_daily_transaction_summary,
        )
        
        credit_risk = PythonOperator(
            task_id='create_credit_risk_dashboard',
            python_callable=create_credit_risk_dashboard,
        )
        
        [cust_360, daily_txn, credit_risk]

    refresh_reflections = PythonOperator(
        task_id='update_dremio_reflections',
        python_callable=update_dremio_reflections,
    )

    gold_group >> refresh_reflections
