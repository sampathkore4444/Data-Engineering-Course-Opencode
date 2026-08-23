"""
Silver Layer Transformation DAG
Purpose: Clean, validate, and conform Bronze data to Silver layer
Schedule: After Bronze ingestion (2 AM daily)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    dag_id='silver_transformation',
    default_args=default_args,
    description='Transform Bronze data to Silver layer',
    schedule_interval='0 2 * * *',  # 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['silver', 'transformation', 'banking'],
) as dag:

    def clean_customers(**context):
        """Clean and deduplicate customer data"""
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import *
        
        spark = SparkSession.builder \
            .appName("Silver_Customer_Cleaning") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()
        
        # Read Bronze data
        df_bronze = spark.read.parquet(
            f"s3a://banking-lake/bronze/core-banking/customers/ingestion_date={context['ds']}"
        )
        
        # Clean and deduplicate
        df_silver = df_bronze \
            .dropDuplicates(["customer_id"]) \
            .filter(col("customer_id").isNotNull()) \
            .withColumn("customer_name", trim(upper(col("customer_name")))) \
            .withColumn("email", 
                when(col("email").rlike(".*@.*\\..*"), lower(trim(col("email"))))
                .otherwise(None)) \
            .withColumn("phone",
                when(col("phone").rlike("^[0-9]{10,15}$"), col("phone"))
                .otherwise(None)) \
            .withColumn("cleaned_at", current_timestamp())
        
        # Write to Silver
        df_silver.write \
            .mode("overwrite") \
            .partitionBy("ingestion_date") \
            .parquet("s3a://banking-lake/silver/core-banking/customers/")
        
        # Data quality check
        total_rows = df_bronze.count()
        clean_rows = df_silver.count()
        duplicates_removed = total_rows - clean_rows
        
        logger.info(f"Customers: {total_rows} → {clean_rows} ({duplicates_removed} duplicates removed)")
        
        return {
            'source_rows': total_rows,
            'cleaned_rows': clean_rows,
            'duplicates_removed': duplicates_removed
        }

    def clean_accounts(**context):
        """Clean and validate account data"""
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import *
        
        spark = SparkSession.builder \
            .appName("Silver_Account_Cleaning") \
            .getOrCreate()
        
        df_bronze = spark.read.parquet(
            f"s3a://banking-lake/bronze/core-banking/accounts/ingestion_date={context['ds']}"
        )
        
        df_silver = df_bronze \
            .dropDuplicates(["account_id"]) \
            .filter(col("account_id").isNotNull()) \
            .filter(col("customer_id").isNotNull()) \
            .withColumn("current_balance", greatest(col("current_balance"), lit(0))) \
            .withColumn("available_balance", 
                least(col("available_balance"), greatest(col("current_balance"), lit(0)))) \
            .withColumn("account_type_standardized",
                when(upper(col("account_type")).isin("SAV", "SAVINGS"), "SAVINGS")
                .when(upper(col("account_type")).isin("CUR", "CURRENT"), "CURRENT")
                .when(upper(col("account_type")).isin("FD", "FIXED"), "FIXED_DEPOSIT")
                .otherwise("OTHER")) \
            .withColumn("status_standardized",
                when(upper(col("status")).isin("ACTIVE", "A"), "ACTIVE")
                .when(upper(col("status")).isin("CLOSED", "C"), "CLOSED")
                .when(upper(col("status")).isin("DORMANT", "D"), "DORMANT")
                .otherwise("UNKNOWN")) \
            .withColumn("cleaned_at", current_timestamp())
        
        df_silver.write \
            .mode("overwrite") \
            .parquet("s3a://banking-lake/silver/core-banking/accounts/")
        
        return {'rows_cleaned': df_silver.count()}

    def clean_transactions(**context):
        """Clean and validate transaction data"""
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import *
        
        spark = SparkSession.builder \
            .appName("Silver_Transaction_Cleaning") \
            .getOrCreate()
        
        df_bronze = spark.read.parquet(
            f"s3a://banking-lake/bronze/core-banking/transactions/ingestion_date={context['ds']}"
        )
        
        df_silver = df_bronze \
            .dropDuplicates(["txn_id"]) \
            .filter(col("txn_id").isNotNull()) \
            .filter(col("amount") > 0) \
            .withColumn("amount", abs(col("amount"))) \
            .withColumn("txn_type_standardized",
                when(upper(col("txn_type")).isin("CR", "CREDIT", "DEPOSIT"), "CREDIT")
                .when(upper(col("txn_type")).isin("DR", "DEBIT", "WITHDRAWAL"), "DEBIT")
                .when(upper(col("txn_type")).isin("TRF", "TRANSFER"), "TRANSFER")
                .otherwise("OTHER")) \
            .withColumn("channel_standardized",
                when(upper(col("channel")).isin("ATM"), "ATM")
                .when(upper(col("channel")).isin("MOBILE", "APP"), "MOBILE")
                .when(upper(col("channel")).isin("WEB", "ONLINE"), "ONLINE")
                .when(upper(col("channel")).isin("BRANCH", "COUNTER"), "BRANCH")
                .otherwise("OTHER")) \
            .withColumn("is_weekend", dayofweek(col("txn_date")).isin(1, 7)) \
            .withColumn("cleaned_at", current_timestamp())
        
        df_silver.write \
            .mode("overwrite") \
            .partitionBy("txn_date") \
            .parquet("s3a://banking-lake/silver/core-banking/transactions/")
        
        return {'rows_cleaned': df_silver.count()}

    def clean_cards(**context):
        """Clean credit card data"""
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import *
        
        spark = SparkSession.builder \
            .appName("Silver_Card_Cleaning") \
            .getOrCreate()
        
        df_bronze = spark.read.parquet(
            f"s3a://banking-lake/bronze/credit-cards/cards/ingestion_date={context['ds']}"
        )
        
        df_silver = df_bronze \
            .dropDuplicates(["card_number"]) \
            .filter(col("card_number").isNotNull()) \
            .filter(col("card_limit") > 0) \
            .withColumn("card_number_masked", 
                concat(lit("XXXX-XXXX-XXXX-"), substring(col("card_number"), -4, 4))) \
            .withColumn("utilization_pct",
                when(col("card_limit") > 0, 
                     round((col("credit_used") / col("card_limit")) * 100, 2))
                .otherwise(0)) \
            .withColumn("cleaned_at", current_timestamp())
        
        df_silver.write \
            .mode("overwrite") \
            .parquet("s3a://banking-lake/silver/credit-cards/cards/")
        
        return {'rows_cleaned': df_silver.count()}

    def validate_silver_data(**context):
        """Validate Silver layer data quality"""
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("Silver_Validation") \
            .getOrCreate()
        
        validation_results = {}
        
        # Check each Silver table
        tables = [
            'core-banking/customers',
            'core-banking/accounts',
            'core-banking/transactions',
            'credit-cards/cards'
        ]
        
        for table in tables:
            try:
                df = spark.read.parquet(f"s3a://banking-lake/silver/{table}")
                count = df.count()
                
                # Check for nulls in primary keys
                if 'customer_id' in df.columns:
                    null_pk = df.filter(col("customer_id").isNull()).count()
                elif 'account_id' in df.columns:
                    null_pk = df.filter(col("account_id").isNull()).count()
                elif 'txn_id' in df.columns:
                    null_pk = df.filter(col("txn_id").isNull()).count()
                elif 'card_number' in df.columns:
                    null_pk = df.filter(col("card_number").isNull()).count()
                else:
                    null_pk = 0
                
                validation_results[table] = {
                    'row_count': count,
                    'null_primary_keys': null_pk,
                    'status': 'PASS' if null_pk == 0 else 'FAIL'
                }
            except Exception as e:
                validation_results[table] = {
                    'row_count': 0,
                    'status': 'ERROR',
                    'error': str(e)
                }
        
        # Check for failures
        failures = {k: v for k, v in validation_results.items() if v['status'] != 'PASS'}
        if failures:
            raise ValueError(f"Silver validation failed: {failures}")
        
        return validation_results

    # Task definitions
    with TaskGroup('clean_sources') as clean_group:
        clean_cust = PythonOperator(
            task_id='clean_customers',
            python_callable=clean_customers,
        )
        
        clean_acct = PythonOperator(
            task_id='clean_accounts',
            python_callable=clean_accounts,
        )
        
        clean_txn = PythonOperator(
            task_id='clean_transactions',
            python_callable=clean_transactions,
        )
        
        clean_card = PythonOperator(
            task_id='clean_cards',
            python_callable=clean_cards,
        )
        
        [clean_cust, clean_acct, clean_txn, clean_card]

    validate = PythonOperator(
        task_id='validate_silver_data',
        python_callable=validate_silver_data,
    )

    clean_group >> validate
