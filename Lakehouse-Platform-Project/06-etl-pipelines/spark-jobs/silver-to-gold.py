"""
Silver to Gold Transformation Job
Purpose: Create business-ready aggregations from Silver layer
Tool:    PySpark
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("Silver_to_Gold") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def create_customer_360(spark):
    """Create Customer 360 view"""
    logger.info("Creating Customer 360 view")
    customers = spark.read.parquet("s3a://banking-lake/silver/core-banking/customers/")
    accounts = spark.read.parquet("s3a://banking-lake/silver/core-banking/accounts/")
    cards = spark.read.parquet("s3a://banking-lake/silver/credit-cards/cards/")
    loans = spark.read.parquet("s3a://banking-lake/silver/loans/loan_accounts/")
    transactions = spark.read.parquet("s3a://banking-lake/silver/core-banking/transactions/")

    result = customers.alias("c") \
        .join(accounts.alias("a"), "customer_id", "left") \
        .join(cards.alias("cc"), "customer_id", "left") \
        .join(loans.alias("l"), "customer_id", "left") \
        .groupBy("c.customer_id", "c.customer_name", "c.email", "c.phone", "c.city") \
        .agg(
            countDistinct("a.account_id").alias("total_accounts"),
            sum("a.current_balance").alias("total_balance"),
            countDistinct("cc.card_number").alias("total_cards"),
            sum("cc.card_limit").alias("total_card_limit"),
            sum("cc.credit_used").alias("total_card_outstanding"),
            countDistinct("l.loan_id").alias("total_loans"),
            sum("l.principal_outstanding").alias("total_loan_outstanding"),
            sum("l.emi_amount").alias("total_monthly_emi")
        ) \
        .withColumn("net_relationship_value",
            coalesce(col("total_balance"), lit(0)) +
            coalesce(col("total_card_limit"), lit(0)) -
            coalesce(col("total_loan_outstanding"), lit(0))) \
        .withColumn("customer_segment",
            when(col("net_relationship_value") >= 10000000000, "PLATINUM")
            .when(col("net_relationship_value") >= 5000000000, "GOLD")
            .when(col("net_relationship_value") >= 1000000000, "SILVER")
            .otherwise("STANDARD"))

    result.write.mode("overwrite").parquet("s3a://banking-lake/gold/customer-360/")
    logger.info(f"Customer 360: {result.count()} customers")
    return result.count()

def create_daily_transaction_summary(spark):
    """Create daily transaction summary"""
    logger.info("Creating daily transaction summary")
    transactions = spark.read.parquet("s3a://banking-lake/silver/core-banking/transactions/")

    result = transactions.groupBy("txn_date", "channel_standardized", "txn_type_standardized") \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            countDistinct("account_id").alias("unique_accounts"),
            sum(when(col("amount") > 100000, 1).otherwise(0)).alias("high_value_count")
        )

    result.write.mode("overwrite").partitionBy("txn_date") \
        .parquet("s3a://banking-lake/gold/daily-transaction-summary/")
    logger.info(f"Daily summary: {result.count()} records")
    return result.count()

def create_credit_risk_dashboard(spark):
    """Create credit risk dashboard"""
    logger.info("Creating credit risk dashboard")
    loans = spark.read.parquet("s3a://banking-lake/silver/loans/loan_accounts/")
    payments = spark.read.parquet("s3a://banking-lake/silver/loans/loan_payments/")
    customers = spark.read.parquet("s3a://banking-lake/silver/core-banking/customers/")

    result = loans.alias("l") \
        .join(customers.alias("c"), "customer_id", "left") \
        .join(payments.alias("p"), "loan_id", "left") \
        .groupBy("l.customer_id", "c.customer_name", "l.loan_id", "l.loan_type",
                 "l.principal_amount", "l.principal_outstanding", "l.interest_rate",
                 "l.emi_amount", "l.disbursement_date", "l.maturity_date") \
        .agg(
            count("p.payment_id").alias("total_payments_made"),
            sum(when(col("p.status") == "SUCCESS", 1).otherwise(0)).alias("successful_payments"),
            sum(when(col("p.status") == "FAILED", 1).otherwise(0)).alias("failed_payments")
        ) \
        .withColumn("payment_success_rate",
            when(col("total_payments_made") > 0,
                 round(col("successful_payments") * 100.0 / col("total_payments_made"), 2))
            .otherwise(0)) \
        .withColumn("risk_classification",
            when(col("maturity_date") < current_date(), "MATURED")
            .otherwise("STANDARD"))

    result.write.mode("overwrite").parquet("s3a://banking-lake/gold/credit-risk-dashboard/")
    logger.info(f"Credit risk dashboard: {result.count()} loans")
    return result.count()

def main():
    spark = create_spark_session()
    try:
        results = {
            'customer_360': create_customer_360(spark),
            'daily_transactions': create_daily_transaction_summary(spark),
            'credit_risk': create_credit_risk_dashboard(spark),
        }
        logger.info("=" * 60)
        logger.info("Silver to Gold Transformation Complete")
        for name, count in results.items():
            logger.info(f"  {name}: {count} rows")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
