"""
Bronze to Silver Transformation Job
Purpose: Clean, validate, and conform Bronze data to Silver layer
Tool:    PySpark
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with optimal configuration"""
    return SparkSession.builder \
        .appName("Bronze_to_Silver") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def clean_customers(spark, ingestion_date):
    """Clean and deduplicate customer data"""
    logger.info(f"Cleaning customers for {ingestion_date}")
    
    df_bronze = spark.read.parquet(
        f"s3a://banking-lake/bronze/core-banking/customers/ingestion_date={ingestion_date}"
    )
    
    df_silver = df_bronze \
        .dropDuplicates(["customer_id"]) \
        .filter(col("customer_id").isNotNull()) \
        .filter(col("customer_id") != "") \
        .withColumn("customer_name", trim(upper(col("customer_name")))) \
        .withColumn("email", 
            when(col("email").rlike(".*@.*\\..*"), lower(trim(col("email"))))
            .otherwise(None)) \
        .withColumn("phone",
            when(col("phone").rlike("^[0-9]{10,15}$"), col("phone"))
            .otherwise(None)) \
        .withColumn("gender",
            when(upper(col("gender")).isin("M", "MALE"), "MALE")
            .when(upper(col("gender")).isin("F", "FEMALE"), "FEMALE")
            .otherwise("OTHER")) \
        .withColumn("nationality", trim(upper(col("nationality")))) \
        .withColumn("pan_number", trim(col("pan_number"))) \
        .withColumn("cleaned_at", current_timestamp())
    
    df_silver.write \
        .mode("overwrite") \
        .parquet("s3a://banking-lake/silver/core-banking/customers/")
    
    logger.info(f"Customers cleaned: {df_bronze.count()} → {df_silver.count()}")
    return df_silver.count()

def clean_accounts(spark, ingestion_date):
    """Clean and validate account data"""
    logger.info(f"Cleaning accounts for {ingestion_date}")
    
    df_bronze = spark.read.parquet(
        f"s3a://banking-lake/bronze/core-banking/accounts/ingestion_date={ingestion_date}"
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
            .when(upper(col("account_type")).isin("RD", "RECURRING"), "RECURRING_DEPOSIT")
            .otherwise("OTHER")) \
        .withColumn("status_standardized",
            when(upper(col("status")).isin("ACTIVE", "A"), "ACTIVE")
            .when(upper(col("status")).isin("CLOSED", "C"), "CLOSED")
            .when(upper(col("status")).isin("DORMANT", "D"), "DORMANT")
            .when(upper(col("status")).isin("FROZEN", "F"), "FROZEN")
            .otherwise("UNKNOWN")) \
        .withColumn("currency", upper(trim(col("currency")))) \
        .withColumn("branch_code", trim(col("branch_code"))) \
        .withColumn("cleaned_at", current_timestamp())
    
    df_silver.write \
        .mode("overwrite") \
        .parquet("s3a://banking-lake/silver/core-banking/accounts/")
    
    logger.info(f"Accounts cleaned: {df_bronze.count()} → {df_silver.count()}")
    return df_silver.count()

def clean_transactions(spark, ingestion_date):
    """Clean and validate transaction data"""
    logger.info(f"Cleaning transactions for {ingestion_date}")
    
    df_bronze = spark.read.parquet(
        f"s3a://banking-lake/bronze/core-banking/transactions/ingestion_date={ingestion_date}"
    )
    
    df_silver = df_bronze \
        .dropDuplicates(["txn_id"]) \
        .filter(col("txn_id").isNotNull()) \
        .filter(col("account_id").isNotNull()) \
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
            .when(upper(col("channel")).isin("UPI"), "UPI")
            .when(upper(col("channel")).isin("NEFT", "RTGS", "IMPS"), "BANK_TRANSFER")
            .otherwise("OTHER")) \
        .withColumn("is_weekend", dayofweek(col("txn_date")).isin(1, 7)) \
        .withColumn("time_bucket",
            when(hour(col("txn_timestamp")).between(6, 11), "MORNING")
            .when(hour(col("txn_timestamp")).between(12, 17), "AFTERNOON")
            .when(hour(col("txn_timestamp")).between(18, 22), "EVENING")
            .otherwise("NIGHT")) \
        .withColumn("currency", upper(trim(col("currency")))) \
        .withColumn("description", trim(col("description"))) \
        .withColumn("reference_number", trim(col("reference"))) \
        .withColumn("cleaned_at", current_timestamp())
    
    df_silver.write \
        .mode("overwrite") \
        .partitionBy("txn_date") \
        .parquet("s3a://banking-lake/silver/core-banking/transactions/")
    
    logger.info(f"Transactions cleaned: {df_bronze.count()} → {df_silver.count()}")
    return df_silver.count()

def clean_cards(spark, ingestion_date):
    """Clean credit card data"""
    logger.info(f"Cleaning cards for {ingestion_date}")
    
    df_bronze = spark.read.parquet(
        f"s3a://banking-lake/bronze/credit-cards/cards/ingestion_date={ingestion_date}"
    )
    
    df_silver = df_bronze \
        .dropDuplicates(["card_number"]) \
        .filter(col("card_number").isNotNull()) \
        .filter(col("customer_id").isNotNull()) \
        .filter(col("card_limit") > 0) \
        .withColumn("card_number_masked", 
            concat(lit("XXXX-XXXX-XXXX-"), substring(col("card_number"), -4, 4))) \
        .withColumn("card_brand",
            when(upper(col("card_type")).isin("VISA"), "VISA")
            .when(upper(col("card_type")).isin("MASTERCARD", "MC"), "MASTERCARD")
            .when(upper(col("card_type")).isin("AMEX"), "AMEX")
            .when(upper(col("card_type")).isin("RUPAY"), "RUPAY")
            .otherwise("OTHER")) \
        .withColumn("available_credit", 
            greatest(col("card_limit") - col("credit_used"), lit(0))) \
        .withColumn("utilization_pct",
            when(col("card_limit") > 0, 
                 round((col("credit_used") / col("card_limit")) * 100, 2))
            .otherwise(0)) \
        .withColumn("cleaned_at", current_timestamp())
    
    df_silver.write \
        .mode("overwrite") \
        .parquet("s3a://banking-lake/silver/credit-cards/cards/")
    
    logger.info(f"Cards cleaned: {df_bronze.count()} → {df_silver.count()}")
    return df_silver.count()

def clean_loans(spark, ingestion_date):
    """Clean loan data"""
    logger.info(f"Cleaning loans for {ingestion_date}")
    
    df_bronze = spark.read.parquet(
        f"s3a://banking-lake/bronze/loans/loan_accounts/ingestion_date={ingestion_date}"
    )
    
    df_silver = df_bronze \
        .dropDuplicates(["loan_id"]) \
        .filter(col("loan_id").isNotNull()) \
        .filter(col("customer_id").isNotNull()) \
        .filter(col("principal_amount") > 0) \
        .filter(col("interest_rate") > 0) \
        .filter(col("interest_rate") < 30) \
        .withColumn("loan_type_standardized",
            when(upper(col("loan_type")).isin("HL", "HOME"), "HOME_LOAN")
            .when(upper(col("loan_type")).isin("PL", "PERSONAL"), "PERSONAL_LOAN")
            .when(upper(col("loan_type")).isin("CL", "CAR", "AUTO"), "CAR_LOAN")
            .when(upper(col("loan_type")).isin("BL", "BUSINESS"), "BUSINESS_LOAN")
            .when(upper(col("loan_type")).isin("ED", "EDUCATION"), "EDUCATION_LOAN")
            .when(upper(col("loan_type")).isin("GL", "GOLD"), "GOLD_LOAN")
            .otherwise("OTHER")) \
        .withColumn("principal_outstanding", 
            greatest(col("principal_outstanding"), lit(0))) \
        .withColumn("interest_rate_band",
            when(col("interest_rate") < 8, "LOW")
            .when(col("interest_rate").between(8, 12), "MEDIUM")
            .when(col("interest_rate") > 12, "HIGH")
            .otherwise("UNKNOWN")) \
        .withColumn("loan_status",
            when(col("maturity_date") >= current_date(), "ACTIVE")
            .otherwise("MATURED")) \
        .withColumn("cleaned_at", current_timestamp())
    
    df_silver.write \
        .mode("overwrite") \
        .parquet("s3a://banking-lake/silver/loans/loan_accounts/")
    
    logger.info(f"Loans cleaned: {df_bronze.count()} → {df_silver.count()}")
    return df_silver.count()

def main():
    """Main execution function"""
    import sys
    
    # Get ingestion date from command line or use yesterday
    if len(sys.argv) > 1:
        ingestion_date = sys.argv[1]
    else:
        from datetime import datetime, timedelta
        ingestion_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    logger.info(f"Starting Bronze to Silver transformation for {ingestion_date}")
    
    spark = create_spark_session()
    
    try:
        # Clean each source
        results = {
            'customers': clean_customers(spark, ingestion_date),
            'accounts': clean_accounts(spark, ingestion_date),
            'transactions': clean_transactions(spark, ingestion_date),
            'cards': clean_cards(spark, ingestion_date),
            'loans': clean_loans(spark, ingestion_date),
        }
        
        # Log summary
        logger.info("=" * 60)
        logger.info("Bronze to Silver Transformation Complete")
        logger.info("=" * 60)
        for source, count in results.items():
            logger.info(f"  {source}: {count} rows cleaned")
        
        return results
        
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
