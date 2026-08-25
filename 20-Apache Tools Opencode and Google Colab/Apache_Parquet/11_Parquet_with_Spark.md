# Parquet with Spark

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-large-scale-etl)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-streaming-analytics)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### Spark + Parquet: The Big Data Standard

Apache Spark is the most common engine for processing Parquet at scale:

> **Spark reads and writes Parquet files in parallel across a cluster, enabling processing of terabytes to petabytes of data with automatic optimization.**

### Why Spark + Parquet?

```
Single Machine (DuckDB/Pandas):
  Parquet → Read → Process → Result

Distributed (Spark):
  Parquet → Read (multiple executors) → Process (parallel) → Result
```

**Benefits:**
1. **Horizontal scaling**: Add more nodes for larger data
2. **Automatic partitioning**: Spark parallelizes across Parquet row groups
3. **Predicate pushdown**: Filters pushed to Parquet reader
4. **Column pruning**: Only reads needed columns
5. **Catalyst optimizer**: SQL optimizations applied automatically

### How Spark Reads Parquet

```
Spark Driver
     |
     +-- Creates read plan
     |
     v
Executors (multiple machines)
     |
     +-- Each reads assigned row groups
     +-- Applies predicate pushdown
     +-- Applies column pruning
     |
     v
Arrow / Columnar Memory
     |
     v
Processing (filter, join, aggregate)
     |
     v
Write Parquet (parallel)
```

### Spark Parquet API

#### PySpark

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

spark = SparkSession.builder \
    .appName("Parquet Example") \
    .getOrCreate()

# Read Parquet
df = spark.read.parquet("s3://bucket/transactions/")

# Read with schema
df = spark.read.parquet("s3://bucket/transactions/").schema(explicit_schema)

# Read specific columns (column pruning)
df = spark.read.parquet("s3://bucket/transactions/").select("amount", "status")

# Read with filter (predicate pushdown)
df = spark.read.parquet("s3://bucket/transactions/") \
    .filter(col("amount") > 1000)

# Write Parquet
df.write.parquet("s3://bucket/output/")

# Write partitioned
df.write.partitionBy("date", "status").parquet("s3://bucket/output/")
```

#### Spark SQL

```python
# Register as temp view
df.createOrReplaceTempView("transactions")

# SQL query
result = spark.sql("""
    SELECT 
        status,
        COUNT(*) as count,
        SUM(amount) as total
    FROM transactions
    WHERE amount > 1000
    GROUP BY status
""")
```

### Spark Parquet Optimizations

1. **Predicate pushdown**: Filters pushed to Parquet reader
```python
# Spark pushes this filter to Parquet
df.filter(col("date") >= "2026-08-01")
```

2. **Column pruning**: Only reads needed columns
```python
# Only reads 'amount' and 'status'
df.select("amount", "status")
```

3. **Partition pruning**: Skips irrelevant partitions
```python
# Skips partitions that don't match
df.filter(col("date") == "2026-08-24")
```

4. **AQE (Adaptive Query Execution)**: Dynamically optimizes queries
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### Spark Write Optimizations

1. **Repartition**: Control number of output files
```python
df.repartition(100).write.parquet("output/")
```

2. **Coalesce**: Reduce number of partitions
```python
df.coalesce(10).write.parquet("output/")
```

3. **Bucketing**: Hash-partition by column
```python
df.write.bucketBy(100, "account_id").sortBy("date").saveAsTable("transactions")
```

### Spark + Parquet Architecture

```
Data Sources (S3, HDFS, Kafka)
       |
       v
  Spark Cluster
       |
       +-- Driver (planning)
       +-- Executors (processing)
       |
       v
  Catalyst Optimizer
       |
       +-- Predicate pushdown
       +-- Column pruning
       +- Join optimization
       |
       v
  Tungsten (code generation)
       |
       v
  Parquet Output (S3, HDFS)
```

---

## 2. Example

### Spark Parquet Operations (PySpark)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Create Spark session
spark = SparkSession.builder \
    .appName("Banking Parquet Example") \
    .master("local[*]") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("transaction_id", IntegerType(), False),
    StructField("account_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("status", StringType(), False),
    StructField("date", StringType(), False),
])

# Create sample data
data = [
    (1, "ACC001", 100.0, "COMPLETED", "2026-08-01"),
    (2, "ACC002", 250.0, "PENDING", "2026-08-01"),
    (3, "ACC001", 500.0, "COMPLETED", "2026-08-02"),
    (4, "ACC003", 75.0, "FAILED", "2026-08-02"),
    (5, "ACC002", 1000.0, "COMPLETED", "2026-08-03"),
]

df = spark.createDataFrame(data, schema)

# Write to Parquet
df.write.mode("overwrite").parquet("/tmp/transactions")

# Read back
df_read = spark.read.parquet("/tmp/transactions")

# Query
df_read.filter(col("amount") > 100) \
    .groupBy("status") \
    .agg(sum("amount").alias("total_amount")) \
    .show()

spark.stop()
```

---

## 3. Banking Scenario 1: Large-Scale ETL

### Problem
A bank processes **10 billion transactions daily** across 5,000 branches. The ETL pipeline must:
- Read from Oracle (source) and Kafka (real-time)
- Transform and enrich data
- Write to Parquet in S3
- Complete within 4-hour window

### Why Spark + Parquet?
- Distributed processing across 100+ nodes
- Parallel Parquet reads/writes
- Automatic optimization (predicate pushdown, column pruning)
- Handles data skew and failures

### Architecture
```
Oracle (source) + Kafka (real-time)
       |
       v
  Spark Cluster (100+ executors)
       |
       +-- Read source data
       +-- Transform & enrich
       +-- Write Parquet
       |
       v
  S3 Data Lake (Parquet)
       |
       v
  Analytics (DuckDB / Trino)
```

---

## 4. Python Code - Scenario 1

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, when, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DateType
import os
import tempfile

# ============================================================
# BANKING SCENARIO: Large-Scale ETL with Spark + Parquet
# ============================================================

def create_spark_session():
    """Create Spark session with optimized settings."""
    return SparkSession.builder \
        .appName("Banking ETL") \
        .master("local[*]") \
        .config("spark.sql.parquet.compression.codec", "zstd") \
        .config("spark.sql.parquet.writeLegacyFormat", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()


def generate_source_data(spark, num_rows=100_000):
    """Generate source transaction data."""
    import random
    random.seed(42)

    data = []
    for i in range(num_rows):
        data.append((
            i + 1,
            f"ACC{random.randint(100000, 999999)}",
            f"CUST{random.randint(10000, 99999)}",
            f"BR{random.randint(100, 999)}",
            round(random.uniform(1.0, 100000.0), 2),
            random.choice(["USD", "EUR", "GBP"]),
            random.choice(["DEBIT", "CREDIT", "TRANSFER", "WIRE"]),
            random.choice(["COMPLETED", "PENDING", "FAILED"]),
            random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM", "POS"]),
            f"2026-08-{random.randint(1, 24):02d}",
        ))

    schema = StructType([
        StructField("transaction_id", IntegerType(), False),
        StructField("account_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("branch_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), False),
        StructField("transaction_type", StringType(), False),
        StructField("status", StringType(), False),
        StructField("channel", StringType(), False),
        StructField("transaction_date", StringType(), False),
    ])

    return spark.createDataFrame(data, schema)


def run_etl_pipeline(spark, source_df, output_path):
    """Run ETL pipeline with transformations."""
    from pyspark.sql.functions import to_date, col

    # 1. Data cleansing
    clean_df = source_df.filter(col("amount") > 0) \
        .filter(col("status").isin(["COMPLETED", "PENDING", "FAILED"]))

    # 2. Type conversion
    transformed_df = clean_df \
        .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")) \
        .withColumn("amount_usd", 
            when(col("currency") == "USD", col("amount"))
            .when(col("currency") == "EUR", col("amount") * 1.085)
            .when(col("currency") == "GBP", col("amount") * 1.27)
            .otherwise(col("amount"))
        )

    # 3. Add metadata
    enriched_df = transformed_df \
        .withColumn("etl_timestamp", current_timestamp()) \
        .withColumn("data_source", lit("CORE_BANKING"))

    # 4. Write to Parquet (partitioned)
    enriched_df.write \
        .mode("overwrite") \
        .partitionBy("transaction_date", "status") \
        .parquet(output_path)

    # 5. Verify
    written_df = spark.read.parquet(output_path)
    print(f"\n=== ETL Report ===")
    print(f"Source rows: {source_df.count():,}")
    print(f"Written rows: {written_df.count():,}")
    print(f"Partitions: {len(written_df.inputFiles())}")

    return written_df


def run_analytics(spark, parquet_path):
    """Run analytics on Parquet data."""
    df = spark.read.parquet(parquet_path)

    # 1. Daily volume
    print(f"\n=== Daily Transaction Volume ===")
    df.groupBy("transaction_date") \
        .agg(
            count("*").alias("tx_count"),
            sum("amount_usd").alias("total_volume"),
            avg("amount_usd").alias("avg_amount")
        ) \
        .orderBy("transaction_date") \
        .show(10, truncate=False)

    # 2. By status
    print(f"\n=== Status Distribution ===")
    df.groupBy("status") \
        .agg(
            count("*").alias("count"),
            sum("amount_usd").alias("total_amount")
        ) \
        .orderBy(col("total_amount").desc()) \
        .show(truncate=False)

    # 3. By channel
    print(f"\n=== Channel Analysis ===")
    df.groupBy("channel") \
        .agg(
            count("*").alias("count"),
            avg("amount_usd").alias("avg_amount")
        ) \
        .orderBy(col("count").desc()) \
        .show(truncate=False)


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    spark = create_spark_session()

    try:
        # Generate source data
        print("Generating source data...")
        source_df = generate_source_data(spark, num_rows=100_000)

        # Run ETL
        output_path = os.path.join(tempfile.gettempdir(), "spark_etl_output")
        enriched_df = run_etl_pipeline(spark, source_df, output_path)

        # Run analytics
        run_analytics(spark, output_path)

    finally:
        spark.stop()
```

---

## 5. Banking Scenario 2: Streaming Analytics

### Problem
A bank needs real-time analytics on streaming transaction data:
- Real-time fraud detection
- Live dashboard updates
- Alert generation for suspicious patterns

Data flows: Kafka → Spark Structured Streaming → Parquet (S3)

### Why Spark Streaming + Parquet?
- Spark Structured Streaming handles continuous data
- Micro-batch writes to Parquet
- Exactly-once semantics
- Automatic scaling

### Architecture
```
Kafka (10K events/sec)
       |
       v
  Spark Structured Streaming
       |
       +-- Real-time aggregations
       +-- Fraud detection
       +-- Alert generation
       |
       v
  Parquet Files (S3, micro-batch every 30s)
       |
       v
  Real-time Dashboard
```

---

## 6. Python Code - Scenario 2

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, sum, avg, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import os
import tempfile
import time

# ============================================================
# BANKING SCENARIO: Streaming Analytics with Spark + Parquet
# ============================================================

def create_spark_session():
    """Create Spark session for streaming."""
    return SparkSocketSession.builder \
        .appName("Banking Streaming") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .getOrCreate()


class SparkSocketSession:
    """Wrapper for Spark session creation."""
    pass


def create_spark_session():
    """Create Spark session."""
    return SparkSession.builder \
        .appName("Banking Streaming Analytics") \
        .master("local[*]") \
        .getOrCreate()


def generate_streaming_data(spark, num_batches=10, batch_size=1000):
    """Simulate streaming data by generating micro-batches."""
    import random
    random.seed(42)

    all_data = []

    for batch_idx in range(num_batches):
        batch_data = []
        for _ in range(batch_size):
            batch_data.append((
                f"TX{random.randint(100000, 999999)}",
                f"CARD{random.randint(10000, 99999)}",
                round(random.uniform(1.0, 50000.0), 2),
                random.choice(["POS", "ONLINE", "ATM", "MOBILE"]),
                random.choice(["COMPLETED", "PENDING", "FAILED"]),
                f"2026-08-24 {random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
            ))
        all_data.extend(batch_data)

    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("card_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("channel", StringType(), False),
        StructField("status", StringType(), False),
        StructField("timestamp", StringType(), False),
    ])

    return spark.createDataFrame(all_data, schema)


def run_streaming_analytics(spark, df, output_path):
    """Run streaming analytics and write to Parquet."""
    from pyspark.sql.functions import window, col, count, sum, avg

    # Windowed aggregation
    aggregated_df = df \
        .withWatermark("timestamp", "1 hour") \
        .groupBy(
            window(col("timestamp"), "1 hour"),
            col("channel")
        ) \
        .agg(
            count("*").alias("tx_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        )

    # Write to Parquet (micro-batch)
    aggregated_df.write \
        .mode("append") \
        .partitionBy("channel") \
        .parquet(output_path)

    # Verify
    result_df = spark.read.parquet(output_path)
    print(f"\n=== Streaming Analytics Report ===")
    print(f"Aggregated records: {result_df.count():,}")

    return result_df


def detect_fraud_patterns(spark, df):
    """Detect potential fraud patterns."""
    from pyspark.sql.functions import col, count, avg, when

    # High-value transactions
    high_value = df.filter(col("amount") > 10000)

    # Channel analysis
    channel_stats = df.groupBy("channel").agg(
        count("*").alias("tx_count"),
        avg("amount").alias("avg_amount"),
        sum(when(col("amount") > 10000, 1).otherwise(0)).alias("high_value_count")
    )

    print(f"\n=== Fraud Pattern Analysis ===")
    channel_stats.show(truncate=False)

    # Suspicious patterns
    suspicious = df.filter(
        (col("amount") > 10000) & (col("status") == "FAILED")
    )

    print(f"\n=== Suspicious Transactions ===")
    print(f"High-value failed transactions: {suspicious.count():,}")

    return suspicious


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    spark = create_spark_session()

    try:
        # Generate streaming data
        print("Generating streaming data...")
        streaming_df = generate_streaming_data(spark, num_batches=5, batch_size=1000)

        # Run streaming analytics
        output_path = os.path.join(tempfile.gettempdir(), "streaming_output")
        result_df = run_streaming_analytics(spark, streaming_df, output_path)

        # Detect fraud patterns
        detect_fraud_patterns(spark, streaming_df)

    finally:
        spark.stop()
```

---

## 7. Interview Questions

### Q1: How does Spark optimize Parquet reads?

**Answer:**

Spark applies several optimizations:

1. **Predicate pushdown**: Filters pushed to Parquet reader
```python
# Spark pushes this filter to Parquet
df.filter(col("date") >= "2026-08-01")
# Parquet skips row groups where max(date) < 2026-08-01
```

2. **Column pruning**: Only reads needed columns
```python
# Only reads 'amount' and 'status'
df.select("amount", "status")
```

3. **Partition pruning**: Skips irrelevant partitions
```python
# Skips partitions that don't match
df.filter(col("date") == "2026-08-24")
```

4. **Parallel reads**: Multiple executors read different row groups simultaneously

5. **AQE (Adaptive Query Execution)**: Dynamically optimizes query plan

---

### Q2: What is the difference between repartition and coalesce in Spark Parquet writes?

**Answer:**

**Repartition**:
- Increases or decreases partitions
- Full shuffle (expensive)
- Even distribution of data
```python
df.repartition(100).write.parquet("output/")  # 100 output files
```

**Coalesce**:
- Only decreases partitions
- No shuffle (cheaper)
- May cause data skew
```python
df.coalesce(10).write.parquet("output/")  # 10 output files
```

**When to use which:**
- **Repartition**: When you need even distribution (e.g., before joins)
- **Coalesce**: When reducing partitions (e.g., after filtering reduces data)

---

### Q3: How do you handle schema evolution in Spark + Parquet?

**Answer:**

Spark supports schema evolution:

```python
# Enable schema evolution
spark.conf.set("spark.sql.schemaMerger.enabled", "true")

# Read files with different schemas
df = spark.read.parquet("data/")  # Spark merges schemas

# Write with new schema
new_df = df.withColumn("new_column", lit("value"))
new_df.write.parquet("data/")
```

**Best practices:**
1. Always use explicit schemas
2. Add new columns at the end
3. Test schema compatibility before production
4. Use Iceberg/Delta Lake for complex evolution

---

### Q4: How do you optimize Spark Parquet writes?

**Answer:**

1. **Repartition for even distribution**:
```python
df.repartition(100, "date").write.parquet("output/")
```

2. **Use appropriate compression**:
```python
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
```

3. **Control row group size**:
```python
spark.conf.set("spark.sql.parquet.rowGroupSize", "134217728")  # 128MB
```

4. **Enable dictionary encoding** (default):
```python
spark.conf.set("spark.sql.parquet.enableDictionary", "true")
```

5. **Write partitioned data**:
```python
df.write.partitionBy("date", "status").parquet("output/")
```

6. **Use AQE for dynamic optimization**:
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

---

### Q5: Compare Spark and DuckDB for Parquet operations.

**Answer:**

| Feature | Spark | DuckDB |
|---------|-------|--------|
| **Architecture** | Distributed cluster | Embedded single-node |
| **Scale** | TB to PB | GB to TB |
| **Setup** | Cluster required | pip install |
| **SQL support** | Spark SQL | Full SQL |
| **Streaming** | Structured Streaming | Not native |
| **Write optimization** | Repartition, bucketing | Basic |
| **Best for** | Large-scale ETL | Ad-hoc analysis |

**When to use Spark:**
- Data > 1 TB
- Distributed processing required
- Streaming workloads
- Complex ETL pipelines
- Multi-user concurrent access

**When to use DuckDB:**
- Data < 1 TB
- Ad-hoc analysis
- Single user
- Quick prototyping
- No cluster setup available
