# Concept 13: Arrow and Spark Integration

## 📚 Detailed Explanation

**Arrow and Spark** integration enables efficient data interchange between Apache Spark and Arrow, providing high-performance processing for large-scale datasets.

### Why Arrow + Spark?

**Without Integration:**
```
Spark → Serialize → Arrow → Deserialize → Spark
       (slow)                 (slow)
```

**With Integration:**
```
Spark ↔ Zero-Copy ↔ Arrow
       (fast)
```

### Key Features

| Feature | Spark | Arrow |
|---------|-------|-------|
| **Primary Use** | Distributed processing | In-memory format |
| **Scale** | Cluster | Single node |
| **Optimization** | Query planning | Memory layout |
| **Use Case** | ETL, ML | Data interchange |

### Arrow in Spark

```python
from pyspark.sql import SparkSession
import pyarrow as pa

spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# Arrow optimization enabled
df = spark.read.parquet("data.parquet")
```

---

## 💡 Example: Arrow + Spark in Banking

### Scenario: ETL Processing

```python
from pyspark.sql import SparkSession
import pyarrow as pa

spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# Read data with Arrow optimization
df = spark.read.parquet("transactions.parquet")

# Process with Spark
result = df.groupBy("branch").agg({"amount": "sum"})

# Convert to Arrow for fast processing
arrow_table = result.toArrow()

# Further processing with Arrow
total = pa.compute.sum(arrow_table.column("sum(amount)"))
```

---

## 🏦 Real-World Banking Scenario 1: Large-Scale ETL

### Scenario
A bank's **ETL pipeline** processes **100 TB of data daily**. They need:
- Efficient data processing
- Fast transformations
- Scalable architecture

### Problem
- Large data volumes
- Complex transformations
- Performance requirements

### Solution
Arrow + Spark integration:
- Distributed processing
- Zero-copy interchange
- Vectorized operations

### Python Code

```python
"""
Banking Scenario 1: Large-Scale ETL
Using Arrow and Spark
"""

from pyspark.sql import SparkSession
import pyarrow as pa
import pyarrow.parquet as pq
import random
import time

# ============================================================
# STEP 1: Setup Spark with Arrow
# ============================================================

print("=== LARGE-SCALE ETL WITH ARROW AND SPARK ===\n")

spark = SparkSession.builder \
    .appName("BankingETL") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
    .getOrCreate()

print(f"Spark session created")
print(f"Arrow optimization: Enabled")

# ============================================================
# STEP 2: Generate Large Dataset
# ============================================================

print("\n--- Generating Large Dataset ---")

def generate_etl_data(num_records: int) -> pa.Table:
    """Generate large dataset for ETL."""
    
    data = {
        "transaction_id": [f"TXN-{i:012d}" for i in range(1, num_records + 1)],
        "account_id": [f"ACC-{random.randint(1000, 9999):06d}" for _ in range(num_records)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(num_records)],
        "transaction_type": [random.choice(["CREDIT", "DEBIT", "TRANSFER"]) for _ in range(num_records)],
        "branch": [random.choice(["Mumbai", "Delhi", "Bangalore", "Chennai"]) for _ in range(num_records)],
        "date": ["2026-08-24"] * num_records,
    }
    
    return pa.table(data)

# Generate 10 million records
print("Generating 10 million records...")
arrow_table = generate_etl_data(10000000)
print(f"Generated: {len(arrow_table):,} records")

# ============================================================
# STEP 3: Convert to Spark DataFrame
# ============================================================

print("\n--- Converting to Spark DataFrame ---")

# Convert Arrow to Spark DataFrame
start_time = time.time()
spark_df = spark.createDataFrame(arrow_table.to_pandas())
conversion_time = time.time() - start_time

print(f"\nConversion:")
print(f"  Arrow → Spark: {conversion_time:.3f} seconds")
print(f"  DataFrame partitions: {spark_df.rdd.getNumPartitions()}")

# ============================================================
# STEP 4: ETL Processing with Spark
# ============================================================

print("\n--- ETL Processing with Spark ---")

# Transformation 1: Filter
start_time = time.time()
filtered_df = spark_df.filter(spark_df.amount > 50000)
filter_time = time.time() - start_time

print(f"\nFilter (amount > 50,000):")
print(f"  Records: {filtered_df.count():,}")
print(f"  Time: {filter_time:.3f} seconds")

# Transformation 2: Aggregation
start_time = time.time()
agg_df = spark_df.groupBy("branch", "transaction_type").agg({
    "amount": "sum",
    "transaction_id": "count"
})
agg_time = time.time() - start_time

print(f"\nAggregation:")
print(f"  Groups: {agg_df.count()}")
print(f"  Time: {agg_time:.3f} seconds")

# Transformation 3: Window function
from pyspark.sql.window import Window
from pyspark.sql import functions as F

start_time = time.time()
window_spec = Window.partitionBy("branch").orderBy(F.desc("amount"))
ranked_df = spark_df.withColumn("rank", F.row_number().over(window_spec))
rank_time = time.time() - start_time

print(f"\nWindow Function:")
print(f"  Time: {rank_time:.3f} seconds")

# ============================================================
# STEP 5: Convert Back to Arrow
# ============================================================

print("\n--- Converting Back to Arrow ---")

# Convert Spark DataFrame to Arrow
start_time = time.time()
result_arrow = agg_df.toArrow()
back_conversion_time = time.time() - start_time

print(f"\nConversion:")
print(f"  Spark → Arrow: {back_conversion_time:.3f} seconds")
print(f"  Arrow table size: {result_arrow.nbytes / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 6: Write to Parquet
# ============================================================

print("\n--- Writing to Parquet ---")

# Write with Arrow optimization
start_time = time.time()
pq.write_table(result_arrow, "/tmp/etl_output.parquet", compression="snappy")
write_time = time.time() - start_time

import os
file_size = os.path.getsize("/tmp/etl_output.parquet")

print(f"\nParquet Write:")
print(f"  Time: {write_time:.3f} seconds")
print(f"  File size: {file_size / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 7: Performance Summary
# ============================================================

print("\n--- Performance Summary ---")

print("""
ETL PIPELINE PERFORMANCE:

Dataset: 10 million records

Operations:
  - Conversion (Arrow → Spark): {conversion_time:.3f} seconds
  - Filter: {filter_time:.3f} seconds
  - Aggregation: {agg_time:.3f} seconds
  - Window Function: {rank_time:.3f} seconds
  - Conversion (Spark → Arrow): {back_conversion_time:.3f} seconds
  - Write: {write_time:.3f} seconds

Total Time: {total_time:.3f} seconds

PERFORMANCE CHARACTERISTICS:
  ✓ Arrow optimization enabled
  ✓ Zero-copy conversion
  ✓ Distributed processing
  ✓ Efficient Parquet I/O

vs Without Arrow:
  - 2-3x faster conversions
  - 50% less memory
  - Better performance
""")
```

---

## 🏦 Real-World Banking Scenario 2: Machine Learning Pipeline

### Scenario
A bank's **ML team** needs to:
- Process large datasets
- Train models
- Deploy predictions

### Problem
- Large training data
- Feature engineering
- Model serving

### Solution
Arrow + Spark integration:
- Distributed processing
- Efficient feature computation
- Fast model serving

### Python Code

```python
"""
Banking Scenario 2: Machine Learning Pipeline
Using Arrow and Spark
"""

from pyspark.sql import SparkSession
import pyarrow as pa
import random
import time

# ============================================================
# STEP 1: Setup Spark
# ============================================================

print("=== ML PIPELINE WITH ARROW AND SPARK ===\n")

spark = SparkSession.builder \
    .appName("BankingML") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

print(f"Spark session created")

# ============================================================
# STEP 2: Generate Training Data
# ============================================================

print("\n--- Generating Training Data ---")

def generate_ml_data(num_records: int) -> pa.Table:
    """Generate ML training data."""
    
    data = {
        "customer_id": [f"CUST-{i:06d}" for i in range(1, num_records + 1)],
        "age": [random.randint(18, 80) for _ in range(num_records)],
        "income": [round(random.uniform(20000, 200000), 2) for _ in range(num_records)],
        "account_balance": [round(random.uniform(1000, 500000), 2) for _ in range(num_records)],
        "num_transactions": [random.randint(1, 100) for _ in range(num_records)],
        "credit_score": [random.randint(300, 850) for _ in range(num_records)],
        "is_high_risk": [random.choice([0, 1]) for _ in range(num_records)],
    }
    
    return pa.table(data)

# Generate 5 million records
print("Generating 5 million records...")
ml_data = generate_ml_data(5000000)
print(f"Generated: {len(ml_data):,} records")

# ============================================================
# STEP 3: Feature Engineering with Spark
# ============================================================

print("\n--- Feature Engineering with Spark ---")

# Convert to Spark DataFrame
spark_df = spark.createDataFrame(ml_data.to_pandas())

# Feature engineering
start_time = time.time()

# 1. Age bins
from pyspark.sql import functions as F
spark_df = spark_df.withColumn(
    "age_group",
    F.when(F.col("age") < 30, "Young")
    .when(F.col("age") < 50, "Middle")
    .otherwise("Senior")
)

# 2. Income per age
spark_df = spark_df.withColumn(
    "income_per_age",
    F.col("income") / F.col("age")
)

# 3. Balance to income ratio
spark_df = spark_df.withColumn(
    "balance_to_income",
    F.col("account_balance") / F.col("income")
)

# 4. Transaction intensity
spark_df = spark_df.withColumn(
    "transaction_intensity",
    F.col("num_transactions") * F.col("account_balance")
)

feature_time = time.time() - start_time

print(f"\nFeature Engineering:")
print(f"  Features created: 4")
print(f"  Time: {feature_time:.3f} seconds")

# ============================================================
# STEP 4: Convert to Arrow for ML
# ============================================================

print("\n--- Converting to Arrow for ML ---")

# Convert Spark DataFrame to Arrow
start_time = time.time()
arrow_features = spark_df.toArrow()
conversion_time = time.time() - start_time

print(f"\nConversion:")
print(f"  Spark → Arrow: {conversion_time:.3f} seconds")
print(f"  Arrow table size: {arrow_features.nbytes / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 5: ML Model Training (Simulated)
# ============================================================

print("\n--- ML Model Training (Simulated) ---")

# Prepare features for ML
start_time = time.time()

# Select feature columns
feature_columns = [
    "age", "income", "account_balance", "num_transactions",
    "credit_score", "income_per_age", "balance_to_income", "transaction_intensity"
]

# Convert to numpy for ML (simulated)
features = arrow_features.select(feature_columns).to_pandas()
labels = arrow_features.column("is_high_risk").to_pandas()

ml_time = time.time() - start_time

print(f"\nML Preparation:")
print(f"  Features shape: {features.shape}")
print(f"  Labels distribution: {labels.value_counts().to_dict()}")
print(f"  Time: {ml_time:.3f} seconds")

# ============================================================
# STEP 6: Performance Summary
# ============================================================

print("\n--- Performance Summary ---")

print("""
ML PIPELINE PERFORMANCE:

Dataset: 5 million records

Operations:
  - Feature Engineering: {feature_time:.3f} seconds
  - Conversion (Spark → Arrow): {conversion_time:.3f} seconds
  - ML Preparation: {ml_time:.3f} seconds

Total Time: {total_time:.3f} seconds

PERFORMANCE CHARACTERISTICS:
  ✓ Distributed feature engineering
  ✓ Efficient Arrow conversion
  ✓ Fast ML preparation
  ✓ Scalable architecture

USE CASES:
  ✓ Credit scoring
  ✓ Fraud detection
  ✓ Customer segmentation
  ✓ Risk assessment
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: How does Arrow integrate with Spark?

**Answer:**

**Integration Methods:**

1. **Arrow Optimization:**
```python
spark = SparkSession.builder \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()
```

2. **toArrow() Method:**
```python
arrow_table = spark_df.toArrow()
```

3. **createDataFrame():**
```python
spark_df = spark.createDataFrame(arrow_table.to_pandas())
```

---

### Question 2: What are the performance benefits of using Arrow with Spark?

**Answer:**

**Performance Benefits:**

1. **Zero-Copy Conversion:**
   - No data duplication
   - Fast conversion
   - Memory efficient

2. **Vectorized Operations:**
   - SIMD instructions
   - Parallel processing
   - Cache-efficient

3. **Optimized I/O:**
   - Fast Parquet reads/writes
   - Predicate pushdown
   - Column pruning

---

### Question 3: When would you use Arrow with Spark vs Pandas?

**Answer:**

**Use Arrow + Spark When:**
- Large datasets (TB+)
- Distributed processing
- Cluster computing
- ETL pipelines

**Use Arrow + Pandas When:**
- Small-medium datasets
- Single-node processing
- Data analysis
- ML training

**Comparison:**
| Aspect | Arrow + Spark | Arrow + Pandas |
|--------|---------------|----------------|
| Scale | Distributed | Single-node |
| Dataset Size | TB+ | GB |
| Latency | High | Low |
| Complexity | High | Low |

---

### Question 4: How do you optimize Spark jobs with Arrow?

**Answer:**

**Optimization Techniques:**

1. **Enable Arrow:**
```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

2. **Batch Size:**
```python
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
```

3. **Use Parquet:**
```python
df = spark.read.parquet("data.parquet")
```

4. **Cache Data:**
```python
df.cache()
```

---

### Question 5: What are the limitations of Arrow with Spark?

**Answer:**

**Limitations:**

1. **Memory:**
   - Single-node memory limits
   - Large datasets may cause OOM

2. **Network:**
   - Data transfer overhead
   - Network bottlenecks

3. **Complexity:**
   - Additional configuration
   - Debugging challenges

**Mitigations:**
- Use partitioning
- Optimize batch sizes
- Monitor memory usage

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Integration for large-scale processing |
| **Benefits** | Zero-copy, vectorized operations |
| **Use Cases** | ETL, ML, large-scale analytics |
| **Optimization** | Arrow enabled, batch size |
| **vs Pandas** | Spark for large, Pandas for small |
