# Concept 14: Arrow and Data Lakes

## 📚 Detailed Explanation

**Arrow and Data Lakes** integration enables efficient data processing and analytics on data lake storage. Arrow provides the in-memory format, while data lakes provide scalable storage.

### What is a Data Lake?

A data lake is:
- **Centralized Repository**: Store any data format
- **Scalable Storage**: Object storage (S3, GCS, ADLS)
- **Cost-Effective**: Pay for what you use
- **Schema-on-Read**: Flexible schema

### Why Arrow + Data Lakes?

**Without Arrow:**
```
Data Lake → Read → Convert → Process → Convert → Write
           (slow)          (slow)          (slow)
```

**With Arrow:**
```
Data Lake → Arrow (fast) → Process → Arrow (fast) → Write
```

### Arrow in Data Lake Architecture

```
┌─────────────────────────────────────┐
│         Query Engine                │
│    (Spark, Trino, DuckDB)          │
├─────────────────────────────────────┤
│         Arrow Layer                 │
│    (In-memory processing)          │
├─────────────────────────────────────┤
│         Parquet Layer               │
│    (On-disk storage)               │
├─────────────────────────────────────┤
│         Object Storage              │
│    (S3, GCS, ADLS)                 │
└─────────────────────────────────────┘
```

---

## 💡 Example: Arrow in Data Lake

### Scenario: Data Lake Analytics

```python
import pyarrow.parquet as pq
import pyarrow.compute as pc

# Read from data lake
table = pq.read_table("s3://data-lake/transactions.parquet")

# Process with Arrow
total = pc.sum(table.column("amount"))
filtered = table.filter(pc.greater(table.column("amount"), 50000))

# Write back to data lake
pq.write_table(filtered, "s3://data-lake/high_value.parquet")
```

---

## 🏦 Real-World Banking Scenario 1: Data Lake Analytics

### Scenario
A bank's **data lake** stores **100 TB of transaction data**. They need to:
- Query large datasets
- Run analytics
- Generate reports

### Problem
- Large data volumes
- Query performance
- Cost optimization

### Solution
Arrow + Data Lake integration:
- Efficient Parquet reads
- Predicate pushdown
- Column pruning

### Python Code

```python
"""
Banking Scenario 1: Data Lake Analytics
Using Arrow and Data Lakes
"""

import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow as pa
import random
import os
import time

# ============================================================
# STEP 1: Create Data Lake Structure
# ============================================================

print("=== DATA LAKE ANALYTICS WITH ARROW ===\n")

# Create local data lake simulation
data_lake_dir = "/tmp/banking_data_lake"
os.makedirs(data_lake_dir, exist_ok=True)

def generate_data_lake_data(year: int, month: int, num_records: int) -> pa.Table:
    """Generate data for data lake."""
    
    data = {
        "transaction_id": [f"TXN-{year}{month:02d}-{i:08d}" for i in range(1, num_records + 1)],
        "account_id": [f"ACC-{random.randint(1000, 9999):06d}" for _ in range(num_records)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(num_records)],
        "transaction_type": [random.choice(["CREDIT", "DEBIT", "TRANSFER"]) for _ in range(num_records)],
        "branch": [random.choice(["Mumbai", "Delhi", "Bangalore"]) for _ in range(num_records)],
        "year": [year] * num_records,
        "month": [month] * num_records,
    }
    
    return pa.table(data)

# Generate data for multiple months
print("Generating data lake data...")
for month in range(1, 13):
    data = generate_data_lake_data(2026, month, 1000000)
    
    # Write partitioned Parquet
    pq.write_to_dataset(
        data,
        output_path=f"{data_lake_dir}/transactions",
        partition_cols=["year", "month"],
        compression="snappy"
    )
    
    print(f"  Month {month:02d}: {len(data):,} records")

# ============================================================
# STEP 2: Query Data Lake
# ============================================================

print("\n--- Querying Data Lake ---")

# Query 1: Full scan
start_time = time.time()
full_data = pq.read_table(f"{data_lake_dir}/transactions")
full_scan_time = time.time() - start_time

print(f"\nFull Scan:")
print(f"  Records: {len(full_data):,}")
print(f"  Time: {full_scan_time:.3f} seconds")

# Query 2: Partition pruning
start_time = time.time()
partition_data = pq.read_table(
    f"{data_lake_dir}/transactions",
    filters=[("year", "=", 2026), ("month", "=", 6)]
)
partition_time = time.time() - start_time

print(f"\nPartition Pruning (year=2026, month=6):")
print(f"  Records: {len(partition_data):,}")
print(f"  Time: {partition_time:.3f} seconds")
print(f"  Speedup: {full_scan_time / partition_time:.1f}x faster")

# Query 3: Predicate pushdown
start_time = time.time()
filtered_data = pq.read_table(
    f"{data_lake_dir}/transactions",
    filters=[("amount", ">", 50000)]
)
predicate_time = time.time() - start_time

print(f"\nPredicate Pushdown (amount > 50,000):")
print(f"  Records: {len(filtered_data):,}")
print(f"  Time: {predicate_time:.3f} seconds")
print(f"  Speedup: {full_scan_time / predicate_time:.1f}x faster")

# ============================================================
# STEP 3: Analytics with Arrow
# ============================================================

print("\n--- Analytics with Arrow ---")

# Aggregation
start_time = time.time()
result = full_data.group_by("branch").aggregate({
    "amount": "sum",
    "transaction_id": "count"
})
agg_time = time.time() - start_time

print(f"\nBranch Aggregation:")
print(f"  Time: {agg_time:.3f} seconds")
print(result.to_pandas().to_string(index=False))

# Filtering
start_time = time.time()
high_value = full_data.filter(pc.greater(full_data.column("amount"), 50000))
filter_time = time.time() - start_time

print(f"\nHigh-Value Filter:")
print(f"  Records: {len(high_value):,}")
print(f"  Time: {filter_time:.3f} seconds")

# ============================================================
# STEP 4: Write Results Back
# ============================================================

print("\n--- Writing Results Back ---")

# Write aggregated results
start_time = time.time()
pq.write_table(result, f"{data_lake_dir}/analytics/branch_summary.parquet")
write_time = time.time() - start_time

print(f"\nWrite Results:")
print(f"  Time: {write_time:.3f} seconds")

# ============================================================
# STEP 5: Performance Summary
# ============================================================

print("\n--- Performance Summary ---")

print("""
DATA LAKE ANALYTICS PERFORMANCE:

Dataset: 12 million records (12 months)

Query Performance:
  - Full Scan: {full_scan_time:.3f} seconds
  - Partition Pruning: {partition_time:.3f} seconds ({full_scan_time/partition_time:.1f}x faster)
  - Predicate Pushdown: {predicate_time:.3f} seconds ({full_scan_time/predicate_time:.1f}x faster)

Analytics Performance:
  - Aggregation: {agg_time:.3f} seconds
  - Filtering: {filter_time:.3f} seconds
  - Write: {write_time:.3f} seconds

OPTIMIZATION TECHNIQUES:
  ✓ Partitioning (year, month)
  ✓ Compression (Snappy)
  ✓ Predicate pushdown
  ✓ Column pruning
  ✓ Statistics

USE CASES:
  ✓ Historical analytics
  ✓ Reporting
  ✓ Data exploration
  ✓ ETL pipelines
""")
```

---

## 🏦 Real-World Banking Scenario 2: Data Lakehouse

### Scenario
A bank is building a **lakehouse architecture**:
- Combine data lake and data warehouse
- Enable SQL analytics
- Support ML workloads

### Problem
- Complex architecture
- Multiple tools
- Performance requirements

### Solution
Arrow + Data Lake + Iceberg:
- Unified storage
- SQL analytics
- ML support

### Python Code

```python
"""
Banking Scenario 2: Data Lakehouse
Using Arrow, Parquet, and Iceberg
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import random
import time

# ============================================================
# STEP 1: Define Lakehouse Architecture
# ============================================================

print("=== DATA LAKEHOUSE WITH ARROW ===\n")

print("""
LAKEHOUSE ARCHITECTURE:

┌─────────────────────────────────────┐
│         Query Layer                 │
│    (Spark, Trino, DuckDB)          │
├─────────────────────────────────────┤
│         Arrow Layer                 │
│    (In-memory processing)          │
├─────────────────────────────────────┤
│         Iceberg Layer               │
│    (Table format)                  │
├─────────────────────────────────────┤
│         Parquet Layer               │
│    (On-disk storage)               │
├─────────────────────────────────────┤
│         Object Storage              │
│    (S3, GCS, ADLS)                 │
└─────────────────────────────────────┘
""")

# ============================================================
# STEP 2: Create Lakehouse Tables
# ============================================================

print("--- Creating Lakehouse Tables ---")

# Bronze layer (raw data)
bronze_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("raw_data", pa.string(), nullable=True),
])

# Silver layer (cleaned data)
silver_schema = pa.schema([
    pa.field("transaction_id", pa.string(), nullable=False),
    pa.field("account_id", pa.string(), nullable=False),
    pa.field("amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_type", pa.string(), nullable=False),
    pa.field("status", pa.string(), nullable=False),
])

# Gold layer (aggregated data)
gold_schema = pa.schema([
    pa.field("branch", pa.string(), nullable=False),
    pa.field("total_amount", pa.decimal128(18, 2), nullable=False),
    pa.field("transaction_count", pa.int64(), nullable=False),
])

print(f"Bronze schema: {len(bronze_schema)} fields")
print(f"Silver schema: {len(silver_schema)} fields")
print(f"Gold schema: {len(gold_schema)} fields")

# ============================================================
# STEP 3: Process Data Through Layers
# ============================================================

print("\n--- Processing Data Through Layers ---")

# Generate bronze data
def generate_bronze_data(num_records: int) -> pa.Table:
    """Generate raw transaction data."""
    
    data = {
        "transaction_id": [f"TXN-{i:010d}" for i in range(1, num_records + 1)],
        "account_id": [f"ACC-{random.randint(1000, 9999):06d}" for _ in range(num_records)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(num_records)],
        "raw_data": [f"RAW-{random.randint(100000, 999999)}" for _ in range(num_records)],
    }
    
    return pa.table(data, schema=bronze_schema)

# Generate bronze data
print("\nBronze Layer (Raw Data):")
bronze_data = generate_bronze_data(1000000)
print(f"  Records: {len(bronze_data):,}")

# Transform to silver
print("\nSilver Layer (Cleaned Data):")
silver_data = pa.table({
    "transaction_id": bronze_data.column("transaction_id"),
    "account_id": bronze_data.column("account_id"),
    "amount": bronze_data.column("amount"),
    "transaction_type": pa.array([random.choice(["CREDIT", "DEBIT"]) for _ in range(len(bronze_data))]),
    "status": pa.array(["COMPLETED"] * len(bronze_data)),
}, schema=silver_schema)
print(f"  Records: {len(silver_data):,}")

# Aggregate to gold
print("\nGold Layer (Aggregated Data):")
gold_data = silver_data.group_by("transaction_type").aggregate({
    "amount": "sum",
    "transaction_id": "count"
})
print(f"  Groups: {len(gold_data)}")

# ============================================================
# STEP 4: Query Lakehouse
# ============================================================

print("\n--- Querying Lakehouse ---")

# Query silver layer
start_time = time.time()
high_value = silver_data.filter(pc.greater(silver_data.column("amount"), 50000))
query_time = time.time() - start_time

print(f"\nHigh-Value Transactions:")
print(f"  Records: {len(high_value):,}")
print(f"  Time: {query_time:.3f} seconds")

# Query gold layer
start_time = time.time()
branch_totals = gold_data.sort_by("amount_sum", descending=True)
agg_time = time.time() - start_time

print(f"\nBranch Totals:")
print(f"  Time: {agg_time:.3f} seconds")
print(branch_totals.to_pandas().to_string(index=False))

# ============================================================
# STEP 5: Benefits Summary
# ============================================================

print("\n--- Benefits Summary ---")

print("""
LAKEHOUSE BENEFITS:

1. UNIFIED ARCHITECTURE
   - Single storage layer
   - Multiple query engines
   - Consistent data

2. COST-EFFECTIVE
   - Object storage
   - Pay for what you use
   - Scalable

3. PERFORMANCE
   - Arrow in-memory processing
   - Parquet on-disk storage
   - Iceberg table management

4. FLEXIBILITY
   - SQL analytics
   - ML workloads
   - Real-time processing

5. GOVERNANCE
   - Schema enforcement
   - ACID transactions
   - Time travel

ARCHITECTURE LAYERS:
  Bronze: Raw data
  Silver: Cleaned data
  Gold: Aggregated data
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is a data lake and how does Arrow integrate with it?

**Answer:**

**Data Lake:**
- Centralized repository
- Scalable storage
- Schema-on-read

**Arrow Integration:**
- Efficient Parquet reads
- In-memory processing
- Fast analytics

**Example:**
```python
import pyarrow.parquet as pq

# Read from data lake
table = pq.read_table("s3://data-lake/data.parquet")

# Process with Arrow
result = table.group_by("category").aggregate({"amount": "sum"})
```

---

### Question 2: What are the benefits of using Arrow with data lakes?

**Answer:**

**Benefits:**

1. **Performance:**
   - Fast Parquet reads
   - Vectorized operations
   - Predicate pushdown

2. **Efficiency:**
   - Columnar format
   - Compression
   - Memory-efficient

3. **Integration:**
   - Multiple query engines
   - Cross-platform
   - Standard format

---

### Question 3: How do you optimize data lake queries with Arrow?

**Answer:**

**Optimization Techniques:**

1. **Partitioning:**
```python
pq.write_to_dataset(table, "output", partition_cols=["year", "month"])
```

2. **Predicate Pushdown:**
```python
table = pq.read_table("data.parquet", filters=[("amount", ">", 10000)])
```

3. **Column Pruning:**
```python
table = pq.read_table("data.parquet", columns=["id", "amount"])
```

---

### Question 4: What is the difference between a data lake and a data warehouse?

**Answer:**

**Data Lake:**
- Store any format
- Schema-on-read
- Cost-effective
- Scalable

**Data Warehouse:**
- Structured data
- Schema-on-write
- Optimized for queries
- Expensive

**Comparison:**
| Aspect | Data Lake | Data Warehouse |
|--------|-----------|----------------|
| Data Type | Any | Structured |
| Schema | On-read | On-write |
| Cost | Low | High |
| Performance | Variable | Optimized |

---

### Question 5: How does Arrow support lakehouse architecture?

**Answer:**

**Lakehouse Support:**

1. **In-Memory Processing:**
   - Fast analytics
   - Vectorized operations
   - Zero-copy

2. **Parquet Integration:**
   - Efficient storage
   - Compression
   - Predicate pushdown

3. **Iceberg Support:**
   - ACID transactions
   - Schema evolution
   - Time travel

**Example:**
```python
# Lakehouse architecture
# 1. Read from Parquet (data lake)
# 2. Process with Arrow (in-memory)
# 3. Write to Iceberg (table format)
```

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Integration for modern data architecture |
| **Architecture** | Arrow + Parquet + Iceberg |
| **Benefits** | Performance, cost, flexibility |
| **Optimization** | Partitioning, pushdown, pruning |
| **Use Cases** | Analytics, ML, reporting |
