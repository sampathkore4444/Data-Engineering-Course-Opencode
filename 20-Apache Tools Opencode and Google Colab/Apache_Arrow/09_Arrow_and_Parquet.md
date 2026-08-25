# Concept 09: Arrow and Parquet Integration

## 📚 Detailed Explanation

**Arrow and Parquet** work together seamlessly for efficient data storage and processing. Arrow handles in-memory operations, while Parquet handles on-disk storage.

### Why Arrow + Parquet?

**Without Integration:**
```
Application → Serialize → Parquet → Deserialize → Application
              (slow)                 (slow)
```

**With Arrow + Parquet:**
```
Application → Arrow (fast) → Parquet (efficient storage)
                            → Arrow (fast) → Application
```

### Key Features

| Feature | Arrow | Parquet |
|---------|-------|---------|
| **Location** | In-memory (RAM) | On-disk (S3/HDFS) |
| **Format** | Columnar | Columnar |
| **Optimization** | CPU cache | I/O compression |
| **Use Case** | Processing | Storage |

### Reading Parquet

```python
import pyarrow.parquet as pq

# Read Parquet file
table = pq.read_table("data.parquet")

# Read with filters (predicate pushdown)
table = pq.read_table("data.parquet", filters=[("amount", ">", 10000)])

# Read specific columns
table = pq.read_table("data.parquet", columns=["id", "amount"])
```

### Writing Parquet

```python
import pyarrow.parquet as pq

# Write Parquet file
pq.write_table(table, "output.parquet")

# Write with compression
pq.write_table(table, "output.parquet", compression="snappy")

# Write with partitioning
pq.write_to_dataset(table, "output_dir", partition_cols=["year", "month"])
```

---

## 💡 Example: Arrow + Parquet in Banking

### Scenario: Data Lake Operations

```python
import pyarrow as pa
import pyarrow.parquet as pq

# Create transaction table
table = pa.table({
    "transaction_id": ["TXN-001", "TXN-002"],
    "amount": [50000.00, 75000.00],
    "date": ["2026-08-24", "2026-08-24"]
})

# Write to Parquet (efficient storage)
pq.write_table(table, "transactions.parquet", compression="snappy")

# Read from Parquet (fast access)
result = pq.read_table("transactions.parquet")

# Query with filters (predicate pushdown)
filtered = pq.read_table("transactions.parquet", 
                         filters=[("amount", ">", 60000)])
```

---

## 🏦 Real-World Banking Scenario 1: Data Lake Storage

### Scenario
A bank stores **10 TB of transaction data** in Parquet format. They need:
- Efficient storage
- Fast queries
- Compression

### Problem
- Large storage requirements
- Query performance
- Cost optimization

### Solution
Arrow + Parquet integration:
- Efficient compression
- Predicate pushdown
- Column pruning

### Python Code

```python
"""
Banking Scenario 1: Data Lake Storage
Using Arrow and Parquet
"""

import pyarrow as pa
import pyarrow.parquet as pq
import random
from datetime import datetime, timedelta
import time
import os

# ============================================================
# STEP 1: Generate Transaction Data
# ============================================================

print("=== DATA LAKE STORAGE WITH ARROW AND PARQUET ===\n")

def generate_transaction_table(num_records: int) -> pa.Table:
    """Generate transaction data for Parquet storage."""
    
    data = {
        "transaction_id": [f"TXN-{i:010d}" for i in range(1, num_records + 1)],
        "account_id": [f"ACC-{random.randint(1000, 9999):06d}" for _ in range(num_records)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(num_records)],
        "transaction_type": [random.choice(["CREDIT", "DEBIT", "TRANSFER"]) for _ in range(num_records)],
        "status": [random.choice(["COMPLETED", "PENDING", "FAILED"]) for _ in range(num_records)],
        "branch_id": [f"BR-{random.randint(1, 50):03d}" for _ in range(num_records)],
        "year": [2026] * num_records,
        "month": [random.randint(1, 12) for _ in range(num_records)],
    }
    
    return pa.table(data)

# Generate 10 million transactions
print("Generating 10 million transactions...")
start_time = time.time()
transactions = generate_transaction_table(10000000)
generation_time = time.time() - start_time

print(f"Generated in {generation_time:.3f} seconds")
print(f"Arrow table size: {transactions.nbytes / 1024 / 1024:.2f} MB")

# ============================================================
# STEP 2: Write to Parquet
# ============================================================

print("\n--- Writing to Parquet ---")

# Create output directory
output_dir = "/tmp/banking_parquet"
os.makedirs(output_dir, exist_ok=True)

# Write single Parquet file
start_time = time.time()
pq.write_table(transactions, f"{output_dir}/transactions.parquet", compression="snappy")
write_time = time.time() - start_time

file_size = os.path.getsize(f"{output_dir}/transactions.parquet")
print(f"\nSingle File Write:")
print(f"  Time: {write_time:.3f} seconds")
print(f"  File size: {file_size / 1024 / 1024:.2f} MB")
print(f"  Compression ratio: {transactions.nbytes / file_size:.2f}x")

# ============================================================
# STEP 3: Write Partitioned Parquet
# ============================================================

print("\n--- Writing Partitioned Parquet ---")

# Write with partitioning
start_time = time.time()
pq.write_to_dataset(
    transactions,
    output_path=f"{output_dir}/partitioned",
    partition_cols=["year", "month"],
    compression="snappy"
)
partitioned_time = time.time() - start_time

print(f"\nPartitioned Write:")
print(f"  Time: {partitioned_time:.3f} seconds")
print(f"  Partitioned by: year, month")

# ============================================================
# STEP 4: Read Parquet Files
# ============================================================

print("\n--- Reading Parquet Files ---")

# Read single file
start_time = time.time()
single_file = pq.read_table(f"{output_dir}/transactions.parquet")
read_time = time.time() - start_time

print(f"\nSingle File Read:")
print(f"  Rows: {len(single_file):,}")
print(f"  Time: {read_time:.3f} seconds")

# Read partitioned files
start_time = time.time()
partitioned = pq.read_table(f"{output_dir}/partitioned")
partitioned_read_time = time.time() - start_time

print(f"\nPartitioned Read:")
print(f"  Rows: {len(partitioned):,}")
print(f"  Time: {partitioned_read_time:.3f} seconds")

# ============================================================
# STEP 5: Predicate Pushdown
# ============================================================

print("\n--- Predicate Pushdown ---")

# Read with filter
start_time = time.time()
filtered = pq.read_table(
    f"{output_dir}/transactions.parquet",
    filters=[("amount", ">", 50000)]
)
filter_time = time.time() - start_time

print(f"\nFiltered Read (amount > 50,000):")
print(f"  Rows: {len(filtered):,}")
print(f"  Time: {filter_time:.3f} seconds")
print(f"  Speedup: {read_time / filter_time:.1f}x faster")

# Read specific columns (column pruning)
start_time = time.time()
columns = pq.read_table(
    f"{output_dir}/transactions.parquet",
    columns=["transaction_id", "amount"]
)
column_time = time.time() - start_time

print(f"\nColumn Pruning (2 columns):")
print(f"  Rows: {len(columns):,}")
print(f"  Columns: {len(columns.column_names)}")
print(f"  Time: {column_time:.3f} seconds")
print(f"  Speedup: {read_time / column_time:.1f}x faster")

# ============================================================
# STEP 6: Parquet Statistics
# ============================================================

print("\n--- Parquet Statistics ---")

# Get Parquet metadata
parquet_file = pq.ParquetFile(f"{output_dir}/transactions.parquet")
metadata = parquet_file.metadata

print(f"\nParquet File Metadata:")
print(f"  Rows: {metadata.num_rows:,}")
print(f"  Row groups: {metadata.num_row_groups}")
print(f"  Columns: {metadata.num_columns}")

# Get schema
schema = parquet_file.schema_arrow
print(f"\nSchema:")
for field in schema:
    print(f"  {field.name}: {field.type}")

# ============================================================
# STEP 7: Benefits Summary
# ============================================================

print("\n--- Benefits Summary ---")

print("""
ARROW + PARQUET BENEFITS:

1. EFFICIENT STORAGE
   - Columnar format
   - Compression (Snappy, Gzip, Zstd)
   - Partition pruning

2. FAST QUERIES
   - Predicate pushdown
   - Column pruning
   - Row group statistics

3. COMPRESSION
   - Snappy: Fast compression/decompression
   - Gzip: Better compression
   - Zstd: Best compression

4. INTEGRATION
   - Seamless Arrow ↔ Parquet
   - Zero-copy reads
   - Schema preservation

5. ECOSYSTEM
   - Spark, DuckDB, Trino support
   - Cloud storage (S3, GCS, ADLS)
   - Data lake integration

COMPRESSION COMPARISON:
  Raw: 100 MB
  Snappy: 50 MB (2x compression)
  Gzip: 40 MB (2.5x compression)
  Zstd: 35 MB (3x compression)
""")
```

---

## 🏦 Real-World Banking Scenario 2: Query Optimization

### Scenario
A bank's **analytics team** runs **10,000 queries daily** on Parquet data. They need:
- Fast query response
- Efficient storage
- Cost optimization

### Problem
- Slow queries
- High storage costs
- Poor performance

### Solution
Arrow + Parquet optimization:
- Partitioning
- Compression
- Predicate pushdown

### Python Code

```python
"""
Banking Scenario 2: Query Optimization
Using Arrow and Parquet
"""

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import random
import time

# ============================================================
# STEP 1: Generate Optimized Dataset
# ============================================================

print("=== QUERY OPTIMIZATION WITH ARROW AND PARQUET ===\n")

def generate_optimized_dataset(num_records: int) -> pa.Table:
    """Generate dataset optimized for queries."""
    
    data = {
        "transaction_id": [f"TXN-{i:010d}" for i in range(1, num_records + 1)],
        "account_id": [f"ACC-{random.randint(1000, 9999):06d}" for _ in range(num_records)],
        "amount": [round(random.uniform(100, 100000), 2) for _ in range(num_records)],
        "transaction_type": [random.choice(["CREDIT", "DEBIT", "TRANSFER"]) for _ in range(num_records)],
        "status": [random.choice(["COMPLETED", "PENDING", "FAILED"]) for _ in range(num_records)],
        "branch_id": [f"BR-{random.randint(1, 50):03d}" for _ in range(num_records)],
        "year": [2026] * num_records,
        "month": [random.randint(1, 12) for _ in range(num_records)],
        "day": [random.randint(1, 28) for _ in range(num_records)],
    }
    
    return pa.table(data)

# Generate 50 million records
print("Generating 50 million records...")
dataset = generate_optimized_dataset(50000000)
print(f"Generated: {len(dataset):,} records")

# ============================================================
# STEP 2: Write Optimized Parquet
# ============================================================

print("\n--- Writing Optimized Parquet ---")

# Write with optimal settings
start_time = time.time()
pq.write_to_dataset(
    dataset,
    output_path="/tmp/banking_optimized",
    partition_cols=["year", "month"],
    compression="zstd",
    use_dictionary=True,
    write_statistics=True,
    data_page_version="2.0"
)
write_time = time.time() - start_time

print(f"\nOptimized Write:")
print(f"  Time: {write_time:.3f} seconds")
print(f"  Compression: Zstd")
print(f"  Dictionary: Enabled")
print(f"  Statistics: Enabled")

# ============================================================
# STEP 3: Query Performance Comparison
# ============================================================

print("\n--- Query Performance Comparison ---")

# Query 1: Full scan
start_time = time.time()
full_scan = pq.read_table("/tmp/banking_optimized")
full_scan_time = time.time() - start_time

print(f"\nQuery 1: Full Scan")
print(f"  Rows: {len(full_scan):,}")
print(f"  Time: {full_scan_time:.3f} seconds")

# Query 2: Partition pruning
start_time = time.time()
partition_pruned = pq.read_table(
    "/tmp/banking_optimized",
    filters=[("year", "=", 2026), ("month", "=", 6)]
)
partition_time = time.time() - start_time

print(f"\nQuery 2: Partition Pruning (year=2026, month=6)")
print(f"  Rows: {len(partition_pruned):,}")
print(f"  Time: {partition_time:.3f} seconds")
print(f"  Speedup: {full_scan_time / partition_time:.1f}x faster")

# Query 3: Predicate pushdown
start_time = time.time()
predicate_pushed = pq.read_table(
    "/tmp/banking_optimized",
    filters=[("amount", ">", 50000)]
)
predicate_time = time.time() - start_time

print(f"\nQuery 3: Predicate Pushdown (amount > 50,000)")
print(f"  Rows: {len(predicate_pushed):,}")
print(f"  Time: {predicate_time:.3f} seconds")
print(f"  Speedup: {full_scan_time / predicate_time:.1f}x faster")

# Query 4: Column pruning
start_time = time.time()
column_pruned = pq.read_table(
    "/tmp/banking_optimized",
    columns=["transaction_id", "amount"]
)
column_time = time.time() - start_time

print(f"\nQuery 4: Column Pruning (2 columns)")
print(f"  Rows: {len(column_pruned):,}")
print(f"  Time: {column_time:.3f} seconds")
print(f"  Speedup: {full_scan_time / column_time:.1f}x faster")

# Query 5: Combined optimization
start_time = time.time()
combined = pq.read_table(
    "/tmp/banking_optimized",
    filters=[("year", "=", 2026), ("month", "=", 6), ("amount", ">", 50000)],
    columns=["transaction_id", "amount"]
)
combined_time = time.time() - start_time

print(f"\nQuery 5: Combined Optimization")
print(f"  Rows: {len(combined):,}")
print(f"  Time: {combined_time:.3f} seconds")
print(f"  Speedup: {full_scan_time / combined_time:.1f}x faster")

# ============================================================
# STEP 4: Storage Optimization
# ============================================================

print("\n--- Storage Optimization ---")

# Compare compression algorithms
import os

# Write with different compression
compressions = ["none", "snappy", "gzip", "zstd"]
sizes = {}

for compression in compressions:
    path = f"/tmp/compression_test_{compression}.parquet"
    
    if compression == "none":
        pq.write_table(dataset, path)
    else:
        pq.write_table(dataset, path, compression=compression)
    
    sizes[compression] = os.path.getsize(path)
    os.remove(path)

print(f"\nCompression Comparison:")
print(f"  {'Compression':<15} {'Size (MB)':<15} {'Ratio':<15}")
print(f"  {'-'*45}")

for compression, size in sizes.items():
    ratio = sizes["none"] / size if size > 0 else 0
    print(f"  {compression:<15} {size / 1024 / 1024:<15.2f} {ratio:<15.2f}x")

# ============================================================
# STEP 5: Benefits Summary
# ============================================================

print("\n--- Benefits Summary ---")

print("""
QUERY OPTIMIZATION RESULTS:

Dataset: 50 million records

Query Performance:
  - Full Scan: {full_scan_time:.3f}s
  - Partition Pruning: {partition_time:.3f}s ({full_scan_time/partition_time:.1f}x faster)
  - Predicate Pushdown: {predicate_time:.3f}s ({full_scan_time/predicate_time:.1f}x faster)
  - Column Pruning: {column_time:.3f}s ({full_scan_time/column_time:.1f}x faster)
  - Combined: {combined_time:.3f}s ({full_scan_time/combined_time:.1f}x faster)

Storage Optimization:
  - Zstd compression: 3x smaller than raw
  - Dictionary encoding: 30-50% savings
  - Statistics: Enable fast pruning

OPTIMIZATION TECHNIQUES:
  ✓ Partitioning (year, month)
  ✓ Compression (Zstd)
  ✓ Dictionary encoding
  ✓ Column statistics
  ✓ Predicate pushdown
  ✓ Column pruning
""")
```

---

## 🎯 5 Real-World Interview Questions

### Question 1: What is the difference between Arrow and Parquet?

**Answer:**

**Arrow:**
- In-memory format
- Fast processing
- Zero-copy reads

**Parquet:**
- On-disk format
- Efficient storage
- Compressed

**Key Difference:**
- Arrow: RAM (fast)
- Parquet: Disk (efficient)

**Usage Pattern:**
```
Read Parquet → Convert to Arrow → Process → Write Parquet
```

---

### Question 2: How do you optimize Parquet files for queries?

**Answer:**

**Optimization Techniques:**

1. **Partitioning:**
```python
pq.write_to_dataset(table, "output", partition_cols=["year", "month"])
```

2. **Compression:**
```python
pq.write_table(table, "output.parquet", compression="zstd")
```

3. **Dictionary Encoding:**
```python
pq.write_table(table, "output.parquet", use_dictionary=True)
```

4. **Statistics:**
```python
pq.write_table(table, "output.parquet", write_statistics=True)
```

---

### Question 3: What compression algorithms are available in Parquet?

**Answer:**

**Compression Algorithms:**

| Algorithm | Speed | Compression | Use Case |
|-----------|-------|-------------|----------|
| **None** | Fastest | None | Testing |
| **Snappy** | Fast | Moderate | General purpose |
| **Gzip** | Moderate | Good | Archive |
| **Zstd** | Fast | Best | Production |

**Example:**
```python
# Best for production
pq.write_table(table, "output.parquet", compression="zstd")

# Best for speed
pq.write_table(table, "output.parquet", compression="snappy")
```

---

### Question 4: How does predicate pushdown work in Parquet?

**Answer:**

**Predicate Pushdown:**
- Filter data at file level
- Skip irrelevant row groups
- Reduce I/O

**Example:**
```python
# Only reads row groups where amount > 50000
filtered = pq.read_table(
    "data.parquet",
    filters=[("amount", ">", 50000)]
)
```

**How it works:**
1. Parquet stores min/max statistics per row group
2. Query engine checks statistics
3. Skip row groups that don't match
4. Only read relevant data

---

### Question 5: How do you handle schema evolution in Parquet?

**Answer:**

**Schema Evolution:**

1. **Add Column:**
```python
# New table with additional column
new_table = table.append_column("new_col", pa.array([...]))

# Write new Parquet
pq.write_table(new_table, "new_data.parquet")
```

2. **Read Old + New:**
```python
# Read with schema evolution
table = pq.read_table("new_data.parquet")
```

**Best Practices:**
- Use optional columns (nullable)
- Add columns at the end
- Maintain backward compatibility

---

## 📝 Summary

| Aspect | Key Point |
|--------|-----------|
| **Definition** | Integration for storage and processing |
| **Arrow** | In-memory, fast processing |
| **Parquet** | On-disk, efficient storage |
| **Optimization** | Partitioning, compression, statistics |
| **Performance** | Predicate pushdown, column pruning |
| **Use Cases** | Data lakes, analytics, storage |
