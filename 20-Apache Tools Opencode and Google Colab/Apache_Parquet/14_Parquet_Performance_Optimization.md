# Parquet Performance Optimization

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-query-optimization)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-write-optimization)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### Performance Optimization Principles

> **Parquet performance depends on three factors: how data is stored (write optimization), how data is read (read optimization), and how queries are planned (query optimization).**

### Write Optimization

#### 1. Row Group Sizing

```
Small Row Groups (< 64MB):
  + More parallelism
  + Better predicate pushdown
  - More metadata overhead
  - More file operations

Large Row Groups (> 256MB):
  + Less metadata overhead
  + Better compression
  - Less parallelism
  - Coarser filtering

Sweet spot: 128MB - 256MB
```

#### 2. Compression Selection

```
Workload → Recommended Codec
  Interactive queries → Snappy (fast decompression)
  General purpose → Zstd level 3 (best balance)
  Archival → Gzip level 9 (maximum compression)
  Low latency → LZ4 (fastest)
```

#### 3. Dictionary Encoding

```
Low cardinality (< 1000 unique values):
  Dictionary encoding → 5-10x savings

High cardinality (> 10000 unique values):
  Plain encoding → dictionary overhead
```

#### 4. File Sizing

```
Optimal: 256MB - 1GB per file
  + Good parallelism
  + Low metadata overhead
  + Efficient S3 operations

Too small: < 64MB
  - High metadata overhead
  - Many file opens

Too large: > 2GB
  - Poor parallelism
  - Long compaction times
```

### Read Optimization

#### 1. Column Pruning

```python
# Bad: Read all columns
df = pq.read_table("file.parquet")

# Good: Read only needed columns
df = pq.read_table("file.parquet", columns=["amount", "status"])
```

**Impact**: 20 columns → 2 columns = 90% I/O reduction

#### 2. Predicate Pushdown

```python
# Bad: Read all rows
df = pq.read_table("file.parquet")

# Good: Filter at storage level
df = pq.read_table("file.parquet", filters=[("date", ">=", "2026-08-01")])
```

**Impact**: 1 billion rows → 10 million rows = 99% I/O reduction

#### 3. Partitioned Reading

```python
# Bad: Scan all partitions
df = pq.read_table("data/")

# Good: Read specific partition
df = pq.read_table("data/date=2026-08-24/")
```

**Impact**: 365 partitions → 1 partition = 99.7% I/O reduction

#### 4. Batch Reading

```python
# Bad: Load entire file into memory
table = pq.read_table("large_file.parquet")

# Good: Read in batches
dataset = pq.ParquetDataset("large_file.parquet")
for batch in dataset.read批次(batch_size=100_000):
    process(batch)
```

### Query Optimization

#### 1. Use DuckDB for SQL Queries

```python
import duckdb
con = duckdb.connect()

# DuckDB automatically optimizes Parquet reads
result = con.execute("""
    SELECT status, SUM(amount)
    FROM read_parquet('data/*.parquet')
    WHERE date >= '2026-08-01'
    GROUP BY status
""").fetchdf()
```

#### 2. Pre-filter Data in ETL

```python
# In ETL pipeline, write filtered data
filtered = table.filter(pc.field("status") == "COMPLETED")
pq.write_table(filtered, "completed_transactions.parquet")
```

#### 3. Materialize Common Aggregates

```python
# Pre-compute daily summaries
daily_summary = table.group_by("date").aggregate([
    ("amount", "sum"),
    ("amount", "count"),
])
pq.write_table(daily_summary, "daily_summary.parquet")
```

---

## 2. Example

### Performance Comparison

```python
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import time
import os
import tempfile

# Create large dataset
np.random.seed(42)
num_rows = 2_000_000

table = pa.table({
    "id": pa.array(list(range(num_rows)), type=pa.int64()),
    "status": pa.array(np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows), type=pa.string()),
    "amount": pa.array(np.random.uniform(1.0, 100000.0, num_rows), type=pa.float64()),
    "date": pa.array(np.random.choice(pd.date_range("2026-01-01", periods=365).strftime("%Y-%m-%d"), num_rows), type=pa.date32()),
})

# Write with different settings
base_path = tempfile.mkdtemp()

# Default
pq.write_table(table, os.path.join(base_path, "default.parquet"))

# Optimized
pq.write_table(
    table,
    os.path.join(base_path, "optimized.parquet"),
    compression="zstd",
    use_dictionary=True,
    write_statistics=True,
)

# Benchmark reads
start = time.time()
pq.read_table(os.path.join(base_path, "default.parquet"))
default_time = time.time() - start

start = time.time()
pq.read_table(os.path.join(base_path, "optimized.parquet"))
optimized_time = time.time() - start

print(f"Default read: {default_time:.3f}s")
print(f"Optimized read: {optimized_time:.3f}s")
```

---

## 3. Banking Scenario 1: Query Optimization

### Problem
A bank's analytics queries are slow:
- Dashboard takes 30 seconds to load
- Monthly reports take 2 hours
- Ad-hoc queries timeout

Root causes:
- Reading all columns
- No predicate pushdown
- Poor file sizing
- No partitioning

### Optimization Strategy
```
Before: 30 seconds dashboard load
  - Read 20 columns
  - Scan all 1 billion rows
  - No partitioning

After: 2 seconds dashboard load
  - Read 3 columns (column pruning)
  - Scan 10 million rows (predicate pushdown)
  - Read specific partition
```

### Architecture
```
Dashboard Query
       |
       v
  DuckDB (automatic optimization)
       |
       +-- Column pruning (20 → 3 columns)
       +-- Predicate pushdown (1B → 10M rows)
       +-- Partition pruning (365 → 1 partition)
       |
       v
  Parquet Files (S3)
       |
       v
  Result (2 seconds)
```

---

## 4. Python Code - Scenario 1

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import numpy as np
from datetime import datetime, timedelta
import random
import time
import os
import tempfile

# ============================================================
# BANKING SCENARIO: Query Optimization
# ============================================================

def generate_large_dataset(num_rows=2_000_000):
    """Generate large transaction dataset."""
    random.seed(42)
    np.random.seed(42)

    dates = [(datetime(2026, 1, 1) + timedelta(days=i)).strftime("%Y-%m-%d")
             for i in range(365)]

    table = pa.table({
        "transaction_id": pa.array(list(range(1, num_rows + 1)), type=pa.int64()),
        "account_id": pa.array([f"ACC{random.randint(100000, 999999)}" for _ in range(num_rows)], type=pa.string()),
        "customer_id": pa.array([f"CUST{random.randint(10000, 99999)}" for _ in range(num_rows)], type=pa.string()),
        "branch_id": pa.array([f"BR{random.randint(100, 999)}" for _ in range(num_rows)], type=pa.string()),
        "amount": pa.array(np.random.uniform(1.0, 100000.0, num_rows).round(2), type=pa.float64()),
        "currency": pa.array(np.random.choice(["USD", "EUR", "GBP"], num_rows), type=pa.string()),
        "transaction_type": pa.array(np.random.choice(
            ["DEBIT", "CREDIT", "TRANSFER", "WIRE", "ACH"], num_rows
        ), type=pa.string()),
        "status": pa.array(np.random.choice(
            ["COMPLETED", "PENDING", "FAILED"], num_rows, p=[0.90, 0.07, 0.03]
        ), type=pa.string()),
        "channel": pa.array(np.random.choice(
            ["ONLINE", "MOBILE", "BRANCH", "ATM", "POS"], num_rows
        ), type=pa.string()),
        "description": pa.array([f"Transaction {i}" for i in range(1, num_rows + 1)], type=pa.string()),
        "reference": pa.array([f"REF{random.randint(100000, 999999)}" for _ in range(num_rows)], type=pa.string()),
        "transaction_date": pa.array(np.random.choice(dates, num_rows), type=pa.date32()),
    })

    return table


def benchmark_read_strategies(table, base_path):
    """Benchmark different read strategies."""
    # Write unoptimized
    pq.write_table(table, os.path.join(base_path, "unoptimized.parquet"), compression="none")

    # Write optimized
    pq.write_to_dataset(
        table,
        root_path=os.path.join(base_path, "optimized"),
        partition_cols=["transaction_date"],
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
    )

    results = {}

    # Strategy 1: Full read (no optimization)
    start = time.time()
    pq.read_table(os.path.join(base_path, "unoptimized.parquet"))
    results["full_read"] = time.time() - start

    # Strategy 2: Column pruning
    start = time.time()
    pq.read_table(
        os.path.join(base_path, "unoptimized.parquet"),
        columns=["transaction_id", "amount", "status"]
    )
    results["column_pruning"] = time.time() - start

    # Strategy 3: Predicate pushdown
    start = time.time()
    pq.read_table(
        os.path.join(base_path, "unoptimized.parquet"),
        filters=[("transaction_date", ">=", "2026-08-01"), ("transaction_date", "<=", "2026-08-31")]
    )
    results["predicate_pushdown"] = time.time() - start

    # Strategy 4: Both (optimal)
    start = time.time()
    pq.read_table(
        os.path.join(base_path, "unoptimized.parquet"),
        columns=["transaction_id", "amount", "status"],
        filters=[("transaction_date", ">=", "2026-08-01"), ("transaction_date", "<=", "2026-08-31")]
    )
    results["both"] = time.time() - start

    # Strategy 5: Partitioned read
    start = time.time()
    dataset = pq.ParquetDataset(
        os.path.join(base_path, "optimized"),
        filters=[("transaction_date", ">=", "2026-08-01"), ("transaction_date", "<=", "2026-08-31")],
        use_legacy_dataset=False,
    )
    dataset.read(columns=["transaction_id", "amount", "status"])
    results["partitioned"] = time.time() - start

    print(f"\n=== Read Strategy Benchmark ===")
    print(f"{'Strategy':<25} {'Time (s)':<12} {'Speedup':<10}")
    print("-" * 47)
    baseline = results["full_read"]
    for name, elapsed in results.items():
        speedup = baseline / elapsed
        print(f"{name:<25} {elapsed:<12.3f} {speedup:<10.1f}x")

    return results


def benchmark_compression_codecs(table, base_path):
    """Benchmark different compression codecs."""
    codecs = ["none", "snappy", "gzip", "zstd", "lz4"]
    results = {}

    for codec in codecs:
        path = os.path.join(base_path, f"test_{codec}.parquet")
        start = time.time()
        pq.write_table(table, path, compression=codec)
        write_time = time.time() - start

        size = os.path.getsize(path)

        start = time.time()
        pq.read_table(path)
        read_time = time.time() - start

        results[codec] = {"write": write_time, "read": read_time, "size": size}

    print(f"\n=== Compression Benchmark ===")
    print(f"{'Codec':<12} {'Size (MB)':<12} {'Write (s)':<12} {'Read (s)':<12}")
    print("-" * 48)
    for name, metrics in results.items():
        print(f"{name:<12} {metrics['size']/1024/1024:<12.1f} {metrics['write']:<12.3f} {metrics['read']:<12.3f}")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "optimization_bench")
    os.makedirs(base_path, exist_ok=True)

    # Generate dataset
    print("Generating large dataset...")
    table = generate_large_dataset(num_rows=2_000_000)

    # Benchmark read strategies
    benchmark_read_strategies(table, base_path)

    # Benchmark compression
    benchmark_compression_codecs(table, base_path)
```

---

## 5. Banking Scenario 2: Write Optimization

### Problem
A bank's ETL pipeline writes 500 GB daily but:
- Write takes 4 hours (must complete in 2 hours)
- Too many small files (streaming writes every 30 seconds)
- Poor compression (using default Snappy)

### Optimization Strategy
```
Before:
  - 100,000 small files (5 MB each)
  - Snappy compression
  - No dictionary encoding
  - Write time: 4 hours

After:
  - 500 large files (1 GB each)
  - Zstd compression
  - Dictionary encoding enabled
  - Write time: 1.5 hours
```

### Architecture
```
Streaming Writer (every 30s)
       |
       v
  Small files (5 MB each)
       |
       v
  Compaction Job (hourly)
       |
       v
  Large files (1 GB each)
       |
       v
  S3 Data Lake
```

---

## 6. Python Code - Scenario 2

```python
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime, timedelta
import random
import time
import os
import tempfile

# ============================================================
# BANKING SCENARIO: Write Optimization
# ============================================================

class OptimizedParquetWriter:
    """Optimized Parquet writer with compaction."""

    def __init__(self, base_path, target_file_size_mb=256):
        self.base_path = base_path
        self.target_size = target_file_size_mb * 1024 * 1024
        self.buffer = []
        self.file_count = 0

    def write_batch(self, batch_data):
        """Write a batch of data."""
        self.buffer.extend(batch_data)

        # Check if buffer is large enough
        estimated_size = len(self.buffer) * 200  # ~200 bytes per row
        if estimated_size >= self.target_size:
            self._flush()

    def _flush(self):
        """Flush buffer to Parquet file."""
        if not self.buffer:
            return

        start = time.time()

        # Create table
        table = pa.table({
            "transaction_id": pa.array([r["transaction_id"] for r in self.buffer], type=pa.int64()),
            "amount": pa.array([r["amount"] for r in self.buffer], type=pa.float64()),
            "status": pa.array([r["status"] for r in self.buffer], type=pa.string()),
        })

        # Write with optimized settings
        path = os.path.join(self.base_path, f"data_{self.file_count:06d}.parquet")
        pq.write_table(
            table,
            path,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
        )

        self.file_count += 1
        self.buffer = []

        elapsed = time.time() - start
        size = os.path.getsize(path)
        return {"path": path, "rows": len(table), "size": size, "time": elapsed}

    def close(self):
        """Flush remaining data."""
        return self._flush()


def compact_files(input_path, output_path, target_size_mb=256):
    """Compact small files into larger ones."""
    start = time.time()

    # Find all Parquet files
    parquet_files = []
    for f in sorted(os.listdir(input_path)):
        if f.endswith(".parquet"):
            parquet_files.append(os.path.join(input_path, f))

    if not parquet_files:
        return

    # Read all files
    tables = [pq.read_table(f) for f in parquet_files]
    combined = pa.concat_tables(tables)

    # Write compacted file
    pq.write_table(
        combined,
        output_path,
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
    )

    # Remove original files
    for f in parquet_files:
        os.remove(f)

    elapsed = time.time() - start
    size = os.path.getsize(output_path)

    print(f"\n=== Compaction Report ===")
    print(f"Files compacted: {len(parquet_files)} → 1")
    print(f"Rows: {combined.num_rows:,}")
    print(f"Output size: {size / (1024*1024):.1f} MB")
    print(f"Compaction time: {elapsed:.3f}s")

    return output_path


def benchmark_write_strategies(num_rows=500_000):
    """Benchmark different write strategies."""
    random.seed(42)
    np.random.seed(42)

    # Generate data
    data = [{
        "transaction_id": i,
        "amount": round(random.uniform(1.0, 100000.0), 2),
        "status": random.choice(["COMPLETED", "PENDING", "FAILED"]),
    } for i in range(num_rows)]

    results = {}

    # Strategy 1: No optimization
    path1 = os.path.join(tempfile.mkdtemp(), "no_opt.parquet")
    start = time.time()
    table = pa.table({
        "transaction_id": pa.array([d["transaction_id"] for d in data], type=pa.int64()),
        "amount": pa.array([d["amount"] for d in data], type=pa.float64()),
        "status": pa.array([d["status"] for d in data], type=pa.string()),
    })
    pq.write_table(table, path1, compression="none")
    results["no_optimization"] = {"time": time.time() - start, "size": os.path.getsize(path1)}

    # Strategy 2: Snappy
    path2 = os.path.join(tempfile.mkdtemp(), "snappy.parquet")
    start = time.time()
    pq.write_table(table, path2, compression="snappy")
    results["snappy"] = {"time": time.time() - start, "size": os.path.getsize(path2)}

    # Strategy 3: Zstd + dictionary
    path3 = os.path.join(tempfile.mkdtemp(), "zstd_dict.parquet")
    start = time.time()
    pq.write_table(table, path3, compression="zstd", use_dictionary=True, write_statistics=True)
    results["zstd_optimized"] = {"time": time.time() - start, "size": os.path.getsize(path3)}

    print(f"\n=== Write Strategy Benchmark ===")
    print(f"{'Strategy':<20} {'Time (s)':<12} {'Size (MB)':<12} {'Ratio':<10}")
    print("-" * 54)
    baseline = results["no_optimization"]["size"]
    for name, metrics in results.items():
        ratio = baseline / metrics["size"]
        print(f"{name:<20} {metrics['time']:<12.3f} {metrics['size']/1024/1024:<12.1f} {ratio:<10.1f}x")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    # Benchmark write strategies
    benchmark_write_strategies(num_rows=500_000)

    # Demo compaction
    base_path = os.path.join(tempfile.gettempdir(), "compaction_demo")
    os.makedirs(base_path, exist_ok=True)

    # Write small files
    writer = OptimizedParquetWriter(base_path, target_file_size_mb=10)
    for i in range(100):
        batch = [{"transaction_id": j, "amount": round(random.uniform(1, 10000), 2), "status": "COMPLETED"}
                 for j in range(i*1000, (i+1)*1000)]
        writer.write_batch(batch)
    writer.close()

    print(f"\nCreated {writer.file_count} small files")

    # Compact
    compacted_path = os.path.join(tempfile.mkdtemp(), "compacted.parquet")
    compact_files(base_path, compacted_path)
```

---

## 7. Interview Questions

### Q1: What are the top 5 Parquet optimization techniques?

**Answer:**

1. **Column Pruning**: Read only needed columns
```python
pq.read_table("file.parquet", columns=["amount", "status"])
```

2. **Predicate Pushdown**: Filter at storage level
```python
pq.read_table("file.parquet", filters=[("date", ">=", "2026-08-01")])
```

3. **Partitioning**: Organize data by filter columns
```python
pq.write_to_dataset(table, root_path="output/", partition_cols=["date"])
```

4. **Compression**: Use Zstd for best balance
```python
pq.write_table(table, "file.parquet", compression="zstd")
```

5. **File Sizing**: Target 256MB - 1GB per file
```python
pq.write_table(table, "file.parquet", row_group_size=1_000_000)
```

---

### Q2: How do you diagnose slow Parquet queries?

**Answer:**

**Checklist:**

1. **Are you reading all columns?**
```python
# Check: Are you using columns=[]?
```

2. **Are you filtering?**
```python
# Check: Are you using filters=[]?
```

3. **Is the file too large?**
```python
# Check: File size > 1GB?
```

4. **Is compression slowing reads?**
```python
# Check: Using Gzip (slow decompression)?
```

5. **Are there too many small files?**
```python
# Check: File count > 10,000?
```

**Diagnostic query:**
```python
metadata = pq.read_metadata("file.parquet")
print(f"Row groups: {metadata.num_row_groups}")
print(f"Rows: {metadata.num_rows}")
print(f"Size: {os.path.getsize('file.parquet') / 1024 / 1024:.1f} MB")
```

---

### Q3: When should you use compaction?

**Answer:**

**When to compact:**
- Many small files (< 64MB each)
- Streaming writes create frequent files
- After bulk deletes/updates
- Before query-heavy periods

**When NOT to compact:**
- Files are already well-sized (256MB - 1GB)
- Write-heavy workload (compaction competes with writes)
- Cold data (rarely queried)

**Compaction strategy:**
```python
# Read small files
tables = [pq.read_table(f) for f in small_files]
combined = pa.concat_tables(tables)

# Write larger file
pq.write_table(combined, "compacted.parquet", compression="zstd")
```

**Target**: 256MB - 1GB per file after compaction

---

### Q4: How does compression affect query performance?

**Answer:**

| Codec | Write Speed | Read Speed | Ratio | Best For |
|-------|------------|------------|-------|----------|
| Snappy | Fastest | Fastest | ~5x | Interactive queries |
| Zstd | Fast | Fast | ~7x | General purpose |
| Gzip | Slow | Slow | ~8x | Archival |
| LZ4 | Very fast | Very fast | ~3x | Low latency |

**Trade-off:**
- Higher compression → slower reads (decompression overhead)
- Lower compression → faster reads but more I/O

**Recommendation:**
- Hot data: Snappy (fast reads)
- Warm data: Zstd level 3 (balanced)
- Cold data: Gzip level 9 (maximum compression)

---

### Q5: What is the small file problem and how do you solve it?

**Answer:**

**Problem**: Many small Parquet files (< 64MB) cause:
- High metadata overhead
- Many file open operations
- Poor parallelism
- S3 API costs

**Causes:**
- Streaming writes (every 30 seconds)
- Too frequent batch jobs
- Partitioning by high-cardinality columns

**Solutions:**

1. **Compaction**: Merge small files into larger ones
```python
# Read all small files, write one large file
tables = [pq.read_table(f) for f in small_files]
combined = pa.concat_tables(tables)
pq.write_table(combined, "compacted.parquet")
```

2. **Batch writes**: Write less frequently but larger batches
```python
# Instead of writing every 30s, write every 30 minutes
```

3. **Optimize partitioning**: Use low-cardinality partition columns
```python
# Bad: partition by user_id (millions of partitions)
# Good: partition by date (365 partitions per year)
```

4. **Use Iceberg/Delta Lake**: Automatic file management
```python
# Iceberg compaction
spark.sql("CALL system.compact('db.table')")
```
