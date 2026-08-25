# Writing Parquet Files

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-batch-etl-pipeline)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-streaming-ingestion)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### How Parquet Writing Works

Writing Parquet is fundamentally different from writing CSV. The key insight:

> **Parquet writes are buffered in memory, then flushed as optimized columnar files with encoding, compression, and statistics — all in a single write operation.**

### The Write Pipeline

```
In-Memory Data (Arrow Table / Pandas DataFrame)
         |
Step 1: Encode columns
         |
         +-- Dictionary encoding (if enabled)
         +-- Run-length encoding
         +-- Bit-packing
         +-- Delta encoding
         |
Step 2: Compress encoded data
         |
         +-- Snappy / Zstd / Gzip
         |
Step 3: Write Row Groups
         |
         +-- Column chunks (compressed pages)
         +-- Page headers
         +-- Column statistics
         |
Step 4: Write Footer
         |
         +-- Schema
         +-- Row group offsets
         +-- Key-value metadata
         |
Step 5: Flush to disk
```

### Three Writing Interfaces in PyArrow

#### 1. `pq.write_table()` — Write a single file

```python
pq.write_table(
    table,
    "output.parquet",
    compression="snappy",
    use_dictionary=True,
    write_statistics=True,
)
```

#### 2. `pq.write_to_dataset()` — Write partitioned dataset

```python
pq.write_to_dataset(
    table,
    root_path="output_dir/",
    partition_cols=["date", "region"],
    compression="zstd",
)
```

#### 3. `ParquetWriter` — Streaming/incremental writes

```python
writer = pq.ParquetWriter("output.parquet", schema)
writer.write_table(batch1)
writer.write_table(batch2)
writer.close()
```

### Key Write Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `compression` | snappy | Compression codec |
| `compression_level` | codec default | Compression intensity |
| `use_dictionary` | True | Enable dictionary encoding |
| `dictionary_pagesize_limit` | 1MB | Max dictionary page size |
| `write_statistics` | True | Write min/max/count stats |
| `data_page_size` | 1MB | Size of each data page |
| `row_group_size` | 64M rows | Max rows per row group |
| `version` | 1.0 | Parquet format version |
| `use_deprecated_int96_timestamps` | False | Legacy timestamp format |

### Partitioned Writing

Partitioning creates a directory hierarchy:

```python
pq.write_to_dataset(
    table,
    root_path="output/",
    partition_cols=["year", "month"],
)
```

Creates:
```
output/
  year=2026/
    month=08/
      part-0.parquet
      part-1.parquet
    month=09/
      part-0.parquet
  year=2025/
    month=12/
      part-0.parquet
```

### Row Group Sizing Strategy

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

Recommended: 128MB - 256MB for most workloads
```

### File Sizing Strategy

```
Too many small files (< 64MB each):
  + Good parallelism
  - High metadata overhead
  - Many file open operations
  - S3 API costs

Too few large files (> 1GB each):
  + Low overhead
  - Poor parallelism
  - Long compaction times
  - Memory pressure

Optimal: 256MB - 1GB per file
```

### Writing from Pandas

```python
import pandas as pd
import pyarrow.parquet as pq

df = pd.DataFrame({...})
pq.write_table(pa.Table.from_pandas(df), "output.parquet")

# Or directly from Pandas
df.to_parquet("output.parquet", engine="pyarrow")
```

### Writing with Schema Control

```python
import pyarrow as pa

# Define explicit schema
schema = pa.schema([
    ("id", pa.int64()),
    ("name", pa.string()),
    ("amount", pa.decimal128(18, 2)),
])

table = pa.table({...}, schema=schema)
pq.write_table(table, "output.parquet")
```

---

## 2. Example

### Basic Write vs Optimized Write

```python
import pyarrow as pa
import pyarrow.parquet as pq
import os

# Create sample data
data = {
    "id": list(range(1_000_000)),
    "status": ["COMPLETED"] * 900_000 + ["PENDING"] * 70_000 + ["FAILED"] * 30_000,
    "amount": [float(i * 100) for i in range(1_000_000)],
}
table = pa.table(data)

# Write 1: Default settings
pq.write_table(table, "default.parquet")
size_default = os.path.getsize("default.parquet")

# Write 2: Optimized settings
pq.write_table(
    table,
    "optimized.parquet",
    compression="zstd",
    use_dictionary=True,
    write_statistics=True,
    data_page_size=1_048_576,
)
size_optimized = os.path.getsize("optimized.parquet")

print(f"Default: {size_default / (1024*1024):.1f} MB")
print(f"Optimized: {size_optimized / (1024*1024):.1f} MB")
print(f"Savings: {(1 - size_optimized/size_default)*100:.1f}%")
```

---

## 3. Banking Scenario 1: Batch ETL Pipeline

### Problem
A bank's nightly ETL processes **500 million transactions** from Oracle into Parquet for analytics. The pipeline must:
- Write data efficiently (complete within 2-hour window)
- Optimize for morning query performance
- Partition by date for easy data management
- Handle schema changes over time

### Why Write Strategy Matters?
- Wrong compression: Write takes 3 hours instead of 1 hour
- No partitioning: Queries must scan all data
- Poor row group sizing: Query parallelism suffers
- No statistics: Predicate pushdown disabled

### Architecture
```
Oracle DB (source)
       |
       v
  ETL Pipeline (Spark / Python)
       |
       v
  Parquet Files (partitioned, compressed, with statistics)
       |
       v
  S3 Data Lake
       |
       v
  Analytics Queries (DuckDB / Trino)
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
# BANKING SCENARIO: Batch ETL Pipeline
# ============================================================

def simulate_oracle_source(num_records=2_000_000):
    """Simulate extracting data from Oracle (source system)."""
    random.seed(42)
    np.random.seed(42)

    start_time = datetime(2026, 8, 24)
    timestamps = np.array([
        start_time + timedelta(seconds=i * 0.0432)
        for i in range(num_records)
    ], dtype='datetime64[us]')

    data = {
        "transaction_id": list(range(1, num_records + 1)),
        "account_id": [f"ACC{random.randint(100000, 999999)}" for _ in range(num_records)],
        "customer_id": [f"CUST{random.randint(10000, 99999)}" for _ in range(num_records)],
        "amount": np.random.uniform(1.0, 50000.0, num_records).round(2).tolist(),
        "currency": np.random.choice(["USD", "EUR", "GBP"], num_records).tolist(),
        "transaction_type": np.random.choice(
            ["DEBIT", "CREDIT", "TRANSFER", "WIRE", "ACH"], num_records
        ).tolist(),
        "status": np.random.choice(
            ["COMPLETED"] * 95 + ["PENDING"] * 3 + ["FAILED"] * 2, num_records
        ).tolist(),
        "channel": np.random.choice(
            ["ONLINE", "MOBILE", "BRANCH", "ATM", "API"], num_records
        ).tolist(),
        "merchant": np.random.choice(
            ["Walmart", "Amazon", "Starbucks", "Shell", "Direct Deposit",
             "Wire Transfer", "ATM", "POS Terminal"], num_records
        ).tolist(),
        "transaction_timestamp": timestamps,
        "transaction_date": [
            (start_time + timedelta(days=i // 500000)).strftime("%Y-%m-%d")
            for i in range(num_records)
        ],
    }

    return pa.table(data)


def etl_write_optimized(table, output_path, partition_col="transaction_date"):
    """Write Parquet with production-optimized settings."""
    start = time.time()

    # Production-optimized write
    pq.write_to_dataset(
        table,
        root_path=output_path,
        partition_cols=[partition_col],
        compression="zstd",
        compression_level=3,
        use_dictionary=True,
        dictionary_pagesize_limit=1_048_576,
        write_statistics=True,
        data_page_size=1_048_576,
        version="2.6",
        use_deprecated_int96_timestamps=False,
    )

    elapsed = time.time() - start

    # Calculate metrics
    total_size = 0
    file_count = 0
    for root, dirs, files in os.walk(output_path):
        for f in files:
            if f.endswith(".parquet"):
                total_size += os.path.getsize(os.path.join(root, f))
                file_count += 1

    # Estimate raw CSV size
    estimated_csv = table.num_rows * 200  # ~200 bytes per row in CSV

    print(f"\n=== ETL Write Report ===")
    print(f"Records processed: {table.num_rows:,}")
    print(f"Write time: {elapsed:.3f}s")
    print(f"Throughput: {table.num_rows / elapsed:,.0f} records/sec")
    print(f"Files created: {file_count}")
    print(f"Parquet size: {total_size / (1024*1024):.1f} MB")
    print(f"Estimated CSV size: {estimated_csv / (1024*1024):.1f} MB")
    print(f"Compression ratio: {estimated_csv / total_size:.1f}x")
    print(f"Partition column: {partition_col}")

    return elapsed


def compare_write_strategies(table, base_path):
    """Compare different write strategies."""
    strategies = {}

    # Strategy 1: No optimization
    path1 = os.path.join(base_path, "strategy1")
    os.makedirs(path1, exist_ok=True)
    start = time.time()
    pq.write_table(table, os.path.join(path1, "data.parquet"), compression="none")
    strategies["no_compression"] = {
        "time": time.time() - start,
        "size": os.path.getsize(os.path.join(path1, "data.parquet")),
    }

    # Strategy 2: Snappy (default)
    path2 = os.path.join(base_path, "strategy2")
    os.makedirs(path2, exist_ok=True)
    start = time.time()
    pq.write_table(table, os.path.join(path2, "data.parquet"), compression="snappy")
    strategies["snappy"] = {
        "time": time.time() - start,
        "size": os.path.getsize(os.path.join(path2, "data.parquet")),
    }

    # Strategy 3: Zstd
    path3 = os.path.join(base_path, "strategy3")
    os.makedirs(path3, exist_ok=True)
    start = time.time()
    pq.write_table(table, os.path.join(path3, "data.parquet"), compression="zstd")
    strategies["zstd"] = {
        "time": time.time() - start,
        "size": os.path.getsize(os.path.join(path3, "data.parquet")),
    }

    # Strategy 4: Zstd + dictionary + statistics
    path4 = os.path.join(base_path, "strategy4")
    os.makedirs(path4, exist_ok=True)
    start = time.time()
    pq.write_table(
        table,
        os.path.join(path4, "data.parquet"),
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
    )
    strategies["zstd_optimized"] = {
        "time": time.time() - start,
        "size": os.path.getsize(os.path.join(path4, "data.parquet")),
    }

    print(f"\n=== Write Strategy Comparison ===")
    print(f"{'Strategy':<20} {'Time (s)':<12} {'Size (MB)':<12} {'Ratio':<10}")
    print("-" * 54)
    for name, metrics in strategies.items():
        ratio = strategies["no_compression"]["size"] / metrics["size"]
        print(f"{name:<20} {metrics['time']:<12.3f} {metrics['size']/1024/1024:<12.1f} {ratio:<10.1f}x")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "etl_output")
    os.makedirs(base_path, exist_ok=True)

    # Simulate Oracle extraction
    print("Extracting from Oracle (simulated)...")
    source_data = simulate_oracle_source(num_records=2_000_000)

    # Write with optimized settings
    output_path = os.path.join(base_path, "transactions")
    os.makedirs(output_path, exist_ok=True)
    etl_write_optimized(source_data, output_path)

    # Compare write strategies
    compare_write_strategies(source_data, base_path)
```

---

## 5. Banking Scenario 2: Streaming Ingestion

### Problem
A bank's streaming platform (Kafka → Flink → Parquet) ingests **10,000 transactions per second**. Files must be:
- Written every 60 seconds (micro-batch)
- Compacted periodically to avoid small files
- Readable immediately after writing (no corruption)
- Optimized for downstream query performance

### Why Write Strategy Matters?
- Too frequent writes → thousands of tiny files → query degradation
- Too infrequent writes → data latency too high
- No compaction → metadata explosion
- Wrong file sizing → poor parallelism

### Architecture
```
Kafka (10K events/sec)
       |
       v
  Flink (micro-batch every 60s)
       |
       v
  Parquet Writer (rolling files)
       |
       v
  Compaction Job (hourly)
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
import threading
import queue

# ============================================================
# BANKING SCENARIO: Streaming Ingestion with Rolling Writer
# ============================================================

class RollingParquetWriter:
    """Simulates a streaming Parquet writer that creates rolling files."""

    def __init__(self, base_path, max_rows_per_file=100_000, compression="snappy"):
        self.base_path = base_path
        self.max_rows = max_rows_per_file
        self.compression = compression
        self.current_rows = []
        self.file_count = 0
        self.total_rows_written = 0
        self.schema = pa.schema([
            ("transaction_id", pa.int64()),
            ("account_id", pa.string()),
            ("amount", pa.float64()),
            ("status", pa.string()),
            ("channel", pa.string()),
            ("timestamp", pa.timestamp("us")),
        ])

    def write_batch(self, batch_data):
        """Write a batch of transactions."""
        self.current_rows.extend(batch_data)

        # Flush if we've accumulated enough rows
        if len(self.current_rows) >= self.max_rows:
            self._flush()

    def _flush(self):
        """Flush current rows to a Parquet file."""
        if not self.current_rows:
            return

        start = time.time()

        # Create Arrow table
        table = pa.table({
            "transaction_id": pa.array([r["transaction_id"] for r in self.current_rows], type=pa.int64()),
            "account_id": pa.array([r["account_id"] for r in self.current_rows], type=pa.string()),
            "amount": pa.array([r["amount"] for r in self.current_rows], type=pa.float64()),
            "status": pa.array([r["status"] for r in self.current_rows], type=pa.string()),
            "channel": pa.array([r["channel"] for r in self.current_rows], type=pa.string()),
            "timestamp": pa.array([r["timestamp"] for r in self.current_rows], type=pa.timestamp("us")),
        })

        # Write with streaming-optimized settings
        filename = f"transactions_{self.file_count:06d}.parquet"
        filepath = os.path.join(self.base_path, filename)

        pq.write_table(
            table,
            filepath,
            compression=self.compression,
            use_dictionary=True,
            write_statistics=True,
            data_page_size=524_288,  # 512KB pages for streaming
        )

        self.total_rows_written += len(self.current_rows)
        self.file_count += 1
        self.current_rows = []

        elapsed = time.time() - start
        size = os.path.getsize(filepath)
        return {"filename": filename, "rows": len(table), "size": size, "time": elapsed}

    def close(self):
        """Flush any remaining rows."""
        return self._flush()


class ParquetCompactor:
    """Compacts small Parquet files into larger ones."""

    def __init__(self, base_path, target_file_size_mb=256):
        self.base_path = base_path
        self.target_size = target_file_size_mb * 1024 * 1024

    def compact(self):
        """Read small files and write larger ones."""
        start = time.time()

        # Find all Parquet files
        parquet_files = []
        for f in sorted(os.listdir(self.base_path)):
            if f.endswith(".parquet"):
                parquet_files.append(os.path.join(self.base_path, f))

        if not parquet_files:
            return

        # Read all files
        tables = [pq.read_table(f) for f in parquet_files]
        combined = pa.concat_tables(tables)

        # Write compacted file
        compacted_path = os.path.join(self.base_path, "compacted_transactions.parquet")
        pq.write_table(
            combined,
            compacted_path,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
        )

        # Remove original files
        for f in parquet_files:
            os.remove(f)

        elapsed = time.time() - start
        size = os.path.getsize(compacted_path)

        print(f"\n=== Compaction Report ===")
        print(f"Files compacted: {len(parquet_files)} → 1")
        print(f"Rows: {combined.num_rows:,}")
        print(f"Output size: {size / (1024*1024):.1f} MB")
        print(f"Compaction time: {elapsed:.3f}s")

        return compacted_path


def simulate_streaming_ingestion(base_path, duration_seconds=10, events_per_second=1000):
    """Simulate streaming ingestion with rolling Parquet files."""
    random.seed(42)

    writer = RollingParquetWriter(
        base_path=base_path,
        max_rows_per_file=10_000,  # Flush every 10K rows
        compression="snappy",
    )

    print(f"\n=== Streaming Ingestion Simulation ===")
    print(f"Events/sec: {events_per_second}")
    print(f"Duration: {duration_seconds}s")
    print(f"File threshold: 10,000 rows")

    batch_size = events_per_second // 10  # 10 batches per second
    total_events = 0
    write_results = []

    start_time = time.time()

    for second in range(duration_seconds):
        for _ in range(10):  # 10 sub-batches per second
            batch = []
            for _ in range(batch_size):
                batch.append({
                    "transaction_id": total_events + 1,
                    "account_id": f"ACC{random.randint(100000, 999999)}",
                    "amount": round(random.uniform(1.0, 10000.0), 2),
                    "status": random.choice(["COMPLETED", "PENDING", "FAILED"]),
                    "channel": random.choice(["ONLINE", "MOBILE", "ATM", "POS"]),
                    "timestamp": datetime.now(),
                })
                total_events += 1

            writer.write_batch(batch)

    # Flush remaining
    result = writer.close()
    if result:
        write_results.append(result)

    elapsed = time.time() - start_time

    print(f"\nTotal events ingested: {total_events:,}")
    print(f"Files created: {writer.file_count}")
    print(f"Total time: {elapsed:.3f}s")

    # Now compact
    compactor = ParquetCompactor(base_path, target_file_size_mb=256)
    compacted_path = compactor.compact()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "streaming_parquet")
    os.makedirs(base_path, exist_ok=True)

    # Simulate streaming ingestion
    simulate_streaming_ingestion(
        base_path,
        duration_seconds=5,
        events_per_second=5000,
    )
```

---

## 7. Interview Questions

### Q1: What is the difference between `pq.write_table()` and `pq.write_to_dataset()`?

**Answer:**

**`pq.write_table()`**:
- Writes a single Parquet file
- No partitioning
- Best for: small datasets, single-file output

```python
pq.write_table(table, "output.parquet")
```

**`pq.write_to_dataset()`**:
- Writes multiple files in a directory hierarchy
- Supports partition columns
- Creates Hive-style partitioning
- Best for: large datasets, data lake storage

```python
pq.write_to_dataset(
    table,
    root_path="output/",
    partition_cols=["date", "region"],
)
```

**Output structure**:
```
output/
  date=2026-08-01/
    region=US/
      part-0.parquet
    region=EU/
      part-0.parquet
  date=2026-08-02/
    ...
```

---

### Q2: How do you handle schema evolution when writing Parquet files?

**Answer:**

Parquet supports limited schema evolution:

**Adding columns:**
```python
# Old schema
schema_v1 = pa.schema([("id", pa.int64()), ("name", pa.string())])

# New schema (added column)
schema_v2 = pa.schema([("id", pa.int64()), ("name", pa.string()), ("email", pa.string())])
```

When writing with schema_v2, old files with schema_v1 remain readable. New column appears as NULL in old files.

**Best practices:**
1. Always define explicit schemas
2. Add new columns at the end (not middle)
3. Use Iceberg/Delta Lake for complex schema evolution
4. Test schema compatibility before production deployment

---

### Q3: What are the trade-offs between different compression codecs for writes?

**Answer:**

| Codec | Write Speed | Read Speed | Compression Ratio | Best For |
|-------|------------|------------|-------------------|----------|
| Snappy | Fastest | Fastest | Moderate | Interactive queries |
| Zstd (level 3) | Fast | Fast | High | General purpose |
| Gzip | Slow | Slow | High | Archival |
| LZ4 | Very fast | Very fast | Low | Low-latency |
| Brotli | Very slow | Slow | Highest | Maximum compression |

**Write-heavy workloads**: Snappy or LZ4 (minimize write latency)
**Read-heavy workloads**: Zstd or Gzip (minimize storage and read I/O)
**Balanced**: Zstd level 3 (best overall for most use cases)

---

### Q4: How do you optimize Parquet file sizes?

**Answer:**

**Problem**: Too many small files or too few large files.

**Solutions:**

1. **Control row group size**:
```python
pq.write_table(table, "output.parquet", row_group_size=1_000_000)
```

2. **Control data page size**:
```python
pq.write_table(table, "output.parquet", data_page_size=1_048_576)
```

3. **Use compaction**:
```python
# Read multiple small files, write one large file
tables = [pq.read_table(f) for f in small_files]
combined = pa.concat_tables(tables)
pq.write_table(combined, "compacted.parquet")
```

4. **Target file size**: 256MB - 1GB for optimal parallelism and metadata overhead.

---

### Q5: What metadata is written in the Parquet footer?

**Answer:**

The footer contains all metadata about the file:

1. **Schema**: Column names, types, repetition/definition levels
2. **Row Group locations**: File offsets for each row group
3. **Column statistics**: Min, max, null_count, distinct_count per column per row group
4. **Key-value metadata**: Custom metadata (e.g., "created_by", "pandas_version")
5. **File path**: Original file path (for distributed writes)

**Why this matters:**
- Footer is read **first** when opening a Parquet file
- Statistics enable predicate pushdown without reading data
- Schema enables type-safe reads
- Row group offsets enable parallel reads

**Footer size**: Typically small (KB to MB) even for large files (GB), because it only contains metadata, not data.
