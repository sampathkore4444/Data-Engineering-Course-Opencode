# Writing Parquet Files

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Writing Interfaces Comparison](#2-writing-interfaces-comparison)
3. [Example](#3-example)
4. [Real-World Scenario: Writing Interface Comparison](#4-real-world-scenario-writing-interface-comparison)
5. [Real-World Banking Scenario 1](#5-banking-scenario-1-batch-etl-pipeline)
6. [Python Code - Scenario 1](#6-python-code---scenario-1)
7. [Real-World Banking Scenario 2](#7-banking-scenario-2-streaming-ingestion)
8. [Python Code - Scenario 2](#8-python-code---scenario-2)
9. [Interview Questions](#9-interview-questions)

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

## 2. Writing Interfaces Comparison

### Overview: The Five Main Ways to Write Parquet in Python

There are five primary interfaces for writing Parquet files. Each serves a different use case:

| Interface | Source | Input Type | Best For |
|-----------|--------|------------|----------|
| `df.to_parquet()` | pandas | `pd.DataFrame` | Quick writes, notebooks |
| `pq.write_table()` | PyArrow | `pa.Table` | Single-file writes, pipelines |
| `pq.write_to_dataset()` | PyArrow | `pa.Table` | Partitioned data lakes |
| `pq.ParquetWriter` | PyArrow | `pa.Table` (incremental) | Streaming, large datasets |
| `pl.write_parquet()` | Polars | `pl.DataFrame` | High-performance writes |
| `fastparquet.write()` | Fastparquet | `pd.DataFrame` | Dask workflows |

---

### 2.1 `df.to_parquet()` — Pandas Native

```python
import pandas as pd

# Simple — write DataFrame directly
df.to_parquet("output.parquet")

# With compression
df.to_parquet("output.parquet", engine="pyarrow", compression="zstd")

# With partitioning
df.to_parquet("output_dir/", engine="pyarrow", partition_cols=["date"])

# With column selection
df.to_parquet("output.parquet", engine="pyarrow", columns=["amount", "date"])
```

**Under the hood:** `df.to_parquet()` converts the pandas DataFrame to an Arrow Table, then calls `pq.write_table()` internally. It is a thin convenience wrapper.

---

### 2.2 `pq.write_table()` — PyArrow Single-File Write

```python
import pyarrow.parquet as pq

# Write entire table
pq.write_table(table, "output.parquet")

# With full control
pq.write_table(
    table,
    "output.parquet",
    compression="zstd",
    compression_level=3,
    use_dictionary=True,
    write_statistics=True,
    data_page_size=1_048_576,
    row_group_size=1_000_000,
    version="2.6",
)
```

**Under the hood:** Directly writes the Arrow Table to a single Parquet file. No pandas overhead.

---

### 2.3 `pq.write_to_dataset()` — Partitioned Write

```python
import pyarrow.parquet as pq

# Write partitioned dataset
pq.write_to_dataset(
    table,
    root_path="output/",
    partition_cols=["year", "month"],
    compression="zstd",
    use_dictionary=True,
    write_statistics=True,
)
```

**Under the hood:** Splits the table by partition columns, writes each partition as a separate Parquet file in a directory hierarchy.

---

### 2.4 `pq.ParquetWriter` — Streaming/Incremental Write

```python
import pyarrow.parquet as pq

# Create writer with schema
writer = pq.ParquetWriter(
    "output.parquet",
    schema=table.schema,
    compression="zstd",
    use_dictionary=True,
)

# Write batches incrementally
writer.write_table(batch1)
writer.write_table(batch2)
writer.write_table(batch3)

# Close to flush footer
writer.close()
```

**Under the hood:** Writes row groups incrementally. Footer is written only when `close()` is called. Ideal for streaming data.

---

### 2.5 `pl.write_parquet()` — Polars Native

```python
import polars as pl

# Write entire DataFrame
df.write_parquet("output.parquet")

# With compression
df.write_parquet("output.parquet", compression="zstd")

# With row group size
df.write_parquet("output.parquet", row_group_size=1_000_000)

# Lazy streaming write (larger-than-RAM)
(
    pl.scan_parquet("input.parquet")
    .filter(pl.col("amount") > 1000)
    .sink_parquet("output.parquet")  # streams to disk
)
```

**Under the hood:** Polars writes directly from its Rust core. `sink_parquet()` enables streaming writes for datasets larger than RAM.

---

### 2.6 `fastparquet.write()` — Fastparquet Native

```python
import fastparquet

# Write from pandas DataFrame
fastparquet.write("output.parquet", df)

# With compression
fastparquet.write("output.parquet", df, compression="zstd")

# With partitioning
fastparquet.write("output_dir/", df, write_related_files=True,
                   partition_on=["date"])
```

**Under the hood:** Writes directly from pandas without Arrow conversion. Uses thrift for metadata.

---

### 2.7 Feature Comparison

| Feature | `df.to_parquet()` | `pq.write_table()` | `pq.write_to_dataset()` | `pq.ParquetWriter` | `pl.write_parquet()` | `fastparquet.write()` |
|---------|-------------------|--------------------|------------------------|--------------------|--------------------|---------------------|
| **Input** | DataFrame | Arrow Table | Arrow Table | Arrow Table (batch) | Polars DataFrame | DataFrame |
| **Single file** | ✅ Yes | ✅ Yes | ❌ No | ✅ Yes | ✅ Yes | ✅ Yes |
| **Partitioning** | ✅ Via engine | ❌ No | ✅ Yes | ❌ No | ⚠️ Manual | ✅ Yes |
| **Streaming** | ❌ No | ❌ No | ❌ No | ✅ Yes | ✅ `sink_parquet()` | ❌ No |
| **Compression** | ✅ All codecs | ✅ All codecs | ✅ All codecs | ✅ All codecs | ✅ All codecs | ✅ All codecs |
| **Dictionary encoding** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| **Statistics** | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes | ✅ Yes |
| **Schema control** | ⚠️ Limited | ✅ Full | ✅ Full | ✅ Full | ⚠️ Limited | ⚠️ Limited |
| **Memory efficiency** | ⚠️ pandas overhead | ✅ Arrow format | ✅ Arrow format | ✅ Arrow format | ✅ Rust core | ⚠️ pandas overhead |
| **Speed** | ⚠️ Moderate | ✅ Fast | ✅ Fast | ✅ Fast | ✅ Fastest | ⚠️ Slower |
| **Larger-than-RAM** | ❌ No | ❌ No | ❌ No | ⚠️ Manual batching | ✅ `sink_parquet()` | ❌ No |
| **Cloud storage** | ⚠️ Via fsspec | ✅ Native | ✅ Native | ✅ Native | ✅ Native | ⚠️ Via fsspec |
| **Complexity** | Low | Low | Medium | High | Low | Low |

---

### 2.8 Advantages & Disadvantages

#### `df.to_parquet()` (Pandas)

| Advantages | Disadvantages |
|------------|---------------|
| ✅ Simplest API — one line to write | ❌ Always converts through Arrow (overhead) |
| ✅ Familiar to pandas users | ❌ No direct schema control |
| ✅ Works with any engine (pyarrow, fastparquet) | ❌ No streaming support |
| ✅ Supports partitioning via engine | ❌ Memory-inefficient for large DataFrames |
| ✅ Great for quick prototyping | ❌ Cannot write row groups incrementally |

#### `pq.write_table()` (PyArrow)

| Advantages | Disadvantages |
|------------|---------------|
| ✅ Direct Arrow write (no pandas overhead) | ❌ Single file only — no partitioning |
| ✅ Full control over all write parameters | ❌ Must manually manage partitioning |
| ✅ Fastest single-file write | ❌ No streaming support |
| ✅ Schema control | ❌ Requires Arrow Table input |
| ✅ All compression codecs | |

#### `pq.write_to_dataset()` (PyArrow Partitioned)

| Advantages | Disadvantages |
|------------|---------------|
| ✅ Automatic partitioning (Hive-style) | ❌ Creates many small files per partition |
| ✅ Directory hierarchy created automatically | ❌ No streaming support |
| ✅ Optimized for data lake queries | ❌ Overhead from directory management |
| ✅ Full compression and encoding control | ❌ Cannot write single large file |

#### `pq.ParquetWriter` (PyArrow Streaming)

| Advantages | Disadvantages |
|------------|---------------|
| ✅ Incremental writes (streaming) | ❌ Most complex API |
| ✅ Footer written only at close() | ❌ Must manage schema manually |
| ✅ Memory-efficient for large data | ❌ No partitioning support |
| ✅ Ideal for real-time ingestion | ❌ Error handling more complex |

#### `pl.write_parquet()` (Polars)

| Advantages | Disadvantages |
|------------|---------------|
| ✅ Fastest write speed (Rust core) | ❌ Different API from pandas |
| ✅ `sink_parquet()` for streaming writes | ❌ Smaller ecosystem |
| ✅ No pandas/Arrow conversion needed | ❌ Some features experimental |
| ✅ Memory-efficient | ❌ Learning curve for pandas users |
| ✅ Lazy evaluation + query optimization | |

#### `fastparquet.write()` (Fastparquet)

| Advantages | Disadvantages |
|------------|---------------|
| ✅ Direct pandas write (no Arrow) | ❌ Slower than PyArrow |
| ✅ Lightweight dependency | ❌ No streaming support |
| ✅ Good Dask integration | ❌ Limited feature set |
| ✅ Simple API | ❌ Less active maintenance |

---

### 2.9 When to Use Which Approach?

| Scenario | Recommendation | Why |
|----------|---------------|-----|
| Quick write in notebook | `df.to_parquet()` | Simplest API |
| Single Parquet file, full control | `pq.write_table()` | Direct Arrow write |
| Partitioned data lake | `pq.write_to_dataset()` | Automatic partitioning |
| Streaming ingestion (Kafka → Parquet) | `pq.ParquetWriter` or `pl.sink_parquet()` | Incremental/streaming writes |
| High-performance write pipeline | `pl.write_parquet()` | Fastest (Rust core) |
| Dask distributed writes | `fastparquet.write()` | Native Dask integration |
| Larger-than-RAM data | `pl.sink_parquet()` | Streaming write support |
| Production ETL (partitioned + optimized) | `pq.write_to_dataset()` | Full control + partitioning |
| Converting pandas → Parquet quickly | `df.to_parquet(engine="pyarrow")` | PyArrow engine, simple |
| Schema-critical writes | `pq.write_table()` with explicit schema | Full schema control |

---

### 2.10 Performance Comparison

#### Writing 10M Rows (10 columns)

| Method | Write Time | File Size | Notes |
|--------|-----------|-----------|-------|
| `df.to_parquet(engine="pyarrow")` | ~3.2s | ~180 MB | pandas → Arrow → Parquet |
| `df.to_parquet(engine="fastparquet")` | ~5.8s | ~195 MB | Direct pandas → Parquet |
| `pq.write_table()` | ~1.8s | ~175 MB | Direct Arrow → Parquet |
| `pq.write_to_dataset()` | ~2.1s | ~180 MB | Partitioned write overhead |
| `pl.write_parquet()` | ~1.2s | ~170 MB | Rust core, fastest |
| `pl.sink_parquet()` (lazy) | ~1.0s | ~170 MB | Streaming write |

> **Key insight:** Polars is ~2.5x faster than pandas for writing Parquet. PyArrow `write_table` is ~1.8x faster than pandas.

#### The Hidden Cost of `df.to_parquet()`

```python
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import numpy as np
import time

# Create test DataFrame
df = pd.DataFrame({
    "id": np.arange(10_000_000),
    "value": np.random.randn(10_000_000),
    "category": np.random.choice(["A", "B", "C", "D"], 10_000_000),
})

# Method 1: df.to_parquet (pandas way)
t0 = time.time()
df.to_parquet("test_pandas.parquet", engine="pyarrow")
print(f"df.to_parquet: {time.time()-t0:.2f}s")

# Method 2: pq.write_table (Arrow way)
t0 = time.time()
table = pa.Table.from_pandas(df)  # convert once
pq.write_table(table, "test_arrow.parquet")
print(f"pq.write_table: {time.time()-t0:.2f}s")

# Method 3: Polars
import polars as pl
df_pl = pl.from_pandas(df)
t0 = time.time()
df_pl.write_parquet("test_polars.parquet")
print(f"pl.write_parquet: {time.time()-t0:.2f}s")
```

**Typical output:**
```
df.to_parquet:     3.24s
pq.write_table:    1.82s   ← 1.8x faster (no pandas overhead)
pl.write_parquet:  1.18s   ← 2.7x faster (Rust core)
```

---

### 2.11 Common Pitfalls When Choosing a Write Approach

1. **Using `df.to_parquet()` for large datasets**: If your data is >1 GB, prefer `pq.write_table()` to avoid the pandas→Arrow conversion overhead.

2. **Not using partitioning for data lakes**: Always use `pq.write_to_dataset()` with partition columns for data lake storage. Without partitioning, queries must scan all files.

3. **Using `pq.write_table()` for streaming**: For real-time ingestion, use `pq.ParquetWriter` or `pl.sink_parquet()` to write incrementally.

4. **Ignoring statistics**: Always set `write_statistics=True` (default). Statistics enable predicate pushdown during reads.

5. **Wrong compression for use case**: Use Snappy for interactive queries, Zstd for general purpose, Gzip for archival.

6. **Not closing ParquetWriter**: Always call `writer.close()` to flush the footer. Unclosed writers produce corrupt files.

---

## 3. Example

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

## 4. Real-World Scenario: Writing Interface Comparison

### Problem
A data engineering team manages multiple data pipelines that write Parquet files. Each pipeline has different requirements:

- **Batch ETL**: Writes 500M rows nightly, needs partitioning
- **Streaming Ingestion**: Writes 10K events/sec, needs rolling files
- **ML Feature Store**: Writes feature tables, needs schema control
- **Data Lake Export**: Exports from data warehouse, needs optimized compression

Each team uses a different writing interface. This scenario compares their approaches.

### Architecture
```
Batch ETL (Spark/Python)
       |
       v
  pq.write_to_dataset() → Partitioned Data Lake

Streaming (Kafka → Flink)
       |
       v
  pq.ParquetWriter / pl.sink_parquet() → Rolling Files

ML Feature Store
       |
       v
  pq.write_table() → Schema-Controlled Files

Data Lake Export
       |
       v
  pl.write_parquet() → High-Performance Writes
```

### Python Code

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pandas as pd
import numpy as np
import polars as pl
import time
import os
import tempfile
from datetime import datetime, timedelta
import random

# ============================================================
# WRITING INTERFACE COMPARISON: Multi-Pipeline Scenario
# ============================================================

def generate_batch_data(num_rows=5_000_000):
    """Generate batch ETL data."""
    random.seed(42)
    np.random.seed(42)

    table = pa.table({
        "id": pa.array(range(1, num_rows + 1), type=pa.int64()),
        "customer_id": pa.array([f"CUST{random.randint(10000, 99999)}" for _ in range(num_rows)], type=pa.string()),
        "amount": pa.array(np.random.uniform(1.0, 50000.0, num_rows).round(2), type=pa.float64()),
        "status": pa.array(np.random.choice(["COMPLETED"] * 95 + ["PENDING"] * 3 + ["FAILED"] * 2, num_rows), type=pa.string()),
        "date": pa.array([(datetime(2026, 1, 1) + timedelta(days=i // 100000)).strftime("%Y-%m-%d") for i in range(num_rows)], type=pa.string()),
    })
    return table


# ============================================================
# TEAM 1: Batch ETL — pq.write_to_dataset()
# ============================================================

def batch_etl_workflow(table, base_path):
    """Batch ETL team writes partitioned data."""
    print("\n" + "="*60)
    print("TEAM 1: Batch ETL (pq.write_to_dataset)")
    print("="*60)

    output_path = os.path.join(base_path, "batch_etl")
    os.makedirs(output_path, exist_ok=True)

    start = time.time()

    # Partitioned write for data lake
    pq.write_to_dataset(
        table,
        root_path=output_path,
        partition_cols=["date"],
        compression="zstd",
        compression_level=3,
        use_dictionary=True,
        write_statistics=True,
    )

    elapsed = time.time() - start

    # Count files
    file_count = sum(1 for _, _, files in os.walk(output_path) for f in files if f.endswith(".parquet"))

    print(f"  Write time: {elapsed:.3f}s")
    print(f"  Rows written: {table.num_rows:,}")
    print(f"  Files created: {file_count}")
    print(f"  Why pq.write_to_dataset? → Automatic partitioning for data lake")


# ============================================================
# TEAM 2: ML Feature Store — pq.write_table()
# ============================================================

def ml_feature_store_workflow(table, base_path):
    """ML team writes schema-controlled feature tables."""
    print("\n" + "="*60)
    print("TEAM 2: ML Feature Store (pq.write_table)")
    print("="*60)

    output_path = os.path.join(base_path, "ml_features")
    os.makedirs(output_path, exist_ok=True)

    start = time.time()

    # Schema-controlled write for ML features
    pq.write_table(
        table,
        os.path.join(output_path, "features.parquet"),
        compression="snappy",  # Fast reads for ML training
        use_dictionary=True,
        write_statistics=True,
        version="2.6",
    )

    elapsed = time.time() - start
    size = os.path.getsize(os.path.join(output_path, "features.parquet"))

    print(f"  Write time: {elapsed:.3f}s")
    print(f"  Rows written: {table.num_rows:,}
    print(f"  File size: {size / (1024*1024):.1f} MB")
    print(f"  Why pq.write_table? → Single file, schema control, fast reads for ML")


# ============================================================
# TEAM 3: Data Lake Export — pl.write_parquet()
# ============================================================

def data_lake_export_workflow(table, base_path):
    """Data lake export team writes high-performance Parquet."""
    print("\n" + "="*60)
    print("TEAM 3: Data Lake Export (pl.write_parquet)")
    print("="*60)

    output_path = os.path.join(base_path, "data_lake")
    os.makedirs(output_path, exist_ok=True)

    start = time.time()

    # Convert to Polars and write
    df_pl = pl.from_arrow(table)
    df_pl.write_parquet(
        os.path.join(output_path, "export.parquet"),
        compression="zstd",
        row_group_size=1_000_000,
    )

    elapsed = time.time() - start
    size = os.path.getsize(os.path.join(output_path, "export.parquet"))

    print(f"  Write time: {elapsed:.3f}s")
    print(f"  Rows written: {table.num_rows:,}")
    print(f"  File size: {size / (1024*1024):.1f} MB")
    print(f"  Why pl.write_parquet? → Fastest write speed (Rust core)")


# ============================================================
# TEAM 4: Streaming — pq.ParquetWriter
# ============================================================

def streaming_ingestion_workflow(base_path):
    """Streaming team writes incrementally."""
    print("\n" + "="*60)
    print("TEAM 4: Streaming Ingestion (pq.ParquetWriter)")
    print("="*60)

    output_path = os.path.join(base_path, "streaming")
    os.makedirs(output_path, exist_ok=True)

    schema = pa.schema([
        ("event_id", pa.int64()),
        ("timestamp", pa.timestamp("us")),
        ("user_id", pa.string()),
        ("action", pa.string()),
    ])

    start = time.time()

    # Create writer
    writer = pq.ParquetWriter(
        os.path.join(output_path, "events.parquet"),
        schema=schema,
        compression="snappy",
        use_dictionary=True,
    )

    # Write 10 batches (simulating streaming)
    total_rows = 0
    for batch_idx in range(10):
        batch_size = 100_000
        batch = pa.table({
            "event_id": pa.array(range(batch_idx * batch_size + 1, (batch_idx + 1) * batch_size + 1), type=pa.int64()),
            "timestamp": pa.array([datetime.now()] * batch_size, type=pa.timestamp("us")),
            "user_id": pa.array([f"USER{random.randint(1000, 9999)}" for _ in range(batch_size)], type=pa.string()),
            "action": pa.array(np.random.choice(["click", "view", "purchase"], batch_size), type=pa.string()),
        })
        writer.write_table(batch)
        total_rows += batch_size

    writer.close()  # Flush footer
    elapsed = time.time() - start

    size = os.path.getsize(os.path.join(output_path, "events.parquet"))

    print(f"  Write time: {elapsed:.3f}s")
    print(f"  Rows written: {total_rows:,}")
    print(f"  File size: {size / (1024*1024):.1f} MB")
    print(f"  Why pq.ParquetWriter? → Incremental writes, memory-efficient")


# ============================================================
# HEAD-TO-HEAD COMPARISON
# ============================================================

def compare_all_write_interfaces(table, base_path):
    """Run the same write through all approaches."""
    print("\n" + "="*60)
    print("HEAD-TO-HEAD: Same Data, Different Write Interfaces")
    print("="*60)

    results = {}

    # 1. df.to_parquet (pandas way)
    df = table.to_pandas()
    path1 = os.path.join(base_path, "test_pandas.parquet")
    start = time.time()
    df.to_parquet(path1, engine="pyarrow")
    results["df.to_parquet"] = {
        "time": time.time() - start,
        "size": os.path.getsize(path1),
    }
    del df

    # 2. pq.write_table (Arrow way)
    path2 = os.path.join(base_path, "test_arrow.parquet")
    start = time.time()
    pq.write_table(table, path2)
    results["pq.write_table"] = {
        "time": time.time() - start,
        "size": os.path.getsize(path2),
    }

    # 3. pq.write_to_dataset (partitioned)
    path3 = os.path.join(base_path, "test_partitioned")
    os.makedirs(path3, exist_ok=True)
    start = time.time()
    pq.write_to_dataset(table, root_path=path3, partition_cols=["date"])
    file_count = sum(1 for _, _, files in os.walk(path3) for f in files if f.endswith(".parquet"))
    total_size = sum(os.path.getsize(os.path.join(r, f)) for r, _, files in os.walk(path3) for f in files if f.endswith(".parquet"))
    results["pq.write_to_dataset"] = {
        "time": time.time() - start,
        "size": total_size,
    }

    # 4. pl.write_parquet (Polars way)
    df_pl = pl.from_arrow(table)
    path4 = os.path.join(base_path, "test_polars.parquet")
    start = time.time()
    df_pl.write_parquet(path4)
    results["pl.write_parquet"] = {
        "time": time.time() - start,
        "size": os.path.getsize(path4),
    }

    # 5. pq.ParquetWriter (streaming)
    path5 = os.path.join(base_path, "test_streaming.parquet")
    writer = pq.ParquetWriter(path5, schema=table.schema)
    start = time.time()
    # Split into 10 batches
    batch_size = table.num_rows // 10
    for i in range(10):
        batch = table.slice(i * batch_size, batch_size)
        writer.write_table(batch)
    writer.close()
    results["pq.ParquetWriter"] = {
        "time": time.time() - start,
        "size": os.path.getsize(path5),
    }

    print(f"\n  {'Interface':<25} {'Time (s)':<12} {'Size (MB)':<12} {'Speedup':<10}")
    print("  " + "-" * 59)
    baseline = results["df.to_parquet"]["time"]
    for interface, metrics in results.items():
        speedup = baseline / metrics["time"] if metrics["time"] > 0 else float('inf')
        print(f"  {interface:<25} {metrics['time']:<12.3f} {metrics['size']/1024/1024:<12.1f} {speedup:<10.1f}x")


# ============================================================
# RUN THE COMPARISON
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "write_comparison")
    os.makedirs(base_path, exist_ok=True)

    # Generate data
    print("Generating test data...")
    table = generate_batch_data(num_rows=2_000_000)

    # Run each team's workflow
    batch_etl_workflow(table, base_path)
    ml_feature_store_workflow(table, base_path)
    data_lake_export_workflow(table, base_path)
    streaming_ingestion_workflow(base_path)

    # Head-to-head comparison
    compare_all_write_interfaces(table, base_path)
```

### Key Takeaways

| Interface | When to Use | When NOT to Use |
|-----------|------------|------------------|
| `df.to_parquet()` | Quick writes, notebooks | Large datasets, production pipelines |
| `pq.write_table()` | Single files, full control | Partitioned data, streaming |
| `pq.write_to_dataset()` | Partitioned data lakes | Single files, streaming |
| `pq.ParquetWriter` | Streaming ingestion | Simple writes, partitioned data |
| `pl.write_parquet()` | High-performance writes | Existing pandas codebases |
| `pl.sink_parquet()` | Larger-than-RAM data | Small datasets |

> **Rule of thumb:** Start with `df.to_parquet()` for prototyping. For production, use `pq.write_to_dataset()` for partitioned data or `pq.write_table()` for single files. For maximum performance, use Polars.

---

## 5. Banking Scenario 1: Batch ETL Pipeline

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

## 6. Python Code - Scenario 1

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

## 7. Banking Scenario 2: Streaming Ingestion

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

## 8. Python Code - Scenario 2

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

## 9. Interview Questions

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

---

### Q6: When should you use `df.to_parquet()` vs `pq.write_table()` vs `pq.write_to_dataset()`?

**Answer:**

**`df.to_parquet()`** — Use when:
- Quick writes in notebooks or prototyping
- Working with pandas DataFrames
- Don't need fine-grained control

```python
# Simple write
df.to_parquet("output.parquet", engine="pyarrow")
```

**`pq.write_table()`** — Use when:
- Writing single Parquet files with full control
- Need schema control or specific compression settings
- Working with Arrow Tables directly

```python
# Full control
pq.write_table(table, "output.parquet", compression="zstd", write_statistics=True)
```

**`pq.write_to_dataset()`** — Use when:
- Writing partitioned data for data lakes
- Need Hive-style directory hierarchy
- Large datasets that benefit from partitioning

```python
# Partitioned write
pq.write_to_dataset(table, root_path="output/", partition_cols=["date"])
```

**Key rule of thumb:**
- Prototyping → `df.to_parquet()`
- Single file, full control → `pq.write_table()`
- Partitioned data lake → `pq.write_to_dataset()`
- Streaming → `pq.ParquetWriter` or `pl.sink_parquet()`

---

### Q7: What is the difference between PyArrow, Polars, and Fastparquet for writing Parquet?

**Answer:**

| Aspect | PyArrow | Polars | Fastparquet |
|--------|---------|--------|------------|
| **Speed** | Fast (C++) | Fastest (Rust) | Slower (Python) |
| **Memory** | Arrow format | Arrow format | Direct pandas |
| **Streaming** | `ParquetWriter` | `sink_parquet()` | No |
| **Partitioning** | `write_to_dataset()` | Manual | `write(partition_on=)` |
| **Schema control** | Full | Limited | Limited |
| **Dependencies** | Large (C++) | Medium | Lightweight |
| **Default in pandas** | Yes (2.0+) | No | Was default |

**Use PyArrow when:**
- Building data pipelines (Arrow format)
- Need full schema and compression control
- Writing partitioned data lakes

**Use Polars when:**
- Maximum write speed is critical
- Writing larger-than-RAM data (streaming)
- Building new pipelines from scratch

**Use Fastparquet when:
- Restricted environments (no C++ compilation)
- Dask-first workflows
- Legacy codebases

**Performance comparison (10M rows):**
```
pq.write_table:    1.82s   (baseline)
pl.write_parquet:  1.18s   (1.5x faster)
df.to_parquet:     3.24s   (1.8x slower)
fastparquet.write: 5.81s   (3.2x slower)
```

---

### Q8: How do you handle the small files problem when writing Parquet?

**Answer:**

**Problem:** Streaming ingestion creates thousands of small files (<64MB each), causing:
- High metadata overhead
- Slow query performance
- S3 API cost explosion

**Solutions:**

1. **Rolling writer with size threshold:**
```python
# Flush to new file every 100K rows or 256MB
class RollingWriter:
    def write_batch(self, batch):
        self.buffer.append(batch)
        if self.estimated_size > 256 * 1024 * 1024:
            self.flush_to_file()
```

2. **Compaction job:**
```python
# Read small files, write larger ones
tables = [pq.read_table(f) for f in small_files]
combined = pa.concat_tables(tables)
pq.write_table(combined, "compacted.parquet")
```

3. **Target file size:** Write files targeting 256MB-1GB per file.

4. **Partition-aware writing:** Use `pq.write_to_dataset()` to organize files by date/region.

**Best practices:**
- Set `row_group_size` to control internal structure
- Use compaction jobs (hourly/daily) to merge small files
- Monitor file count and average file size
- Target 256MB-1GB per file for optimal performance
