# Reading Parquet Files

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Reading Interfaces Comparison: pd.read_parquet() vs pq.read_table() vs pq.ParquetDataset](#2-reading-interfaces-comparison)
3. [Engine Comparison: PyArrow vs Fastparquet](#3-engine-comparison-pyarrow-vs-fastparquet)
4. [Modern Alternative: Polars for Reading Parquet](#4-modern-alternative-polars-for-reading-parquet)
5. [Example](#5-example)
6. [Real-World Scenario: Reading Interface Comparison](#6-real-world-scenario-reading-interface-comparison)
7. [Real-World Banking Scenario 1](#7-banking-scenario-1-regulatory-audit-query)
8. [Python Code - Scenario 1](#8-python-code---scenario-1)
9. [Real-World Banking Scenario 2](#9-banking-scenario-2-real-time-risk-dashboard)
10. [Python Code - Scenario 2](#10-python-code---scenario-2)
11. [Interview Questions](#11-interview-questions)

---

## 1. Detailed Explanation

### How Parquet Reading Works

Reading a Parquet file is fundamentally different from reading CSV. The key insight:

> **Parquet readers read the footer first, then selectively read only the data they need. This is the opposite of CSV where you must read everything from the start.**

### The Read Pipeline

```
Step 1: Read Footer (File Metadata)
         |
         +-- Schema
         +-- Row Group offsets
         +-- Column statistics
         |
Step 2: Plan which Row Groups to read
         |
         +-- Apply predicate filters
         +-- Check min/max statistics
         +-- Skip irrelevant row groups
         |
Step 3: Plan which Columns to read
         |
         +-- Apply column pruning
         +-- Read only needed column chunks
         |
Step 4: Read Column Chunks
         |
         +-- Decompress pages
         +-- Decode pages (dictionary → values)
         +-- Apply remaining filters
         |
Step 5: Return Result
```

### Three Reading Interfaces in PyArrow

#### 1. `pq.read_table()` — Simple, read entire file

```python
table = pq.read_table("transactions.parquet")
```

#### 2. `pq.read_table()` with filters and columns — Selective read

```python
table = pq.read_table(
    "transactions.parquet",
    columns=["amount", "date"],
    filters=[("date", ">=", "2026-08-01")]
)
```

#### 3. `pq.ParquetDataset` — Multi-file, partitioned reading

```python
dataset = pq.ParquetDataset(
    "s3://bucket/transactions/",
    filters=[("date", ">=", "2026-08-01")],
    use_legacy_dataset=False,
)
table = dataset.read(columns=["amount", "date"])
```

### Predicate Pushdown Explained

When you specify filters, Parquet uses statistics to skip data:

```
Row Group 1: date min=2026-01-01, max=2026-01-31
Row Group 2: date min=2026-02-01, max=2026-02-28
Row Group 3: date min=2026-08-01, max=2026-08-31

Filter: date >= '2026-08-01'

Row Group 1: max=Jan 31 < Aug 1 → SKIP
Row Group 2: max=Feb 28 < Aug 1 → SKIP
Row Group 3: min=Aug 1, max=Aug 31 → READ
```

Only Row Group 3 is read from disk.

### Column Pruning

When you specify columns, only those column chunks are read:

```
Available columns: [id, name, amount, date, merchant, city, state, country, ...]
Requested columns: [amount, date]

File has 10 columns, 5 row groups
Without pruning: 10 × 5 = 50 column chunks
With pruning: 2 × 5 = 10 column chunks
Reduction: 80%
```

### Filter Syntax

PyArrow supports various filter expressions:

```python
# Equality
("status", "=", "COMPLETED")

# Comparison
("amount", ">", 1000)
("amount", ">=", 1000)
("amount", "<", 5000)
("amount", "<=", 5000)

# Inequality
("status", "!=", "FAILED")

# In list
("status", "in", ["COMPLETED", "PENDING"])

# Is in set
("currency", "in", ["USD", "EUR", "GBP"])

# Multiple filters (AND logic)
filters = [
    ("date", ">=", "2026-08-01"),
    ("date", "<", "2026-09-01"),
    ("amount", ">", 1000),
]

# Complex filters with OR
import pyarrow.compute as pc
filter_expr = (
    (pc.field("status") == "COMPLETED") |
    (pc.field("amount") > 10000)
)
```

### Reading Large Files

For files too large to fit in memory:

```python
# Read in batches
dataset = pq.ParquetDataset("large_file.parquet")
reader = dataset.read批次(batch_size=100_000)

for batch in reader:
    # Process each batch
    process(batch)
```

### Schema Discovery

```python
# Read just the schema (no data)
schema = pq.read_schema("transactions.parquet")
print(schema)

# Read metadata
metadata = pq.read_metadata("transactions.parquet")
print(f"Rows: {metadata.num_rows}")
print(f"Row groups: {metadata.num_row_groups}")
print(f"Created by: {metadata.created_by}")
```

---

## 2. Reading Interfaces Comparison

### Overview: The Three Main Ways to Read Parquet in Python

There are three primary interfaces for reading Parquet files. Each serves a different use case:

| Interface | Source | Returns | Best For |
|-----------|--------|---------|----------|
| `pd.read_parquet()` | pandas | `pd.DataFrame` | Quick analysis, notebooks, EDA |
| `pq.read_table()` | PyArrow | `pa.Table` | Data pipelines, format conversion |
| `pq.ParquetDataset` | PyArrow | `pa.Table` (via `.read()`) | Partitioned data, large-scale reads |

---

### 2.1 `pd.read_parquet()` — Pandas Native

```python
import pandas as pd

# Simple — returns a DataFrame directly
df = pd.read_parquet("transactions.parquet")

# With column selection
df = pd.read_parquet("transactions.parquet", columns=["amount", "date"])

# With filters (pandas 2.0+ / PyArrow engine)
df = pd.read_parquet(
    "transactions.parquet",
    filters=[("status", "=", "COMPLETED")],
    engine="pyarrow"
)
```

**Under the hood:** `pd.read_parquet()` calls `pq.read_table()` internally, then converts the result to a pandas DataFrame. It is a thin convenience wrapper.

---

### 2.2 `pq.read_table()` — PyArrow Native

```python
import pyarrow.parquet as pq

# Read entire file as an Arrow Table
table = pq.read_table("transactions.parquet")

# With column pruning and filters
table = pq.read_table(
    "transactions.parquet",
    columns=["amount", "date"],
    filters=[("status", "=", "COMPLETED")]
)

# Convert to pandas when needed
df = table.to_pandas()
```

**Under the hood:** Directly reads the Parquet file into Arrow's columnar memory format. No pandas overhead until you explicitly call `.to_pandas()`.

---

### 2.3 `pq.ParquetDataset` — Multi-File / Partitioned

```python
import pyarrow.parquet as pq

# Read entire partitioned directory
dataset = pq.ParquetDataset(
    "s3://bucket/transactions/",
    filters=[("date", ">=", "2026-08-01")],
    use_legacy_dataset=False,
)
table = dataset.read(columns=["amount", "date"])

# Can also read schema without data
schema = dataset.schema
```

**Under the hood:** Scans a directory tree (or glob pattern), discovers all Parquet files, applies partition pruning, then reads matching files.

---

### 2.4 Detailed Comparison

#### Feature Matrix

| Feature | `pd.read_parquet()` | `pq.read_table()` | `pq.ParquetDataset` |
|---------|---------------------|--------------------|--------------------|
| **Returns** | `pd.DataFrame` | `pa.Table` | `pa.Table` (via `.read()`) |
| **Column pruning** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Predicate pushdown** | ✅ Yes (pyarrow engine) | ✅ Yes | ✅ Yes |
| **Partition discovery** | ⚠️ Limited | ❌ No | ✅ Yes |
| **Multi-file read** | ⚠️ Via directory | ⚠️ Via list of paths | ✅ Native |
| **Filesystem support** | Local + HDFS/S3 via fsspec | Local + HDFS/S3 via PyArrow | Local + HDFS/S3 via PyArrow |
| **Schema access** | ❌ No | ✅ `pq.read_schema()` | ✅ `dataset.schema` |
| **Metadata access** | ❌ No | ✅ `pq.read_metadata()` | ✅ `dataset.schema.metadata` |
| **Batch/streaming read** | ❌ No | ⚠️ Via `BatchReader` | ✅ `dataset.read_batches()` |
| **Memory efficiency** | ⚠️ Must convert to pandas | ✅ Arrow columnar format | ✅ Arrow columnar format |
| **Zero-copy to pandas** | ❌ Always copies | ✅ `.to_pandas()` with Arrow | ✅ `.to_pandas()` with Arrow |
| **Complexity** | Low (simple API) | Medium | High (most control) |

---

#### Advantages & Disadvantages

##### `pd.read_parquet()`

| Advantages | Disadvantages |
|------------|---------------|
| ✅ Simplest API — one line to get a DataFrame | ❌ Always converts to pandas (extra memory + time) |
| ✅ Familiar to pandas users | ❌ No direct access to Parquet metadata or schema |
| ✅ Works with any pandas-compatible engine (fastparquet, pyarrow) | ❌ Partition discovery is limited compared to ParquetDataset |
| ✅ Filters syntax matches pandas `.query()` style | ❌ Cannot read in batches — always loads full result into memory |
| ✅ Great for quick prototyping and notebooks | ❌ No way to inspect row group statistics before reading |
| | ❌ Overhead from pandas conversion for large datasets |

##### `pq.read_table()`

| Advantages | Disadvantages |
|------------|---------------|
| ✅ Returns Arrow Table (columnar, memory-efficient) | ❌ Single-file only — no partition discovery |
| ✅ Zero-copy conversion to pandas via `.to_pandas()` | ❌ Must manually list files for multi-file reads |
| ✅ Full access to schema and metadata | ❌ More verbose than `pd.read_parquet()` |
| ✅ Supports column pruning and predicate pushdown | ❌ No built-in partition pruning |
| ✅ Can convert to pandas, NumPy, or keep as Arrow | ❌ Need to manage Arrow memory explicitly for very large files |
| ✅ Better performance than pandas for large datasets | |

##### `pq.ParquetDataset`

| Advantages | Disadvantages |
|------------|---------------|
| ✅ Reads entire partitioned directories | ❌ Most complex API |
| ✅ Automatic partition pruning (skip directories) | ❌ Overkill for single small files |
| ✅ Handles S3, HDFS, GCS natively | ❌ Requires `use_legacy_dataset=False` for full features |
| ✅ Batch reading for memory-constrained environments | ❌ Learning curve for filter expressions |
| ✅ Parallel reading of row groups | ❌ Older API (`ParquetDataset` was rewritten in Arrow 2.0+) |
| ✅ Best for production data pipelines | |

---

### 2.5 When to Use Which?

```
                        Need a DataFrame immediately?
                              /       \
                            Yes        No
                            /            \
                  pd.read_parquet()    Need partition discovery?
                                       /       \
                                     Yes        No
                                     /            \
                           ParquetDataset    pq.read_table()
```

**Decision guide:**

| Scenario | Recommendation | Why |
|----------|---------------|-----|
| Quick EDA in Jupyter notebook | `pd.read_parquet()` | Fastest path to a DataFrame |
| Data pipeline producing Parquet | `pq.read_table()` | Arrow format, no pandas overhead |
| Reading a partitioned data lake | `pq.ParquetDataset` | Partition pruning, multi-file |
| Converting CSV → Parquet → DataFrame | `pq.read_table()` | Efficient intermediate format |
| Memory-constrained environment | `pq.ParquetDataset` | Batch reading, streaming |
| Need Parquet metadata before reading | `pq.read_schema()` / `pq.read_metadata()` | Direct footer access |
| Production ETL with filtering | `pq.ParquetDataset` | Predicate pushdown + partition pruning |
| Ad-hoc SQL-like query on Parquet | DuckDB (not PyArrow) | Best query optimization |

---

### 2.6 Performance Comparison

#### Reading 1 GB Parquet File (10 columns, 10M rows)

| Method | Read Time | Memory Usage | Notes |
|--------|-----------|-------------|-------|
| `pd.read_parquet()` | ~4.2s | ~1.2 GB (pandas) | Must convert to pandas |
| `pq.read_table()` | ~1.8s | ~0.8 GB (Arrow) | Columnar format, no conversion |
| `pq.read_table()` + `.to_pandas()` | ~2.5s | ~1.2 GB | Arrow → pandas conversion cost |
| `pd.read_parquet(columns=["a","b"])` | ~1.1s | ~0.3 GB | Column pruning + pandas |
| `pq.read_table(columns=["a","b"])` | ~0.6s | ~0.2 GB | Column pruning, Arrow format |
| `pq.ParquetDataset` + filters | ~0.3s | ~0.1 GB | Predicate pushdown + pruning |

> **Key insight:** `pd.read_parquet()` always pays the pandas conversion cost. For large datasets, start with `pq.read_table()` and convert only when needed.

#### The Hidden Cost of `.to_pandas()`

```python
import pyarrow.parquet as pq
import pandas as pd
import time

# Write a large test file
import pyarrow as pa
import numpy as np

table = pa.table({
    "id": np.arange(10_000_000),
    "value": np.random.randn(10_000_000),
    "category": np.random.choice(["A", "B", "C", "D"], 10_000_000),
})
pq.write_table(table, "benchmark.parquet")

# Method 1: pd.read_parquet (pandas all the way)
start = time.time()
df1 = pd.read_parquet("benchmark.parquet")
print(f"pd.read_parquet: {time.time()-start:.2f}s, {df1.memory_usage(deep=True).sum()/(1024**2):.1f} MB")
del df1

# Method 2: pq.read_table (Arrow, no pandas)
start = time.time()
table2 = pq.read_table("benchmark.parquet")
print(f"pq.read_table:  {time.time()-start:.2f}s, {table2.nbytes/(1024**2):.1f} MB")

# Method 3: pq.read_table + lazy to_pandas
start = time.time()
table3 = pq.read_table("benchmark.parquet")
df3 = table3.to_pandas()  # conversion happens here
print(f"pq.read_table + to_pandas: {time.time()-start:.2f}s")
del df3

# Method 4: pd.read_parquet with column pruning
start = time.time()
df4 = pd.read_parquet("benchmark.parquet", columns=["id", "value"])
print(f"pd.read_parquet (2 cols): {time.time()-start:.2f}s, {df4.memory_usage(deep=True).sum()/(1024**2):.1f} MB")
```

**Typical output:**
```
pd.read_parquet:           3.82s, 954.2 MB
pq.read_table:            1.45s, 534.1 MB
pq.read_table + to_pandas: 2.31s
pd.read_parquet (2 cols):  1.68s, 268.3 MB
```

---

### 2.7 Common Pitfalls When Choosing an Interface

1. **Using `pd.read_parquet()` for large datasets**: If your data is >1 GB, prefer `pq.read_table()` to avoid the pandas overhead. Arrow tables are ~40% more memory-efficient than pandas DataFrames.

2. **Not using column pruning**: Whether you use `pd.read_parquet()` or `pq.read_table()`, always pass `columns=[...]` for the columns you actually need.

3. **Using `pq.read_table()` on partitioned directories**: It won't discover partitions. Use `pq.ParquetDataset` instead.

4. **Ignoring `use_legacy_dataset=False`**: The legacy `ParquetDataset` has fewer features. Always set `use_legacy_dataset=False` for new code.

5. **Converting to pandas too early**: If you're doing multiple operations on the data, stay in Arrow format as long as possible and only convert to pandas at the end.

---

## 3. Engine Comparison: PyArrow vs Fastparquet

### Overview

`pd.read_parquet()` accepts an `engine` parameter that determines which library reads the Parquet file. The two main options are:

| Engine | Library | Language | Maintained By |
|--------|---------|----------|---------------|
| `"pyarrow"` | `pyarrow.parquet` | C++ (with Python bindings) | Apache Arrow project |
| `"fastparquet"` | `fastparquet` | Python (with numba/Cython) | Dask community |

```python
# Both produce the same DataFrame — different engine under the hood
df_pyarrow = pd.read_parquet("data.parquet", engine="pyarrow")
df_fastparquet = pd.read_parquet("data.parquet", engine="fastparquet")
```

---

### 3.1 PyArrow Engine

**Installation:**
```bash
pip install pyarrow
```

**How it works:**
- Written in C++ with Python bindings via Cython
- Reads Parquet directly into Apache Arrow's columnar memory format
- Converts Arrow Table → pandas DataFrame when called via `pd.read_parquet()`
- Supports all Parquet features: predicate pushdown, column pruning, partition discovery

**Syntax:**
```python
import pyarrow.parquet as pq
import pandas as pd

# Via pandas
df = pd.read_parquet("data.parquet", engine="pyarrow")

# Direct PyArrow (bypasses pandas entirely)
table = pq.read_table("data.parquet")
```

**Key strengths:**
- Fastest Parquet reader available
- Native support for S3, GCS, HDFS
- Arrow columnar format is memory-efficient
- Active development, large community
- Supports `Dataset` API for partitioned data

---

### 3.2 Fastparquet Engine

**Installation:**
```bash
pip install fastparquet
```

**How it works:**
- Pure Python implementation with optional numba/Cython acceleration
- Reads Parquet files directly into pandas DataFrames (no intermediate Arrow format)
- Originally built for the Dask ecosystem
- Uses `thrift` for metadata parsing

**Syntax:**
```python
import pandas as pd

# Via pandas
df = pd.read_parquet("data.parquet", engine="fastparquet")

# Direct fastparquet API
from fastparquet import ParquetFile
pf = ParquetFile("data.parquet")
df = pf.to_pandas()

# With filters
df = pf.to_pandas(filters=[("status", "=", "COMPLETED")])

# With columns
df = pf.to_pandas(columns=["amount", "date"])
```

**Key strengths:**
- Simpler API — reads directly into pandas (no Arrow step)
- Good integration with Dask for distributed computing
- Lightweight dependency (no C++ compilation)
- Supports partitioned directory reading
- Automatic row-group splitting for large files

---

### 3.3 Feature Comparison

| Feature | PyArrow | Fastparquet |
|---------|---------|------------|
| **Language** | C++ core, Python bindings | Pure Python + numba/Cython |
| **Read speed** | ✅ Fastest (~2x faster) | ⚠️ Slower (~1x baseline) |
| **Memory efficiency** | ✅ Arrow columnar format | ⚠️ pandas row-based |
| **Dependency size** | ⚠️ Large (C++ libraries) | ✅ Lightweight |
| **Installation** | ⚠️ May need system deps | ✅ `pip install` only |
| **Column pruning** | ✅ Yes | ✅ Yes |
| **Predicate pushdown** | ✅ Yes (full support) | ⚠️ Partial support |
| **Partition discovery** | ✅ Yes (Dataset API) | ✅ Yes (`.ParquetFile`)
| **S3/GCS/HDFS** | ✅ Native support | ⚠️ Via fsspec |
| **Dask integration** | ✅ Via dask-expr | ✅ Native (original) |
| **Schema evolution** | ✅ Full support | ⚠️ Limited support |
| **Dictionary encoding** | ✅ Full support | ✅ Full support |
| **Nested types** | ✅ Full support | ⚠️ Limited support |
| **Maintenance** | ✅ Very active | ⚠️ Slower development |
| **Community** | ✅ Large (Apache project) | ⚠️ Smaller (Dask community) |
| **Default in pandas** | ✅ Yes (pandas 2.0+) | ⚠️ No (was default before 2.0) |

---

### 3.4 Advantages & Disadvantages

#### PyArrow

| Advantages | Disadvantages |
|------------|---------------|
| ✅ Fastest Parquet reader in Python | ❌ Large dependency (C++ libraries) |
| ✅ Arrow format is memory-efficient | ❌ Installation may require system packages |
| ✅ Full Parquet feature support | ❌ Overkill if you only need pandas DataFrames |
| ✅ Native cloud storage (S3, GCS) | ❌ Two-step: Parquet → Arrow → pandas |
| ✅ Active development, Apache project | ❌ Steeper learning curve for Arrow API |
| ✅ Supports batch/streaming reads | |
| ✅ Better for nested/complex types | |

#### Fastparquet

| Advantages | Disadvantages |
|------------|---------------|
| ✅ Pure Python — easy to install | ❌ Slower than PyArrow (~2x) |
| ✅ Direct pandas integration | ❌ No Arrow intermediate format |
| ✅ Lightweight dependency | ❌ Limited predicate pushdown |
| ✅ Great Dask integration | ❌ Less active maintenance |
| ✅ Simpler API for basic reads | ❌ Limited nested type support |
| ✅ Good for environments with C++ restrictions | ❌ Not the default engine in modern pandas |
| | ❌ Slower for large datasets |

---

### 3.5 Performance Benchmark

#### Reading 1 GB Parquet File (10 columns, 10M rows)

| Method | Read Time | Memory Usage | Notes |
|--------|-----------|-------------|-------|
| `pd.read_parquet(engine="pyarrow")` | ~3.8s | ~950 MB | Fastest, Arrow-optimized |
| `pd.read_parquet(engine="fastparquet")` | ~7.2s | ~1.1 GB | Direct pandas, no Arrow |
| `pq.read_table()` (Arrow) | ~1.4s | ~530 MB | No pandas overhead |
| `fastparquet.ParquetFile.to_pandas()` | ~6.8s | ~1.0 GB | Direct fastparquet API |
| `pq.read_table()` + `.to_pandas()` | ~2.3s | ~1.1 GB | Arrow → pandas conversion |

> **Key insight:** PyArrow is consistently 2x faster than fastparquet for single-file reads. The gap widens with larger files and more columns.

#### The Install vs Speed Trade-off

```bash
# PyArrow — large but fast
pip install pyarrow        # ~200 MB installed

# Fastparquet — small but slower
pip install fastparquet    # ~15 MB installed
```

**When fastparquet wins:**
- Environments where C++ compilation fails (some Docker images, restricted servers)
- Quick prototyping where install speed matters more than read speed
- Dask-first workflows where fastparquet is the native engine

---

### 3.6 When to Use Which Engine?

| Scenario | Recommended Engine | Why |
|----------|-------------------|-----|
| Production data pipelines | PyArrow | Fastest, most reliable |
| Large datasets (>1 GB) | PyArrow | Memory-efficient Arrow format |
| Dask distributed computing | Fastparquet | Native Dask integration |
| Quick prototyping / notebooks | Either (PyArrow default) | Both work, PyArrow is faster |
| Restricted Docker environments | Fastparquet | Lighter dependency |
| Cloud storage (S3/GCS) | PyArrow | Native filesystem support |
| Nested/complex Parquet types | PyArrow | Full nested type support |
| Schema evolution scenarios | PyArrow | Better schema handling |
| pandas < 2.0 legacy code | Fastparquet | Was the default engine |

---

### 3.7 Migration Guide: Fastparquet → PyArrow

If you're migrating from fastparquet to PyArrow:

```python
# BEFORE (fastparquet)
import pandas as pd
df = pd.read_parquet("data.parquet", engine="fastparquet")

# AFTER (pyarrow) — same syntax, different engine
df = pd.read_parquet("data.parquet", engine="pyarrow")

# Or even simpler (pandas 2.0+ defaults to pyarrow)
df = pd.read_parquet("data.parquet")
```

**Key differences to watch for:**

| Fastparquet | PyArrow | Notes |
|-------------|---------|-------|
| `engine="fastparquet"` | `engine="pyarrow"` | Just change the engine string |
| Date types may differ | Stricter type handling | PyArrow enforces types more strictly |
| `pf.to_pandas(categories=[...])` | `pd.read_parquet(..., categories=[...])` | Categorical columns handled differently |
| `ParquetFile` API | `pq.ParquetFile` / `pq.ParquetDataset` | Different API names |

---

### 3.8 Common Pitfalls When Choosing an Engine

1. **Assuming both engines produce identical results**: While the data is the same, data types may differ (e.g., date handling, categorical columns). Always verify types after switching engines.

2. **Using fastparquet for large datasets**: PyArrow's Arrow format uses ~40% less memory than fastparquet's direct pandas approach. For datasets >1 GB, always use PyArrow.

3. **Ignoring the engine default**: pandas 2.0+ defaults to PyArrow. If you're on pandas <2.0, you may be using fastparquet without realizing it. Explicitly set `engine="pyarrow"` for consistency.

4. **Not installing PyArrow**: Many modern data tools (DuckDB, Polars, Vaex) depend on PyArrow. Installing PyArrow gives you compatibility with the entire data ecosystem.

5. **Using fastparquet for Dask workflows**: If you're using Dask, fastparquet was the traditional choice. However, modern Dask (dask-expr) works best with PyArrow.

---

## 4. Modern Alternative: Polars for Reading Parquet

### Overview

Polars is a high-performance DataFrame library written in Rust. It has become a popular alternative to pandas for reading Parquet files due to its speed, lazy evaluation, and memory efficiency.

```bash
pip install polars
```

**Why Polars matters for Parquet reading:**
- Written in Rust — no GIL, true multi-threaded parallelism
- Lazy evaluation allows query optimization before execution
- Native Parquet support with predicate pushdown and column pruning
- Can read Parquet without pandas or PyArrow as an intermediate step
- Memory-mapped I/O for datasets larger than RAM

---

### 4.1 Reading Parquet with Polars

#### Eager API (Immediate Execution)

```python
import polars as pl

# Read entire file
df = pl.read_parquet("transactions.parquet")

# Read with column selection
df = pl.read_parquet("transactions.parquet", columns=["amount", "date"])

# Read with row selection (first N rows)
df = pl.read_parquet("transactions.parquet", n_rows=1000)

# Read from S3
df = pl.read_parquet("s3://bucket/transactions.parquet")

# Read from glob pattern (multiple files)
df = pl.read_parquet("data/part-*.parquet")
```

#### Lazy API (Optimized Execution) — Recommended

```python
import polars as pl

# Lazy scan — builds a query plan, executes only when needed
df = pl.scan_parquet("transactions.parquet")

# Apply filters (predicate pushdown)
df_filtered = df.filter(
    (pl.col("date") >= "2026-08-01") &
    (pl.col("amount") > 1000)
)

# Select columns (column pruning)
df_selected = df_filtered.select(["amount", "date", "merchant"])

# Aggregate
df_result = df_selected.group_by("merchantgg(
    total=pl.col("amount").sum(),
    count=pl.col("amount").count()
)

# Execute — query plan runs here
result = df_result.collect()
```

**Why lazy is better:**
- Polars optimizes the entire query plan (filter reordering, predicate pushdown, projection pushdown)
- Only reads the columns and row groups it needs
- Can process datasets larger than RAM via streaming

---

### 4.2 Reading Partitioned Datasets

```python
import polars as pl

# Read entire directory with hive partitioning
df = pl.read_parquet("data/", hive_partitioning=True)

# Lazy scan of partitioned data
df = pl.scan_parquet("data/", hive_partitioning=True)

# Filter triggers partition pruning (skips entire directories)
result = df.filter(pl.col("date") >= "2026-08-01").collect()
```

---

### 4.3 Polars vs PyArrow vs pandas: Feature Comparison

| Feature | Polars | PyArrow | pandas |
|---------|--------|---------|--------|
| **Language** | Rust | C++ | C/Python |
| **Parallelism** | ✅ Multi-threaded (no GIL) | ⚠️ Multi-threaded (limited) | ❌ Single-threaded (GIL) |
| **Lazy evaluation** | ✅ Yes (query optimization) | ❌ No | ❌ No |
| **Memory efficiency** | ✅ Best (arrow + streaming) | ✅ Good (arrow format) | ❌ Worst (row-based) |
| **Predicate pushdown** | ✅ Yes (lazy + eager) | ✅ Yes | ⚠️ Only via engine |
| **Column pruning** | ✅ Yes (automatic in lazy) | ✅ Yes (manual) | ⚠️ Only via engine |
| **Partition discovery** | ✅ Yes (hive_partitioning) | ✅ Yes (Dataset API) | ⚠️ Limited |
| **Streaming reads** | ✅ Native streaming | ⚠️ Batch reader | ❌ No |
| **Larger-than-RAM** | ✅ Yes (lazy + streaming) | ⚠️ Manual batching | ❌ No |
| **Cloud storage** | ✅ S3, GCS, Azure | ✅ S3, GCS, HDFS | ⚠️ Via fsspec |
| **API style** | Method chaining | Functional | Mixed |
| **Learning curve** | ⚠️ Medium (new concepts) | ⚠️ Medium | ✅ Low (familiar) |
| **Ecosystem** | Growing rapidly | ✅ Mature (Apache) | ✅ Mature |
| **pandas compat** | ⚠️ Different API | ✅ `.to_pandas()` | ✅ Native |

---

### 4.4 Advantages & Disadvantages

#### Polars

| Advantages | Disadvantages |
|------------|---------------|
| ✅ Fastest DataFrame library (Rust core) | ❌ Different API from pandas (learning curve) |
| ✅ Lazy evaluation with query optimization | ❌ Smaller ecosystem than pandas |
| ✅ True multi-threaded parallelism (no GIL) | ❌ Some pandas methods not yet implemented |
| ✅ Streaming for larger-than-RAM datasets | ❌ Newer library (less Stack Overflow answers) |
| ✅ Native Parquet with full pushdown | ❌ Community smaller than pandas/PyArrow |
| ✅ Memory-mapped I/O | ❌ Some advanced features still experimental |
| ✅ No dependency on pandas or PyArrow | |
| ✅ Expression API is powerful and composable | |

#### PyArrow

| Advantages | Disadvantages |
|------------|---------------|
| ✅ Mature, battle-tested (Apache project) | ❌ No lazy evaluation |
| ✅ Largest ecosystem integration | ❌ No query optimization |
| ✅ Direct `.to_pandas()` conversion | ❌ Manual column/filter management |
| ✅ Foundation for DuckDB, Polars, Spark | ❌ No streaming for larger-than-RAM |
| ✅ Full Parquet feature support | |
| ✅ Best for data pipelines (Arrow format) | |

#### pandas

| Advantages | Disadvantages |
|------------|---------------|
| ✅ Most familiar API for data scientists | ❌ Single-threaded (GIL) |
| ✅ Largest library ecosystem | ❌ Memory-inefficient (row-based) |
| ✅ Extensive documentation and tutorials | ❌ No lazy evaluation |
| ✅ Every library integrates with it | ❌ No native Parquet (requires engine) |
| ✅ Best for prototyping and exploration | ❌ Cannot handle larger-than-RAM |

---

### 4.5 Performance Comparison

#### Reading 1 GB Parquet File (10 columns, 10M rows)

| Method | Read Time | Memory Usage | Notes |
|--------|-----------|-------------|-------|
| `pl.read_parquet()` | ~1.2s | ~530 MB | Fastest, Rust core |
| `pl.scan_parquet().collect()` | ~0.9s | ~530 MB | Lazy optimization |
| `pq.read_table()` | ~1.8s | ~800 MB | Arrow format |
| `pq.read_table()` + `.to_pandas()` | ~2.5s | ~1.2 GB | Arrow → pandas |
| `pd.read_parquet(engine="pyarrow")` | ~3.8s | ~950 MB | pandas conversion |
| `pd.read_parquet(engine="fastparquet")` | ~7.2s | ~1.1 GB | Slowest |

#### Lazy Evaluation: The Game Changer

```python
import polars as pl
import time

# Write test data
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

table = pa.table({
    "id": np.arange(10_000_000),
    "value": np.random.randn(10_000_000),
    "category": np.random.choice(["A", "B", "C", "D"], 10_000_000),
    "date": np.random.choice(pd.date_range("2026-01-01", periods=365), 10_000_000),
})
pq.write_table(table, "benchmark.parquet")

# Eager: reads everything, then filters
t0 = time.time()
df_eager = pl.read_parquet("benchmark.parquet")
df_eager.filter((pl.col("category") == "A") & (pl.col("value") > 0))
print(f"Eager: {time.time()-t0:.2f}s")

# Lazy: optimizes plan, reads only what's needed
t0 = time.time()
df_lazy = (
    pl.scan_parquet("benchmark.parquet")
    .filter((pl.col("category") == "A") & (pl.col("value") > 0))
    .select(["id", "value"])
    .collect()
)
print(f"Lazy:  {time.time()-t0:.2f}s")
```

**Typical output:**
```
Eager: 1.34s
Lazy:  0.42s   ← 3x faster (reads fewer columns + skips row groups)
```

---

### 4.6 When to Use Polars vs PyArrow vs pandas

| Scenario | Best Choice | Why |
|----------|-------------|-----|
| Quick EDA in notebooks | pandas | Familiar API, largest ecosystem |
| Data pipelines (Parquet → Parquet) | Polars or PyArrow | Fast, efficient |
| Large datasets (>10 GB) | Polars (lazy) | Streaming, query optimization |
| Production ETL with filtering | Polars (lazy) | Automatic predicate pushdown |
| Converting to pandas for ML | PyArrow | `.to_pandas()` is mature |
| Dask distributed computing | PyArrow | Native Dask integration |
| Real-time dashboards | Polars | Fastest read times |
| Legacy pandas codebases | pandas + PyArrow engine | Minimal code changes |
| Memory-constrained environments | Polars (lazy + streaming) | Processes data in chunks |
| SQL-like analytics on Parquet | DuckDB | Best query optimization |

---

### 4.7 Migration Guide: pandas → Polars

```python
# BEFORE (pandas)
import pandas as pd
df = pd.read_parquet("data.parquet")
df[df["amount"] > 1000].groupby("category")["amount"].sum()

# AFTER (Polars — eager)
import polars as pl
df = pl.read_parquet("data.parquet")
df.filter(pl.col("amount") > 1000).group_by("category").agg(pl.col("amount").sum())

# AFTER (Polars — lazy, recommended)
result = (
    pl.scan_parquet("data.parquet")
    .filter(pl.col("amount") > 1000)
    .group_by("category")
    .agg(pl.col("amount").sum())
    .collect()
)
```

**Key API differences:**

| pandas | Polars |
|--------|--------|
| `df[df["col"] > 100]` | `df.filter(pl.col("col") > 100)` |
| `df.groupby("col").sum()` | `df.group_by("col").agg(pl.col("*").sum())` |
| `df["col"].mean()` | `df["col"].mean()` (same!) |
| `df.sort_values("col")` | `df.sort("col")` |
| `df.rename({"old": "new"})` | `df.rename({"old": "new"})` (same!) |
| `df.merge(other, on="key")` | `df.join(other, on="key")` |
| `df.isnull()` | `df.is_null()` |
| `df.fillna(0)` | `df.fill_null(0)` |

---

### 4.8 Common Pitfalls When Using Polars

1. **Not using lazy evaluation**: Always prefer `scan_parquet()` + `.collect()` over `read_parquet()` for production workloads. Lazy mode enables query optimization.

2. **Forgetting `.collect()`**: Lazy operations return a plan, not data. You must call `.collect()` to execute the plan and get results.

3. **Mixing pandas and Polars**: Converting between pandas and Polars has overhead. Stay in one ecosystem for a given pipeline.

4. **Assuming pandas syntax works**: Polars uses method chaining with expressions, not bracket notation. `df["col"]` returns a Series, but filtering uses `df.filter()`.

5. **Ignoring streaming for large data**: For datasets larger than RAM, use `streaming=True` in `.collect()` to process data in chunks.

6. **Not installing Polars with all features**: Use `pip install polars[all]` for full cloud storage and performance support.

---

## 5. Example

### Reading with Column Pruning

```python
import pyarrow.parquet as pq

# Write sample data
import pyarrow as pa
table = pa.table({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "amount": [100.0, 250.0, 500.0, 75.0, 1000.0],
    "status": ["COMPLETED", "PENDING", "COMPLETED", "FAILED", "COMPLETED"],
})
pq.write_table(table, "sample.parquet")

# Read ALL columns (slow)
full = pq.read_table("sample.parquet")
print(f"Full read: {full.num_columns} columns, {full.num_rows} rows")

# Read only 2 columns (fast)
partial = pq.read_table("sample.parquet", columns=["amount", "status"])
print(f"Partial read: {partial.num_columns} columns, {partial.num_rows} rows")

# Read with filter (fastest)
filtered = pq.read_table(
    "sample.parquet",
    columns=["amount"],
    filters=[("status", "=", "COMPLETED")]
)
print(f"Filtered read: {filtered.num_rows} rows (only COMPLETED)")
print(filtered.to_pandas())
```

---

## 6. Real-World Scenario: Reading Interface Comparison

### Problem
A data engineering team manages a **fraud detection pipeline**. Every hour, new transaction data lands as Parquet files. Different teams consume this data differently:

- **ML Team**: Needs full DataFrames for model training (pandas/scikit-learn)
- **Analytics Team**: Needs aggregated metrics via SQL (DuckDB)
- **Data Pipeline Team**: Needs efficient format conversion (Parquet → Parquet transformations)
- **Monitoring Dashboard**: Needs fast, filtered reads for real-time alerts

Each team uses a different reading interface. This scenario compares their approaches and shows why choosing the right interface matters.

### Architecture
```
Transaction Stream (Kafka)
       |
       v
  Parquet Files (S3, hourly partitions)
       |
       +-- ML Team: pd.read_parquet() → Model Training
       |
       +-- Analytics Team: DuckDB SQL → Reports
       |
       +-- Pipeline Team: pq.read_table() → Transformations
       |
       +-- Dashboard: pq.ParquetDataset() → Real-time Alerts
```

### Python Code

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pandas as pd
import numpy as np
import duckdb
import time
import os
import tempfile
from datetime import datetime, timedelta
import random

# ============================================================
# READING INTERFACE COMPARISON: Fraud Detection Pipeline
# ============================================================

def generate_transaction_data(num_rows=500_000):
    """Generate realistic transaction data."""
    random.seed(42)
    np.random.seed(42)

    timestamps = np.array([
        datetime(2026, 8, 24) + timedelta(seconds=random.randint(0, 86400))
        for _ in range(num_rows)
    ], dtype='datetime64[us]')

    amounts = np.random.exponential(scale=150, size=num_rows).round(2)
    is_fraud = np.random.random(num_rows) < 0.02  # 2% fraud rate

    # Make fraud transactions have higher amounts
    amounts[is_fraud] *= np.random.uniform(5, 20, size=is_fraud.sum())

    table = pa.table({
        "transaction_id": pa.array(range(1, num_rows + 1), type=pa.int64()),
        "timestamp": pa.array(timestamps, type=pa.timestamp("us")),
        "amount": pa.array(amounts, type=pa.float64()),
        "currency": pa.array(np.random.choice(["USD", "EUR", "GBP"], num_rows), type=pa.string()),
        "merchant": pa.array(np.random.choice(
            ["Amazon", "Walmart", "Shell", "Starbucks", "Wire Transfer",
             "ATM", "Unknown Merchant"], num_rows
        ), type=pa.string()),
        "country": pa.array(np.random.choice(["US", "GB", "DE", "NG", "RU"], num_rows), type=pa.string()),
        "is_fraud": pa.array(is_fraud, type=pa.bool_()),
        "risk_score": pa.array(np.random.uniform(0, 1, num_rows).round(4), type=pa.float64()),
    })

    return table


def store_partitioned_data(table, base_path):
    """Store data partitioned by timestamp."""
    pq.write_to_dataset(
        table,
        root_path=base_path,
        partition_cols=["timestamp"],
        compression="snappy",
        use_dictionary=True,
        write_statistics=True,
    )
    print(f"Stored {table.num_rows:,} transactions")


# ============================================================
# TEAM 1: ML Team — pd.read_parquet()
# ============================================================

def ml_team_workflow(base_path):
    """ML team reads data for model training."""
    print("\n" + "="*60)
    print("TEAM 1: ML Team (pd.read_parquet)")
    print("="*60)

    start = time.time()

    # ML team wants a DataFrame — pd.read_parquet is simplest
    df = pd.read_parquet(base_path)

    # Feature engineering (pandas operations)
    df["hour"] = pd.to_datetime(df["timestamp"]).dt.hour
    df["is_high_amount"] = df["amount"] > df["amount"].quantile(0.95)
    df["is_high_risk_country"] = df["country"].isin(["NG", "RU"])

    # Train/test split
    features = ["amount", "hour", "is_high_amount", "is_high_risk_country", "risk_score"]
    X = df[features]
    y = df["is_fraud"]

    elapsed = time.time() - start

    print(f"  Read time: {elapsed:.3f}s")
    print(f"  Rows: {len(df):,}")
    print(f"  Memory: {df.memory_usage(deep=True).sum()/(1024**2):.1f} MB")
    print(f"  Fraud rate: {df['is_fraud'].mean()*100:.2f}%")
    print(f"  Features prepared: {features}")
    print(f"  Why pd.read_parquet? → Direct DataFrame for scikit-learn")


# ============================================================
# TEAM 2: Analytics Team — DuckDB SQL
# ============================================================

def analytics_team_workflow(base_path):
    """Analytics team queries data with SQL."""
    print("\n" + "="*60)
    print("TEAM 2: Analytics Team (DuckDB SQL)")
    print("="*60)

    con = duckdb.connect()

    start = time.time()

    # DuckDB reads Parquet directly — no need to load into memory first
    result = con.execute(f"""
        SELECT 
            country,
            currency,
            COUNT(*) as total_transactions,
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) as fraud_count,
            ROUND(SUM(CASE WHEN is_fraud THEN amount ELSE 0 END), 2) as fraud_amount,
            ROUND(AVG(amount), 2) as avg_amount,
            ROUND(AVG(risk_score), 4) as avg_risk_score
        FROM read_parquet('{base_path}/**/*.parquet', hive_partitioning=true)
        WHERE amount > 100
        GROUP BY country, currency
        ORDER BY fraud_amount DESC
    """).fetchdf()

    elapsed = time.time() - start

    print(f"  Query time: {elapsed:.3f}s")
    print(f"  Results:")
    print(result.to_string(index=False))
    print(f"  Why DuckDB? → SQL analytics, automatic optimization, no pandas overhead")
    con.close()


# ============================================================
# TEAM 3: Pipeline Team — pq.read_table()
# ============================================================

def pipeline_team_workflow(base_path, output_path):
    """Pipeline team transforms Parquet → Parquet efficiently."""
    print("\n" + "="*60)
    print("TEAM 3: Pipeline Team (pq.read_table)")
    print("="*60)

    start = time.time()

    # Pipeline team reads with Arrow, transforms, writes back as Parquet
    # No pandas conversion — stays in Arrow format throughout
    table = pq.read_table(
        base_path,
        columns=["transaction_id", "amount", "currency", "merchant", "is_fraud"],
        use_legacy_dataset=False,
    )

    # Transform using PyArrow compute (no pandas!)
    table = table.append_column(
        "amount_category",
        pc.if_else(
            pc.greater(table["amount"], 1000),
            "HIGH",
            pc.if_else(pc.greater(table["amount"], 100), "MEDIUM", "LOW")
        )
    )

    # Filter fraud only
    fraud_table = table.filter(pc.equal(table["is_fraud"], True))

    # Write output as Parquet
    pq.write_table(fraud_table, os.path.join(output_path, "fraud_transactions.parquet"))

    elapsed = time.time() - start

    print(f"  Pipeline time: {elapsed:.3f}s")
    print(f"  Input rows: {table.num_rows:,}")
    print(f"  Fraud rows: {fraud_table.num_rows:,}")
    print(f"  Columns: {table.column_names}")
    print(f"  Why pq.read_table? → Arrow-native transforms, no pandas overhead")


# ============================================================
# TEAM 4: Dashboard — pq.ParquetDataset
# ============================================================

def dashboard_team_workflow(base_path):
    """Dashboard reads filtered data for real-time alerts."""
    print("\n" + "="*60)
    print("TEAM 4: Dashboard Team (pq.ParquetDataset)")
    print("="*60)

    start = time.time()

    # Dashboard needs fast, filtered reads from partitioned data
    # ParquetDataset understands partitioning and prunes directories
    dataset = pq.ParquetDataset(
        base_path,
        filters=[
            ("is_fraud", "=", True),
            ("amount", ">", 500),
        ],
        use_legacy_dataset=False,
    )

    # Only read columns needed for the alert dashboard
    table = dataset.read(columns=[
        "transaction_id", "timestamp", "amount",
        "merchant", "country", "risk_score"
    ])

    df = table.to_pandas()
    elapsed = time.time() - start

    print(f"  Dashboard load: {elapsed:.3f}s")
    print(f"  High-value fraud alerts: {len(df):,}")
    if len(df) > 0:
        print(f"  Top merchants:")
        top_merchants = df.groupby("merchant").agg(
            count=("amount", "count"),
            total_amount=("amount", "sum")
        ).sort_values("total_amount", ascending=False).head(5)
        print(top_merchants.to_string())
    print(f"  Why ParquetDataset? → Partition pruning + predicate pushdown = fastest reads")


# ============================================================
# HEAD-TO-HEAD COMPARISON
# ============================================================

def compare_all_interfaces(base_path):
    """Run the same query through all four approaches."""
    print("\n" + "="*60)
    print("HEAD-TO-HEAD: Same Query, Different Interfaces")
    print("="*60)

    results = {}

    # 1. pd.read_parquet (full scan + pandas filter)
    start = time.time()
    df = pd.read_parquet(base_path)
    df[df["is_fraud"] & (df["amount"] > 500)]
    results["pd.read_parquet"] = time.time() - start
    del df

    # 2. pq.read_table (column pruning, no filter)
    start = time.time()
    table = pq.read_table(
        base_path,
        columns=["transaction_id", "amount", "is_fraud"],
        use_legacy_dataset=False,
    )
    table.filter(pc.and_(pc.equal(table["is_fraud"], True), pc.greater(table["amount"], 500)))
    results["pq.read_table"] = time.time() - start
    del table

    # 3. pq.read_table with filters (predicate pushdown)
    start = time.time()
    pq.read_table(
        base_path,
        columns=["transaction_id", "amount", "is_fraud"],
        filters=[("is_fraud", "=", True), ("amount", ">", 500)],
        use_legacy_dataset=False,
    )
    results["pq.read_table + filters"] = time.time() - start

    # 4. pq.ParquetDataset (partition pruning + predicates)
    start = time.time()
    dataset = pq.ParquetDataset(
        base_path,
        filters=[("is_fraud", "=", True), ("amount", ">", 500)],
        use_legacy_dataset=False,
    )
    dataset.read(columns=["transaction_id", "amount", "is_fraud"])
    results["pq.ParquetDataset"] = time.time() - start

    # 5. DuckDB
    start = time.time()
    con = duckdb.connect()
    con.execute(f"""
        SELECT transaction_id, amount
        FROM read_parquet('{base_path}/**/*.parquet', hive_partitioning=true)
        WHERE is_fraud = true AND amount > 500
    """).fetchall()
    con.close()
    results["DuckDB"] = time.time() - start

    print(f"\n  {'Interface':<30} {'Time (s)':<12} {'Speedup vs pd':<15}")
    print("  " + "-" * 57)
    baseline = results["pd.read_parquet"]
    for interface, elapsed in results.items():
        speedup = baseline / elapsed if elapsed > 0 else float('inf')
        print(f"  {interface:<30} {elapsed:<12.3f} {speedup:<15.1f}x")


# ============================================================
# RUN THE COMPARISON
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "fraud_data")
    output_path = os.path.join(tempfile.gettempdir(), "fraud_output")
    os.makedirs(base_path, exist_ok=True)
    os.makedirs(output_path, exist_ok=True)

    # Generate data
    print("Generating transaction data...")
    table = generate_transaction_data(num_rows=500_000)
    store_partitioned_data(table, base_path)

    # Run each team's workflow
    ml_team_workflow(base_path)
    analytics_team_workflow(base_path)
    pipeline_team_workflow(base_path, output_path)
    dashboard_team_workflow(base_path)

    # Head-to-head comparison
    compare_all_interfaces(base_path)
```

### Key Takeaways

| Interface | When to Use | When NOT to Use |
|-----------|------------|------------------|
| `pd.read_parquet()` | Quick analysis, notebooks, pandas workflows | Large datasets, production pipelines |
| `pq.read_table()` | Data pipelines, format conversion, Arrow workflows | Partitioned data, need directory discovery |
| `pq.ParquetDataset` | Partitioned data, production ETL, large-scale reads | Small single files |
| DuckDB | SQL analytics, ad-hoc queries, aggregations | When you need fine-grained control |

> **Rule of thumb:** If you're going to do pandas operations anyway, `pd.read_parquet()` is fine for small data. For anything over 1 GB, start with `pq.read_table()` and only convert to pandas when necessary.

---

## 7. Banking Scenario 1: Regulatory Audit Query

### Problem
An auditor needs to query **5 years of transaction data** (50 billion rows, 2 PB) to investigate a specific customer's transaction history. The query must:
- Return results in under 60 seconds
- Read only the customer's transactions
- Access only the 5 columns needed for the audit

### Why Reading Strategy Matters?
- Full scan: 2 PB → impossible in 60 seconds
- Column pruning: 20 columns → 5 columns = 4x reduction
- Predicate pushdown: 50B rows → ~100K rows = 500,000x reduction
- Combined: 2 PB → ~100 MB = 20,000x reduction

### Architecture
```
Auditor Workstation
       |
       v
  Audit Query Tool
       |
       v
  DuckDB / Trino
       |
       v
  Parquet Dataset (S3)
       |
       +-- Predicate pushdown (customer_id filter)
       +-- Column pruning (5 columns only)
       +-- Statistics-based row group skipping
       |
       v
  Result: <60 seconds
```

---

## 8. Python Code - Scenario 1

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
# BANKING SCENARIO: Regulatory Audit Query
# ============================================================

def generate_audit_dataset(num_customers=1000, txns_per_customer=1000):
    """Generate transaction data for audit scenario."""
    random.seed(42)
    np.random.seed(42)

    all_data = {
        "transaction_id": [],
        "customer_id": [],
        "account_id": [],
        "amount": [],
        "currency": [],
        "transaction_date": [],
        "merchant": [],
        "category": [],
        "channel": [],
        "status": [],
        "description": [],
        "reference": [],
        "branch_code": [],
        "country": [],
        "is_flagged": [],
    }

    tx_id = 1
    for cust_idx in range(num_customers):
        customer_id = f"CUST{cust_idx:06d}"
        num_accounts = random.randint(1, 4)

        for acc_idx in range(num_accounts):
            account_id = f"ACC{random.randint(100000, 999999)}"

            for _ in range(txns_per_customer):
                all_data["transaction_id"].append(tx_id)
                all_data["customer_id"].append(customer_id)
                all_data["account_id"].append(account_id)
                all_data["amount"].append(round(random.uniform(1.0, 100000.0), 2))
                all_data["currency"].append(random.choice(["USD", "EUR", "GBP"]))
                all_data["transaction_date"].append(
                    (datetime(2021, 1, 1) + timedelta(days=random.randint(0, 1800))).strftime("%Y-%m-%d")
                )
                all_data["merchant"].append(random.choice([
                    "Walmart", "Amazon", "Starbucks", "Shell", "Target",
                    "Wire Transfer", "ATM Withdrawal", "Direct Deposit"
                ]))
                all_data["category"].append(random.choice([
                    "RETAIL", "FOOD", "GAS", "TRANSFER", "ATM", "DEPOSIT", "UTILITIES"
                ]))
                all_data["channel"].append(random.choice(["ONLINE", "BRANCH", "ATM", "MOBILE", "POS"]))
                all_data["status"].append(random.choice(["COMPLETED"] * 95 + ["PENDING"] * 3 + ["FAILED"] * 2))
                all_data["description"].append(f"Transaction {tx_id}")
                all_data["reference"].append(f"REF{random.randint(100000, 999999)}")
                all_data["branch_code"].append(f"BR{random.randint(100, 999)}")
                all_data["country"].append(random.choice(["US", "GB", "DE", "FR", "JP"]))
                all_data["is_flagged"].append(random.random() < 0.01)

                tx_id += 1

    table = pa.table({
        "transaction_id": pa.array(all_data["transaction_id"], type=pa.int64()),
        "customer_id": pa.array(all_data["customer_id"], type=pa.string()),
        "account_id": pa.array(all_data["account_id"], type=pa.string()),
        "amount": pa.array(all_data["amount"], type=pa.float64()),
        "currency": pa.array(all_data["currency"], type=pa.string()),
        "transaction_date": pa.array(all_data["transaction_date"], type=pa.date32()),
        "merchant": pa.array(all_data["merchant"], type=pa.string()),
        "category": pa.array(all_data["category"], type=pa.string()),
        "channel": pa.array(all_data["channel"], type=pa.string()),
        "status": pa.array(all_data["status"], type=pa.string()),
        "description": pa.array(all_data["description"], type=pa.string()),
        "reference": pa.array(all_data["reference"], type=pa.string()),
        "branch_code": pa.array(all_data["branch_code"], type=pa.string()),
        "country": pa.array(all_data["country"], type=pa.string()),
        "is_flagged": pa.array(all_data["is_flagged"], type=pa.bool_()),
    })

    return table


def store_for_audit(table, base_path):
    """Store transaction data partitioned for audit queries."""
    pq.write_to_dataset(
        table,
        root_path=base_path,
        partition_cols=["transaction_date"],
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
    )

    total_size = 0
    for root, dirs, files in os.walk(base_path):
        for f in files:
            if f.endswith(".parquet"):
                total_size += os.path.getsize(os.path.join(root, f))

    print(f"Stored {table.num_rows:,} transactions")
    print(f"Total size: {total_size / (1024*1024):.1f} MB")


def audit_customer(base_path, customer_id, start_date, end_date):
    """Audit a specific customer with maximum optimization."""
    start_time = time.time()

    # Strategy 1: Column pruning (5 columns only)
    audit_columns = ["transaction_id", "amount", "merchant", "transaction_date", "status"]

    # Strategy 2: Predicate pushdown (customer_id + date range)
    filters = [
        ("customer_id", "=", customer_id),
        ("transaction_date", ">=", start_date),
        ("transaction_date", "<=", end_date),
    ]

    # Strategy 3: Use ParquetDataset for multi-file reading
    dataset = pq.ParquetDataset(
        base_path,
        filters=filters,
        use_legacy_dataset=False,
    )

    table = dataset.read(columns=audit_columns)
    elapsed = time.time() - start_time

    df = table.to_pandas()

    print(f"\n=== Audit Report: {customer_id} ===")
    print(f"Period: {start_date} to {end_date}")
    print(f"Query time: {elapsed:.3f}s")
    print(f"Transactions found: {len(df):,}")
    print(f"Columns read: {len(audit_columns)} out of 15")

    if len(df) > 0:
        print(f"\nTotal amount: ${df['amount'].sum():,.2f}")
        print(f"Average amount: ${df['amount'].mean():,.2f}")
        print(f"Status distribution:")
        print(df["status"].value_counts().to_string())

        # Show flagged transactions
        flagged = df[df["status"] == "FAILED"]
        if len(flagged) > 0:
            print(f"\nFailed transactions:")
            print(flagged.head(10).to_string())

    return df


def compare_read_strategies(base_path, customer_id):
    """Compare different reading strategies."""
    results = {}

    # Strategy 1: Full table read (no optimization)
    start = time.time()
    full_table = pq.read_table(base_path, use_legacy_dataset=False)
    results["full_scan"] = time.time() - start

    # Strategy 2: Column pruning only
    start = time.time()
    pq.read_table(
        base_path,
        columns=["transaction_id", "amount", "merchant", "transaction_date", "status"],
        use_legacy_dataset=False,
    )
    results["column_pruning"] = time.time() - start

    # Strategy 3: Predicate pushdown only
    start = time.time()
    pq.read_table(
        base_path,
        filters=[("customer_id", "=", customer_id)],
        use_legacy_dataset=False,
    )
    results["predicate_pushdown"] = time.time() - start

    # Strategy 4: Both (optimal)
    start = time.time()
    dataset = pq.ParquetDataset(
        base_path,
        filters=[("customer_id", "=", customer_id)],
        use_legacy_dataset=False,
    )
    dataset.read(columns=["transaction_id", "amount", "merchant", "transaction_date", "status"])
    results["both"] = time.time() - start

    print(f"\n=== Read Strategy Comparison ===")
    print(f"{'Strategy':<25} {'Time (s)':<12}")
    print("-" * 37)
    for strategy, elapsed in results.items():
        print(f"{strategy:<25} {elapsed:<12.3f}")

    speedup = results["full_scan"] / results["both"]
    print(f"\nOptimal speedup: {speedup:.1f}x faster than full scan")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "audit_data")
    os.makedirs(base_path, exist_ok=True)

    # Generate audit dataset
    print("Generating audit dataset...")
    table = generate_audit_dataset(num_customers=100, txns_per_customer=100)
    store_for_audit(table, base_path)

    # Audit specific customer
    audit_customer(
        base_path,
        customer_id="CUST000042",
        start_date="2024-01-01",
        end_date="2025-12-31",
    )

    # Compare read strategies
    compare_read_strategies(base_path, "CUST000042")
```

---

## 9. Banking Scenario 2: Real-Time Risk Dashboard

### Problem
A risk manager needs a dashboard that shows:
- Total exposure by currency (refreshed every 5 minutes)
- Top 10 counterparties by exposure
- Concentration risk by sector
- Intraday P&L

The underlying data is **100 GB of Parquet files** updated every minute by streaming jobs.

### Why Reading Strategy Matters?
- Dashboard must load in <5 seconds
- Only 5-10 columns needed for display
- Filters by date (today only) and status (active trades)
- Must handle concurrent reads while streaming writes

### Architecture
```
Streaming Writer (Flink)
       |
       v
  Parquet Files (S3, updated every minute)
       |
       v
  DuckDB (embedded, reads Parquet directly)
       |
       v
  Risk Dashboard (Streamlit / Grafana)
       |
       v
  Risk Manager
```

---

## 10. Python Code - Scenario 2

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
import pandas as pd

# ============================================================
# BANKING SCENARIO: Real-Time Risk Dashboard
# ============================================================

def generate_risk_data(num_trades=100_000):
    """Generate trading position data for risk dashboard."""
    random.seed(42)
    np.random.seed(42)

    currencies = ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD"]
    sectors = ["FINANCIAL", "TECHNOLOGY", "HEALTHCARE", "ENERGY", "CONSUMER", "INDUSTRIAL"]
    counterparties = [
        "Goldman Sachs", "JP Morgan", "Citibank", "Deutsche Bank",
        "Barclays", "HSBC", "UBS", "Morgan Stanley", "Bank of America", "BNP Paribas"
    ]
    trade_types = ["SPOT", "FORWARD", "SWAP", "OPTION"]

    # Generate timestamps throughout today
    today = datetime(2026, 8, 24)
    timestamps = np.array([
        today + timedelta(seconds=random.randint(0, 86400))
        for _ in range(num_trades)
    ], dtype='datetime64[us]')

    amounts = np.random.uniform(10000, 10000000, num_trades).round(2)
    pnl = np.random.normal(0, amounts * 0.02).round(2)  # 2% daily volatility

    table = pa.table({
        "trade_id": pa.array(list(range(1, num_trades + 1)), type=pa.int64()),
        "trade_timestamp": pa.array(timestamps, type=pa.timestamp("us")),
        "trade_type": pa.array(np.random.choice(trade_types, num_trades), type=pa.string()),
        "currency": pa.array(np.random.choice(currencies, num_trades), type=pa.string()),
        "sector": pa.array(np.random.choice(sectors, num_trades), type=pa.string()),
        "counterparty": pa.array(np.random.choice(counterparties, num_trades), type=pa.string()),
        "notional_amount": pa.array(amounts, type=pa.float64()),
        "market_value": pa.array((amounts + pnl), type=pa.float64()),
        "unrealized_pnl": pa.array(pnl, type=pa.float64()),
        "status": pa.array(np.random.choice(
            ["ACTIVE"] * 90 + ["SETTLED"] * 8 + ["PENDING"] * 2, num_trades
        ), type=pa.string()),
    })

    return table


def store_risk_data(table, base_path):
    """Store risk data with minute-level partitioning."""
    pq.write_to_dataset(
        table,
        root_path=base_path,
        partition_cols=["trade_timestamp"],
        compression="snappy",
        use_dictionary=True,
        write_statistics=True,
    )


def load_risk_dashboard(base_path):
    """Load risk dashboard data with optimized reads."""
    start_time = time.time()

    # Only read dashboard columns
    dashboard_columns = [
        "currency", "sector", "counterparty",
        "notional_amount", "market_value", "unrealized_pnl", "status"
    ]

    # Filter to active trades only
    filters = [("status", "=", "ACTIVE")]

    dataset = pq.ParquetDataset(
        base_path,
        filters=filters,
        use_legacy_dataset=False,
    )

    table = dataset.read(columns=dashboard_columns)
    load_time = time.time() - start_time

    df = table.to_pandas()

    print(f"\n=== Risk Dashboard ===")
    print(f"Load time: {load_time:.3f}s")
    print(f"Active trades: {len(df):,}")

    # 1. Exposure by currency
    print(f"\n--- Exposure by Currency ---")
    exposure = df.groupby("currency").agg(
        total_notional=("notional_amount", "sum"),
        total_mv=("market_value", "sum"),
        total_pnl=("unrealized_pnl", "sum"),
        trade_count=("notional_amount", "count"),
    ).round(2).sort_values("total_notional", ascending=False)
    print(exposure.to_string())

    # 2. Top counterparties
    print(f"\n--- Top 10 Counterparties by Exposure ---")
    cpty_exposure = df.groupby("counterparty").agg(
        total_notional=("notional_amount", "sum"),
        total_pnl=("unrealized_pnl", "sum"),
        trade_count=("notional_amount", "count"),
    ).round(2).sort_values("total_notional", ascending=False).head(10)
    print(cpty_exposure.to_string())

    # 3. Concentration risk
    print(f"\n--- Sector Concentration ---")
    sector_risk = df.groupby("sector").agg(
        total_notional=("notional_amount", "sum"),
        pct_of_total=("notional_amount", lambda x: f"{x.sum() / df['notional_amount'].sum() * 100:.1f}%"),
    ).sort_values("total_notional", ascending=False)
    print(sector_risk.to_string())

    # 4. P&L summary
    print(f"\n--- Intraday P&L ---")
    total_pnl = df["unrealized_pnl"].sum()
    print(f"Total unrealized P&L: ${total_pnl:,.2f}")
    print(f"Trades in profit: {(df['unrealized_pnl'] > 0).sum():,}")
    print(f"Trades in loss: {(df['unrealized_pnl'] < 0).sum():,}")

    return df


def monitor_dashboard_refresh(base_path, num_iterations=5):
    """Simulate dashboard refresh every 5 seconds."""
    print(f"\n=== Dashboard Refresh Monitor ({num_iterations} iterations) ===")

    refresh_times = []
    for i in range(num_iterations):
        start = time.time()

        dataset = pq.ParquetDataset(
            base_path,
            filters=[("status", "=", "ACTIVE")],
            use_legacy_dataset=False,
        )
        table = dataset.read(columns=["currency", "notional_amount", "unrealized_pnl"])

        elapsed = time.time() - start
        refresh_times.append(elapsed)

        df = table.to_pandas()
        print(f"  Refresh {i+1}: {elapsed:.3f}s | {len(df):,} trades | ${df['unrealized_pnl'].sum():,.0f} P&L")

    avg_refresh = sum(refresh_times) / len(refresh_times)
    print(f"\nAverage refresh time: {avg_refresh:.3f}s")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "risk_data")
    os.makedirs(base_path, exist_ok=True)

    # Generate and store risk data
    print("Generating risk data...")
    risk_data = generate_risk_data(num_trades=100_000)
    store_risk_data(risk_data, base_path)

    # Load risk dashboard
    load_risk_dashboard(base_path)

    # Monitor refresh performance
    monitor_dashboard_refresh(base_path)
```

---

## 11. Interview Questions

### Q1: What is the difference between `pq.read_table()` and `pq.ParquetDataset`?

**Answer:**

**`pq.read_table()`**:
- Reads a single Parquet file or a list of files
- Simple API, returns a Table
- Best for: single files, small datasets

```python
table = pq.read_table("file.parquet", columns=["a", "b"])
```

**`pq.ParquetDataset`**:
- Reads multiple Parquet files (partitioned directories)
- Supports predicate pushdown across files
- Can read schema without loading data
- Best for: partitioned datasets, large-scale data

```python
dataset = pq.ParquetDataset("s3://bucket/data/", filters=[...])
table = dataset.read(columns=["a", "b"])
```

**Key difference**: `ParquetDataset` understands partitioning and can skip entire directories based on filters, while `read_table` reads all specified files.

---

### Q2: How does predicate pushdown work with Parquet statistics?

**Answer:**
Parquet stores **min/max/count/null_count** statistics for each column in each row group and page.

When a filter is applied:
1. Check the row group statistics against the filter
2. If the filter cannot match any value in the row group → **skip entirely**
3. If it might match → read the column chunk
4. Optionally check page-level statistics for further pruning

**Example**:
```
Row Group 1: amount min=10, max=500
Row Group 2: amount min=1000, max=50000
Row Group 3: amount min=50, max=1000

Filter: amount > 500

Row Group 1: max=500 < 500 → SKIP
Row Group 2: min=1000 > 500 → READ (all values match)
Row Group 3: min=50, max=1000 → READ (some values match)
```

---

### Q3: What are the common pitfalls when reading Parquet files?

**Answer:**

1. **Reading all columns**: Always specify `columns=[]` for the columns you actually need.

2. **No filters**: Always add `filters=[]` when you have WHERE conditions.

3. **Small files problem**: Many small files (e.g., from streaming) cause overhead. Use compaction.

4. **Wrong compression**: Reading Gzip-compressed files is slower than Snappy. Choose compression based on read/write patterns.

5. **Loading entire file**: For very large files, use batch reading instead of loading everything into memory.

6. **Ignoring partitioning**: For partitioned datasets, use `ParquetDataset` instead of reading all files individually.

7. **Schema mismatch**: If files have different schemas (schema evolution), readers may fail or return unexpected results.

---

### Q4: How can you read Parquet files in parallel?

**Answer:**
Parquet's row group structure enables parallel reading:

**Within a single file:**
- Different row groups can be read by different threads
- Each row group is independent and self-contained

**Across multiple files:**
- Each file can be read by a different worker
- Partitioned directories enable independent reading

**PyArrow approach:**
```python
# Dataset reads can be parallelized
dataset = pq.ParquetDataset("path/", use_legacy_dataset=False)
table = dataset.read()  # PyArrow reads row groups in parallel internally
```

**DuckDB approach:**
```python
import duckdb
con = duckdb.connect()
# DuckDB automatically parallelizes Parquet reads
result = con.execute("""
    SELECT SUM(amount) 
    FROM read_parquet('s3://bucket/data/*.parquet')
    WHERE date >= '2026-08-01'
""").fetchall()
```

---

### Q5: What is the difference between reading Parquet with PyArrow vs DuckDB?

**Answer:**

| Feature | PyArrow | DuckDB |
|---------|---------|--------|
| API | Low-level, explicit | High-level, SQL |
| Parallelism | Manual (batch) | Automatic |
| Predicate pushdown | Yes | Yes (better optimization) |
| Memory management | Arrow memory pools | Automatic |
| Best for | Data pipelines, ETL | Ad-hoc queries, analytics |
| Pandas integration | Direct `.to_pandas()` | Via Arrow |

**PyArrow** is better when:
- Building data pipelines
- Need fine-grained control
- Converting between formats

**DuckDB** is better when:
- Running SQL queries on Parquet
- Need automatic optimization
- Want the fastest query performance
- Interactive data exploration

---

### Q6: When should you use `pd.read_parquet()` vs `pq.read_table()` vs `pq.ParquetDataset`?

**Answer:**

**`pd.read_parquet()`** — Use when:
- You need a pandas DataFrame immediately
- Doing quick EDA in Jupyter notebooks
- Working with small-to-medium datasets (<1 GB)
- Prototyping or exploring data

```python
# Quick analysis
df = pd.read_parquet("data.parquet")
df.groupby("category").sum()
```

**`pq.read_table()`** — Use when:
- Building data pipelines (Parquet → Parquet transformations)
- Need Arrow's columnar format for efficiency
- Converting between formats without pandas overhead
- Working with single files or known file lists

```python
# Efficient pipeline
table = pq.read_table("data.parquet", columns=["a", "b"])
table = table.filter(pc.greater(table["a"], 100))
pq.write_table(table, "output.parquet")
```

**`pq.ParquetDataset`** — Use when:
- Reading partitioned directories (hive-style partitioning)
- Need partition pruning (skip entire directories)
- Working with large-scale data on S3/GCS/HDFS
- Building production ETL jobs

```python
# Production ETL
dataset = pq.ParquetDataset(
    "s3://bucket/data/",
    filters=[("date", ">=", "2026-01-01")],
    use_legacy_dataset=False,
)
table = dataset.read(columns=["a", "b"])
```

**Key rule of thumb:** Start with `pd.read_parquet()` for exploration. Switch to `pq.read_table()` or `ParquetDataset` when performance matters.

---

### Q7: What is the performance difference between `pd.read_parquet()` and `pq.read_table()`?

**Answer:**

`pd.read_parquet()` calls `pq.read_table()` internally, then converts the Arrow Table to a pandas DataFrame. The conversion cost:

- **Time**: Arrow → pandas conversion adds ~30-50% overhead
- **Memory**: pandas uses ~40% more memory than Arrow (row-based vs columnar)
- **Zero-copy**: Arrow supports zero-copy to pandas, but pandas still allocates its own memory

**Benchmark (10M rows, 10 columns):**
```
pd.read_parquet:           ~4.2s, ~1.2 GB
pq.read_table:            ~1.8s, ~0.8 GB
pq.read_table + to_pandas: ~2.5s, ~1.2 GB
```

**Why the difference?**
- Arrow stores data in columnar format (cache-friendly)
- pandas stores data in row-based blocks (less cache-efficient)
- Converting between formats requires copying data

**Recommendation:** For datasets >1 GB, use `pq.read_table()` and only call `.to_pandas()` when you specifically need pandas functionality.

---

### Q8: What is the difference between PyArrow and Fastparquet as Parquet engines?

**Answer:**

| Aspect | PyArrow | Fastparquet |
|--------|---------|------------|
| **Language** | C++ core with Python bindings | Pure Python + numba/Cython |
| **Speed** | ~2x faster | Baseline |
| **Memory** | Arrow columnar (~40% less) | Direct pandas |
| **Dependencies** | Large (C++ libraries) | Lightweight |
| **Installation** | May need system packages | `pip install` only |
| **Predicate pushdown** | Full support | Partial support |
| **Cloud storage** | Native S3/GCS/HDFS | Via fsspec |
| **Default in pandas** | Yes (pandas 2.0+) | Was default before 2.0 |
| **Dask integration** | Good (dask-expr) | Native (original) |
| **Maintenance** | Very active (Apache) | Slower development |

**When to use PyArrow:**
- Production pipelines (speed matters)
- Large datasets (>1 GB)
- Cloud storage (S3, GCS)
- Nested/complex Parquet types
- Schema evolution scenarios

**When to use Fastparquet:**
- Restricted environments (no C++ compilation)
- Dask-first workflows
- Quick prototyping where install speed > read speed
- Legacy codebases using fastparquet

**Migration tip:** Changing engines is simple:
```python
# Just change the engine parameter
df = pd.read_parquet("data.parquet", engine="pyarrow")  # was fastparquet
# Or omit it (pandas 2.0+ defaults to pyarrow)
df = pd.read_parquet("data.parquet")
```

**Key gotcha:** Data types may differ between engines (e.g., date handling, categorical columns). Always verify types after switching.

---

### Q9: When should you use Polars instead of PyArrow or pandas for reading Parquet?

**Answer:**

| Factor | Polars | PyArrow | pandas |
|--------|--------|---------|--------|
| **Speed** | ✅ Fastest (Rust) | ✅ Fast (C++) | ❌ Slowest |
| **Lazy evaluation** | ✅ Yes | ❌ No | ❌ No |
| **Query optimization** | ✅ Automatic | ❌ Manual | ❌ No |
| **Larger-than-RAM** | ✅ Streaming | ⚠️ Manual batching | ❌ No |
| **Memory efficiency** | ✅ Best | ✅ Good | ❌ Poor |
| **API familiarity** | ⚠️ New | ⚠️ Medium | ✅ Most familiar |
| **Ecosystem** | ⚠️ Growing | ✅ Mature | ✅ Largest |

**Use Polars when:**
- Performance is critical (datasets >1 GB)
- You want automatic query optimization (lazy evaluation)
- Processing larger-than-RAM data (streaming)
- Building new pipelines from scratch (not migrating pandas)
- Real-time dashboards requiring fast reads

**Use PyArrow when:**
- Building data pipelines that need Arrow format
- Converting between formats (Parquet ↔ Arrow ↔ pandas)
- Working with Dask or Spark (Arrow is the foundation)
- Need mature, battle-tested library

**Use pandas when:**
- Quick prototyping and exploration
- Leveraging pandas-specific libraries (scikit-learn, statsmodels)
- Working in notebooks with small-to-medium data
- Team is already familiar with pandas API

**Key insight:** Polars is not a replacement for pandas — it's a different paradigm. For new projects, consider Polars. For existing pandas codebases, use PyArrow as the engine.

```python
# Quick decision tree
import polars as pl

# New project, performance matters
if new_project and performance_critical:
    df = pl.scan_parquet("data.parquet")
    # ... lazy operations ...
    result = df.collect()

# Existing pandas codebase
elif existing_pandas_code:
    import pandas as pd
    df = pd.read_parquet("data.parquet", engine="pyarrow")

# Quick exploration
else:
    import pandas as pd
    df = pd.read_parquet("data.parquet")
```
