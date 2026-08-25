# Parquet vs Other Data Formats

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-format-selection-for-data-lake)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-migration-from-csv-to-parquet)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### Format Comparison Overview

| Format | Type | Compression | Schema | Columnar | Splittable | Best For |
|--------|------|-------------|--------|----------|------------|----------|
| **CSV** | Text | None/Gzip | No | No | Yes | Simple data exchange |
| **JSON** | Text | None/Gzip | No | No | No | Semi-structured data |
| **Avro** | Binary | Deflate | Yes | No | Yes | Write-heavy, streaming |
| **ORC** | Binary | Zlib/Snappy | Yes | Yes | Yes | Hive ecosystem |
| **Parquet** | Binary | Snappy/Zstd | Yes | Yes | Yes | Analytics, data lakes |

### CSV (Comma-Separated Values)

**Structure:**
```
id,name,amount,date
1,Alice,100.00,2026-08-01
2,Bob,250.00,2026-08-01
```

**Pros:**
- Universal support (every tool reads CSV)
- Human-readable
- Simple to generate
- No special libraries needed

**Cons:**
- No schema (types inferred or guessed)
- No compression (unless gzipped)
- No column pruning (must read entire file)
- No predicate pushdown
- Large file sizes
- Type ambiguity (is "100" a string or number?)

**When to use:**
- Data exchange between incompatible systems
- Human-readable exports
- Small datasets (< 1 GB)
- Quick prototyping

### JSON (JavaScript Object Notation)

**Structure:**
```json
[
  {"id": 1, "name": "Alice", "amount": 100.00},
  {"id": 2, "name": "Bob", "amount": 250.00}
]
```

**Pros:**
- Supports nested structures
- Human-readable
- Widely supported
- Schema flexibility

**Cons:**
- Very verbose (repeated keys)
- No columnar layout
- No compression
- Parsing overhead
- Not splittable (must parse entire file)

**When to use:**
- API responses
- Configuration files
- Semi-structured data with nested structures
- Small datasets

### Avro (Apache Avro)

**Structure:**
```
Schema + Binary Data
```

**Pros:**
- Compact binary format
- Schema embedded
- Excellent for streaming (Kafka)
- Schema evolution support
- Row-based (fast reads/writes)

**Cons:**
- Not columnar (poor for analytics)
- Less tool support than Parquet
- Not ideal for analytical queries

**When to use:**
- Kafka message format
- Write-heavy workloads
- Streaming pipelines
- Schema evolution requirements

### ORC (Optimized Row Columnar)

**Structure:**
```
Similar to Parquet (columnar, row groups, stripes)
```

**Pros:**
- Excellent compression
- Built-in indexes
- Strong Hive integration
- ACID transactions (with Hive)

**Cons:**
- Mainly Hive ecosystem
- Less tool support than Parquet
- Not as widely adopted

**When to use:**
- Hive-based data warehouses
- Existing ORC infrastructure
- Hive ACID requirements

### Parquet

**Structure:**
```
Row Groups → Column Chunks → Pages
```

**Pros:**
- Columnar (analytics optimized)
- Excellent compression
- Predicate pushdown
- Schema evolution
- Widest tool support
- Cloud-native

**Cons:**
- Not ideal for single-row lookups
- Write-heavy workloads slower
- Schema changes can be complex

**When to use:**
- Data lakes
- Analytical queries
- Cloud storage (S3, GCS, ADLS)
- Large datasets (TB+)
- Multi-engine environments

---

## 2. Example

### File Size Comparison

```python
import pandas as pd
import numpy as np
import os
import time

# Create sample data
np.random.seed(42)
num_rows = 1_000_000
df = pd.DataFrame({
    "id": range(num_rows),
    "name": np.random.choice(["Alice", "Bob", "Charlie", "Diana"], num_rows),
    "amount": np.random.uniform(1.0, 10000.0, num_rows),
    "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
    "date": pd.date_range("2026-01-01", periods=num_rows, freq="s"),
})

# Write to different formats
formats = {}

# CSV
start = time.time()
df.to_csv("data.csv", index=False)
formats["CSV"] = {"size": os.path.getsize("data.csv"), "time": time.time() - start}

# CSV.GZ
start = time.time()
df.to_csv("data.csv.gz", index=False, compression="gzip")
formats["CSV.GZ"] = {"size": os.path.getsize("data.csv.gz"), "time": time.time() - start}

# Parquet (Snappy)
start = time.time()
df.to_parquet("data_snappy.parquet", engine="pyarrow", compression="snappy", index=False)
formats["Parquet_Snappy"] = {"size": os.path.getsize("data_snappy.parquet"), "time": time.time() - start}

# Parquet (Zstd)
start = time.time()
df.to_parquet("data_zstd.parquet", engine="pyarrow", compression="zstd", index=False)
formats["Parquet_Zstd"] = {"size": os.path.getsize("data_zstd.parquet"), "time": time.time() - start}

# Compare
print(f"\n=== Format Comparison ({num_rows:,} rows) ===")
print(f"{'Format':<20} {'Size (MB)':<12} {'Ratio':<10} {'Write Time (s)':<15}")
print("-" * 57)
csv_size = formats["CSV"]["size"]
for name, metrics in formats.items():
    ratio = csv_size / metrics["size"]
    print(f"{name:<20} {metrics['size']/1024/1024:<12.1f} {ratio:<10.1f}x {metrics['time']:<15.3f}")
```

---

## 3. Banking Scenario 1: Format Selection for Data Lake

### Problem
A bank is designing a data lake and must choose file formats for different data types:
- Transaction data (10 TB, analytical queries)
- Kafka events (real-time streaming)
- API responses (semi-structured JSON)
- Legacy exports (CSV from mainframe)

### Why Format Selection Matters?
- Wrong format = 10x more storage cost
- Wrong format = 100x slower queries
- Wrong format = incompatible tools

### Architecture
```
Data Sources
     |
     +-- Mainframe (CSV exports)
     |       → Convert to Parquet
     |
     +-- Kafka (streaming events)
     |       → Avro (in-flight) → Parquet (at rest)
     |
     +-- REST APIs (JSON responses)
     |       → Parse JSON → Parquet
     |
     +-- Oracle (transaction data)
             → ETL → Parquet
```

---

## 4. Python Code - Scenario 1

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import json
import os
import tempfile
import time

# ============================================================
# BANKING SCENARIO: Format Selection for Data Lake
# ============================================================

def generate_transaction_data(num_rows=100_000):
    """Generate transaction data in various formats."""
    np.random.seed(42)

    df = pd.DataFrame({
        "transaction_id": range(1, num_rows + 1),
        "account_id": [f"ACC{i:06d}" for i in range(1, num_rows + 1)],
        "amount": np.random.uniform(1.0, 100000.0, num_rows).round(2),
        "currency": np.random.choice(["USD", "EUR", "GBP"], num_rows),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
        "date": pd.date_range("2026-01-01", periods=num_rows, freq="30s"),
    })

    return df


def write_all_formats(df, output_dir):
    """Write data to all formats for comparison."""
    formats = {}

    # CSV
    path = os.path.join(output_dir, "transactions.csv")
    start = time.time()
    df.to_csv(path, index=False)
    formats["CSV"] = {"path": path, "size": os.path.getsize(path), "write_time": time.time() - start}

    # CSV.GZ
    path = os.path.join(output_dir, "transactions.csv.gz")
    start = time.time()
    df.to_csv(path, index=False, compression="gzip")
    formats["CSV.GZ"] = {"path": path, "size": os.path.getsize(path), "write_time": time.time() - start}

    # Parquet Snappy
    path = os.path.join(output_dir, "transactions_snappy.parquet")
    start = time.time()
    df.to_parquet(path, engine="pyarrow", compression="snappy", index=False)
    formats["Parquet_Snappy"] = {"path": path, "size": os.path.getsize(path), "write_time": time.time() - start}

    # Parquet Zstd
    path = os.path.join(output_dir, "transactions_zstd.parquet")
    start = time.time()
    df.to_parquet(path, engine="pyarrow", compression="zstd", index=False)
    formats["Parquet_Zstd"] = {"path": path, "size": os.path.getsize(path), "write_time": time.time() - start}

    # JSON
    path = os.path.join(output_dir, "transactions.json")
    start = time.time()
    df.to_json(path, orient="records", lines=True)
    formats["JSON"] = {"path": path, "size": os.path.getsize(path), "write_time": time.time() - start}

    # JSON.GZ
    path = os.path.join(output_dir, "transactions.json.gz")
    start = time.time()
    df.to_json(path, orient="records", lines=True, compression="gzip")
    formats["JSON.GZ"] = {"path": path, "size": os.path.getsize(path), "write_time": time.time() - start}

    return formats


def compare_read_performance(formats):
    """Compare read performance across formats."""
    results = {}

    for name, info in formats.items():
        path = info["path"]

        start = time.time()
        if "parquet" in name:
            df = pd.read_parquet(path, columns=["transaction_id", "amount", "status"])
        elif "csv" in name:
            df = pd.read_csv(path, usecols=["transaction_id", "amount", "status"])
        elif "json" in name:
            df = pd.read_json(path, lines=True)
            df = df[["transaction_id", "amount", "status"]]

        elapsed = time.time() - start
        results[name] = {"read_time": elapsed, "rows": len(df)}

    return results


def compare_filtered_reads(formats):
    """Compare filtered reads (amount > 50000)."""
    results = {}

    for name, info in formats.items():
        path = info["path"]

        start = time.time()
        if "parquet" in name:
            # Parquet supports predicate pushdown
            df = pd.read_parquet(
                path,
                columns=["transaction_id", "amount", "status"],
                filters=[("amount", ">", 50000)]
            )
        elif "csv" in name:
            # CSV must read all data then filter
            df = pd.read_csv(path, usecols=["transaction_id", "amount", "status"])
            df = df[df["amount"] > 50000]
        elif "json" in name:
            df = pd.read_json(path, lines=True)
            df = df[df["amount"] > 50000]
            df = df[["transaction_id", "amount", "status"]]

        elapsed = time.time() - start
        results[name] = {"read_time": elapsed, "rows": len(df)}

    return results


def print_comparison_report(formats, read_results, filter_results):
    """Print comprehensive comparison report."""
    print(f"\n{'='*70}")
    print(f"FORMAT COMPARISON REPORT")
    print(f"{'='*70}")

    # Write performance
    print(f"\n--- Write Performance ---")
    print(f"{'Format':<20} {'Size (MB)':<12} {'Compression':<12} {'Write (s)':<10}")
    print("-" * 54)
    csv_size = formats["CSV"]["size"]
    for name, info in formats.items():
        compression = csv_size / info["size"]
        print(f"{name:<20} {info['size']/1024/1024:<12.1f} {compression:<12.1f}x {info['write_time']:<10.3f}")

    # Read performance
    print(f"\n--- Read Performance (column pruning) ---")
    print(f"{'Format':<20} {'Rows':<12} {'Read (s)':<10}")
    print("-" * 42)
    for name, info in read_results.items():
        print(f"{name:<20} {info['rows']:<12,} {info['read_time']:<10.3f}")

    # Filtered read performance
    print(f"\n--- Filtered Read Performance (amount > 50000) ---")
    print(f"{'Format':<20} {'Rows':<12} {'Read (s)':<10}")
    print("-" * 42)
    for name, info in filter_results.items():
        print(f"{name:<20} {info['rows']:<12,} {info['read_time']:<10.3f}")

    # Recommendation
    print(f"\n--- Recommendations ---")
    print(f"CSV:     Data exchange, small files, human readability")
    print(f"JSON:    APIs, semi-structured data, nested structures")
    print(f"Parquet: Data lakes, analytics, large datasets, cloud storage")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    output_dir = os.path.join(tempfile.gettempdir(), "format_comparison")
    os.makedirs(output_dir, exist_ok=True)

    # Generate data
    print("Generating transaction data...")
    df = generate_transaction_data(num_rows=100_000)

    # Write all formats
    print("Writing to all formats...")
    formats = write_all_formats(df, output_dir)

    # Compare reads
    read_results = compare_read_performance(formats)
    filter_results = compare_filtered_reads(formats)

    # Print report
    print_comparison_report(formats, read_results, filter_results)
```

---

## 5. Banking Scenario 2: Migration from CSV to Parquet

### Problem
A bank has 500 GB of CSV files from legacy systems. They need to:
- Migrate to Parquet for 10x compression
- Validate data integrity during migration
- Update all downstream queries
- Measure performance improvement

### Why Migration Matters?
- 500 GB CSV → ~50 GB Parquet = 90% storage savings
- Query performance: 10-100x improvement
- Type safety: No more "is this a number or string?" issues

### Architecture
```
Legacy CSV Files (500 GB)
       |
       v
  Migration Pipeline
       |
       +-- Schema inference
       +-- Type validation
       +-- Data quality checks
       +-- Parquet write
       |
       v
  Parquet Files (50 GB)
       |
       v
  Updated Analytics Queries
```

---

## 6. Python Code - Scenario 2

```python
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import os
import tempfile
import time

# ============================================================
# BANKING SCENARIO: CSV to Parquet Migration
# ============================================================

def generate_legacy_csv(output_path, num_rows=500_000):
    """Generate legacy CSV data with realistic issues."""
    np.random.seed(42)

    # Create data with common CSV issues
    data = {
        "transaction_id": range(1, num_rows + 1),
        "account_id": [f"ACC{i:06d}" for i in range(1, num_rows + 1)],
        "amount": np.random.uniform(1.0, 100000.0, num_rows).round(2),
        "currency": np.random.choice(["USD", "EUR", "GBP"], num_rows),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
        "date": pd.date_range("2026-01-01", periods=num_rows, freq="30s").strftime("%Y-%m-%d %H:%M:%S"),
    }

    df = pd.DataFrame(data)

    # Introduce some CSV issues
    # Some amounts as strings
    mask = np.random.choice(num_rows, 100, replace=False)
    df.loc[mask, "amount"] = df.loc[mask, "amount"].astype(str)

    # Some null values
    mask = np.random.choice(num_rows, 50, replace=False)
    df.loc[mask, "currency"] = ""

    # Write CSV
    df.to_csv(output_path, index=False)

    return df


def validate_csv_schema(csv_path):
    """Validate CSV data before migration."""
    print("=== CSV Validation ===")

    df = pd.read_csv(csv_path)

    # Check for nulls
    null_counts = df.isnull().sum()
    print(f"Null values:\n{null_counts[null_counts > 0]}")

    # Check data types
    print(f"\nData types:\n{df.dtypes}")

    # Check for empty strings
    for col in df.select_dtypes(include=["object"]).columns:
        empty_count = (df[col] == "").sum()
        if empty_count > 0:
            print(f"Empty strings in {col}: {empty_count}")

    return df


def migrate_csv_to_parquet(csv_path, output_path):
    """Migrate CSV to Parquet with type conversion."""
    start = time.time()

    # Read CSV
    df = pd.read_csv(csv_path)

    # Clean and convert types
    # Convert amount to numeric
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    # Convert date to datetime
    df["date"] = pd.to_datetime(df["date"])

    # Handle empty currencies
    df["currency"] = df["currency"].replace("", np.nan)

    # Define explicit schema
    schema = pa.schema([
        ("transaction_id", pa.int64()),
        ("account_id", pa.string()),
        ("amount", pa.decimal128(18, 2)),
        ("currency", pa.string()),
        ("status", pa.string()),
        ("date", pa.timestamp("us")),
    ])

    # Convert to Arrow table
    table = pa.Table.from_pandas(df, schema=schema)

    # Write Parquet
    pq.write_table(
        table,
        output_path,
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
    )

    elapsed = time.time() - start
    csv_size = os.path.getsize(csv_path)
    parquet_size = os.path.getsize(output_path)

    print(f"\n=== Migration Report ===")
    print(f"Rows migrated: {len(df):,}")
    print(f"CSV size: {csv_size / (1024*1024):.1f} MB")
    print(f"Parquet size: {parquet_size / (1024*1024):.1f} MB")
    print(f"Compression: {csv_size / parquet_size:.1f}x")
    print(f"Migration time: {elapsed:.3f}s")

    return table


def compare_query_performance(csv_path, parquet_path):
    """Compare query performance between CSV and Parquet."""
    # CSV: Full scan required
    start = time.time()
    df_csv = pd.read_csv(csv_path, usecols=["transaction_id", "amount", "status"])
    df_csv = df_csv[df_csv["amount"] > 50000]
    csv_time = time.time() - start

    # Parquet: Column pruning + predicate pushdown
    start = time.time()
    df_parquet = pd.read_parquet(
        parquet_path,
        columns=["transaction_id", "amount", "status"],
        filters=[("amount", ">", 50000)],
    )
    parquet_time = time.time() - start

    print(f"\n=== Query Performance Comparison ===")
    print(f"Query: SELECT transaction_id, amount, status WHERE amount > 50000")
    print(f"CSV: {csv_time:.3f}s ({len(df_csv):,} rows)")
    print(f"Parquet: {parquet_time:.3f}s ({len(df_parquet):,} rows)")
    print(f"Speedup: {csv_time/parquet_time:.1f}x")

    return csv_time, parquet_time


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "csv_migration")
    os.makedirs(base_path, exist_ok=True)

    # Generate legacy CSV
    csv_path = os.path.join(base_path, "legacy_transactions.csv")
    print("Generating legacy CSV data...")
    original_df = generate_legacy_csv(csv_path, num_rows=500_000)

    # Validate CSV
    validate_csv_schema(csv_path)

    # Migrate to Parquet
    parquet_path = os.path.join(base_path, "transactions.parquet")
    print("\nMigrating to Parquet...")
    migrated_table = migrate_csv_to_parquet(csv_path, parquet_path)

    # Compare query performance
    compare_query_performance(csv_path, parquet_path)
```

---

## 7. Interview Questions

### Q1: When would you choose CSV over Parquet?

**Answer:**

**Choose CSV when:**
1. **Data exchange**: Sharing data with systems that don't support Parquet
2. **Human readability**: Need to manually inspect data
3. **Small datasets**: < 1 GB where compression doesn't matter
4. **Quick prototyping**: Fast to generate and read
5. **Legacy systems**: Mainframe exports, legacy tools
6. **Text processing**: Data that's primarily text (logs, documents)

**Example**: A bank exports customer contact info to a third-party CRM that only accepts CSV.

**Choose Parquet when:**
1. **Analytics**: Columnar storage for analytical queries
2. **Large datasets**: > 1 GB where compression matters
3. **Cloud storage**: S3, GCS, ADLS
4. **Multiple tools**: Spark, DuckDB, Trino, Pandas
5. **Data lakes**: Long-term storage with query optimization

---

### Q2: What are the advantages of Parquet over ORC?

**Answer:**

| Feature | Parquet | ORC |
|---------|---------|-----|
| Tool support | Widest (Spark, DuckDB, Trino, Pandas) | Mainly Hive |
| Cloud storage | Excellent | Good |
| Schema evolution | Good | Good |
| Compression | Excellent | Excellent |
| Predicate pushdown | Yes | Yes |
| Ecosystem | Open, multi-engine | Hive-centric |

**Parquet advantages:**
1. **Broader tool support**: Works with virtually every data tool
2. **Cloud-native**: Better integration with S3, GCS, ADLS
3. **Multi-engine**: Spark, DuckDB, Trino, Flink, Pandas
4. **Industry standard**: Most widely adopted columnar format

**ORC advantages:**
1. **Hive integration**: Native ACID transactions with Hive
2. **Built-in indexes**: Bloom filters, row index
3. **Hive ecosystem**: Better for existing Hive deployments

**Recommendation**: Choose Parquet unless you're deeply invested in the Hive ecosystem.

---

### Q3: When would you choose Avro over Parquet?

**Answer:**

**Choose Avro when:**
1. **Streaming**: Kafka message format (row-based, compact)
2. **Write-heavy**: Frequent small writes (events, logs)
3. **Schema evolution**: Complex schema changes
4. **Row-level operations**: Need to read/write individual records

**Example**: Kafka topics containing real-time transaction events.

**Choose Parquet when:**
1. **Analytics**: Columnar queries (SUM, AVG, GROUP BY)
2. **Read-heavy**: Data lake storage for BI/ML
3. **Large files**: Batch processing of large datasets
4. **Column access**: Need specific columns, not entire rows

**Example**: Data lake storage for historical transaction analysis.

**Common pattern**: Avro (in-flight) → Parquet (at rest):
```
Kafka (Avro) → Flink → Parquet (S3)
```

---

### Q4: How does compression differ across formats?

**Answer:**

| Format | Default Codec | Compression Ratio | Splittable |
|--------|--------------|-------------------|------------|
| CSV | None/Gzip | 1x / 5x | Yes (Gzip: no) |
| JSON | None/Gzip | 1x / 5x | No |
| Avro | Deflate | 5-7x | Yes |
| ORC | Zlib | 7-10x | Yes |
| Parquet | Snappy | 5-8x | Yes |

**Key insight**: Columnar formats (Parquet, ORC) achieve better compression than row-based formats because:
1. Similar data types together → better encoding
2. Dictionary encoding more effective
3. Run-length encoding more effective

**Example**:
```
Status column (1M rows, 3 unique values):
  CSV:     ~7 MB (raw strings)
  Parquet: ~0.5 MB (dictionary + bit-packing + Snappy)
  Ratio:   14x compression
```

---

### Q5: What is the future of data formats in the data lake ecosystem?

**Answer:**

**Current trends:**
1. **Parquet dominance**: Becoming the de facto standard for data lakes
2. **Open table formats**: Iceberg, Delta Lake, Hudi adding transaction layers
3. **Lakehouse architecture**: Combining data lake flexibility with warehouse reliability
4. **Cloud-native formats**: Optimized for object storage (S3, GCS)

**Emerging patterns:**
```
Traditional:          Modern:
CSV/JSON              Parquet
  ↓                     ↓
Data Warehouse        Lakehouse
  ↓                     ↓
Single engine         Multi-engine
```

**Future predictions:**
1. Parquet will remain the dominant columnar format
2. Iceberg/Delta Lake will add transaction capabilities
3. New formats may emerge for specific use cases (streaming, ML)
4. Format convergence (fewer formats, better interoperability)

**Recommendation**: Invest in Parquet + Iceberg/Delta Lake for long-term viability.
