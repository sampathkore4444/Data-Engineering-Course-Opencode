# Parquet FAQ & Interview Guide

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-technical-interview-prep)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-system-design-interview)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### Frequently Asked Questions

#### Q: What is Apache Parquet?
**A:** Apache Parquet is an **open-source columnar storage file format** designed for efficient data storage and retrieval. It stores data by columns (not rows), enabling:
- 5-10x compression vs CSV
- Column pruning (read only needed columns)
- Predicate pushdown (skip irrelevant data)
- Self-describing schema
- Splittable for parallel processing

#### Q: Why is Parquet preferred over CSV?
**A:**

| Feature | CSV | Parquet |
|---------|-----|---------|
| Storage | Row-based | Columnar |
| Compression | None/Gzip | Snappy/Zstd |
| Schema | No | Embedded |
| Column pruning | No | Yes |
| Predicate pushdown | No | Yes |
| Type safety | No | Yes |

**Example**: 100 GB CSV → 10 GB Parquet, queries 10-100x faster.

#### Q: How does Parquet achieve compression?
**A:** Through two stages:
1. **Encoding**: Dictionary, run-length, bit-packing, delta encoding
2. **Compression**: Snappy, Zstd, Gzip applied to encoded data

**Example**: Status column with 3 values:
```
Raw: 7 bytes × 1M rows = 7 MB
Dictionary: 4 bytes × 1M rows + 20 bytes = 4 MB
Compressed: ~0.5 MB
Ratio: 14x
```

#### Q: What is predicate pushdown?
**A:** An optimization where **filter conditions are evaluated at the storage layer** before data is read into memory.

```
Query: WHERE date = '2026-08-24'

Without pushdown: Read 100 GB → Filter → Return 1 GB
With pushdown: Check statistics → Read 1 GB → Return 1 GB
Speedup: 100x
```

#### Q: When should I use Parquet vs ORC vs Avro?
**A:**

| Format | Best For |
|--------|----------|
| **Parquet** | Data lakes, analytics, cloud storage |
| **ORC** | Hive ecosystem, ACID transactions |
| **Avro** | Streaming (Kafka), write-heavy workloads |

**Recommendation**: Use Parquet unless you have specific Hive or streaming requirements.

#### Q: How do I optimize Parquet file sizes?
**A:**

1. **Target 256MB - 1GB per file**
2. **Use compaction** for small files
3. **Repartition** before writing
4. **Control row group size**

```python
# Write with target file size
pq.write_table(table, "file.parquet", row_group_size=1_000_000)
```

#### Q: How does schema evolution work in Parquet?
**A:**

**Supported:**
- ✅ Add columns (appear as NULLs in old files)
- ✅ Widen types (INT32 → INT64)
- ✅ Reorder columns

**Not supported:**
- ❌ Remove columns (persist in old files)
- ❌ Rename columns (name embedded in file)
- ❌ Change types arbitrarily

**Best practice**: Use Iceberg/Delta Lake for complex schema evolution.

#### Q: What compression codec should I use?
**A:**

| Workload | Codec | Rationale |
|----------|-------|-----------|
| Interactive queries | Snappy | Fast decompression |
| General purpose | Zstd level 3 | Best balance |
| Archival | Gzip level 9 | Maximum compression |
| Low latency | LZ4 | Fastest |

#### Q: How do I read Parquet files larger than memory?
**A:**

1. **Column pruning**: Read only needed columns
2. **Predicate pushdown**: Filter at storage level
3. **Batch reading**: Process in chunks
4. **Use DuckDB**: Handles memory automatically

```python
# DuckDB handles large files automatically
import duckdb
con = duckdb.connect()
result = con.execute("SELECT * FROM read_parquet('large.parquet') WHERE amount > 1000").fetchdf()
```

#### Q: What are the common pitfalls with Parquet?
**A:**

1. **Too many small files**: Use compaction
2. **No compression**: Always use Snappy or Zstd
3. **Reading all columns**: Use column pruning
4. **No filters**: Use predicate pushdown
5. **Wrong types**: Use DECIMAL for financial amounts
6. **No statistics**: Enable write_statistics=True
7. **Over-partitioning**: Keep partitions > 100K rows

---

## 2. Example

### Complete Interview-Ready Example

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import tempfile
import time

# ============================================================
# INTERVIEW-READY PARQUET EXAMPLE
# ============================================================

def create_optimized_table(num_rows=1_000_000):
    """Create an optimized Parquet table with best practices."""
    random.seed(42)
    np.random.seed(42)

    from decimal import Decimal

    # Best Practice 1: Define explicit schema
    schema = pa.schema([
        ("transaction_id", pa.int64()),
        ("account_id", pa.string()),
        ("amount", pa.decimal128(18, 2)),  # Best Practice 2: Use DECIMAL for money
        ("currency", pa.string()),
        ("status", pa.string()),
        ("channel", pa.string()),
        ("transaction_date", pa.date32()),
        ("created_at", pa.timestamp("us")),
    ], metadata={  # Best Practice 3: Add metadata
        "created_by": "interview_demo",
        "schema_version": "1.0",
        "description": "Banking transactions for interview demo",
    })

    # Generate data
    table = pa.table({
        "transaction_id": pa.array(list(range(1, num_rows + 1)), type=pa.int64()),
        "account_id": pa.array([f"ACC{random.randint(100000, 999999)}" for _ in range(num_rows)], type=pa.string()),
        "amount": pa.array([Decimal(str(round(random.uniform(1.0, 100000.0), 2))) for _ in range(num_rows)], type=pa.decimal128(18, 2)),
        "currency": pa.array(np.random.choice(["USD", "EUR", "GBP"], num_rows), type=pa.string()),
        "status": pa.array(np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows), type=pa.string()),
        "channel": pa.array(np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM"], num_rows), type=pa.string()),
        "transaction_date": pa.array([
            (datetime(2026, 1, 1) + timedelta(days=random.randint(0, 364))).strftime("%Y-%m-%d")
            for _ in range(num_rows)
        ], type=pa.date32()),
        "created_at": pa.array([datetime.now()] * num_rows, type=pa.timestamp("us")),
    }, schema=schema)

    return table


def write_with_best_practices(table, output_path):
    """Write Parquet with production best practices."""
    start = time.time()

    pq.write_table(
        table,
        output_path,
        compression="zstd",           # Best Practice 4: Use Zstd
        compression_level=3,
        use_dictionary=True,          # Best Practice 5: Enable dictionary
        write_statistics=True,        # Best Practice 6: Enable statistics
        data_page_size=1_048_576,     # Best Practice 7: 1MB pages
        version="2.6",
    )

    elapsed = time.time() - start
    size = os.path.getsize(output_path)

    print(f"Written {table.num_rows:,} rows")
    print(f"File size: {size / (1024*1024):.1f} MB")
    print(f"Write time: {elapsed:.3f}s")

    return output_path


def demonstrate_optimizations(table, base_path):
    """Demonstrate key Parquet optimizations."""
    # Write unoptimized
    pq.write_table(table, os.path.join(base_path, "unoptimized.parquet"), compression="none")

    # Write optimized
    pq.write_table(
        table,
        os.path.join(base_path, "optimized.parquet"),
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
    )

    results = {}

    # 1. Full read
    start = time.time()
    pq.read_table(os.path.join(base_path, "unoptimized.parquet"))
    results["full_read"] = time.time() - start

    # 2. Column pruning
    start = time.time()
    pq.read_table(
        os.path.join(base_path, "unoptimized.parquet"),
        columns=["transaction_id", "amount", "status"]
    )
    results["column_pruning"] = time.time() - start

    # 3. Predicate pushdown
    start = time.time()
    pq.read_table(
        os.path.join(base_path, "unoptimized.parquet"),
        filters=[("transaction_date", ">=", "2026-08-01"), ("transaction_date", "<=", "2026-08-31")]
    )
    results["predicate_pushdown"] = time.time() - start

    # 4. Both optimizations
    start = time.time()
    pq.read_table(
        os.path.join(base_path, "optimized.parquet"),
        columns=["transaction_id", "amount", "status"],
        filters=[("transaction_date", ">=", "2026-08-01"), ("transaction_date", "<=", "2026-08-31")]
    )
    results["both_optimizations"] = time.time() - start

    print(f"\n=== Optimization Demo ===")
    print(f"{'Strategy':<25} {'Time (s)':<12} {'Speedup':<10}")
    print("-" * 47)
    baseline = results["full_read"]
    for name, elapsed in results.items():
        speedup = baseline / elapsed
        print(f"{name:<25} {elapsed:<12.3f} {speedup:<10.1f}x")


# ============================================================
# RUN THE DEMO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "interview_demo")
    os.makedirs(base_path, exist_ok=True)

    # Create and write optimized table
    print("Creating optimized table...")
    table = create_optimized_table(num_rows=500_000)
    output_path = os.path.join(base_path, "transactions.parquet")
    write_with_best_practices(table, output_path)

    # Demonstrate optimizations
    demonstrate_optimizations(table, base_path)
```

---

## 3. Banking Scenario 1: Technical Interview Prep

### Problem
You're interviewing for a Data Engineer role at a bank. You need to demonstrate:
- Parquet fundamentals
- Performance optimization
- Data modeling
- Production best practices

### Common Interview Topics

1. **Parquet vs CSV**: Why columnar?
2. **Predicate pushdown**: How it works
3. **Schema design**: Best practices
4. **Compression**: Codec selection
5. **File sizing**: Optimization
6. **Partitioning**: Strategy
7. **Data quality**: Validation
8. **Production**: Monitoring, governance

---

## 4. Python Code - Scenario 1

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import numpy as np
from datetime import datetime, timedelta
import random
import os
import tempfile
import time

# ============================================================
# BANKING SCENARIO: Technical Interview Prep
# ============================================================

def interview_question_1():
    """Q: Explain Parquet's internal structure."""
    print("\n=== Q1: Parquet Internal Structure ===")
    print("""
    Parquet File Structure:
    
    File Metadata (Footer)
    ├── Schema (column names, types)
    ├── Row Group offsets
    ├── Column statistics (min, max, null_count)
    └── Key-value metadata
    
    Row Group 1 (horizontal partition, ~128-256MB)
    ├── Column Chunk: col_1
    │   ├── Dictionary Page (optional)
    │   ├── Data Page 1
    │   └── Data Page 2
    ├── Column Chunk: col_2
    │   └── Data Page 1
    └── Column Chunk: col_3
        └── Data Page 1
    
    Key points:
    - Row Group: Unit of parallelism
    - Column Chunk: Unit of compression
    - Page: Unit of I/O (~1MB)
    """)


def interview_question_2():
    """Q: How does predicate pushdown work?"""
    print("\n=== Q2: Predicate Pushdown ===")
    print("""
    Predicate pushdown moves filters to the storage layer.
    
    Query: WHERE date >= '2026-08-01' AND amount > 1000
    
    Step 1: Read footer (schema, row group stats)
    
    Step 2: Check row group statistics:
    - Row Group 1: date min=2026-01-01, max=2026-03-31
      → SKIP (date filter eliminates)
    - Row Group 2: date min=2026-04-01, max=2026-07-31
      → SKIP (date filter eliminates)
    - Row Group 3: date min=2026-08-01, max=2026-08-31
      → CHECK (amount min=10, max=50000)
      → READ (both filters might match)
    
    Step 3: Read only relevant row groups
    
    Impact: 100 GB → 1 GB (100x reduction)
    """)


def interview_question_3():
    """Q: When would you use Parquet vs ORC vs Avro?"""
    print("\n=== Q3: Format Selection ===")
    print("""
    Parquet:
    - Data lakes, analytics, cloud storage
    - Columnar: fast analytical queries
    - Widest tool support (Spark, DuckDB, Trino, Pandas)
    
    ORC:
    - Hive ecosystem, ACID transactions
    - Built-in indexes (Bloom filters)
    - Better for Hive-specific workloads
    
    Avro:
    - Streaming (Kafka), write-heavy workloads
    - Row-based: fast reads/writes
    - Schema evolution: complex changes
    
    Recommendation: Use Parquet unless you need Hive ACID or streaming.
    """)


def interview_question_4():
    """Q: How do you optimize Parquet queries?"""
    print("\n=== Q4: Query Optimization ===")
    print("""
    Top 5 optimizations:
    
    1. Column Pruning (read only needed columns)
       pq.read_table("file.parquet", columns=["amount", "status"])
       Impact: 20 cols → 2 cols = 90% I/O reduction
    
    2. Predicate Pushdown (filter at storage level)
       pq.read_table("file.parquet", filters=[("date", ">=", "2026-08-01")])
       Impact: 1B rows → 10M rows = 99% I/O reduction
    
    3. Partitioning (organize by filter columns)
       pq.write_to_dataset(table, partition_cols=["date"])
       Impact: 365 partitions → 1 partition
    
    4. Compression (Zstd for best balance)
       pq.write_table(table, "file.parquet", compression="zstd")
       Impact: 5-10x storage savings
    
    5. File Sizing (256MB - 1GB per file)
       Balance parallelism vs metadata overhead
    """)


def interview_question_5():
    """Q: Design a banking data lake with Parquet."""
    print("\n=== Q5: Data Lake Design ===")
    print("""
    Architecture:
    
    Data Sources
         │
         ▼
    ETL Pipeline (Spark)
         │
         ▼
    S3 Data Lake (Parquet)
         │
    ┌────┼────┐
    │    │    │
    Bronze Silver Gold
    (raw) (clean) (aggregate)
         │
         ▼
    Query Layer
    ├── DuckDB (ad-hoc)
    ├── Spark (batch)
    └── Trino (interactive)
    
    Key decisions:
    - Partitioning: By date (low cardinality)
    - File size: 256MB - 1GB
    - Compression: Zstd
    - Schema: Explicit, with DECIMAL for money
    - Governance: Metadata, lineage, access control
    """)


def run_interview_prep():
    """Run all interview questions."""
    interview_question_1()
    interview_question_2()
    interview_question_3()
    interview_question_4()
    interview_question_5()


# ============================================================
# RUN THE DEMO
# ============================================================
if __name__ == "__main__":
    run_interview_prep()
```

---

## 5. Banking Scenario 2: System Design Interview

### Problem
Design a system to process **1 billion daily transactions** for a bank's analytics platform.

### Requirements
- Ingest 1 billion transactions/day
- Store in Parquet for analytics
- Support real-time and batch queries
- 7-year retention
- Cost-effective

### Architecture Design

```
┌─────────────────────────────────────────────────────────┐
│                    Data Sources                         │
├─────────────────────────────────────────────────────────┤
│ Core Banking (Oracle) ──► Debezium CDC ──► Kafka       │
│ Card Processing (MySQL) ──► Debezium CDC ──► Kafka     │
│ Online Banking (PostgreSQL) ──► Kafka                   │
│ Mobile Events ──► Kafka                                 │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│              Streaming Layer (Flink)                     │
│  ──► Real-time fraud detection                          │
│  ──► Write to Kafka ( enriched events)                  │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│              Batch Layer (Spark)                         │
│  ──► Read from Kafka                                    │
│  ──► Transform & enrich                                 │
│  ──► Write Parquet to S3                                │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│              Storage Layer (S3)                          │
│                                                          │
│  Bronze (raw): 100 GB/day                               │
│  ├── Parquet files (256MB each)                         │
│  ├── Partitioned by date                                │
│  └── Snappy compression                                 │
│                                                          │
│  Silver (clean): 20 GB/day                              │
│  ├── Parquet files (256MB each)                         │
│  ├── Partitioned by date + status                       │
│  └── Zstd compression                                   │
│                                                          │
│  Gold (aggregated): 1 GB/day                            │
│  ├── Parquet files (64MB each)                          │
│  ├── Pre-aggregated summaries                           │
│  └── Zstd compression                                   │
└─────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────┐
│              Query Layer                                 │
│  ──► DuckDB (ad-hoc analysis)                           │
│  ──► Spark (batch processing)                           │
│  ──► Trino (interactive SQL)                            │
│  ──► BI Tools (Tableau, PowerBI)                        │
└─────────────────────────────────────────────────────────┘
```

### Key Design Decisions

1. **Partitioning**: By date (low cardinality, ~365 partitions/year)
2. **File sizing**: 256MB (balance parallelism vs metadata)
3. **Compression**: Zstd level 3 (best balance)
4. **Schema**: Explicit, DECIMAL for financial amounts
5. **Retention**: S3 lifecycle policies (Glacier after 1 year, delete after 7 years)

---

## 6. Python Code - Scenario 2

```python
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime, timedelta
import random
import os
import tempfile
import time

# ============================================================
# BANKING SCENARIO: System Design Interview
# ============================================================

def design_data_lake():
    """Present data lake design for interview."""
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║         BANKING DATA LAKE DESIGN - SYSTEM DESIGN            ║
    ╠══════════════════════════════════════════════════════════════╣
    ║                                                              ║
    ║  Requirements:                                               ║
    ║  - 1 billion transactions/day                                ║
    ║  - 7-year retention                                          ║
    ║  - Real-time + batch analytics                               ║
    ║  - Cost-effective storage                                    ║
    ║                                                              ║
    ║  Design Decisions:                                           ║
    ║  1. Format: Parquet (columnar, compressed)                   ║
    ║  2. Storage: S3 (object storage, lifecycle policies)         ║
    ║  3. Partitioning: By date (low cardinality)                  ║
    ║  4. File size: 256MB (optimal parallelism)                   ║
    ║  5. Compression: Zstd (best balance)                         ║
    ║  6. Schema: Explicit, DECIMAL for money                      ║
    ║                                                              ║
    ║  Architecture:                                               ║
    ║  Kafka → Spark → Parquet (S3) → DuckDB/Trino                ║
    ║                                                              ║
    ║  Storage Calculation:                                        ║
    ║  - 1B rows/day × 200 bytes = 200 GB/day raw                 ║
    ║  - Parquet (5x compression) = 40 GB/day                     ║
    ║  - 7 years = 40 GB × 365 × 7 = 102 TB                      ║
    ║  - S3 Standard: ~$2.3/TB/month = $235/month                 ║
    ║  - S3 Glacier (after 1 year): ~$0.004/TB/month              ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """)


def calculate_storage_costs():
    """Calculate storage costs for interview."""
    print("\n=== Storage Cost Calculation ===")

    # Parameters
    daily_rows = 1_000_000_000
    bytes_per_row = 200
    retention_years = 7

    # Calculations
    daily_raw_gb = (daily_rows * bytes_per_row) / (1024**3)
    daily_parquet_gb = daily_raw_gb / 5  # 5x compression
    total_tb = (daily_parquet_gb * 365 * retention_years) / 1024

    print(f"Daily rows: {daily_rows:,}")
    print(f"Bytes per row: {bytes_per_row}")
    print(f"Daily raw size: {daily_raw_gb:.1f} GB")
    print(f"Daily Parquet size: {daily_parquet_gb:.1f} GB (5x compression)")
    print(f"Total storage (7 years): {total_tb:.1f} TB")

    # S3 costs
    s3_standard_per_tb = 23.00  # $/month
    s3_glacier_per_tb = 0.40    # $/month

    # Assume 1 year hot, 6 years cold
    hot_tb = daily_parquet_gb * 365 / 1024
    cold_tb = total_tb - hot_tb

    monthly_cost = (hot_tb * s3_standard_per_tb) + (cold_tb * s3_glacier_per_tb)

    print(f"\nStorage tiers:")
    print(f"  Hot (S3 Standard, 1 year): {hot_tb:.1f} TB")
    print(f"  Cold (S3 Glacier, 6 years): {cold_tb:.1f} TB")
    print(f"  Monthly cost: ${monthly_cost:,.2f}")
    print(f"  Annual cost: ${monthly_cost * 12:,.2f}")


def demonstrate_system_design():
    """Demonstrate complete system design."""
    design_data_lake()
    calculate_storage_costs()

    # Show code example
    print("\n=== Code Example (Spark → Parquet) ===")
    print("""
    # Spark ETL to Parquet
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("BankingETL").getOrCreate()
    
    # Read from Kafka
    df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", "broker:9092") \
        .option("subscribe", "transactions") \
        .load()
    
    # Transform
    transformed = df.select(
        col("value").cast("string").alias("json"),
        from_json(col("value"), schema).alias("data")
    ).select("data.*")
    
    # Write to Parquet (optimized)
    transformed.write \
        .mode("append") \
        .partitionBy("transaction_date") \
        .option("compression", "zstd") \
        .parquet("s3://banking-lake/silver/transactions/")
    
    # Query with DuckDB
    import duckdb
    con = duckdb.connect()
    result = con.execute(\"\"\"
        SELECT status, SUM(amount) as total
        FROM read_parquet('s3://banking-lake/silver/transactions/**/*.parquet')
        WHERE transaction_date >= '2026-08-01'
        GROUP BY status
    \"\"\").fetchdf()
    """)


# ============================================================
# RUN THE DEMO
# ============================================================
if __name__ == "__main__":
    demonstrate_system_design()
```

---

## 7. Interview Questions

### Q1: Explain Parquet's architecture and why it's efficient for analytics.

**Answer:**

**Architecture:**
```
Parquet File
├── Footer (metadata)
│   ├── Schema
│   ├── Row Group locations
│   └── Column statistics (min, max, count)
├── Row Group 1
│   ├── Column Chunk A (compressed pages)
│   ├── Column Chunk B
│   └── Column Chunk C
├── Row Group 2
│   └── ...
```

**Why efficient:**
1. **Columnar storage**: Read only needed columns (column pruning)
2. **Statistics**: Skip irrelevant row groups (predicate pushdown)
3. **Encoding**: Dictionary, run-length, bit-packing reduce size
4. **Compression**: Snappy/Zstd further compress encoded data
5. **Splittable**: Row groups enable parallel processing

**Example**: `SELECT SUM(amount) FROM transactions WHERE date = '2026-08-24'`
- Reads only `amount` and `date` columns
- Skips row groups where date doesn't match
- 100x faster than CSV

---

### Q2: Design a Parquet-based data lake for a bank.

**Answer:**

**Requirements**: 1B transactions/day, 7-year retention, real-time + batch analytics.

**Design:**
```
S3 Data Lake
├── Bronze (raw)
│   └── Parquet, Snappy, partitioned by date
├── Silver (cleaned)
│   └── Parquet, Zstd, partitioned by date + status
├── Gold (aggregated)
│   └── Parquet, Zstd, pre-computed summaries
```

**Key decisions:**
1. **Format**: Parquet (columnar, compressed)
2. **Partitioning**: By date (low cardinality)
3. **File size**: 256MB (optimal parallelism)
4. **Compression**: Zstd (best balance)
5. **Schema**: Explicit, DECIMAL for financial amounts
6. **Retention**: S3 lifecycle (Glacier after 1 year)

**Storage**: ~100 TB over 7 years, ~$200-500/month on S3.

---

### Q3: How do you handle schema evolution in Parquet?

**Answer:**

**Parquet supports limited evolution:**
- ✅ Add columns (NULLs in old files)
- ✅ Widen types (INT32 → INT64)
- ✅ Reorder columns

**Limitations:**
- ❌ Remove columns (persist in old files)
- ❌ Rename columns (name embedded)
- ❌ Change types arbitrarily

**Solutions:**
1. **Add columns at end**: Safe, backward compatible
2. **Use Iceberg/Delta Lake**: Full schema evolution
3. **Version schemas**: Include version in metadata

**Example:**
```python
# Old schema: [id, name]
# New schema: [id, name, email]
# Old files: email appears as NULL
```

---

### Q4: What are the common interview coding tasks for Parquet?

**Answer:**

1. **Write optimized Parquet**:
```python
pq.write_table(table, "file.parquet", compression="zstd", use_dictionary=True)
```

2. **Read with column pruning and filters**:
```python
pq.read_table("file.parquet", columns=["a", "b"], filters=[("c", ">", 100)])
```

3. **Partitioned writes**:
```python
pq.write_to_dataset(table, root_path="output/", partition_cols=["date"])
```

4. **Schema validation**:
```python
assert table.schema == expected_schema
```

5. **File compaction**:
```python
tables = [pq.read_table(f) for f in small_files]
combined = pa.concat_tables(tables)
pq.write_table(combined, "compacted.parquet")
```

---

### Q5: Explain predicate pushdown with an example.

**Answer:**

**Definition**: Moving filter conditions from the query engine to the storage layer.

**How it works:**
1. Parquet stores min/max statistics per column per row group
2. When a filter is applied, check statistics first
3. If statistics show no match → skip entire row group
4. Only matching row groups are read

**Example:**
```
Query: WHERE date = '2026-08-24'

Row Group 1: date min=2026-01-01, max=2026-03-31
  → SKIP (max < 2026-08-24)

Row Group 2: date min=2026-04-01, max=2026-07-31
  → SKIP (max < 2026-08-24)

Row Group 3: date min=2026-08-01, max=2026-08-31
  → READ (2026-08-24 is in range)
```

**Impact**: 100 GB → 1 GB (100x reduction)

**Code:**
```python
pq.read_table("file.parquet", filters=[("date", ">=", "2026-08-01")])
```
