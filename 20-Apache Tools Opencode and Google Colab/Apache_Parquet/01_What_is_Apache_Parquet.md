# What is Apache Parquet?

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-daily-transaction-archival)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-cross-branch-reporting)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### What is Apache Parquet?

Apache Parquet is an **open-source, columnar storage file format** designed for efficient data storage and retrieval in distributed systems. It was originally developed by Twitter and Cloudera in 2013 and has since become the de facto standard for storing analytical data in data lakes and data warehouses.

The single most important thing to understand about Parquet:

> **Parquet stores data by columns, not by rows. This makes analytical queries dramatically faster because most analytical workloads read a subset of columns across many rows.**

### Why Columnar Storage Matters

Consider a transaction table with 100 columns and 1 billion rows.

**Row-based storage (like CSV or MySQL InnoDB):**

```
Row 1: [tx_id, account_id, amount, date, merchant, city, state, country, ... 93 more columns]
Row 2: [tx_id, account_id, amount, date, merchant, city, state, country, ... 93 more columns]
Row 3: [tx_id, account_id, amount, date, merchant, city, state, country, ... 93 more columns]
```

If you query:
```sql
SELECT SUM(amount) FROM transactions WHERE date = '2026-08-24'
```

The engine must read **all 100 columns** for every row, even though you only need `amount` and `date`.

**Columnar storage (Parquet):**

```
tx_id:      [1001, 1002, 1003, ...]
account_id: [A001, A002, A003, ...]
amount:     [100, 250, 500, ...]
date:       [2026-08-01, 2026-08-01, 2026-08-02, ...]
merchant:   ["Walmart", "Amazon", "Starbucks", ...]
city:       ["NYC", "LA", "Chicago", ...]
state:      ["NY", "CA", "IL", ...]
country:    ["US", "US", "US", ...]
```

Now the engine reads **only** the `amount` and `date` columns. That's a 98% reduction in I/O.

### The One-Line Definition

> **Apache Parquet is a columnar file format optimized for analytics that compresses data efficiently and enables reading only the columns needed for a query.**

### Core Characteristics

| Characteristic | Description |
|---------------|-------------|
| **Columnar** | Data stored column-by-column, not row-by-row |
| **Self-describing** | Schema embedded in the file itself |
| **Compressed** | Supports multiple compression codecs (Snappy, Gzip, Zstd, LZ4) |
| **Encodings** | Uses dictionary, run-length, bit-packing encodings |
| **Splittable** | Files can be split across parallel workers |
| **Portable** | Language-neutral, supported by virtually every data tool |
| **Predicate pushdown** | Filters pushed down to file level |
| **Nested support** | Supports complex nested data structures |

### Where Parquet Fits in the Data Ecosystem

```
          Application Layer
                 |
          Query Engines
       /       |       \
    Spark    DuckDB    Trino
       \       |       /
        \      |      /
         File Format Layer
              |
         Apache Parquet
              |
         Compression
         (Snappy, Zstd)
              |
         Storage Layer
    /         |         \
  S3        HDFS       GCS
```

### How Parquet Files Are Structured

A Parquet file has a clear internal hierarchy:

```
Parquet File
    |
    +-- File Metadata
    |       |
    |       +-- Schema
    |       +-- Row Group offsets
    |       +-- Key-value pairs
    |
    +-- Row Group 1
    |       |
    |       +-- Column Chunk: tx_id
    |       |       +-- Page 1 (data)
    |       |       +-- Page 2 (data)
    |       |
    |       +-- Column Chunk: amount
    |       |       +-- Page 1 (data)
    |       |       +-- Page 2 (data)
    |       |
    |       +-- Column Chunk: date
    |               +-- Page 1 (data)
    |
    +-- Row Group 2
    |       |
    |       +-- Column Chunk: tx_id
    |       +-- Column Chunk: amount
    |       +-- Column Chunk: date
    |
    +-- Footer (File Metadata)
```

### Key Terminology

**Row Group**: A logical horizontal partition of the data. Each row group contains a subset of rows, organized by column.

**Column Chunk**: All the data for a single column within a row group. This is the unit of compression and encoding.

**Page**: The smallest unit of storage within a column chunk. A page is either a data page, a dictionary page, or an index page.

**Schema**: Describes the column names and data types. Embedded in the file footer.

**Footer**: Contains all metadata about the file, including schema, row group locations, and column statistics.

### Why Parquet Became the Standard

Before Parquet, the data ecosystem used:

| Format | Problem |
|--------|---------|
| CSV | No schema, no compression, no type info, huge files |
| JSON | Verbose, no columnar layout, no compression |
| Avro | Row-based, not ideal for analytics |
| ORC | Good but mainly used in Hive ecosystem |

Parquet solved all these problems:

```
CSV/JSON           --> Parquet
100 GB             --> ~10 GB (10x compression)
Full scan needed   --> Column pruning + predicate pushdown
No schema          --> Embedded schema with types
No statistics      --> Min/max/count per column per page
```

---

## 2. Example

Consider a bank's transaction table:

```
| tx_id | account_id | amount   | currency | tx_date       | merchant       | status   |
|-------|------------|----------|----------|---------------|----------------|----------|
| 1001  | A001       | 100.00   | USD      | 2026-08-01    | Walmart        | COMPLETED|
| 1002  | A002       | 2500.00  | USD      | 2026-08-01    | Amazon         | COMPLETED|
| 1003  | A001       | 50.00    | USD      | 2026-08-02    | Starbucks      | PENDING  |
| 1004  | A003       | 15000.00 | EUR      | 2026-08-02    | Wire Transfer  | COMPLETED|
| 1005  | A002       | 75.00    | USD      | 2026-08-03    | Gas Station    | COMPLETED|
```

**CSV representation (row-based):**
```
tx_id,account_id,amount,currency,tx_date,merchant,status
1001,A001,100.00,USD,2026-08-01,Walmart,COMPLETED
1002,A002,2500.00,USD,2026-08-01,Amazon,COMPLETED
1003,A001,50.00,USD,2026-08-02,Starbucks,PENDING
1004,A003,15000.00,EUR,2026-08-02,Wire Transfer,COMPLETED
1005,A002,75.00,USD,2026-08-03,Gas Station,COMPLETED
```

**Parquet representation (columnar):**
```
tx_id:      [1001, 1002, 1003, 1004, 1005]
account_id: [A001, A002, A001, A003, A002]
amount:     [100.00, 2500.00, 50.00, 15000.00, 75.00]
currency:   [USD, USD, USD, EUR, USD]
tx_date:    [2026-08-01, 2026-08-01, 2026-08-02, 2026-08-02, 2026-08-03]
merchant:   [Walmart, Amazon, Starbucks, Wire Transfer, Gas Station]
status:     [COMPLETED, COMPLETED, PENDING, COMPLETED, COMPLETED]
```

If you only need `SUM(amount)`, the engine reads **only the amount column** — skipping 6 out of 7 columns entirely.

---

## 3. Banking Scenario 1: Daily Transaction Archival

### Problem
A bank processes **50 million transactions daily** across 5,000 branches. At end of day, all transactions must be archived for regulatory compliance (7-year retention). The archive must be:
- Compressed to minimize storage costs
- Queryable by auditors for the next 7 years
- Efficient for monthly aggregate reports

### Why Parquet?
- Columnar storage allows auditors to query specific columns without reading entire files
- Built-in compression reduces storage costs by 8-10x compared to CSV
- Embedded schema ensures data integrity over 7 years
- Statistics enable fast filtering by date ranges

### Architecture
```
Core Banking System
       |
       v
  ETL Pipeline (daily batch)
       |
       v
  Parquet Files (partitioned by date)
       |
       v
  Object Storage (S3 / MinIO)
       |
       v
  Query Layer (DuckDB / Spark / Trino)
       |
       v
  Regulatory Reports
```

---

## 4. Python Code - Scenario 1

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pandas as pd
from datetime import datetime, timedelta
import random

# ============================================================
# BANKING SCENARIO: Daily Transaction Archival
# ============================================================

def generate_daily_transactions(num_transactions=1000):
    """Generate realistic banking transactions for a single day."""
    random.seed(42)

    transaction_ids = list(range(1000000, 1000000 + num_transactions))
    account_ids = [f"A{random.randint(1000, 9999)}" for _ in range(num_transactions)]
    branch_ids = [f"B{random.randint(100, 500)}" for _ in range(num_transactions)]
    amounts = [round(random.uniform(1.0, 50000.0), 2) for _ in range(num_transactions)]
    currencies = [random.choice(["USD", "USD", "USD", "EUR", "GBP"]) for _ in range(num_transactions)]
    tx_dates = ["2026-08-24"] * num_transactions
    tx_times = [f"{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
                for _ in range(num_transactions)]
    merchants = random.sample([
        "Walmart", "Amazon", "Starbucks", "Shell Gas", "Costco",
        "McDonald's", "Target", "Best Buy", "CVS Pharmacy", "Home Depot",
        "Wire Transfer", "ATM Withdrawal", "Direct Deposit", "ACH Payment"
    ], 14) * (num_transactions // 14 + 1)
    merchants = merchants[:num_transactions]
    channels = [random.choice(["ONLINE", "BRANCH", "ATM", "MOBILE", "POS"]) for _ in range(num_transactions)]
    statuses = [random.choice(["COMPLETED", "COMPLETED", "COMPLETED", "PENDING", "FAILED"])
                for _ in range(num_transactions)]

    table = pa.table({
        "transaction_id": pa.array(transaction_ids, type=pa.int64()),
        "account_id": pa.array(account_ids, type=pa.string()),
        "branch_id": pa.array(branch_ids, type=pa.string()),
        "amount": pa.array(amounts, type=pa.float64()),
        "currency": pa.array(currencies, type=pa.string()),
        "transaction_date": pa.array(tx_dates, type=pa.date32()),
        "transaction_time": pa.array(tx_times, type=pa.string()),
        "merchant": pa.array(merchants, type=pa.string()),
        "channel": pa.array(channels, type=pa.string()),
        "status": pa.array(statuses, type=pa.string()),
    })

    return table


def archive_daily_transactions(transaction_table, archive_path, date_str):
    """Archive daily transactions to Parquet with date partitioning."""
    # Create partitioned path
    partition_path = f"{archive_path}/transaction_date={date_str}"

    # Write Parquet file with optimal settings
    pq.write_table(
        transaction_table,
        f"{partition_path}/transactions.parquet",
        compression="snappy",           # Snappy: fast compression, good ratio
        row_group_size=1_000_000,       # ~1M rows per row group
        use_dictionary=True,            # Dictionary encoding for low-cardinality columns
        write_statistics=True,          # Min/max stats for predicate pushdown
        use_deprecated_int96_timestamps=False,
    )

    print(f"Archived {transaction_table.num_rows} transactions to {partition_path}")
    return partition_path


def query_archived_transactions(archive_path, start_date, end_date, min_amount=None):
    """Query archived transactions with predicate pushdown."""
    # Parquet reader automatically skips files based on statistics
    filters = [
        ("transaction_date", ">=", start_date),
        ("transaction_date", "<=", end_date),
    ]

    if min_amount is not None:
        filters.append(("amount", ">=", min_amount))

    dataset = pq.ParquetDataset(
        archive_path,
        filters=filters,
        use_legacy_dataset=False,
    )

    table = dataset.read()
    print(f"Read {table.num_rows} rows after predicate pushdown")
    return table


def generate_monthly_archive(archive_path):
    """Simulate archiving 30 days of transaction data."""
    for day_offset in range(30):
        date = datetime(2026, 8, 1) + timedelta(days=day_offset)
        date_str = date.strftime("%Y-%m-%d")

        # Generate 50K transactions per day
        tx_table = generate_daily_transactions(num_transactions=50_000)

        # Archive
        archive_daily_transactions(tx_table, archive_path, date_str)

    print(f"\n30-day archive created at {archive_path}")


def generate_monthly_summary(archive_path):
    """Generate monthly summary report from archived Parquet files."""
    # Read all archived data with projection (only needed columns)
    columns_needed = ["branch_id", "amount", "currency", "status", "transaction_date"]

    dataset = pq.ParquetDataset(
        archive_path,
        use_legacy_dataset=False,
    )

    table = dataset.read(columns=columns_needed)

    # Aggregate: total amount per branch
    branch_totals = table.group_by("branch_id").aggregate([
        ("amount", "sum"),
        ("amount", "count"),
    ])

    print("\n=== Monthly Branch Summary ===")
    print(branch_totals.to_pandas().head(10))

    # Status distribution
    status_counts = table.group_by("status").aggregate([
        ("amount", "count"),
    ])
    print("\n=== Transaction Status Distribution ===")
    print(status_counts.to_pandas())


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    import tempfile
    import os

    # Use a temporary directory for the archive
    archive_path = os.path.join(tempfile.gettempdir(), "banking_archive")
    os.makedirs(archive_path, exist_ok=True)

    # Generate 30 days of archived data
    generate_monthly_archive(archive_path)

    # Query specific date range with amount filter
    filtered = query_archived_transactions(
        archive_path,
        start_date="2026-08-10",
        end_date="2026-08-20",
        min_amount=1000.0,
    )
    print(f"\nFiltered query returned {filtered.num_rows} rows")
    print(filtered.to_pandas().head(5))

    # Generate monthly summary
    generate_monthly_summary(archive_path)
```

---

## 5. Banking Scenario 2: Cross-Branch Performance Reporting

### Problem
A bank with **5,000 branches** needs a daily cross-branch performance report. The executive team wants to see:
- Total transaction volume per branch
- Average transaction size
- Success/failure rates
- Channel distribution (online vs branch vs ATM)

The underlying data is **2 billion rows** across 2 years of history. Reports must run in **under 30 seconds**.

### Why Parquet?
- Column pruning: Reports only need 4-5 columns out of 15+
- Predicate pushdown: Filter by date without scanning all data
- Compression: 2 years of data compressed from 500 GB to ~50 GB
- Statistics: Min/max per column eliminates irrelevant row groups

### Architecture
```
Parquet Files (partitioned by year/month/day)
       |
       v
  DuckDB (lightweight SQL engine)
       |
       v
  Executive Dashboard (under 30 seconds)
```

---

## 6. Python Code - Scenario 2

```python
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import time

# ============================================================
# BANKING SCENARIO: Cross-Branch Performance Reporting
# ============================================================

def generate_historical_data(base_path, num_days=730):
    """Generate 2 years of historical transaction data."""
    random.seed(123)

    all_data = {
        "transaction_id": [],
        "branch_id": [],
        "amount": [],
        "currency": [],
        "channel": [],
        "status": [],
        "transaction_date": [],
    }

    tx_id_counter = 1

    for day_offset in range(num_days):
        date = datetime(2024, 1, 1) + timedelta(days=day_offset)
        date_str = date.strftime("%Y-%m-%d")

        # Variable transaction volume per day (weekdays higher)
        is_weekday = date.weekday() < 5
        num_txns = random.randint(60_000, 100_000) if is_weekday else random.randint(20_000, 40_000)

        for _ in range(num_txns):
            all_data["transaction_id"].append(tx_id_counter)
            all_data["branch_id"].append(f"B{random.randint(100, 5000):04d}")
            all_data["amount"].append(round(random.uniform(5.0, 100_000.0), 2))
            all_data["currency"].append(random.choice(["USD"] * 8 + ["EUR", "GBP"]))
            all_data["channel"].append(random.choice(["ONLINE"] * 3 + ["BRANCH"] * 2 + ["ATM"] * 2 + ["MOBILE"] * 2 + ["POS"]))
            all_data["status"].append(random.choice(["COMPLETED"] * 90 + ["PENDING"] * 7 + ["FAILED"] * 3))
            all_data["transaction_date"].append(date_str)

            tx_id_counter += 1

    # Create Arrow table
    table = pa.table({
        "transaction_id": pa.array(all_data["transaction_id"], type=pa.int64()),
        "branch_id": pa.array(all_data["branch_id"], type=pa.string()),
        "amount": pa.array(all_data["amount"], type=pa.float64()),
        "currency": pa.array(all_data["currency"], type=pa.string()),
        "channel": pa.array(all_data["channel"], type=pa.string()),
        "status": pa.array(all_data["status"], type=pa.string()),
        "transaction_date": pa.array(all_data["transaction_date"], type=pa.date32()),
    })

    # Write partitioned Parquet files
    pq.write_to_dataset(
        table,
        root_path=base_path,
        partition_cols=["transaction_date"],
        compression="snappy",
        use_dictionary=True,
        write_statistics=True,
    )

    print(f"Generated {table.num_rows} transactions across {num_days} days")
    return table


def run_executive_report(base_path, report_date_start, report_date_end):
    """Run executive report with predicate pushdown and column pruning."""
    start_time = time.time()

    # Define filters for predicate pushdown
    filters = [
        ("transaction_date", ">=", report_date_start),
        ("transaction_date", "<=", report_date_end),
    ]

    # Read with column pruning (only columns needed for report)
    columns = ["branch_id", "amount", "channel", "status"]

    dataset = pq.ParquetDataset(
        base_path,
        filters=filters,
        use_legacy_dataset=False,
    )

    table = dataset.read(columns=columns)
    elapsed = time.time() - start_time

    print(f"\n=== Executive Report ({report_date_start} to {report_date_end}) ===")
    print(f"Data read in {elapsed:.3f} seconds")
    print(f"Rows loaded: {table.num_rows:,}")

    # Branch volume report
    branch_stats = table.group_by("branch_id").aggregate([
        ("amount", "sum"),
        ("amount", "mean"),
        ("amount", "count"),
    ])

    # Sort by total volume descending
    branch_df = branch_stats.to_pandas()
    branch_df.columns = ["branch_id", "total_volume", "avg_transaction", "tx_count"]
    branch_df = branch_df.sort_values("total_volume", ascending=False)

    print("\n--- Top 10 Branches by Volume ---")
    print(branch_df.head(10).to_string(index=False))

    # Channel distribution
    channel_stats = table.group_by("channel").aggregate([
        ("amount", "sum"),
        ("amount", "count"),
    ])
    channel_df = channel_stats.to_pandas()
    channel_df.columns = ["channel", "total_volume", "tx_count"]
    print("\n--- Channel Distribution ---")
    print(channel_df.to_string(index=False))

    # Status breakdown
    status_stats = table.group_by("status").aggregate([
        ("amount", "count"),
    ])
    status_df = status_stats.to_pandas()
    status_df.columns = ["status", "count"]
    status_df["pct"] = (status_df["count"] / status_df["count"].sum() * 100).round(2)
    print("\n--- Status Breakdown ---")
    print(status_df.to_string(index=False))

    return branch_df


def compare_file_sizes(base_path):
    """Compare Parquet file sizes vs equivalent CSV."""
    import os

    total_parquet_size = 0
    total_rows = 0

    for root, dirs, files in os.walk(base_path):
        for f in files:
            if f.endswith(".parquet"):
                filepath = os.path.join(root, f)
                total_parquet_size += os.path.getsize(filepath)

    print(f"\n=== Storage Analysis ===")
    print(f"Total Parquet size: {total_parquet_size / (1024**3):.2f} GB")
    print(f"Estimated CSV size: ~{(total_parquet_size / (1024**3)) * 8:.2f} GB (8x estimate)")
    print(f"Compression ratio: ~{8:.0f}x vs CSV")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    import tempfile
    import os

    base_path = os.path.join(tempfile.gettempdir(), "banking_historical")
    os.makedirs(base_path, exist_ok=True)

    # Generate 2 years of data
    print("Generating 2 years of historical data...")
    generate_historical_data(base_path, num_days=60)  # 60 days for demo

    # Run executive report
    run_executive_report(base_path, "2024-01-01", "2024-03-01")

    # Compare storage
    compare_file_sizes(base_path)
```

---

## 7. Interview Questions

### Q1: What is Apache Parquet and why is it preferred over CSV for analytics?

**Answer:**
Apache Parquet is a **columnar storage file format** optimized for analytical workloads. Unlike CSV (which stores data row-by-row), Parquet stores each column separately. This provides:

1. **Column pruning**: Only the columns needed for a query are read from disk
2. **Compression**: Columnar layout achieves 5-10x compression vs CSV
3. **Predicate pushdown**: Statistics (min/max/count) per column allow skipping irrelevant data
4. **Type safety**: Schema is embedded with proper data types (no ambiguity like CSV)
5. **Splittability**: Files can be split across parallel workers

For example, a query like `SELECT AVG(amount) FROM transactions WHERE branch_id = 'B001'` reads only the `amount` and `branch_id` columns, not all 50 columns in the table.

---

### Q2: Explain the internal structure of a Parquet file.

**Answer:**
A Parquet file consists of:

```
File Metadata (Footer)
    |
    +-- Schema (column names and types)
    +-- Row Group offsets
    +-- Key-value metadata
    +-- Column statistics (min, max, count, null_count)

Row Group 1 (horizontal slice of data, typically ~128MB-1GB)
    |
    +-- Column Chunk: col_1
    |       +-- Page: Dictionary Page (optional)
    |       +-- Page: Data Page 1
    |       +-- Page: Data Page 2
    |
    +-- Column Chunk: col_2
    |       +-- Page: Data Page 1
    |
    +-- Column Chunk: col_3
            +-- Page: Data Page 1

Row Group 2
    ... (same structure)
```

**Row Group**: A horizontal partition of rows. Contains column chunks for all columns.
**Column Chunk**: All data for one column in a row group. This is the unit of compression.
**Page**: The smallest unit of I/O. Typically 1MB. Can be data, dictionary, or index pages.

The footer (file metadata) is read first, allowing the engine to decide which row groups and columns to read.

---

### Q3: What is predicate pushdown and how does Parquet support it?

**Answer:**
Predicate pushdown is an optimization where **filter conditions are pushed down to the storage layer** so irrelevant data is never read into memory.

Parquet supports this through **column statistics** stored in the file footer and page headers:

```
Column: transaction_date
  Row Group 1: min = 2026-01-01, max = 2026-01-31
  Row Group 2: min = 2026-02-01, max = 2026-02-28
  Row Group 3: min = 2026-03-01, max = 2026-03-31
```

Query: `WHERE transaction_date = '2026-02-15'`

The engine checks statistics:
- Row Group 1: max date = Jan 31 → **SKIP**
- Row Group 2: min = Feb 1, max = Feb 28 → **READ**
- Row Group 3: min = Mar 1 → **SKIP**

Only Row Group 2 is read. This can eliminate 90%+ of I/O for date-filtered queries.

Parquet also supports **page-level filtering** via page statistics and bloom filters (in newer versions).

---

### Q4: What compression codecs are available in Parquet and when to use each?

**Answer:**

| Codec | Speed | Compression Ratio | Use Case |
|-------|-------|-------------------|----------|
| **Snappy** | Fast | Moderate (~3-5x) | Default for most workloads |
| **Gzip** | Slow | High (~6-8x) | Archival, cold storage |
| **Zstd** | Fast | High (~6-8x) | Best balance (modern default) |
| **LZ4** | Fastest | Low (~2-3x) | Latency-sensitive workloads |
| **Brotli** | Slow | Highest (~8-10x) | Maximum compression |
| **None** | N/A | None | Debugging, uncompressed |

**Recommendations:**
- **Snappy**: Default for Spark, good for hot data
- **Zstd**: Best overall — fast AND high compression (recommended for new projects)
- **Gzip**: When storage cost matters more than CPU
- **LZ4**: When CPU is the bottleneck
- **Brotli**: When storage is extremely expensive

**Rule of thumb**: Use Snappy for interactive queries, Zstd for general purpose, Gzip for archival.

---

### Q5: What are the limitations of Apache Parquet?

**Answer:**

1. **Not optimized for writes**: Parquet is an immutable file format. Writing/appending requires creating new files. Not suitable for transactional workloads.

2. **Poor for single-row lookups**: Columnar layout means reading a single row requires reading all columns. Row-oriented databases are better for point queries.

3. **Schema evolution complexity**: While Parquet supports adding columns, complex schema changes (renaming, reordering) can be challenging across large datasets.

4. **File sizing**: Too many small files cause metadata overhead. Too few large files reduce parallelism. Optimal file size is 128MB-1GB.

5. **No built-in versioning**: Unlike Iceberg or Delta Lake, Parquet has no built-in snapshot or time-travel capability.

6. **No transaction support**: No ACID guarantees at the file level. Concurrent writes to the same file can cause corruption.

7. **Nested data overhead**: While supported, complex nested structures can be less efficient than flat schemas.

8. **Read-only semantics**: Once written, a Parquet file cannot be modified. Updates require rewriting the entire file.
