# Parquet Predicate Pushdown & Filtering

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-time-range-queries)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-multi-dimensional-filtering)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### What is Predicate Pushdown?

Predicate pushdown is the **most important optimization** in Parquet:

> **Predicate pushdown moves filter conditions from the query engine down to the storage layer, so irrelevant data is never read into memory.**

Without pushdown:
```
Read ALL 100 GB → Filter in memory → Return 1 GB
```

With pushdown:
```
Check statistics → Skip 99 GB → Read 1 GB → Return 1 GB
```

**Speedup: 100x**

### How It Works

```
Query: SELECT * FROM transactions WHERE date = '2026-08-24' AND amount > 1000

Step 1: Read Footer
         |
         +-- Get schema
         +-- Get row group locations
         +-- Get column statistics
         |
Step 2: Check Row Group Statistics
         |
         +-- Row Group 1: date min=2026-01-01, max=2026-03-31
         |   → SKIP (date filter eliminates this)
         |
         +-- Row Group 2: date min=2026-04-01, max=2026-07-31
         |   → SKIP (date filter eliminates this)
         |
         +-- Row Group 3: date min=2026-08-01, max=2026-08-31
         |   → CHECK (might match date filter)
         |   +-- amount min=10, max=50000
         |   → CHECK (might match amount filter)
         |   → READ this row group
         |
Step 3: Read Only Relevant Row Groups
         |
Step 4: Apply Remaining Filters in Memory
```

### Statistics Used for Pushdown

Parquet stores these statistics per column per row group:

```
Column: transaction_date
  Row Group 1:
    min = 2026-01-01
    max = 2026-03-31
    null_count = 0
    distinct_count = 90
    count = 1000000

  Row Group 2:
    min = 2026-04-01
    max = 2026-07-31
    null_count = 0
    distinct_count = 122
    count = 1000000
```

### Filter Types Supported

| Filter Type | Example | Pushdown Support |
|-------------|---------|------------------|
| Equality | `= 'COMPLETED'` | ✅ Full |
| Less than | `< 1000` | ✅ Full |
| Greater than | `> 1000` | ✅ Full |
| Range | `BETWEEN 100 AND 1000` | ✅ Full |
| In list | `IN ('USD', 'EUR')` | ✅ Full |
| Like | `LIKE 'ACC%'` | ⚠️ Limited |
| Is null | `IS NULL` | ✅ Full |
| And | `A AND B` | ✅ Full |
| Or | `A OR B` | ⚠️ Limited |

### Page-Level Pushdown

Newer Parquet versions support page-level statistics:

```
Row Group 3 (date min=2026-08-01, max=2026-08-31)
  Column Chunk: date
    Page 1: min=2026-08-01, max=2026-08-10
    Page 2: min=2026-08-11, max=2026-08-20
    Page 3: min=2026-08-21, max=2026-08-31

Query: WHERE date = '2026-08-24'

Row Group 3: CHECK (date range matches)
  Page 1: max=2026-08-10 < 2026-08-24 → SKIP
  Page 2: max=2026-08-20 < 2026-08-24 → SKIP
  Page 3: min=2026-08-21, max=2026-08-31 → READ
```

### Bloom Filters

Parquet supports Bloom filters for equality checks:

```
Column: status
  Row Group 1: Bloom filter for 'COMPLETED', 'PENDING', 'FAILED'
  Row Group 2: Bloom filter for 'COMPLETED', 'FAILED'

Query: WHERE status = 'COMPLETED'

Row Group 1: Bloom filter says 'COMPLETED' might exist → READ
Row Group 2: Bloom filter says 'COMPLETED' might exist → READ
```

Bloom filters reduce false positives but can't guarantee absence.

### How Different Engines Handle Pushdown

| Engine | Pushdown Support | Notes |
|--------|------------------|-------|
| PyArrow | ✅ Full | Via filters parameter |
| DuckDB | ✅ Full | Automatic |
| Spark | ✅ Full | Catalyst optimizer |
| Trino | ✅ Full | Automatic |
| Pandas | ⚠️ Partial | Via PyArrow |

---

## 2. Example

### Visual: Predicate Pushdown in Action

```
Table: 5 Row Groups, 100M rows total
Query: WHERE date = '2026-08-24' AND amount > 1000

Row Group 1: date min=2026-01-01, max=2026-03-31
  → SKIP (date filter eliminates)

Row Group 2: date min=2026-04-01, max=2026-07-31
  → SKIP (date filter eliminates)

Row Group 3: date min=2026-08-01, max=2026-08-31
  → CHECK: date might match
  → amount min=10, max=50000
  → READ (both filters might match)

Row Group 4: date min=2026-09-01, max=2026-12-31
  → SKIP (date filter eliminates)

Row Group 5: date min=2026-08-24, max=2026-08-24
  → CHECK: date matches exactly
  → amount min=500, max=5000
  → READ (both filters match)

Result: Read 2 out of 5 row groups = 60% I/O reduction
```

---

## 3. Banking Scenario 1: Time-Range Queries

### Problem
A bank's compliance team needs to query transactions for specific date ranges:
- "All transactions from January to March 2026"
- "Transactions on August 24, 2026 between 2 PM and 4 PM"
- "Wire transfers from last week"

Data: 10 billion rows across 3 years. Queries must complete in seconds.

### Why Pushdown Matters?
- Without pushdown: Scan 10 billion rows
- With pushdown (date filter): Scan ~10 million rows (0.1%)
- With pushdown (date + type): Scan ~1 million rows (0.01%)

### Architecture
```
Query: SELECT * FROM transactions 
       WHERE date BETWEEN '2026-08-01' AND '2026-08-31'
       AND type = 'WIRE'

Parquet Reader
  |
  +-- Read footer (1 ms)
  +-- Check statistics (5 ms)
  +-- Skip 99% of row groups
  +-- Read 1% of data
  |
  v
Result (< 1 second)
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
# BANKING SCENARIO: Time-Range Queries with Predicate Pushdown
# ============================================================

def generate_time_series_data(num_rows=2_000_000):
    """Generate time-series transaction data."""
    random.seed(42)
    np.random.seed(42)

    # Generate dates over 3 years
    start_date = datetime(2024, 1, 1)
    dates = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d")
             for i in range(365 * 3)]

    # Generate timestamps within each day
    all_timestamps = []
    all_dates = []
    for date_str in dates:
        for _ in range(num_rows // (365 * 3)):
            hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            timestamp = f"{date_str} {hour:02d}:{minute:02d}:00"
            all_timestamps.append(timestamp)
            all_dates.append(date_str)

    # Pad or truncate to exact num_rows
    while len(all_timestamps) < num_rows:
        all_timestamps.append(all_timestamps[-1])
        all_dates.append(all_dates[-1])
    all_timestamps = all_timestamps[:num_rows]
    all_dates = all_dates[:num_rows]

    table = pa.table({
        "transaction_id": pa.array(list(range(1, num_rows + 1)), type=pa.int64()),
        "account_id": pa.array([f"ACC{random.randint(100000, 999999)}" for _ in range(num_rows)], type=pa.string()),
        "amount": pa.array(np.random.uniform(1.0, 100000.0, num_rows).round(2), type=pa.float64()),
        "transaction_type": pa.array(np.random.choice(
            ["DEBIT", "CREDIT", "TRANSFER", "WIRE", "ACH"], num_rows
        ), type=pa.string()),
        "status": pa.array(np.random.choice(
            ["COMPLETED", "PENDING", "FAILED"], num_rows, p=[0.90, 0.07, 0.03]
        ), type=pa.string()),
        "channel": pa.array(np.random.choice(
            ["ONLINE", "MOBILE", "BRANCH", "ATM", "POS"], num_rows
        ), type=pa.string()),
        "transaction_date": pa.array(all_dates, type=pa.date32()),
        "transaction_timestamp": pa.array(all_timestamps, type=pa.string()),
    })

    return table


def store_time_series_data(table, base_path):
    """Store time-series data partitioned by date."""
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

    print(f"Stored {table.num_rows:,} rows")
    print(f"Total size: {total_size / (1024*1024):.1f} MB")


def query_with_pushdown(base_path, start_date, end_date, tx_type=None):
    """Query with predicate pushdown."""
    start_time = time.time()

    filters = [
        ("transaction_date", ">=", start_date),
        ("transaction_date", "<=", end_date),
    ]

    if tx_type:
        filters.append(("transaction_type", "=", tx_type))

    dataset = pq.ParquetDataset(
        base_path,
        filters=filters,
        use_legacy_dataset=False,
    )

    table = dataset.read(columns=["transaction_id", "amount", "transaction_type", "status"])
    elapsed = time.time() - start_time

    print(f"\n=== Query Results ===")
    print(f"Date range: {start_date} to {end_date}")
    if tx_type:
        print(f"Type filter: {tx_type}")
    print(f"Rows returned: {table.num_rows:,}")
    print(f"Query time: {elapsed:.3f}s")
    print(f"Total rows in dataset: 2,000,000")
    print(f"Rows scanned: {table.num_rows / 2_000_000 * 100:.2f}%")

    return table


def compare_pushdown_scenarios(base_path):
    """Compare different pushdown scenarios."""
    scenarios = [
        ("No filter", []),
        ("Date range (1 month)", [("transaction_date", ">=", "2026-08-01"), ("transaction_date", "<=", "2026-08-31")]),
        ("Date range (1 week)", [("transaction_date", ">=", "2026-08-18"), ("transaction_date", "<=", "2026-08-24")]),
        ("Date + Type", [("transaction_date", ">=", "2026-08-01"), ("transaction_date", "<=", "2026-08-31"), ("transaction_type", "=", "WIRE")]),
        ("Date + Status", [("transaction_date", ">=", "2026-08-01"), ("transaction_date", "<=", "2026-08-31"), ("status", "=", "COMPLETED")]),
    ]

    print(f"\n=== Pushdown Comparison ===")
    print(f"{'Scenario':<25} {'Rows':<12} {'Time (s)':<10} {'Reduction':<12}")
    print("-" * 59)

    for name, filters in scenarios:
        start = time.time()
        if filters:
            dataset = pq.ParquetDataset(base_path, filters=filters, use_legacy_dataset=False)
        else:
            dataset = pq.ParquetDataset(base_path, use_legacy_dataset=False)
        table = dataset.read(columns=["transaction_id", "amount"])
        elapsed = time.time() - start

        reduction = (1 - table.num_rows / 2_000_000) * 100
        print(f"{name:<25} {table.num_rows:<12,} {elapsed:<10.3f} {reuction:<12.1f}%")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "pushdown_data")
    os.makedirs(base_path, exist_ok=True)

    # Generate and store data
    print("Generating time-series data...")
    table = generate_time_series_data(num_rows=2_000_000)
    store_time_series_data(table, base_path)

    # Query with pushdown
    query_with_pushdown(base_path, "2026-08-01", "2026-08-31")
    query_with_pushdown(base_path, "2026-08-01", "2026-08-31", tx_type="WIRE")

    # Compare scenarios
    compare_pushdown_scenarios(base_path)
```

---

## 5. Banking Scenario 2: Multi-Dimensional Filtering

### Problem
A bank's analytics team needs to answer complex questions:
- "Premium customers who used online channel in August 2026"
- "Wire transfers over $10,000 from New York branches"
- "Failed transactions by channel and region"

These queries involve multiple filter dimensions (date, amount, type, channel, region).

### Why Pushdown Matters?
- Each additional filter can eliminate more data
- Combined filters can reduce I/O by 99.9%
- Without pushdown, even simple filters are slow on large datasets

### Architecture
```
Multi-dimensional Query
       |
       v
  Parquet Statistics Check
       |
       +-- Filter 1: date → eliminates 70%
       +-- Filter 2: type → eliminates 80% of remaining
       +-- Filter 3: amount → eliminates 90% of remaining
       +-- Filter 4: channel → eliminates 50% of remaining
       |
       v
  Net: 0.3% of data read
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
# BANKING SCENARIO: Multi-Dimensional Filtering
# ============================================================

def generate_multi_dimensional_data(num_rows=1_000_000):
    """Generate data with multiple filterable dimensions."""
    random.seed(42)
    np.random.seed(42)

    regions = ["NORTH", "SOUTH", "EAST", "WEST"]
    branches = {f"BR{i:03d}": random.choice(regions) for i in range(1, 101)}

    table = pa.table({
        "transaction_id": pa.array(list(range(1, num_rows + 1)), type=pa.int64()),
        "account_id": pa.array([f"ACC{random.randint(100000, 999999)}" for _ in range(num_rows)], type=pa.string()),
        "branch_id": pa.array([f"BR{random.randint(1, 100):03d}" for _ in range(num_rows)], type=pa.string()),
        "amount": pa.array(np.random.lognormal(6, 2, num_rows).round(2), type=pa.float64()),
        "transaction_type": pa.array(np.random.choice(
            ["DEBIT", "CREDIT", "TRANSFER", "WIRE", "ACH"], num_rows
        ), type=pa.string()),
        "channel": pa.array(np.random.choice(
            ["ONLINE", "MOBILE", "BRANCH", "ATM", "POS"], num_rows
        ), type=pa.string()),
        "status": pa.array(np.random.choice(
            ["COMPLETED", "PENDING", "FAILED"], num_rows, p=[0.90, 0.07, 0.03]
        ), type=pa.string()),
        "customer_segment": pa.array(np.random.choice(
            ["PREMIUM", "STANDARD", "BASIC"], num_rows, p=[0.2, 0.5, 0.3]
        ), type=pa.string()),
        "transaction_date": pa.array([
            (datetime(2026, 1, 1) + timedelta(days=random.randint(0, 364))).strftime("%Y-%m-%d")
            for _ in range(num_rows)
        ], type=pa.date32()),
    })

    return table


def store_multi_dimensional_data(table, base_path):
    """Store data with multiple partition columns."""
    pq.write_to_dataset(
        table,
        root_path=base_path,
        partition_cols=["transaction_date", "channel"],
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
    )


def query_multi_dimensional(base_path, filters, columns=None):
    """Query with multi-dimensional filters."""
    start = time.time()

    dataset = pq.ParquetDataset(
        base_path,
        filters=filters,
        use_legacy_dataset=False,
    )

    if columns:
        table = dataset.read(columns=columns)
    else:
        table = dataset.read()

    elapsed = time.time() - start

    return table.num_rows, elapsed


def run_multi_dimensional_analysis(base_path):
    """Run various multi-dimensional queries."""
    queries = [
        ("All data", [], None),
        ("Single date", [("transaction_date", "=", "2026-08-24")], None),
        ("Date range", [("transaction_date", ">=", "2026-08-01"), ("transaction_date", "<=", "2026-08-31")], None),
        ("Date + Channel", [
            ("transaction_date", ">=", "2026-08-01"),
            ("transaction_date", "<=", "2026-08-31"),
            ("channel", "=", "ONLINE")
        ], None),
        ("Date + Channel + Type", [
            ("transaction_date", ">=", "2026-08-01"),
            ("transaction_date", "<=", "2026-08-31"),
            ("channel", "=", "ONLINE"),
            ("transaction_type", "=", "WIRE")
        ], None),
        ("Date + Channel + Amount", [
            ("transaction_date", ">=", "2026-08-01"),
            ("transaction_date", "<=", "2026-08-31"),
            ("channel", "=", "ONLINE"),
            ("amount", ">", 10000)
        ], None),
        ("All filters", [
            ("transaction_date", ">=", "2026-08-01"),
            ("transaction_date", "<=", "2026-08-31"),
            ("channel", "=", "ONLINE"),
            ("transaction_type", "=", "WIRE"),
            ("amount", ">", 10000),
            ("status", "=", "COMPLETED")
        ], ["transaction_id", "amount", "channel", "status"]),
    ]

    total_rows = 1_000_000

    print(f"\n=== Multi-Dimensional Filtering Analysis ===")
    print(f"{'Query':<30} {'Rows':<12} {'Time (s)':<10} {'Reduction':<12}")
    print("-" * 64)

    for name, filters, columns in queries:
        rows, elapsed = query_multi_dimensional(base_path, filters, columns)
        reduction = (1 - rows / total_rows) * 100
        print(f"{name:<30} {rows:<12,} {elapsed:<10.3f} {reduction:<12.1f}%")

    # Show specific results
    print(f"\n=== Sample Query Results ===")
    rows, elapsed = query_multi_dimensional(
        base_path,
        [
            ("transaction_date", ">=", "2026-08-01"),
            ("transaction_date", "<=", "2026-08-31"),
            ("channel", "=", "ONLINE"),
            ("amount", ">", 10000),
        ],
        ["transaction_id", "amount", "channel", "status"]
    )
    print(f"Online transactions > $10,000 in August 2026: {rows:,} rows")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "multi_dim_data")
    os.makedirs(base_path, exist_ok=True)

    # Generate and store data
    print("Generating multi-dimensional data...")
    table = generate_multi_dimensional_data(num_rows=1_000_000)
    store_multi_dimensional_data(table, base_path)

    # Run analysis
    run_multi_dimensional_analysis(base_path)
```

---

## 7. Interview Questions

### Q1: Explain predicate pushdown and why it's important for Parquet.

**Answer:**

Predicate pushdown is an optimization where **filter conditions are evaluated at the storage layer** before data is read into memory.

**How it works:**
1. Parquet stores min/max statistics for each column in each row group
2. When a filter is applied, the engine checks statistics first
3. If statistics show no values can match → skip entire row group
4. Only matching row groups are read from disk

**Example:**
```
Row Group 1: date min=2026-01-01, max=2026-03-31
Row Group 2: date min=2026-04-01, max=2026-07-31
Row Group 3: date min=2026-08-01, max=2026-08-31

Filter: WHERE date >= '2026-08-01'

Row Group 1: max < 2026-08-01 → SKIP
Row Group 2: max < 2026-08-01 → SKIP
Row Group 3: min >= 2026-08-01 → READ
```

**Importance:**
- Reduces I/O by 90-99%
- Enables sub-second queries on TB-scale data
- Works across partitioned and non-partitioned data

---

### Q2: What statistics does Parquet store for predicate pushdown?

**Answer:**

Parquet stores these statistics per column per row group:

1. **Min value**: Lowest value in the column chunk
2. **Max value**: Highest value in the column chunk
3. **Null count**: Number of null values
4. **Distinct count**: Number of distinct values (if enabled)
5. **Count**: Total number of values

**Additional statistics (newer Parquet versions):**
- **Page-level statistics**: Min/max per page within a row group
- **Bloom filters**: Probabilistic structure for equality checks
- **Column indexes**: Zone maps for page-level filtering

**Example:**
```
Column: amount
  Row Group 1:
    min = 1.00
    max = 50000.00
    null_count = 0
    distinct_count = 950000
    count = 1000000
```

---

### Q3: What are the limitations of predicate pushdown in Parquet?

**Answer:**

**Fully supported:**
- Equality (`=`, `!=`)
- Range (`<`, `>`, `<=`, `>=`, `BETWEEN`)
- In list (`IN`)
- Is null / Is not null

**Limited support:**
- `LIKE` patterns (only prefix matching)
- `OR` conditions (may not push down fully)
- Complex expressions (may partially push down)

**Not supported:**
- Subqueries
- Functions (`UPPER(col) = 'VALUE'`)
- Negation (`NOT IN`)

**Workarounds:**
1. Rewrite queries to use supported filters
2. Use Iceberg/Delta Lake for more advanced pushdown
3. Pre-filter data in ETL pipeline

---

### Q4: How does predicate pushdown differ between Parquet and CSV?

**Answer:**

| Feature | Parquet | CSV |
|---------|---------|-----|
| Statistics | ✅ Min/max per column per row group | ❌ None |
| Pushdown | ✅ Automatic | ❌ Not possible |
| Column pruning | ✅ Read only needed columns | ❌ Read entire row |
| Skip row groups | ✅ Based on statistics | ❌ Must read all |
| Filter at storage | ✅ Before reading data | ❌ After reading all data |

**Example:**
```sql
SELECT * FROM data WHERE date = '2026-08-24'

CSV:    Read ALL 100 GB → Filter → Return 1 GB
Parquet: Check statistics → Read 1 GB → Return 1 GB
```

**Speedup: 100x**

---

### Q5: How can you verify that predicate pushdown is working?

**Answer:**

**Method 1: Check query plan (Spark)**
```python
df.filter(col("date") >= "2026-08-01").explain()
# Look for "PushedFilters" in the plan
```

**Method 2: Measure performance**
```python
# Without filter
start = time.time()
pq.read_table("large_file.parquet")
print(f"Without filter: {time.time() - start:.3f}s")

# With filter
start = time.time()
pq.read_table("large_file.parquet", filters=[("date", ">=", "2026-08-01")])
print(f"With filter: {time.time() - start:.3f}s")
```

**Method 3: Check row count**
```python
# If pushdown works, row count should be much smaller
table = pq.read_table("file.parquet", filters=[...])
print(f"Rows returned: {table.num_rows}")  # Should be << total rows
```

**Method 4: Read metadata**
```python
metadata = pq.read_metadata("file.parquet")
for i in range(metadata.num_row_groups):
    rg = metadata.row_group(i)
    print(f"Row Group {i}: {rg.num_rows} rows")
```
