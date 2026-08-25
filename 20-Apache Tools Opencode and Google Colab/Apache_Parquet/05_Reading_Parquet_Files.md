# Reading Parquet Files

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-regulatory-audit-query)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-real-time-risk-dashboard)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

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

## 2. Example

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

## 3. Banking Scenario 1: Regulatory Audit Query

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

## 5. Banking Scenario 2: Real-Time Risk Dashboard

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

## 6. Python Code - Scenario 2

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

## 7. Interview Questions

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
