# Parquet with DuckDB

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-ad-hoc-analytics)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-data-profiling)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### DuckDB + Parquet: The Perfect Match

DuckDB is an **embedded analytical SQL database** that reads Parquet files directly, without loading them into memory:

> **DuckDB treats Parquet files as if they were database tables. You can run SQL queries directly on Parquet files on disk — no ETL, no data loading, no conversion.**

### Why DuckDB + Parquet?

```
Traditional Approach:
  Parquet → Load into DB → Query → Results

DuckDB Approach:
  Parquet ← Query directly → Results
```

**Benefits:**
1. **No data loading**: Query Parquet files directly
2. **No data movement**: Data stays on disk
3. **Full SQL support**: JOINs, aggregations, window functions
4. **Automatic optimization**: Query planner optimizes Parquet reads
5. **Memory efficient**: Only reads needed data

### How DuckDB Reads Parquet

```
SQL Query
    ↓
DuckDB Query Planner
    ↓
Parquet Reader (reads footer first)
    ↓
Row Group Selection (predicate pushdown)
    ↓
Column Pruning (only needed columns)
    ↓
Decompression + Decoding
    ↓
Vectorized Execution
    ↓
Result
```

### DuckDB Parquet Features

1. **Direct file queries**:
```sql
SELECT * FROM read_parquet('file.parquet');
```

2. **Glob patterns**:
```sql
SELECT * FROM read_parquet('s3://bucket/data/*.parquet');
```

3. **Partitioned datasets**:
```sql
SELECT * FROM read_parquet('data/**/*.parquet');
```

4. **Multiple files**:
```sql
SELECT * FROM read_parquet(['file1.parquet', 'file2.parquet']);
```

5. **Predicate pushdown**:
```sql
SELECT * FROM read_parquet('file.parquet') WHERE amount > 1000;
```

6. **Column pruning**:
```sql
SELECT amount, status FROM read_parquet('file.parquet');
```

### DuckDB SQL on Parquet

```python
import duckdb

con = duckdb.connect()

# Simple query
result = con.execute("""
    SELECT status, COUNT(*), SUM(amount)
    FROM read_parquet('transactions.parquet')
    GROUP BY status
""").fetchdf()

# Complex query with JOINs
result = con.execute("""
    SELECT 
        a.account_id,
        a.customer_name,
        SUM(t.amount) as total_amount
    FROM read_parquet('accounts.parquet') a
    JOIN read_parquet('transactions.parquet') t
        ON a.account_id = t.account_id
    WHERE t.date >= '2026-08-01'
    GROUP BY a.account_id, a.customer_name
    ORDER BY total_amount DESC
""").fetchdf()

# Window functions
result = con.execute("""
    SELECT 
        transaction_id,
        amount,
        SUM(amount) OVER (PARTITION BY account_id ORDER BY date) as running_total
    FROM read_parquet('transactions.parquet')
""").fetchdf()
```

### DuckDB vs Pandas for Parquet

| Feature | DuckDB | Pandas |
|---------|--------|--------|
| Memory usage | Efficient (streaming) | Loads entire file |
| SQL support | Full SQL | DataFrame API |
| Large files | Yes (> RAM) | Limited by RAM |
| Multiple files | Native | Manual concat |
| Optimization | Automatic | Manual |
| Best for | SQL queries | Data manipulation |

### DuckDB + Parquet Architecture

```
Python Application
       |
       v
  DuckDB (embedded)
       |
       v
  SQL Query Planner
       |
       v
  Parquet Reader
       |
       +-- Predicate pushdown
       +-- Column pruning
       +-- Parallel reads
       |
       v
  S3 / GCS / Local Disk
       |
       v
  Parquet Files
```

---

## 2. Example

### DuckDB Querying Parquet Files

```python
import duckdb
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import os
import tempfile

# Create sample data
np.random.seed(42)
num_rows = 100_000

df = pd.DataFrame({
    "transaction_id": range(1, num_rows + 1),
    "account_id": [f"ACC{i:06d}" for i in range(1, num_rows + 1)],
    "amount": np.random.uniform(1.0, 100000.0, num_rows).round(2),
    "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
    "date": pd.date_range("2026-01-01", periods=num_rows, freq="30s"),
})

# Write to Parquet
pq.write_table(
    pq.Table.from_pandas(df),
    "transactions.parquet",
    compression="zstd"
)

# Query with DuckDB
con = duckdb.connect()

# 1. Simple aggregation
result = con.execute("""
    SELECT 
        status,
        COUNT(*) as count,
        SUM(amount) as total,
        AVG(amount) as avg_amount
    FROM read_parquet('transactions.parquet')
    GROUP BY status
    ORDER BY total DESC
""").fetchdf()
print("Status Summary:")
print(result)

# 2. Filtered query
result = con.execute("""
    SELECT COUNT(*), SUM(amount)
    FROM read_parquet('transactions.parquet')
    WHERE amount > 50000 AND status = 'COMPLETED'
""").fetchdf()
print(f"\nHigh-value completed: {result['count_star()[0]']}")
print(f"Total amount: ${result['sum(amount)'][0]:,.2f}")

# 3. Window function
result = con.execute("""
    SELECT 
        transaction_id,
        amount,
        SUM(amount) OVER (ORDER BY transaction_id) as running_total
    FROM read_parquet('transactions.parquet')
    LIMIT 10
""").fetchdf()
print(f"\nRunning totals:")
print(result)
```

---

## 3. Banking Scenario 1: Ad-Hoc Analytics

### Problem
A bank's analyst needs to answer urgent questions:
- "What's the total transaction volume by branch this month?"
- "Which customers have the highest balance?"
- "What's the fraud rate by channel?"

Data is stored in Parquet files on S3. Analysts use DuckDB for fast ad-hoc queries.

### Why DuckDB + Parquet?
- No data loading required
- SQL interface familiar to analysts
- Automatic optimization (predicate pushdown, column pruning)
- Results in seconds, not minutes

### Architecture
```
S3 Data Lake (Parquet)
       |
       v
  DuckDB (embedded, SQL)
       |
       v
  Analyst (Jupyter / Python)
       |
       v
  Business Insights
```

---

## 4. Python Code - Scenario 1

```python
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime, timedelta
import random
import os
import tempfile
import time

# ============================================================
# BANKING SCENARIO: Ad-Hoc Analytics with DuckDB + Parquet
# ============================================================

def generate_analytics_dataset():
    """Generate transaction data for analytics."""
    np.random.seed(42)

    num_transactions = 500_000
    num_customers = 10_000
    num_branches = 100

    # Transactions
    transactions = pd.DataFrame({
        "transaction_id": range(1, num_transactions + 1),
        "account_id": [f"ACC{random.randint(100000, 999999)}" for _ in range(num_transactions)],
        "customer_id": [f"CUST{random.randint(1, num_customers):06d}" for _ in range(num_transactions)],
        "branch_id": [f"BR{random.randint(1, num_branches):03d}" for _ in range(num_transactions)],
        "amount": np.random.lognormal(6, 2, num_transactions).round(2),
        "currency": np.random.choice(["USD", "EUR", "GBP"], num_transactions),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_transactions, p=[0.90, 0.07, 0.03]),
        "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM", "POS"], num_transactions),
        "date": pd.date_range("2026-01-01", periods=num_transactions, freq="1min"),
    })

    # Customers
    customers = pd.DataFrame({
        "customer_id": [f"CUST{i:06d}" for i in range(1, num_customers + 1)],
        "name": [f"Customer {i}" for i in range(1, num_customers + 1)],
        "segment": np.random.choice(["PREMIUM", "STANDARD", "BASIC"], num_customers),
        "balance": np.random.lognormal(10, 2, num_customers).round(2),
    })

    # Branches
    branches = pd.DataFrame({
        "branch_id": [f"BR{i:03d}" for i in range(1, num_branches + 1)],
        "branch_name": [f"Branch {i}" for i in range(1, num_branches + 1)],
        "region": np.random.choice(["NORTH", "SOUTH", "EAST", "WEST"], num_branches),
    })

    return transactions, customers, branches


def store_datasets(transactions, customers, branches, base_path):
    """Store datasets as Parquet files."""
    pq.write_table(pq.Table.from_pandas(transactions), os.path.join(base_path, "transactions.parquet"), compression="zstd")
    pq.write_table(pq.Table.from_pandas(customers), os.path.join(base_path, "customers.parquet"), compression="zstd")
    pq.write_table(pq.Table.from_pandas(branches), os.path.join(base_path, "branches.parquet"), compression="zstd")


def run_analytics(base_path):
    """Run ad-hoc analytics queries with DuckDB."""
    con = duckdb.connect()

    # 1. Transaction volume by branch
    start = time.time()
    result = con.execute(f"""
        SELECT 
            branch_id,
            COUNT(*) as tx_count,
            SUM(amount) as total_volume,
            AVG(amount) as avg_amount
        FROM read_parquet('{base_path}/transactions.parquet')
        WHERE status = 'COMPLETED'
        GROUP BY branch_id
        ORDER BY total_volume DESC
        LIMIT 10
    """).fetchdf()
    elapsed = time.time() - start

    print(f"\n=== Top 10 Branches by Volume ===")
    print(f"Query time: {elapsed:.3f}s")
    print(result.to_string(index=False))

    # 2. Customer balance analysis
    start = time.time()
    result = con.execute(f"""
        SELECT 
            c.segment,
            COUNT(DISTINCT c.customer_id) as customer_count,
            AVG(c.balance) as avg_balance,
            SUM(c.balance) as total_balance
        FROM read_parquet('{base_path}/customers.parquet') c
        GROUP BY c.segment
        ORDER BY total_balance DESC
    """).fetchdf()
    elapsed = time.time() - start

    print(f"\n=== Customer Segment Analysis ===")
    print(f"Query time: {elapsed:.3f}s")
    print(result.to_string(index=False))

    # 3. Fraud rate by channel
    start = time.time()
    result = con.execute(f"""
        SELECT 
            channel,
            COUNT(*) as total_tx,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_tx,
            ROUND(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as failure_rate_pct
        FROM read_parquet('{base_path}/transactions.parquet')
        GROUP BY channel
        ORDER BY failure_rate_pct DESC
    """).fetchdf()
    elapsed = time.time() - start

    print(f"\n=== Failure Rate by Channel ===")
    print(f"Query time: {elapsed:.3f}s")
    print(result.to_string(index=False))

    # 4. JOIN query: Transactions with customer info
    start = time.time()
    result = con.execute(f"""
        SELECT 
            c.segment,
            COUNT(*) as tx_count,
            SUM(t.amount) as total_amount,
            AVG(t.amount) as avg_amount
        FROM read_parquet('{base_path}/transactions.parquet') t
        JOIN read_parquet('{base_path}/customers.parquet') c
            ON t.customer_id = c.customer_id
        WHERE t.status = 'COMPLETED'
        GROUP BY c.segment
        ORDER BY total_amount DESC
    """).fetchdf()
    elapsed = time.time() - start

    print(f"\n=== Transaction Analysis by Customer Segment ===")
    print(f"Query time: {elapsed:.3f}s")
    print(result.to_string(index=False))

    # 5. Time series analysis
    start = time.time()
    result = con.execute(f"""
        SELECT 
            DATE_TRUNC('day', date) as day,
            COUNT(*) as tx_count,
            SUM(amount) as daily_volume
        FROM read_parquet('{base_path}/transactions.parquet')
        WHERE status = 'COMPLETED'
        GROUP BY DATE_TRUNC('day', date)
        ORDER BY day
        LIMIT 30
    """).fetchdf()
    elapsed = time.time() - start

    print(f"\n=== Daily Transaction Volume (First 30 Days) ===")
    print(f"Query time: {elapsed:.3f}s")
    print(result.head(10).to_string(index=False))


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "duckdb_analytics")
    os.makedirs(base_path, exist_ok=True)

    # Generate and store data
    print("Generating analytics dataset...")
    transactions, customers, branches = generate_analytics_dataset()
    store_datasets(transactions, customers, branches, base_path)

    # Run analytics
    run_analytics(base_path)
```

---

## 5. Banking Scenario 2: Data Profiling

### Problem
A bank needs to profile data quality across all Parquet tables:
- Column statistics (min, max, mean, nulls)
- Data distribution analysis
- Schema validation
- Anomaly detection

### Why DuckDB + Parquet?
- DuckDB has built-in profiling functions
- Can query Parquet metadata directly
- Fast even on large datasets
- SQL interface for non-technical users

### Architecture
```
Parquet Tables
       |
       v
  DuckDB Profiling Queries
       |
       +-- Schema discovery
       +-- Column statistics
       +- Distribution analysis
       +-- Anomaly detection
       |
       v
  Data Quality Report
```

---

## 6. Python Code - Scenario 2

```python
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import os
import tempfile
import time

# ============================================================
# BANKING SCENARIO: Data Profiling with DuckDB + Parquet
# ============================================================

def generate_transaction_data_for_profiling(num_rows=200_000):
    """Generate transaction data with quality issues for profiling."""
    np.random.seed(42)

    # Introduce some data quality issues
    amounts = np.random.lognormal(6, 2, num_rows).round(2)
    amounts[np.random.choice(num_rows, 100)] = np.nan  # 100 nulls
    amounts[np.random.choice(num_rows, 50)] = -100.0   # 50 negative
    amounts[np.random.choice(num_rows, 20)] = 99999999  # 20 extreme

    df = pd.DataFrame({
        "transaction_id": range(1, num_rows + 1),
        "account_id": [f"ACC{np.random.randint(100000, 999999)}" for _ in range(num_rows)],
        "amount": amounts,
        "currency": np.random.choice(["USD", "EUR", "GBP", None], num_rows, p=[0.7, 0.15, 0.1, 0.05]),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED", ""], num_rows, p=[0.85, 0.08, 0.05, 0.02]),
        "date": pd.date_range("2026-01-01", periods=num_rows, freq="30s"),
    })

    return df


def profile_table(con, table_path, table_name):
    """Profile a Parquet table using DuckDB."""
    print(f"\n{'='*60}")
    print(f"TABLE PROFILING: {table_name}")
    print(f"{'='*60}")

    # 1. Basic stats
    result = con.execute(f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT account_id) as unique_accounts,
            MIN(date) as min_date,
            MAX(date) as max_date
        FROM read_parquet('{table_path}')
    """).fetchdf()
    print(f"\n--- Basic Statistics ---")
    print(result.to_string(index=False))

    # 2. Column-level profiling
    result = con.execute(f"""
        SELECT 
            'amount' as column_name,
            COUNT(*) as total_count,
            COUNT(amount) as non_null_count,
            COUNT(*) - COUNT(amount) as null_count,
            ROUND((COUNT(*) - COUNT(amount)) * 100.0 / COUNT(*), 2) as null_pct,
            ROUND(MIN(amount), 2) as min_value,
            ROUND(MAX(amount), 2) as max_value,
            ROUND(AVG(amount), 2) as avg_value,
            ROUND(STDDEV(amount), 2) as std_dev
        FROM read_parquet('{table_path}')
    """).fetchdf()
    print(f"\n--- Amount Column Profile ---")
    print(result.to_string(index=False))

    # 3. Distribution analysis
    result = con.execute(f"""
        SELECT 
            status,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as pct
        FROM read_parquet('{table_path}')
        GROUP BY status
        ORDER BY count DESC
    """).fetchdf()
    print(f"\n--- Status Distribution ---")
    print(result.to_string(index=False))

    # 4. Currency distribution
    result = con.execute(f"""
        SELECT 
            COALESCE(currency, 'NULL') as currency,
            COUNT(*) as count,
            ROUND(SUM(amount), 2) as total_amount
        FROM read_parquet('{table_path}')
        GROUP BY currency
        ORDER BY total_amount DESC
    """).fetchdf()
    print(f"\n--- Currency Distribution ---")
    print(result.to_string(index=False))

    # 5. Anomaly detection
    result = con.execute(f"""
        SELECT 
            'Negative amounts' as anomaly,
            COUNT(*) as count
        FROM read_parquet('{table_path}')
        WHERE amount < 0
        UNION ALL
        SELECT 
            'Extreme amounts (>1M)' as anomaly,
            COUNT(*) as count
        FROM read_parquet('{table_path}')
        WHERE amount > 1000000
        UNION ALL
        SELECT 
            'Empty status' as anomaly,
            COUNT(*) as count
        FROM read_parquet('{table_path}')
        WHERE status = ''
    """).fetchdf()
    print(f"\n--- Anomaly Detection ---")
    print(result.to_string(index=False))


def run_data_profiling(base_path):
    """Run comprehensive data profiling."""
    con = duckdb.connect()

    # Profile transaction table
    profile_table(
        con,
        os.path.join(base_path, "transactions.parquet"),
        "transactions"
    )

    # Cross-table analysis
    result = con.execute(f"""
        SELECT 
            t.currency,
            COUNT(*) as tx_count,
            SUM(t.amount) as total_amount,
            AVG(t.amount) as avg_amount
        FROM read_parquet('{base_path}/transactions.parquet') t
        GROUP BY t.currency
        ORDER BY total_amount DESC
    """).fetchdf()

    print(f"\n--- Cross-Table Analysis ---")
    print(result.to_string(index=False))


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "data_profiling")
    os.makedirs(base_path, exist_ok=True)

    # Generate data
    print("Generating transaction data...")
    df = generate_transaction_data_for_profiling(num_rows=200_000)

    # Save as Parquet
    pq.write_table(
        pq.Table.from_pandas(df),
        os.path.join(base_path, "transactions.parquet"),
        compression="zstd"
    )

    # Run profiling
    run_data_profiling(base_path)
```

---

## 7. Interview Questions

### Q1: Why is DuckDB particularly well-suited for querying Parquet files?

**Answer:**

1. **Zero-copy reads**: DuckDB reads Parquet files directly without loading into memory
2. **Automatic optimization**: Query planner applies predicate pushdown and column pruning automatically
3. **Vectorized execution**: Processes data in batches for high throughput
4. **Parallel reads**: Reads multiple row groups in parallel
5. **Memory efficient**: Only reads needed data, handles datasets larger than RAM
6. **Full SQL support**: Complex queries (JOINs, window functions, CTEs) on Parquet files

```python
import duckdb
# Direct Parquet query - no loading required
con = duckdb.connect()
result = con.execute("""
    SELECT status, SUM(amount)
    FROM read_parquet('s3://bucket/transactions/*.parquet')
    WHERE date >= '2026-08-01'
    GROUP BY status
""").fetchdf()
```

---

### Q2: How does DuckDB optimize Parquet queries?

**Answer:**

DuckDB applies several optimizations:

1. **Predicate pushdown**: Filters pushed to Parquet reader
```sql
-- DuckDB pushes this filter to Parquet
WHERE amount > 1000
-- Parquet skips row groups where max(amount) < 1000
```

2. **Column pruning**: Only reads needed columns
```sql
-- Only reads 'amount' and 'status' columns
SELECT amount, status FROM read_parquet('file.parquet')
```

3. **Parallel row group reads**: Reads multiple row groups simultaneously
4. **Late materialization**: Delays type conversions until needed
5. **Filter pushdown into Parquet pages**: Checks page-level statistics

---

### Q3: What are the limitations of DuckDB for Parquet queries?

**Answer:**

1. **Single-node**: DuckDB runs on one machine (not distributed)
2. **Memory constraints**: Very large datasets may require streaming
3. **Write limitations**: DuckDB can write Parquet but not as efficiently as Spark
4. **No ACID on Parquet**: DuckDB doesn't provide transaction support on Parquet files
5. **Schema evolution**: Limited support for schema changes across files

**When to use alternatives:**
- **Spark**: Distributed processing, > 1 TB data
- **Trino**: Distributed SQL, multi-user concurrent queries
- **DuckDB**: Ad-hoc analysis, < 1 TB, single user

---

### Q4: How do you read multiple Parquet files with DuckDB?

**Answer:**

**Glob patterns:**
```sql
SELECT * FROM read_parquet('s3://bucket/data/*.parquet');
```

**Multiple specific files:**
```sql
SELECT * FROM read_parquet(['file1.parquet', 'file2.parquet']);
```

**Recursive directory:**
```sql
SELECT * FROM read_parquet('data/**/*.parquet');
```

**Partitioned datasets:**
```sql
SELECT * FROM read_parquet('data/year=2026/month=08/*.parquet');
```

**From Python:**
```python
import duckdb
con = duckdb.connect()

# All files in directory
df = con.execute("""
    SELECT * FROM read_parquet('/path/to/data/*.parquet')
    WHERE amount > 1000
""").fetchdf()
```

---

### Q5: Compare DuckDB, Pandas, and Spark for Parquet operations.

**Answer:**

| Feature | DuckDB | Pandas | Spark |
|---------|--------|--------|-------|
| **Architecture** | Embedded | Library | Distributed |
| **Memory model** | Streaming | In-memory | Distributed |
| **SQL support** | Full SQL | DataFrame API | Spark SQL |
| **Large data** | Yes (> RAM) | Limited | Yes (TB+) |
| **Parallelism** | Multi-threaded | Single-threaded | Distributed |
| **Setup** | pip install | pip install | Cluster setup |
| **Best for** | Ad-hoc analysis | Data manipulation | Large-scale ETL |

**Decision framework:**
- **< 10 GB, SQL queries**: DuckDB
- **< 10 GB, data manipulation**: Pandas
- **> 10 GB, distributed processing**: Spark
- **Production data pipelines**: Spark or DuckDB (depending on scale)
- **Interactive analysis**: DuckDB
