# DuckDB and Parquet

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-data-lake-analytics)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-reporting-pipeline)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### DuckDB + Parquet: The Perfect Combination

> **DuckDB reads Parquet files directly as if they were database tables — no ETL, no data loading, no conversion. This makes it the ideal query engine for Parquet-based data lakes.**

### How DuckDB Reads Parquet

```
SQL Query
    ↓
DuckDB Query Planner
    ↓
Parquet Reader
    ↓
┌─────────────────────────┐
│ Read Footer (metadata)  │  ← Schema, row groups, statistics
│ Check Statistics         │  ← Predicate pushdown
│ Select Row Groups        │  ← Skip irrelevant data
│ Read Column Chunks       │  ← Column pruning
│ Decompress Pages         │  ← Snappy/Zstd/Gzip
│ Decode Pages             │  ← Dictionary/plain encoding
└─────────────────────────┘
    ↓
Arrow Memory Format
    ↓
Result (DataFrame / Table)
```

### DuckDB Parquet Features

#### 1. Direct File Queries

```sql
-- Single file
SELECT * FROM read_parquet('file.parquet')

-- Multiple files
SELECT * FROM read_parquet(['file1.parquet', 'file2.parquet'])

-- Glob pattern
SELECT * FROM read_parquet('data/*.parquet')

-- Recursive
SELECT * FROM read_parquet('data/**/*.parquet')
```

#### 2. Automatic Optimizations

```sql
-- Predicate pushdown (filters pushed to Parquet reader)
SELECT * FROM read_parquet('file.parquet') WHERE amount > 1000

-- Column pruning (only reads needed columns)
SELECT amount, status FROM read_parquet('file.parquet')

-- Partition pruning (skips irrelevant partitions)
SELECT * FROM read_parquet('data/year=2026/**/*.parquet') WHERE year = 2026
```

#### 3. Schema Handling

```sql
-- Auto-detect schema
SELECT * FROM read_parquet('file.parquet')

-- Specify schema
SELECT * FROM read_parquet('file.parquet', hive_partitioning=true)

-- Handle schema evolution
SELECT * FROM read_parquet(['v1.parquet', 'v2.parquet'])
```

#### 4. File Metadata

```sql
-- Read Parquet metadata
SELECT * FROM parquet_metadata('file.parquet')

-- Read file statistics
SELECT * FROM parquet_statistics('file.parquet')
```

### DuckDB Parquet Performance

| Operation | DuckDB | Pandas | Speedup |
|-----------|--------|--------|---------|
| Read 1GB Parquet | 0.5s | 2.0s | 4x |
| Filter + Aggregation | 0.3s | 1.5s | 5x |
| JOIN two files | 0.8s | 3.0s | 4x |
| Complex analytics | 1.2s | 5.0s | 4x |

### DuckDB Parquet Best Practices

```
1. Query Parquet directly (don't load into memory)
   SELECT * FROM read_parquet('file.parquet')

2. Use column pruning
   SELECT amount, status FROM read_parquet('file.parquet')
   -- Not: SELECT * FROM read_parquet('file.parquet')

3. Use predicate pushdown
   WHERE amount > 1000
   -- DuckDB pushes this to Parquet reader

4. Query multiple files efficiently
   SELECT * FROM read_parquet('s3://bucket/*.parquet')

5. Use partitions
   SELECT * FROM read_parquet('data/year=2026/**/*.parquet')
```

---

## 2. Example

### DuckDB + Parquet Demo

```python
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import os
import tempfile
import time

# Create sample data
np.random.seed(42)
num_rows = 1_000_000

df = pd.DataFrame({
    "transaction_id": range(1, num_rows + 1),
    "account_id": [f"ACC{np.random.randint(100000, 999999)}" for _ in range(num_rows)],
    "amount": np.random.uniform(1.0, 100000.0, num_rows).round(2),
    "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
    "date": pd.date_range("2026-01-01", periods=num_rows, freq="30s"),
})

# Write to Parquet
parquet_path = os.path.join(tempfile.mkdtemp(), "transactions.parquet")
pq.write_table(pq.Table.from_pandas(df), parquet_path, compression="zstd")

# Query with DuckDB
con = duckdb.connect()

# 1. Simple query
start = time.time()
result = con.execute(f"""
    SELECT status, COUNT(*), SUM(amount)
    FROM read_parquet('{parquet_path}')
    GROUP BY status
""").fetchdf()
print(f"Query 1: {time.time() - start:.3f}s")
print(result)

# 2. Filtered query (predicate pushdown)
start = time.time()
result = con.execute(f"""
    SELECT COUNT(*), SUM(amount)
    FROM read_parquet('{parquet_path}')
    WHERE amount > 50000 AND status = 'COMPLETED'
""").fetchdf()
print(f"\nQuery 2: {time.time() - start:.3f}s")
print(result)

# 3. Column pruning
start = time.time()
result = con.execute(f"""
    SELECT amount, status
    FROM read_parquet('{parquet_path}')
    LIMIT 1000
""").fetchdf()
print(f"\nQuery 3: {time.time() - start:.3f}s")
print(f"Rows: {len(result)}")

con.close()
```

---

## 3. Banking Scenario 1: Data Lake Analytics

### Problem
A bank has 10 TB of Parquet files in S3:
- Transactions (5 TB)
- Customers (1 TB)
- Accounts (1 TB)
- Loans (1 TB)
- Payments (2 TB)

Analysts need to query this data without loading it into a database.

### Why DuckDB + Parquet?
- Query S3 files directly
- No data movement
- Automatic optimization
- SQL interface for analysts

### Architecture
```
S3 Data Lake (10 TB Parquet)
       |
       v
  DuckDB (embedded, queries S3)
       |
       +-- Predicate pushdown
       +-- Column pruning
       +-- Partition pruning
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
# BANKING SCENARIO: Data Lake Analytics with DuckDB + Parquet
# ============================================================

def generate_data_lake(base_path, num_days=30):
    """Generate a data lake with multiple Parquet files."""
    random.seed(42)
    np.random.seed(42)

    # Transactions (partitioned by date)
    for day in range(num_days):
        date = datetime(2026, 8, 1) + timedelta(days=day)
        date_str = date.strftime("%Y-%m-%d")
        
        day_path = os.path.join(base_path, "transactions", f"date={date_str}")
        os.makedirs(day_path, exist_ok=True)
        
        num_txns = random.randint(5000, 10000)
        table = pa.table({
            "transaction_id": list(range(day * 10000 + 1, day * 10000 + num_txns + 1)),
            "account_id": [f"ACC{random.randint(100000, 999999)}" for _ in range(num_txns)],
            "amount": np.random.lognormal(6, 2, num_txns).round(2),
            "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_txns),
            "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM"], num_txns),
        })
        
        pq.write_table(table, os.path.join(day_path, "data.parquet"), compression="zstd")

    # Customers (single file)
    customers_path = os.path.join(base_path, "customers")
    os.makedirs(customers_path, exist_ok=True)
    
    customers = pa.table({
        "customer_id": [f"CUST{i:05d}" for i in range(1, 10001)],
        "name": [f"Customer {i}" for i in range(1, 10001)],
        "segment": np.random.choice(["PREMIUM", "STANDARD", "BASIC"], 10000),
        "balance": np.random.lognormal(10, 2, 10000).round(2),
    })
    pq.write_table(customers, os.path.join(customers_path, "customers.parquet"), compression="zstd")

    # Accounts (single file)
    accounts_path = os.path.join(base_path, "accounts")
    os.makedirs(accounts_path, exist_ok=True)
    
    accounts = pa.table({
        "account_id": [f"ACC{i:06d}" for i in range(1, 50001)],
        "customer_id": [f"CUST{random.randint(1, 10000):05d}" for _ in range(50000)],
        "account_type": np.random.choice(["CHECKING", "SAVINGS", "CREDIT_CARD"], 50000),
        "balance": np.random.lognormal(10, 2, 50000).round(2),
    })
    pq.write_table(accounts, os.path.join(accounts_path, "accounts.parquet"), compression="zstd")

    print(f"Generated data lake at {base_path}")
    print(f"Transactions: {num_days} days")
    print(f"Customers: 10,000")
    print(f"Accounts: 50,000")


def run_data_lake_queries(base_path):
    """Run analytical queries on the data lake."""
    con = duckdb.connect()

    # 1. Daily transaction summary
    print("\n=== Daily Transaction Summary ===")
    start = time.time()
    result = con.execute(f"""
        SELECT 
            date,
            COUNT(*) as tx_count,
            SUM(amount) as total_amount
        FROM read_parquet('{base_path}/transactions/*/data.parquet')
        GROUP BY date
        ORDER BY date
        LIMIT 10
    """).fetchdf()
    print(f"Query time: {time.time() - start:.3f}s")
    print(result.to_string(index=False))

    # 2. Customer transaction analysis (JOIN)
    print("\n=== Customer Transaction Analysis ===")
    start = time.time()
    result = con.execute(f"""
        SELECT 
            c.segment,
            COUNT(DISTINCT t.account_id) as unique_accounts,
            SUM(t.amount) as total_volume,
            AVG(t.amount) as avg_amount
        FROM read_parquet('{base_path}/transactions/*/data.parquet') t
        JOIN read_parquet('{base_path}/accounts/accounts.parquet') a
            ON t.account_id = a.account_id
        JOIN read_parquet('{base_path}/customers/customers.parquet') c
            ON a.customer_id = c.customer_id
        WHERE t.status = 'COMPLETED'
        GROUP BY c.segment
        ORDER BY total_volume DESC
    """).fetchdf()
    print(f"Query time: {time.time() - start:.3f}s")
    print(result.to_string(index=False))

    # 3. Channel performance
    print("\n=== Channel Performance ===")
    start = time.time()
    result = con.execute(f"""
        SELECT 
            channel,
            COUNT(*) as tx_count,
            SUM(amount) as total_amount,
            SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate
        FROM read_parquet('{base_path}/transactions/*/data.parquet')
        GROUP BY channel
        ORDER BY total_amount DESC
    """).fetchdf()
    print(f"Query time: {time.time() - start:.3f}s")
    print(result.to_string(index=False))

    # 4. Top accounts by volume
    print("\n=== Top 10 Accounts by Volume ===")
    start = time.time()
    result = con.execute(f"""
        SELECT 
            t.account_id,
            COUNT(*) as tx_count,
            SUM(t.amount) as total_volume
        FROM read_parquet('{base_path}/transactions/*/data.parquet') t
        WHERE t.status = 'COMPLETED'
        GROUP BY t.account_id
        ORDER BY total_volume DESC
        LIMIT 10
    """).fetchdf()
    print(f"Query time: {time.time() - start:.3f}s")
    print(result.to_string(index=False))

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    base_path = os.path.join(tempfile.gettempdir(), "data_lake")
    
    # Generate data lake
    print("Generating data lake...")
    generate_data_lake(base_path, num_days=7)

    # Run queries
    run_data_lake_queries(base_path)
```

---

## 5. Banking Scenario 2: Reporting Pipeline

### Problem
A bank needs to generate daily reports from Parquet data:
- Daily transaction summary
- Customer profitability report
- Risk assessment report

Reports must run in < 5 minutes and output to Parquet.

### Why DuckDB + Parquet?
- Fast SQL on Parquet
- Direct file access
- Output to Parquet
- No database setup

### Architecture
```
Parquet Files (S3)
       |
       v
  DuckDB (SQL queries)
       |
       v
  Report Calculations
       |
       v
  Parquet Reports (S3)
```

---

## 6. Python Code - Scenario 2

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
# BANKING SCENARIO: Reporting Pipeline
# ============================================================

class ReportingPipeline:
    """Generate reports from Parquet data using DuckDB."""

    def __init__(self, data_path, output_path):
        self.data_path = data_path
        self.output_path = output_path
        self.con = duckdb.connect()

    def generate_daily_summary(self, report_date):
        """Generate daily transaction summary report."""
        start = time.time()
        
        result = self.con.execute(f"""
            WITH 
            daily_stats AS (
                SELECT 
                    '{report_date}' as report_date,
                    COUNT(*) as total_transactions,
                    SUM(amount) as total_amount,
                    AVG(amount) as avg_amount,
                    COUNT(DISTINCT account_id) as unique_accounts
                FROM read_parquet('{self.data_path}/transactions/date={report_date}/data.parquet')
            ),
            status_breakdown AS (
                SELECT 
                    status,
                    COUNT(*) as count,
                    SUM(amount) as amount
                FROM read_parquet('{self.data_path}/transactions/date={report_date}/data.parquet')
                GROUP BY status
            )
            SELECT 
                ds.*,
                sb.status,
                sb.count as status_count,
                sb.amount as status_amount
            FROM daily_stats ds
            CROSS JOIN status_breakdown sb
        """).fetchdf()

        elapsed = time.time() - start
        print(f"Daily summary generated in {elapsed:.3f}s")
        
        return result

    def generate_customer_profitability(self, top_n=10):
        """Generate customer profitability report."""
        start = time.time()
        
        result = self.con.execute(f"""
            WITH 
            customer_transactions AS (
                SELECT 
                    a.customer_id,
                    COUNT(*) as tx_count,
                    SUM(t.amount) as total_volume,
                    AVG(t.amount) as avg_amount
                FROM read_parquet('{self.data_path}/transactions/*/data.parquet') t
                JOIN read_parquet('{self.data_path}/accounts/accounts.parquet') a
                    ON t.account_id = a.account_id
                WHERE t.status = 'COMPLETED'
                GROUP BY a.customer_id
            ),
            customer_info AS (
                SELECT 
                    customer_id,
                    name,
                    segment,
                    balance
                FROM read_parquet('{self.data_path}/customers/customers.parquet')
            )
            SELECT 
                ci.customer_id,
                ci.name,
                ci.segment,
                ci.balance,
                ct.tx_count,
                ct.total_volume,
                ct.avg_amount,
                RANK() OVER (ORDER BY ct.total_volume DESC) as volume_rank
            FROM customer_info ci
            JOIN customer_transactions ct ON ci.customer_id = ct.customer_id
            ORDER BY ct.total_volume DESC
            LIMIT {top_n}
        """).fetchdf()

        elapsed = time.time() - start
        print(f"Customer profitability report generated in {elapsed:.3f}s")
        
        return result

    def generate_risk_report(self):
        """Generate risk assessment report."""
        start = time.time()
        
        result = self.con.execute(f"""
            WITH 
            account_risk AS (
                SELECT 
                    a.account_id,
                    a.customer_id,
                    a.account_type,
                    a.balance,
                    COUNT(t.transaction_id) as tx_count,
                    SUM(CASE WHEN t.status = 'FAILED' THEN 1 ELSE 0 END) as failed_txns,
                    SUM(CASE WHEN t.status = 'FAILED' THEN 1 ELSE 0 END) * 100.0 / 
                        NULLIF(COUNT(t.transaction_id), 0) as failure_rate
                FROM read_parquet('{self.data_path}/accounts/accounts.parquet') a
                LEFT JOIN read_parquet('{self.data_path}/transactions/*/data.parquet') t
                    ON a.account_id = t.account_id
                GROUP BY 1, 2, 3, 4
            )
            SELECT 
                *,
                CASE 
                    WHEN failure_rate > 10 THEN 'HIGH'
                    WHEN failure_rate > 5 THEN 'MEDIUM'
                    ELSE 'LOW'
                END as risk_level
            FROM account_risk
            WHERE tx_count > 0
            ORDER BY failure_rate DESC
        """).fetchdf()

        elapsed = time.time() - start
        print(f"Risk report generated in {elapsed:.3f}s")
        
        return result

    def save_report(self, df, report_name):
        """Save report to Parquet."""
        output_file = os.path.join(self.output_path, f"{report_name}.parquet")
        pq.write_table(pq.Table.from_pandas(df), output_file, compression="zstd")
        print(f"Saved report to {output_file}")
        
        return output_file

    def close(self):
        """Close DuckDB connection."""
        self.con.close()


def generate_sample_data(base_path, num_days=7):
    """Generate sample data for reporting."""
    random.seed(42)
    np.random.seed(42)

    # Transactions
    for day in range(num_days):
        date = datetime(2026, 8, 1) + timedelta(days=day)
        date_str = date.strftime("%Y-%m-%d")
        
        day_path = os.path.join(base_path, "transactions", f"date={date_str}")
        os.makedirs(day_path, exist_ok=True)
        
        num_txns = random.randint(5000, 10000)
        table = pa.table({
            "transaction_id": list(range(day * 10000 + 1, day * 10000 + num_txns + 1)),
            "account_id": [f"ACC{random.randint(100000, 999999)}" for _ in range(num_txns)],
            "amount": np.random.lognormal(6, 2, num_txns).round(2),
            "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_txns),
            "date": [date_str] * num_txns,
        })
        
        pq.write_table(table, os.path.join(day_path, "data.parquet"), compression="zstd")

    # Customers
    customers_path = os.path.join(base_path, "customers")
    os.makedirs(customers_path, exist_ok=True)
    
    customers = pa.table({
        "customer_id": [f"CUST{i:05d}" for i in range(1, 1001)],
        "name": [f"Customer {i}" for i in range(1, 1001)],
        "segment": np.random.choice(["PREMIUM", "STANDARD", "BASIC"], 1000),
        "balance": np.random.lognormal(10, 2, 1000).round(2),
    })
    pq.write_table(customers, os.path.join(customers_path, "customers.parquet"), compression="zstd")

    # Accounts
    accounts_path = os.path.join(base_path, "accounts")
    os.makedirs(accounts_path, exist_ok=True)
    
    accounts = pa.table({
        "account_id": [f"ACC{i:06d}" for i in range(1, 5001)],
        "customer_id": [f"CUST{random.randint(1, 1000):05d}" for _ in range(5000)],
        "account_type": np.random.choice(["CHECKING", "SAVINGS", "CREDIT_CARD"], 5000),
        "balance": np.random.lognormal(10, 2, 5000).round(2),
    })
    pq.write_table(accounts, os.path.join(accounts_path, "accounts.parquet"), compression="zstd")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    data_path = os.path.join(tempfile.gettempdir(), "reporting_data")
    output_path = os.path.join(tempfile.gettempdir(), "reports")
    os.makedirs(output_path, exist_ok=True)

    # Generate sample data
    print("Generating sample data...")
    generate_sample_data(data_path, num_days=7)

    # Run reporting pipeline
    pipeline = ReportingPipeline(data_path, output_path)

    # Generate reports
    print("\n=== Generating Reports ===")
    
    daily_summary = pipeline.generate_daily_summary("2026-08-01")
    pipeline.save_report(daily_summary, "daily_summary")

    profitability = pipeline.generate_customer_profitability(top_n=10)
    pipeline.save_report(profitability, "customer_profitability")

    risk = pipeline.generate_risk_report()
    pipeline.save_report(risk, "risk_assessment")

    pipeline.close()

    print("\n=== Reports Generated ===")
    for f in os.listdir(output_path):
        print(f"  - {f}")
```

---

## 7. Interview Questions

### Q1: How does DuckDB optimize Parquet reads?

**Answer:**

1. **Predicate pushdown**: Filters pushed to Parquet reader
```sql
WHERE amount > 1000  -- Parquet skips row groups where max(amount) < 1000
```

2. **Column pruning**: Only reads needed columns
```sql
SELECT amount, status  -- Only reads 2 columns out of 20
```

3. **Partition pruning**: Skips irrelevant partitions
```sql
WHERE date = '2026-08-24'  -- Skips other date partitions
```

4. **Parallel reads**: Reads multiple row groups simultaneously

5. **Vectorized execution**: Processes data in batches

---

### Q2: What Parquet features does DuckDB support?

**Answer:**

| Feature | Support |
|---------|---------|
| Compression (Snappy, Zstd, Gzip) | ✅ Full |
| Dictionary encoding | ✅ Full |
| Predicate pushdown | ✅ Full |
| Column pruning | ✅ Full |
| Nested types (struct, list, map) | ✅ Full |
| Schema evolution | ✅ Partial |
| Bloom filters | ✅ Partial |
| Hive partitioning | ✅ Full |

---

### Q3: How do you query multiple Parquet files in DuckDB?

**Answer:**

**Glob patterns:**
```sql
SELECT * FROM read_parquet('data/*.parquet')
```

**Multiple specific files:**
```sql
SELECT * FROM read_parquet(['file1.parquet', 'file2.parquet'])
```

**Recursive directory:**
```sql
SELECT * FROM read_parquet('data/**/*.parquet')
```

**From S3:**
```sql
SELECT * FROM read_parquet('s3://bucket/data/*.parquet')
```

**Partitioned:**
```sql
SELECT * FROM read_parquet('data/year=2026/month=08/*.parquet')
```

---

### Q4: How do you write Parquet files from DuckDB?

**Answer:**

**COPY command:**
```sql
COPY (SELECT * FROM table) TO 'output.parquet' (FORMAT PARQUET)
```

**With compression:**
```sql
COPY (SELECT * FROM table) TO 'output.parquet' 
(FORMAT PARQUET, COMPRESSION 'ZSTD')
```

**With partitioning:**
```sql
COPY (SELECT * FROM table) TO 'output/' 
(FORMAT PARQUET, PARTITION_BY (date))
```

**Python API:**
```python
con.execute("COPY (SELECT * FROM table) TO 'output.parquet' (FORMAT PARQUET)")
```

---

### Q5: Compare DuckDB vs Spark for Parquet queries.

**Answer:**

| Feature | DuckDB | Spark |
|---------|--------|-------|
| **Architecture** | Embedded, single-node | Distributed cluster |
| **Scale** | GB to TB | TB to PB |
| **Setup** | pip install | Cluster setup |
| **SQL support** | Full SQL | Spark SQL |
| **Performance** | Fast for single-node | Fast for distributed |
| **Best for** | Ad-hoc analysis | Large-scale ETL |

**When to use DuckDB:**
- Data < 1 TB
- Single user
- Ad-hoc analysis
- No cluster available

**When to use Spark:**
- Data > 1 TB
- Multiple users
- Production ETL
- Distributed processing needed
