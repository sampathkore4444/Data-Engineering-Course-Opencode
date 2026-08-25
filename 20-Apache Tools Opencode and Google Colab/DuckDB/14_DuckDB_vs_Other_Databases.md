# DuckDB vs Other Databases

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-database-selection)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-migration-planning)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### DuckDB vs Other Databases

> **DuckDB is an embedded analytical database optimized for OLAP workloads, while other databases like PostgreSQL and SQLite are optimized for different use cases.**

### Database Comparison

| Feature | DuckDB | PostgreSQL | SQLite | MySQL |
|---------|--------|------------|--------|-------|
| **Type** | OLAP | OLTP | OLTP | OLTP |
| **Deployment** | Embedded | Server | Embedded | Server |
| **Storage** | Files (Parquet, CSV) | Internal | Internal | Internal |
| **Setup** | pip install | Server install | pip install | Server install |
| **Concurrency** | Single-writer | Full ACID | Single-writer | Full ACID |
| **Best for** | Analytics | Web apps | Mobile apps | Web apps |

### DuckDB vs Pandas

| Feature | DuckDB | Pandas |
|---------|--------|--------|
| **Type** | SQL database | DataFrame library |
| **Query language** | SQL | Method chains |
| **Large data** | Streaming | In-memory |
| **Complex joins** | Optimized | Manual |
| **Best for** | SQL queries | Data manipulation |

### DuckDB vs Spark

| Feature | DuckDB | Spark |
|---------|--------|-------|
| **Architecture** | Embedded | Distributed |
| **Scale** | GB to TB | TB to PB |
| **Setup** | pip install | Cluster setup |
| **Best for** | Ad-hoc analysis | Large-scale ETL |

### When to Use Each

```
DuckDB:
  - Ad-hoc analysis
  - Data science workflows
  - Embedded analytics
  - Small to medium datasets (< 1 TB)

PostgreSQL:
  - Web applications
  - Transactional workloads
  - Multi-user concurrent access
  - Complex schemas with constraints

SQLite:
  - Mobile applications
  - Embedded applications
  - Single-user local storage
  - Prototyping

Spark:
  - Large-scale ETL
  - Distributed processing
  - Cluster computing
  - Production data pipelines

Pandas:
  - Data manipulation
  - Quick prototyping
  - ML feature engineering
  - In-memory analysis
```

---

## 2. Example

### Database Comparison Demo

```python
import duckdb
import pandas as pd
import numpy as np
import time

# Create sample data
np.random.seed(42)
num_rows = 1_000_000

df = pd.DataFrame({
    "id": range(num_rows),
    "status": np.random.choice(["A", "B", "C"], num_rows),
    "amount": np.random.uniform(1, 1000, num_rows),
})

# DuckDB
con = duckdb.connect()
con.register("data", df)

print("=== DuckDB Query ===")
start = time.time()
result = con.execute("""
    SELECT status, COUNT(*), AVG(amount)
    FROM data
    GROUP BY status
""").fetchdf()
duckdb_time = time.time() - start
print(result.to_string(index=False))
print(f"DuckDB time: {duckdb_time:.3f}s")

# Pandas
print("\n=== Pandas Query ===")
start = time.time()
result = df.groupby("status").agg({"id": "count", "amount": "mean"})
pandas_time = time.time() - start
print(result)
print(f"Pandas time: {pandas_time:.3f}s")

con.close()
```

---

## 3. Banking Scenario 1: Database Selection

### Problem
A bank needs to choose databases for different workloads:
- Customer-facing web app (OLTP)
- Analytics dashboard (OLAP)
- Data science workflows
- Mobile app

### Why Selection Matters?
- Right tool for the job
- Performance optimization
- Cost efficiency
- Scalability

### Architecture
```
Web App → PostgreSQL (OLTP)
Analytics Dashboard → DuckDB (OLAP)
Data Science → DuckDB + Pandas
Mobile App → SQLite
Data Pipeline → Spark
```

---

## 4. Python Code - Scenario 1

```python
import duckdb
import pandas as pd
import numpy as np
import time

# ============================================================
# BANKING SCENARIO: Database Selection
# ============================================================

def benchmark_databases():
    """Benchmark different databases for banking workloads."""
    # Generate sample data
    np.random.seed(42)
    num_rows = 1_000_000

    df = pd.DataFrame({
        "transaction_id": range(1, num_rows + 1),
        "account_id": [f"ACC{np.random.randint(1, 10000):05d}" for _ in range(num_rows)],
        "amount": np.random.lognormal(6, 2, num_rows).round(2),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
        "date": pd.date_range("2026-01-01", periods=num_rows, freq="3s"),
    })

    print(f"=== Database Benchmark ({num_rows:,} rows) ===\n")

    # 1. DuckDB - Analytical query
    print("--- DuckDB (OLAP) ---")
    con = duckdb.connect()
    con.register("transactions", df)

    start = time.time()
    result = con.execute("""
        SELECT 
            status,
            COUNT(*) as count,
            SUM(amount) as total,
            AVG(amount) as average
        FROM transactions
        GROUP BY status
    """).fetchdf()
    duckdb_time = time.time() - start
    print(result.to_string(index=False))
    print(f"DuckDB query time: {duckdb_time:.3f}s\n")
    con.close()

    # 2. Pandas - Aggregation
    print("--- Pandas ---")
    start = time.time()
    result = df.groupby("status").agg({
        "transaction_id": "count",
        "amount": ["sum", "mean"]
    })
    pandas_time = time.time() - start
    print(result)
    print(f"Pandas query time: {pandas_time:.3f}s\n")

    # 3. DuckDB - Complex analytics
    print("--- DuckDB (Complex Analytics) ---")
    con = duckdb.connect()
    con.register("transactions", df)

    start = time.time()
    result = con.execute("""
        WITH 
        daily_stats AS (
            SELECT 
                DATE_TRUNC('day', date) as day,
                status,
                COUNT(*) as tx_count,
                SUM(amount) as total_amount
            FROM transactions
            GROUP BY 1, 2
        ),
        daily_totals AS (
            SELECT 
                day,
                SUM(total_amount) as day_total
            FROM daily_stats
            GROUP BY 1
        )
        SELECT 
            d.day,
            d.status,
            d.tx_count,
            d.total_amount,
            d.total_amount / dt.day_total * 100 as pct_of_day
        FROM daily_stats d
        JOIN daily_totals dt ON d.day = dt.day
        ORDER BY d.day, d.status
        LIMIT 20
    """).fetchdf()
    complex_time = time.time() - start
    print(result.to_string(index=False))
    print(f"DuckDB complex query time: {complex_time:.3f}s\n")
    con.close()

    # Summary
    print("=== Summary ===")
    print(f"DuckDB (simple): {duckdb_time:.3f}s")
    print(f"Pandas: {pandas_time:.3f}s")
    print(f"DuckDB (complex): {complex_time:.3f}s")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    benchmark_databases()
```

---

## 5. Banking Scenario 2: Migration Planning

### Problem**
A bank wants to migrate from MySQL to a modern data stack:
- Keep MySQL for OLTP
- Add DuckDB for analytics
- Add Spark for ETL
- Migrate data to Parquet

### Why Migration?
- Better analytics performance
- Cost reduction
- Modern architecture
- Scalability

### Architecture
```
Before:
  MySQL (OLTP + OLAP)

After:
  MySQL (OLTP)
       |
       v
  ETL Pipeline (Spark)
       |
       v
  Parquet (S3)
       |
       v
  DuckDB (OLAP)
```

---

## 6. Python Code - Scenario 2

```python
import duckdb
import pandas as pd
import numpy as np
import time
import pyarrow.parquet as pq
import os
import tempfile

# ============================================================
# BANKING SCENARIO: Migration Planning
# ============================================================

def simulate_mysql_to_duckdb():
    """Simulate migration from MySQL to DuckDB."""
    # Generate sample MySQL-style data
    np.random.seed(42)
    num_rows = 100_000

    mysql_data = pd.DataFrame({
        "id": range(1, num_rows + 1),
        "customer_id": [f"CU{np.random.randint(1, 5000):05d}" for _ in range(num_rows)],
        "amount": np.random.uniform(100, 10000, num_rows).round(2),
        "status": np.random.choice(["active", "inactive", "pending"], num_rows),
        "created_at": pd.date_range("2025-01-01", periods=num_rows, freq="30s"),
    })

    print("=== Migration: MySQL to DuckDB ===\n")

    # Step 1: Export from MySQL (simulated)
    print("1. Export from MySQL (simulated)...")
    mysql_export = os.path.join(tempfile.mkdtemp(), "mysql_export.parquet")
    pq.write_table(pq.Table.from_pandas(mysql_data), mysql_export, compression="zstd")
    print(f"   Exported to: {mysql_export}")

    # Step 2: Import to DuckDB
    print("\n2. Import to DuckDB...")
    con = duckdb.connect()
    
    start = time.time()
    con.execute(f"""
        CREATE TABLE transactions AS
        SELECT * FROM read_parquet('{mysql_export}')
    """)
    import_time = time.time() - start
    print(f"   Import time: {import_time:.3f}s")

    # Step 3: Run analytics (DuckDB)
    print("\n3. Run analytics (DuckDB)...")
    start = time.time()
    result = con.execute("""
        SELECT 
            status,
            COUNT(*) as count,
            AVG(amount) as avg_amount
        FROM transactions
        GROUP BY status
    """).fetchdf()
    analytics_time = time.time() - start
    print(result.to_string(index=False))
    print(f"   Analytics time: {analytics_time:.3f}s")

    # Step 4: Compare with MySQL (simulated)
    print("\n4. Performance comparison...")
    print(f"   MySQL (simulated): ~2.5s")
    print(f"   DuckDB: {analytics_time:.3f}s")
    print(f"   Speedup: {2.5/analytics_time:.1f}x")

    con.close()

    # Step 5: Show architecture
    print("\n=== New Architecture ===")
    print("""
    ┌─────────────────────────────────────────────────────┐
    │                    Data Sources                      │
    ├─────────────────────────────────────────────────────┤
    │  MySQL (OLTP) ──► ETL (Spark) ──► Parquet (S3)     │
    └─────────────────────────────────────────────────────┘
                           │
                           ▼
    ┌─────────────────────────────────────────────────────┐
    │                    Analytics                         │
    │  DuckDB (embedded) ──► Pandas ──► Visualizations    │
    └─────────────────────────────────────────────────────┘
    """)


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    simulate_mysql_to_duckdb()
```

---

## 7. Interview Questions

### Q1: When would you use DuckDB vs PostgreSQL?

**Answer:**

| Use Case | DuckDB | PostgreSQL |
|----------|--------|------------|
| **Analytics** | ✅ Optimized | ⚠️ Possible |
| **Transactions** | ❌ Not optimized | ✅ Optimized |
| **Concurrent writes** | ❌ Single-writer | ✅ Multi-writer |
| **Data files** | ✅ Direct queries | ❌ Must import |
| **Setup** | ✅ Embedded | ⚠️ Server required |

**Use DuckDB for:**
- Ad-hoc analysis
- Data science workflows
- Querying Parquet/CSV files
- Embedded analytics

**Use PostgreSQL for:**
- Web applications
- Transactional workloads
- Multi-user concurrent access
- Complex schemas with constraints

---

### Q2: When would you use DuckDB vs Pandas?

**Answer:**

| Feature | DuckDB | Pandas |
|---------|--------|--------|
| **Query language** | SQL | Method chains |
| **Large data** | Streaming | In-memory |
| **Complex joins** | Optimized | Manual |
| **Window functions** | Full SQL | Limited |

**Use DuckDB when:**
- Running SQL queries
- Data is larger than memory
- Complex joins and aggregations
- Need window functions

**Use Pandas when:**
- Data manipulation
- Quick prototyping
- Integration with ML libraries
- Data fits in memory

---

### Q3: When would you use DuckDB vs Spark?

**Answer:**

| Feature | DuckDB | Spark |
|---------|--------|-------|
| **Architecture** | Embedded | Distributed |
| **Scale** | GB to TB | TB to PB |
| **Setup** | pip install | Cluster setup |
| **Best for** | Ad-hoc analysis | Large-scale ETL |

**Use DuckDB when:**
- Data < 1 TB
- Single user
- No cluster available
- Quick analysis

**Use Spark when:**
- Data > 1 TB
- Multiple users
- Production ETL
- Distributed processing

---

### Q4: What are the advantages of DuckDB?

**Answer:**

1. **Zero-config**: No server to install or configure
2. **Embedded**: Runs in your application process
3. **Fast**: Vectorized execution for analytics
4. **File-based**: Queries Parquet, CSV, JSON directly
5. **SQL**: Full SQL support with window functions
6. **Cross-platform**: Runs on Windows, macOS, Linux
7. **Extension-rich**: Spatial, JSON, and more
8. **Free**: Open source, MIT license

---

### Q5: What are the limitations of DuckDB?

**Answer:**

1. **Single-node**: Runs on one machine (not distributed)
2. **Single-writer**: Only one process can write at a time
3. **No ACID on external files**: Can't do transactions on Parquet
4. **Memory constraints**: Large datasets may require streaming
5. **No built-in server**: Must embed in application
6. **Limited concurrency**: Not designed for multi-user access

**When to use alternatives:**
- **PostgreSQL**: Transactional workloads
- **Spark**: Distributed processing
- **Trino**: Multi-user concurrent queries
- **Motherduck**: Cloud-based DuckDB
