# DuckDB Performance Optimization

## Table of Contents
1. [Detailed Explanation](#1-detailed-explanation)
2. [Example](#2-example)
3. [Real-World Banking Scenario 1](#3-banking-scenario-1-query-optimization)
4. [Python Code - Scenario 1](#4-python-code---scenario-1)
5. [Real-World Banking Scenario 2](#5-banking-scenario-2-data-pipeline)
6. [Python Code - Scenario 2](#6-python-code---scenario-2)
7. [Interview Questions](#7-interview-questions)

---

## 1. Detailed Explanation

### DuckDB Performance Optimization

> **DuckDB achieves high performance through vectorized execution, automatic query optimization, and efficient memory management.**

### Query Optimization Techniques

#### 1. Predicate Pushdown
```sql
-- DuckDB pushes filters to Parquet reader
SELECT * FROM read_parquet('file.parquet') WHERE amount > 1000
-- Parquet skips row groups where max(amount) < 1000
```

#### 2. Column Pruning
```sql
-- Only reads needed columns
SELECT amount, status FROM read_parquet('file.parquet')
-- Skips all other columns
```

#### 3. Partition Pruning
```sql
-- Skips irrelevant partitions
SELECT * FROM read_parquet('data/year=2026/**/*.parquet') WHERE year = 2026
```

#### 4. Join Reordering
```sql
-- DuckDB automatically reorders joins
SELECT * FROM big_table b
JOIN small_table s ON b.id = s.id
-- Small table scanned first for hash join
```

#### 5. Aggregation Pushdown
```sql
-- Partial aggregation before join
SELECT status, SUM(amount)
FROM transactions
GROUP BY status
-- Aggregated before joining with other tables
```

### Memory Management

```sql
-- Set memory limit
SET memory_limit='4GB'

-- Set thread count
SET threads=4

-- Enable progress bar
SET enable_progress_bar=true
```

### Caching Strategies

```
1. File system cache: OS caches Parquet files
2. DuckDB buffer cache: Caches data pages
3. Query result cache: Reuse query results
```

### Performance Best Practices

```
1. Query Parquet directly (don't load into memory)
2. Use column pruning (SELECT specific columns)
3. Use predicate pushdown (WHERE clauses)
4. Set appropriate memory limits
5. Use CTEs for complex queries
6. Avoid SELECT *
7. Use appropriate data types
```

---

## 2. Example

### Performance Optimization Demo

```python
import duckdb
import pandas as pd
import numpy as np
import time
import pyarrow.parquet as pq
import os
import tempfile

# Create large dataset
np.random.seed(42)
num_rows = 2_000_000

df = pd.DataFrame({
    "id": range(num_rows),
    "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
    "amount": np.random.uniform(1.0, 100000.0, num_rows),
    "category": np.random.choice(["A", "B", "C", "D", "E"], num_rows),
})

# Save as Parquet
parquet_path = os.path.join(tempfile.mkdtemp(), "large.parquet")
pq.write_table(pq.Table.from_pandas(df), parquet_path, compression="zstd")

con = duckdb.connect()

# Benchmark queries
queries = [
    ("Full scan", "SELECT COUNT(*) FROM read_parquet('{}')"),
    ("Column pruning", "SELECT COUNT(amount) FROM read_parquet('{}')"),
    ("Predicate pushdown", "SELECT COUNT(*) FROM read_parquet('{}') WHERE amount > 50000"),
    ("Both optimizations", "SELECT COUNT(*) FROM read_parquet('{}') WHERE amount > 50000 AND status = 'COMPLETED'"),
]

print("=== Performance Benchmark ===")
for name, query in queries:
    start = time.time()
    result = con.execute(query.format(parquet_path)).fetchone()
    elapsed = time.time() - start
    print(f"{name}: {elapsed:.3f}s ({result[0]:,} rows)")

con.close()
```

---

## 3. Banking Scenario 1: Query Optimization

### Problem
A bank's analytics queries are slow:
- Dashboard takes 30 seconds to load
- Monthly reports take 2 hours
- Ad-hoc queries timeout

### Why Optimization Matters?
- 10-100x faster queries
- Better user experience
- Lower infrastructure costs
- Enable real-time analytics

---

## 4. Python Code - Scenario 1

```python
import duckdb
import pandas as pd
import numpy as np
import time
import pyarrow.parquet as pq
import os
import tempfile

# ============================================================
# BANKING SCENARIO: Query Optimization
# ============================================================

def generate_large_dataset(num_rows=5_000_000):
    """Generate large transaction dataset."""
    np.random.seed(42)

    df = pd.DataFrame({
        "transaction_id": range(1, num_rows + 1),
        "account_id": [f"ACC{np.random.randint(100000, 999999)}" for _ in range(num_rows)],
        "amount": np.random.lognormal(6, 2, num_rows).round(2),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
        "channel": np.random.choice(["ONLINE", "MOBILE", "BRANCH", "ATM"], num_rows),
        "date": pd.date_range("2024-01-01", periods=num_rows, freq="2s"),
    })

    return df


def benchmark_optimizations(df):
    """Benchmark different optimization techniques."""
    # Save as Parquet
    parquet_path = os.path.join(tempfile.mkdtemp(), "transactions.parquet")
    pq.write_table(pq.Table.from_pandas(df), parquet_path, compression="zstd")

    con = duckdb.connect()

    print(f"\n=== Optimization Benchmark ({len(df):,} rows) ===")

    # 1. Without optimization
    start = time.time()
    con.execute(f"SELECT * FROM read_parquet('{parquet_path}')").fetchdf()
    full_time = time.time() - start
    print(f"\n1. Full scan (no optimization): {full_time:.3f}s")

    # 2. Column pruning
    start = time.time()
    con.execute(f"SELECT amount, status FROM read_parquet('{parquet_path}')").fetchdf()
    column_time = time.time() - start
    print(f"2. Column pruning (2 columns): {column_time:.3f}s ({full_time/column_time:.1f}x faster)")

    # 3. Predicate pushdown
    start = time.time()
    con.execute(f"SELECT * FROM read_parquet('{parquet_path}') WHERE amount > 50000").fetchdf()
    predicate_time = time.time() - start
    print(f"3. Predicate pushdown (amount > 50000): {predicate_time:.3f}s ({full_time/predicate_time:.1f}x faster)")

    # 4. Both optimizations
    start = time.time()
    con.execute(f"SELECT amount, status FROM read_parquet('{parquet_path}') WHERE amount > 50000 AND status = 'COMPLETED'").fetchdf()
    both_time = time.time() - start
    print(f"4. Both optimizations: {both_time:.3f}s ({full_time/both_time:.1f}x faster)")

    # 5. Aggregation
    start = time.time()
    con.execute(f"SELECT status, COUNT(*), SUM(amount) FROM read_parquet('{parquet_path}') GROUP BY status").fetchdf()
    agg_time = time.time() - start
    print(f"5. Aggregation (GROUP BY): {agg_time:.3f}s ({full_time/agg_time:.1f}x faster)")

    con.close()


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    print("Generating large dataset...")
    df = generate_large_dataset(num_rows=5_000_000)

    benchmark_optimizations(df)
```

---

## 5. Banking Scenario 2: Data Pipeline Optimization

### Problem
A bank's data pipeline is slow:
- ETL jobs take 4 hours
- Must complete in 2 hours
- Processing 100 GB daily

### Why Optimization?
- Faster ETL completion
- Lower compute costs
- Enable near-real-time analytics
- Better resource utilization

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
# BANKING SCENARIO: Data Pipeline Optimization
# ============================================================

class OptimizedPipeline:
    """Optimized data pipeline using DuckDB."""

    def __init__(self):
        self.con = duckdb.connect()
        self.con.execute("SET memory_limit='4GB'")
        self.con.execute("SET threads=4")

    def process_data(self, input_path, output_path):
        """Process data with optimizations."""
        start = time.time()

        # 1. Read with column pruning and filters
        print("1. Reading optimized data...")
        self.con.execute(f"""
            CREATE OR REPLACE TABLE transactions AS
            SELECT 
                transaction_id,
                account_id,
                amount,
                status,
                date
            FROM read_parquet('{input_path}')
            WHERE status = 'COMPLETED'
        """)
        read_time = time.time() - start
        print(f"   Read time: {read_time:.3f}s")

        # 2. Transform with CTEs
        print("2. Transforming data...")
        transform_start = time.time()
        self.con.execute("""
            CREATE OR REPLACE TABLE transformed AS
            WITH 
            daily_stats AS (
                SELECT 
                    DATE_TRUNC('day', date) as day,
                    account_id,
                    COUNT(*) as tx_count,
                    SUM(amount) as total_amount
                FROM transactions
                GROUP BY 1, 2
            ),
            account_stats AS (
                SELECT 
                    account_id,
                    AVG(total_amount) as avg_daily_amount,
                    STDDEV(total_amount) as std_daily_amount
                FROM daily_stats
                GROUP BY account_id
            )
            SELECT 
                d.*,
                a.avg_daily_amount,
                a.std_daily_amount,
                (d.total_amount - a.avg_daily_amount) / NULLIF(a.std_daily_amount, 0) as z_score
            FROM daily_stats d
            JOIN account_stats a ON d.account_id = a.account_id
        """)
        transform_time = time.time() - transform_start
        print(f"   Transform time: {transform_time:.3f}s")

        # 3. Write optimized output
        print("3. Writing output...")
        write_start = time.time()
        self.con.execute(f"""
            COPY (SELECT * FROM transformed) TO '{output_path}' 
            (FORMAT PARQUET, COMPRESSION 'ZSTD')
        """)
        write_time = time.time() - write_start
        print(f"   Write time: {write_time:.3f}s")

        total_time = time.time() - start
        print(f"\nTotal pipeline time: {total_time:.3f}s")

        return total_time

    def close(self):
        self.con.close()


def benchmark_pipeline():
    """Benchmark pipeline performance."""
    # Generate sample data
    np.random.seed(42)
    num_rows = 1_000_000

    df = pd.DataFrame({
        "transaction_id": range(1, num_rows + 1),
        "account_id": [f"ACC{np.random.randint(100000, 999999)}" for _ in range(num_rows)],
        "amount": np.random.lognormal(6, 2, num_rows).round(2),
        "status": np.random.choice(["COMPLETED", "PENDING", "FAILED"], num_rows),
        "date": pd.date_range("2026-01-01", periods=num_rows, freq="3s"),
    })

    input_path = os.path.join(tempfile.mkdtemp(), "input.parquet")
    output_path = os.path.join(tempfile.mkdtemp(), "output.parquet")

    pq.write_table(pq.Table.from_pandas(df), input_path, compression="zstd")

    # Run optimized pipeline
    print("=== Optimized Pipeline ===")
    pipeline = OptimizedPipeline()
    total_time = pipeline.process_data(input_path, output_path)
    pipeline.close()

    # Check output size
    output_size = os.path.getsize(output_path) / (1024 * 1024)
    print(f"\nOutput size: {output_size:.1f} MB")
    print(f"Throughput: {num_rows / total_time:,.0f} rows/sec")


# ============================================================
# RUN THE SCENARIO
# ============================================================
if __name__ == "__main__":
    benchmark_pipeline()
```

---

## 7. Interview Questions

### Q1: How does DuckDB optimize queries automatically?

**Answer:**

1. **Predicate pushdown**: Filters pushed to Parquet reader
2. **Column pruning**: Only reads needed columns
3. **Join reordering**: Small table first for hash join
4. **Aggregation pushdown**: Partial aggregation early
5. **Common subexpression elimination**: Avoid duplicate computation

---

### Q2: How do you set memory limits in DuckDB?

**Answer:**

```sql
-- Set memory limit
SET memory_limit='4GB'

-- Set thread count
SET threads=4

-- Check settings
SELECT * FROM duckdb_settings()
```

**Python API:**
```python
con.execute("SET memory_limit='4GB'")
con.execute("SET threads=4")
```

---

### Q3: What are the best practices for DuckDB performance?

**Answer:**

1. **Query Parquet directly** (don't load into memory)
2. **Use column pruning** (SELECT specific columns)
3. **Use predicate pushdown** (WHERE clauses)
4. **Set appropriate memory limits**
5. **Use CTEs for complex queries**
6. **Avoid SELECT ***
7. **Use appropriate data types**
8. **Index join columns**

---

### Q4: How do you measure query performance?

**Answer:**

```python
import time

start = time.time()
result = con.execute("SELECT ...").fetchdf()
elapsed = time.time() - start
print(f"Query time: {elapsed:.3f}s")
```

**DuckDB built-in:**
```sql
-- Enable progress bar
SET enable_progress_bar=true

-- Profile query
EXPLAIN ANALYZE SELECT ...
```

---

### Q5: How do you optimize Parquet queries in DuckDB?

**Answer:**

1. **Use column pruning**
```sql
SELECT amount, status FROM read_parquet('file.parquet')
```

2. **Use predicate pushdown**
```sql
WHERE amount > 1000
```

3. **Use partitioning**
```sql
SELECT * FROM read_parquet('data/year=2026/**/*.parquet')
```

4. **Use appropriate compression**
```python
pq.write_table(table, "file.parquet", compression="zstd")
```

5. **Use appropriate file sizes**
```python
# Target 256MB - 1GB per file
```
